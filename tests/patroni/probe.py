#!/usr/bin/env python3
"""Patroni election and logical-slot continuation probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import time
from urllib.error import URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[2]
SOURCE_COMPOSE = ["docker", "compose", "-p", "pg-lake-replication-patroni-lab",
                  "-f", str(ROOT / "deploy/patroni/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("patroni_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"
NODE_PORTS = {"pg1": 55437, "pg2": 55438}
API_PORTS = {"pg1": 58008, "pg2": 58009}
DESTINATION_PORT = 55434


def command(args, timeout=180, **kwargs):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True,
                          timeout=timeout, **kwargs)


def sql(port, statement, *, database="postgres", check_result=True):
    result = command(["psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
                      "-p", str(port), "-U", "postgres", "-d", database],
                     input=statement)
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def value(port, statement, *, database="postgres"):
    return sql(port, statement, database=database).stdout.strip()


def destination(statement, *, check_result=True):
    return sql(DESTINATION_PORT, statement, database="analytics",
               check_result=check_result)


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({"name": name, "passed": passed,
                               "actual": actual, "expected": expected})
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(name)


def wait_for(query, predicate, *, port=DESTINATION_PORT, database="analytics", timeout=70):
    deadline = time.monotonic() + timeout
    latest = ""
    while time.monotonic() < deadline:
        result = sql(port, query, database=database, check_result=False)
        if result.returncode == 0:
            latest = result.stdout.strip()
            if predicate(latest):
                return latest
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for query; latest={latest!r}")


def cluster_state():
    for port in API_PORTS.values():
        try:
            with urlopen(f"http://127.0.0.1:{port}/cluster", timeout=2) as response:
                return json.load(response)
        except (OSError, URLError, ValueError):
            continue
    return None


def wait_cluster(predicate, timeout=90):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = cluster_state()
        if latest and predicate(latest):
            return latest
        time.sleep(0.5)
    raise TimeoutError(f"timed out waiting for Patroni cluster; latest={latest!r}")


def member(cluster, role):
    return next((item for item in cluster["members"] if item["role"] == role), None)


def environment(subscription_id, leader_port, standby_port):
    result = os.environ.copy()
    result.update({
        "LAKE_SOURCE_ADDRESS": f"127.0.0.1:{leader_port}",
        "LAKE_SOURCE_ADDRESSES":
            f"127.0.0.1:{leader_port},127.0.0.1:{standby_port}",
        "LAKE_DESTINATION_ADDRESS": f"127.0.0.1:{DESTINATION_PORT}",
        "LAKE_SOURCE_DATABASE": "postgres",
        "LAKE_DESTINATION_DATABASE": "analytics",
        "LAKE_SLOT": SLOT,
        "LAKE_SLOT_FAILOVER": "1",
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_DISCOVER_PUBLICATION": "1",
        "LAKE_DESTINATION_SCHEMA": RUN,
        "LAKE_CREATE_TABLES_USING_ICEBERG": "1",
        "LAKE_TYPE_MAPPING_VERSION": "1",
        "LAKE_SUPERVISOR_POLL_MS": "100",
        "LAKE_RECEIVER_IDLE_TIMEOUT_MS": "200",
        "LAKE_SUPERVISOR_BASE_BACKOFF_MS": "200",
        "LAKE_SUPERVISOR_MAX_BACKOFF_MS": "1000",
    })
    return result


def cleanup_destination(subscription_id):
    destination(f"DROP SCHEMA IF EXISTS {RUN} CASCADE", check_result=False)
    if subscription_id:
        destination(f"""
            DELETE FROM lake_replication.changes WHERE transaction_id IN
                (SELECT transaction_id FROM lake_replication.transactions
                  WHERE subscription_id={subscription_id});
            DELETE FROM lake_replication.transactions WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.batches WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.initial_copies WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.relation_type_contracts
             WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.relations WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.checkpoints WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.subscriptions WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.sources src WHERE NOT EXISTS
                (SELECT 1 FROM lake_replication.subscriptions sub
                  WHERE sub.source_id=src.source_id);
        """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscription_id = None
    supervisor = None
    exit_code = 1
    command(SOURCE_COMPOSE + ["down", "-v"])
    try:
        started = command(SOURCE_COMPOSE + ["up", "-d", "--build", "--wait",
                                            "--wait-timeout", "240"], timeout=270)
        if started.returncode:
            raise RuntimeError(started.stderr.strip())
        build = command(["cargo", "build", "--quiet", "--bins"])
        if build.returncode:
            raise RuntimeError(build.stderr.strip())

        cluster = wait_cluster(lambda state:
            member(state, "leader") is not None and
            member(state, "sync_standby") is not None and
            member(state, "sync_standby").get("state") == "streaming")
        leader = member(cluster, "leader")["name"]
        standby = member(cluster, "sync_standby")["name"]
        leader_port, standby_port = NODE_PORTS[leader], NODE_PORTS[standby]
        check("Patroni elects one leader and one streaming synchronous standby",
              f"{leader}:leader,{standby}:sync_standby",
              f"{leader}:leader,{standby}:sync_standby")

        identity = json.loads(value(leader_port, """
            SELECT json_build_object(
                'system_identifier', (SELECT system_identifier FROM pg_control_system()),
                'database_oid', (SELECT oid FROM pg_database WHERE datname=current_database()),
                'timeline', (('x' || substr(
                    pg_walfile_name(pg_current_wal_flush_lsn()), 1, 8
                ))::bit(32)::bigint))
        """))
        value(leader_port, f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
            INSERT INTO public.{TABLE} VALUES (1, 'snapshot');
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE}
        """)
        destination(f"CREATE SCHEMA {RUN}")
        subscription_id = int(destination(f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """).stdout.strip())
        initial = command([str(ROOT / "target/debug/lake-replication-initial-sync")],
                          env=environment(subscription_id, leader_port, standby_port))
        if initial.returncode:
            raise RuntimeError(initial.stderr.strip())

        value(leader_port, f"INSERT INTO public.{TABLE} VALUES (2, 'before-failover')")
        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")], cwd=ROOT,
            env=environment(subscription_id, leader_port, standby_port), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        before = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item == "1:snapshot,2:before-failover")
        check("supervisor publishes through the Patroni leader", before,
              "1:snapshot,2:before-failover")

        standby_ready = wait_for(f"""
            SELECT synced::text || ':' || failover::text || ':' || temporary::text || ':' ||
                   (invalidation_reason IS NULL)::text
              FROM pg_replication_slots WHERE slot_name='{SLOT}'
        """, lambda item: item == "true:true:false:true",
            port=standby_port, database="postgres")
        check("Patroni standby has a persistent synchronized failover slot",
              standby_ready, "true:true:false:true")
        standby_settings = value(standby_port, """
            SELECT current_setting('sync_replication_slots') || ':' ||
                   current_setting('hot_standby_feedback') || ':' ||
                   current_setting('primary_slot_name')
        """)
        check("Patroni configures PostgreSQL slot synchronization prerequisites",
              standby_settings, f"on:on:{standby}")

        failure_started = time.monotonic()
        killed = command(SOURCE_COMPOSE + ["kill", "--signal", "SIGKILL", leader])
        if killed.returncode:
            raise RuntimeError(killed.stderr.strip())
        promoted_cluster = wait_cluster(lambda state:
            member(state, "leader") is not None and
            member(state, "leader")["name"] == standby and
            member(state, "leader").get("state") == "running")
        failover_seconds = round(time.monotonic() - failure_started, 3)
        promoted = member(promoted_cluster, "leader")["name"]
        check("Patroni automatically promotes the synchronized standby", promoted, standby)
        EVIDENCE["automatic_failover_seconds"] = failover_seconds

        timeline = wait_for(
            "SELECT (('x' || substr(pg_walfile_name(pg_current_wal_flush_lsn()), "
            "1, 8))::bit(32)::bigint)::text",
            lambda item: int(item or 0) > int(identity["timeline"]),
            port=standby_port, database="postgres")
        value(standby_port,
              f"INSERT INTO public.{TABLE} VALUES (3, 'after-failover')")
        after = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item ==
                "1:snapshot,2:before-failover,3:after-failover")
        check("replication resumes after automatic promotion without duplicates", after,
              "1:snapshot,2:before-failover,3:after-failover")

        health = wait_for(f"""
            SELECT active_source_address || ':' || source_switches || ':' ||
                   observed_timeline || ':' || subscription_state || ':' || service_state
              FROM lake_replication.service_health
             WHERE subscription_id={subscription_id}
        """, lambda item: item ==
            f"127.0.0.1:{standby_port}:1:{timeline}:streaming:running")
        check("health records Patroni endpoint and timeline failover", health,
              f"127.0.0.1:{standby_port}:1:{timeline}:streaming:running")
        continuity = value(standby_port, f"""
            SELECT failover::text || ':' || (invalidation_reason IS NULL)::text || ':' ||
                   confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{SLOT}'
        """)
        acknowledged = destination(f"""
            SELECT acknowledged_lsn FROM lake_replication.checkpoints
             WHERE subscription_id={subscription_id}
        """).stdout.strip()
        check("promoted Patroni slot remains valid at the acknowledged LSN",
              continuity, f"true:true:{acknowledged}")

        destination(f"SELECT lake_replication.request_service_stop({subscription_id})")
        stdout, stderr = supervisor.communicate(timeout=10)
        (RESULTS / "supervisor.stdout").write_text(stdout)
        (RESULTS / "supervisor.stderr").write_text(stderr)
        check("supervisor stops cleanly after Patroni failover", supervisor.returncode, 0)
        supervisor = None
        exit_code = 0
    except Exception as error:
        EVIDENCE["error"] = str(error)
        print(f"ERROR {error}", flush=True)
    finally:
        if supervisor is not None:
            if supervisor.poll() is None and subscription_id:
                destination(
                    f"SELECT lake_replication.request_service_stop({subscription_id})",
                    check_result=False)
            try:
                stdout, stderr = supervisor.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                supervisor.terminate()
                stdout, stderr = supervisor.communicate(timeout=5)
            (RESULTS / "supervisor.stdout").write_text(stdout)
            (RESULTS / "supervisor.stderr").write_text(stderr)
        logs = command(SOURCE_COMPOSE + ["logs", "--no-color", "--tail", "400"])
        (RESULTS / "patroni-cluster.log").write_text(logs.stdout + logs.stderr)
        cleanup_destination(subscription_id)
        command(SOURCE_COMPOSE + ["down", "-v"])
        EVIDENCE["completed_at"] = datetime.now(timezone.utc).isoformat()
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"evidence: {RESULTS / 'results.json'}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

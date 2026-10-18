#!/usr/bin/env python3
"""Ordered source endpoint discovery, switch, and retry probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("endpoints_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"
BAD, PRIMARY, ALIAS = "127.0.0.1:1", "127.0.0.1:55434", "localhost:55434"


def command(args, timeout=90, **kwargs):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True,
                          timeout=timeout, **kwargs)


def sql(database, statement, *, check_result=True):
    result = command(COMPOSE + ["exec", "-T", "postgres", "psql", "-XAt",
                     "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database],
                     input=statement)
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def value(database, statement):
    return sql(database, statement).stdout.strip()


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({"name": name, "passed": passed,
                               "actual": actual, "expected": expected})
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(name)


def wait_for(statement, predicate, timeout=15):
    deadline = time.monotonic() + timeout
    latest = ""
    while time.monotonic() < deadline:
        latest = value("analytics", statement)
        if predicate(latest):
            return latest
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for query; latest={latest!r}")


def environment(subscription_id, addresses):
    result = os.environ.copy()
    result.update({
        "LAKE_SLOT": SLOT,
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_DISCOVER_PUBLICATION": "1",
        "LAKE_DESTINATION_SCHEMA": RUN,
        "LAKE_CREATE_TABLES_USING_ICEBERG": "1",
        "LAKE_TYPE_MAPPING_VERSION": "1",
        "LAKE_SOURCE_ADDRESSES": addresses,
        "LAKE_SUPERVISOR_POLL_MS": "100",
        "LAKE_RECEIVER_IDLE_TIMEOUT_MS": "200",
        "LAKE_SUPERVISOR_BASE_BACKOFF_MS": "300",
        "LAKE_SUPERVISOR_MAX_BACKOFF_MS": "600",
    })
    return result


def stop(supervisor, subscription_id, label):
    value("analytics", f"SELECT lake_replication.request_service_stop({subscription_id})")
    stdout, stderr = supervisor.communicate(timeout=10)
    (RESULTS / f"{label}.stdout").write_text(stdout)
    (RESULTS / f"{label}.stderr").write_text(stderr)
    return supervisor.returncode


def cleanup(subscription_id):
    for _ in range(50):
        dropped = sql("application", f"""
            SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
             WHERE slot_name='{SLOT}'
        """, check_result=False)
        if dropped.returncode == 0:
            break
        time.sleep(0.1)
    sql("application", f"DROP PUBLICATION IF EXISTS {PUBLICATION}", check_result=False)
    sql("application", f"DROP TABLE IF EXISTS public.{TABLE}", check_result=False)
    sql("analytics", f"DROP SCHEMA IF EXISTS {RUN} CASCADE", check_result=False)
    if subscription_id:
        sql("analytics", f"""
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
        """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscription_id = None
    supervisor = None
    exit_code = 1
    try:
        build = command(["cargo", "build", "--quiet", "--bins"])
        if build.returncode:
            raise RuntimeError(build.stderr.strip())
        identity = json.loads(value("application", """
            SELECT json_build_object(
                'system_identifier', (SELECT system_identifier FROM pg_control_system()),
                'database_oid', (SELECT oid FROM pg_database WHERE datname=current_database()),
                'timeline', timeline_id) FROM pg_control_checkpoint()
        """))
        value("application", f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
            INSERT INTO public.{TABLE} VALUES (1, 'snapshot');
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE}
        """)
        value("analytics", f"CREATE SCHEMA {RUN}")
        subscription_id = int(value("analytics", f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """))
        initial = command([str(ROOT / "target/debug/lake-replication-initial-sync")],
                          env=environment(subscription_id, PRIMARY))
        if initial.returncode:
            raise RuntimeError(initial.stderr.strip())

        value("application", f"INSERT INTO public.{TABLE} VALUES (2, 'fallback')")
        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")], cwd=ROOT,
            env=environment(subscription_id, f"{BAD},{PRIMARY}"), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rows = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item == "1:snapshot,2:fallback")
        check("unreachable first candidate falls through to the writable source",
              rows, "1:snapshot,2:fallback")
        discovery = value("analytics", f"""
            SELECT active_source_address || ':' || source_switches || ':' ||
                   (last_source_candidate_errors LIKE '{BAD}:%')::text
              FROM lake_replication.service_health
             WHERE subscription_id={subscription_id}
        """)
        check("selected endpoint and failed candidate are observable",
              discovery, f"{PRIMARY}:0:true")
        check("first endpoint session stops cleanly",
              stop(supervisor, subscription_id, "fallback"), 0)
        supervisor = None

        value("application", f"INSERT INTO public.{TABLE} VALUES (3, 'switched')")
        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")], cwd=ROOT,
            env=environment(subscription_id, ALIAS), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        switched_rows = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item == "1:snapshot,2:fallback,3:switched")
        check("receiver uses the newly selected endpoint",
              switched_rows, "1:snapshot,2:fallback,3:switched")
        switched = value("analytics", f"""
            SELECT active_source_address || ':' || source_switches || ':' ||
                   (last_source_switch_at IS NOT NULL)::text
              FROM lake_replication.service_health
             WHERE subscription_id={subscription_id}
        """)
        check("endpoint change increments durable switch telemetry",
              switched, f"{ALIAS}:1:true")
        check("switched endpoint session stops cleanly",
              stop(supervisor, subscription_id, "switched"), 0)
        supervisor = None

        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")], cwd=ROOT,
            env=environment(subscription_id, BAD), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        unavailable = wait_for(f"""
            SELECT sub.state || ':' || ss.state || ':' ||
                   (ss.last_error LIKE 'no safe writable source endpoint:%')::text
              FROM lake_replication.subscriptions sub
              JOIN lake_replication.service_status ss USING (subscription_id)
             WHERE sub.subscription_id={subscription_id}
        """, lambda item: item == "streaming:backoff:true")
        check("temporary endpoint unavailability retries without poisoning subscription",
              unavailable, "streaming:backoff:true")
        check("unavailable endpoint session remains SQL-stoppable",
              stop(supervisor, subscription_id, "unavailable"), 0)
        supervisor = None
        exit_code = 0
    except Exception as error:
        EVIDENCE["error"] = str(error)
        print(f"ERROR {error}", flush=True)
    finally:
        if supervisor is not None and supervisor.poll() is None:
            supervisor.terminate()
            try:
                supervisor.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                supervisor.kill()
                supervisor.communicate()
        cleanup(subscription_id)
        EVIDENCE["completed_at"] = datetime.now(timezone.utc).isoformat()
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"evidence: {RESULTS / 'results.json'}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

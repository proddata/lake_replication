#!/usr/bin/env python3
"""Long-running supervisor recovery, health, and graceful-stop probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("supervisor_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"


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


def environment(subscription_id):
    result = os.environ.copy()
    result.update({
        "LAKE_SLOT": SLOT,
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_DISCOVER_PUBLICATION": "1",
        "LAKE_DESTINATION_SCHEMA": RUN,
        "LAKE_CREATE_TABLES_USING_ICEBERG": "1",
        "LAKE_TYPE_MAPPING_VERSION": "1",
    })
    return result


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
                          env=environment(subscription_id))
        if initial.returncode:
            raise RuntimeError(initial.stderr.strip())
        value("application", f"INSERT INTO public.{TABLE} VALUES (2, 'supervised')")

        supervisor_env = environment(subscription_id)
        supervisor_env.update({
            "LAKE_SUPERVISOR_INJECT_FAILURES": "1",
            "LAKE_SUPERVISOR_BASE_BACKOFF_MS": "1200",
            "LAKE_SUPERVISOR_MAX_BACKOFF_MS": "2400",
            "LAKE_SUPERVISOR_POLL_MS": "100",
            "LAKE_RECEIVER_IDLE_TIMEOUT_MS": "200",
            "LAKE_SUPERVISOR_PUBLISHER_MAX_BATCHES": "10",
        })
        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")],
            cwd=ROOT, env=supervisor_env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        backoff = wait_for(f"""
            SELECT state || ':' || total_failures || ':' || consecutive_failures || ':' ||
                   (next_retry_at > clock_timestamp())::text || ':' ||
                   (last_error LIKE 'lab-injected%')::text
              FROM lake_replication.service_status
             WHERE subscription_id={subscription_id}
        """, lambda item: item == "backoff:1:1:true:true")
        check("recoverable child failure enters observable bounded backoff",
              backoff, "backoff:1:1:true:true")

        duplicate = command(
            [str(ROOT / "target/debug/lake-replication-supervisor")],
            env=supervisor_env)
        check("advisory lock fences a duplicate supervisor",
              duplicate.returncode != 0 and
              "already has an active supervisor" in duplicate.stderr, True)

        rows = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item == "1:snapshot,2:supervised")
        check("supervisor receives and publishes the queued transaction exactly once",
              rows, "1:snapshot,2:supervised")
        health = wait_for(f"""
            SELECT service_state || ':' || consecutive_failures || ':' || total_failures || ':' ||
                   (receiver_cycles > 0)::text || ':' || (publisher_cycles > 0)::text || ':' ||
                   staged_transactions || ':' || staged_changes || ':' ||
                   (staged_lsn = published_lsn)::text
              FROM lake_replication.service_health
             WHERE subscription_id={subscription_id}
        """, lambda item: item == "running:0:1:true:true:0:0:true")
        check("durable health reports recovery, worker cycles, and drained queue",
              health, "running:0:1:true:true:0:0:true")
        operator_status = value("analytics", f"""
            SELECT health_state || ':' ||
                   (heartbeat_age_seconds >= 0)::text || ':' ||
                   (source_lag_bytes >= 0)::text || ':' ||
                   journal_lag_bytes || ':' ||
                   (last_failure_at IS NOT NULL)::text
              FROM lake_replication.subscription_status
             WHERE subscription_id={subscription_id}
        """)
        check("operator status classifies recovery and exposes lag",
              operator_status, "healthy:true:true:0:true")
        metrics = value("analytics", f"""
            SELECT count(*) || ':' ||
                   (max(value) FILTER (
                       WHERE metric='lake_replication_subscription_healthy'
                   ))::bigint
              FROM lake_replication.metrics
             WHERE subscription_id={subscription_id}
        """)
        check("long-form metrics expose every subscription sample",
              metrics, "12:1")

        control_pid = value("analytics", f"""
            SELECT pid FROM pg_stat_activity
             WHERE application_name='lake-replication-supervisor:{subscription_id}'
               AND pid <> pg_backend_pid()
        """)
        terminated = value("analytics", f"SELECT pg_terminate_backend({control_pid})::text")
        check("destination control backend is forcibly disconnected",
              terminated, "true")
        value("application", f"INSERT INTO public.{TABLE} VALUES (3, 'reconnected')")
        reconnection = wait_for(f"""
            SELECT (reconnections > 0)::text || ':' ||
                   (last_reconnected_at IS NOT NULL)::text || ':' ||
                   (last_connection_error LIKE 'destination control connection lost:%')::text
              FROM lake_replication.service_health
             WHERE subscription_id={subscription_id}
        """, lambda item: item == "true:true:true")
        check("supervisor reacquires ownership and records reconnection telemetry",
              reconnection, "true:true:true")
        resumed_rows = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item == "1:snapshot,2:supervised,3:reconnected")
        check("replication resumes after destination control reconnection",
              resumed_rows, "1:snapshot,2:supervised,3:reconnected")

        value("analytics",
              f"SELECT lake_replication.request_service_stop({subscription_id})")
        stdout, stderr = supervisor.communicate(timeout=10)
        (RESULTS / "supervisor.stdout").write_text(stdout)
        (RESULTS / "supervisor.stderr").write_text(stderr)
        check("SQL stop request produces a clean supervisor exit",
              supervisor.returncode, 0)
        stopped = value("analytics", f"""
            SELECT desired_state || ':' || state || ':' ||
                   (supervisor_pid IS NULL)::text || ':' ||
                   (stopped_at IS NOT NULL)::text
              FROM lake_replication.service_status
             WHERE subscription_id={subscription_id}
        """)
        check("graceful shutdown leaves a durable stopped status",
              stopped, "stopped:stopped:true:true")
        slot_lsn = value("application", f"""
            SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{SLOT}'
        """)
        acknowledged = value("analytics", f"""
            SELECT acknowledged_lsn FROM lake_replication.checkpoints
             WHERE subscription_id={subscription_id}
        """)
        check("source slot confirmation matches durable acknowledgement",
              slot_lsn, acknowledged)
        exit_code = 0
    except Exception as error:  # probe records failure details before cleanup
        EVIDENCE["error"] = str(error)
        print(f"ERROR {error}", flush=True)
    finally:
        if supervisor is not None and supervisor.poll() is None:
            supervisor.terminate()
            try:
                stdout, stderr = supervisor.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                supervisor.kill()
                stdout, stderr = supervisor.communicate()
            (RESULTS / "supervisor.stdout").write_text(stdout)
            (RESULTS / "supervisor.stderr").write_text(stderr)
        cleanup(subscription_id)
        EVIDENCE["completed_at"] = datetime.now(timezone.utc).isoformat()
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"evidence: {RESULTS / 'results.json'}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

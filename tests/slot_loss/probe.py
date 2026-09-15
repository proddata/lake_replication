#!/usr/bin/env python3
"""Fail-closed integration probe for a logical slot that loses required WAL."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
SOURCE_COMPOSE = [
    "docker", "compose", "-p", "pg-lake-replication-slot-loss-lab",
    "-f", str(ROOT / "deploy/slot-loss/compose.yml"),
]
RUN = datetime.now(timezone.utc).strftime("slot_loss_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"
SOURCE_PORT, DESTINATION_PORT = 55439, 55434


def command(args, timeout=120, **kwargs):
    return subprocess.run(
        args, cwd=ROOT, text=True, capture_output=True, timeout=timeout, **kwargs
    )


def sql(port, database, statement, *, check_result=True):
    result = command([
        "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
        "-p", str(port), "-U", "postgres", "-d", database,
    ], input=statement)
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def value(port, database, statement):
    return sql(port, database, statement).stdout.strip()


def source(statement, *, check_result=True):
    return sql(SOURCE_PORT, "application", statement, check_result=check_result)


def destination(statement, *, check_result=True):
    return sql(DESTINATION_PORT, "analytics", statement, check_result=check_result)


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({
        "name": name, "passed": passed, "actual": actual, "expected": expected,
    })
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(name)


def wait_for(query, predicate, *, timeout=30):
    deadline = time.monotonic() + timeout
    latest = ""
    while time.monotonic() < deadline:
        result = destination(query, check_result=False)
        if result.returncode == 0:
            latest = result.stdout.strip()
            if predicate(latest):
                return latest
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for query; latest={latest!r}")


def environment(subscription_id):
    result = os.environ.copy()
    result.update({
        "LAKE_SOURCE_ADDRESS": f"127.0.0.1:{SOURCE_PORT}",
        "LAKE_DESTINATION_ADDRESS": f"127.0.0.1:{DESTINATION_PORT}",
        "LAKE_SOURCE_DATABASE": "application",
        "LAKE_DESTINATION_DATABASE": "analytics",
        "LAKE_SLOT": SLOT,
        "LAKE_SLOT_FAILOVER": "0",
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_DISCOVER_PUBLICATION": "1",
        "LAKE_DESTINATION_SCHEMA": RUN,
        "LAKE_CREATE_TABLES_USING_ICEBERG": "1",
        "LAKE_TYPE_MAPPING_VERSION": "1",
        "LAKE_SUPERVISOR_POLL_MS": "100",
        "LAKE_RECEIVER_IDLE_TIMEOUT_MS": "200",
        "LAKE_SUPERVISOR_BASE_BACKOFF_MS": "200",
        "LAKE_SUPERVISOR_MAX_BACKOFF_MS": "800",
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


def invalidate_slot():
    latest = ""
    for _ in range(8):
        source("""
            DO $$
            BEGIN
              FOR counter IN 1..24 LOOP
                PERFORM pg_logical_emit_message(
                    false, 'lake_slot_loss', repeat('x', 1048576), true
                );
              END LOOP;
            END
            $$;
            CHECKPOINT;
        """)
        latest = value(SOURCE_PORT, "application", f"""
            SELECT COALESCE(wal_status, 'null') || ':' ||
                   COALESCE(invalidation_reason, 'null')
              FROM pg_replication_slots WHERE slot_name='{SLOT}'
        """)
        if latest == "lost:wal_removed":
            return latest
    raise RuntimeError(f"slot did not lose WAL; latest={latest!r}")


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscription_id = None
    supervisor = None
    exit_code = 1
    command(SOURCE_COMPOSE + ["down", "-v"])
    try:
        started = command(
            SOURCE_COMPOSE + ["up", "-d", "--wait", "--wait-timeout", "180"],
            timeout=210,
        )
        if started.returncode:
            raise RuntimeError(started.stderr.strip())
        build = command(["cargo", "build", "--quiet", "--bins"])
        if build.returncode:
            raise RuntimeError(build.stderr.strip())

        identity = json.loads(value(SOURCE_PORT, "application", """
            SELECT json_build_object(
                'system_identifier', (SELECT system_identifier FROM pg_control_system()),
                'database_oid', (SELECT oid FROM pg_database WHERE datname=current_database()),
                'timeline', (('x' || substr(
                    pg_walfile_name(pg_current_wal_flush_lsn()), 1, 8
                ))::bit(32)::bigint))
        """))
        source(f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
            INSERT INTO public.{TABLE} VALUES (1, 'snapshot');
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};
        """)
        destination(f"CREATE SCHEMA {RUN}")
        subscription_id = int(destination(f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """).stdout.strip())
        env = environment(subscription_id)
        initial = command(
            [str(ROOT / "target/debug/lake-replication-initial-sync")], env=env
        )
        if initial.returncode:
            raise RuntimeError(initial.stderr.strip())

        source(f"INSERT INTO public.{TABLE} VALUES (2, 'before-slot-loss')")
        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")], cwd=ROOT,
            env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        baseline_rows = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item == "1:snapshot,2:before-slot-loss",
        )
        check("healthy slot publishes before WAL loss", baseline_rows,
              "1:snapshot,2:before-slot-loss")
        destination(f"SELECT lake_replication.request_service_stop({subscription_id})")
        stdout, stderr = supervisor.communicate(timeout=10)
        (RESULTS / "before-loss.stdout").write_text(stdout)
        (RESULTS / "before-loss.stderr").write_text(stderr)
        check("supervisor drains before forced retention overrun", supervisor.returncode, 0)
        supervisor = None

        baseline_lsn = destination(f"""
            SELECT staged_lsn || ':' || published_lsn || ':' || acknowledged_lsn
              FROM lake_replication.checkpoints WHERE subscription_id={subscription_id}
        """).stdout.strip()
        slot_lsn = value(SOURCE_PORT, "application", f"""
            SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{SLOT}'
        """)
        check("source confirmation equals the durable acknowledged checkpoint",
              slot_lsn, baseline_lsn.split(":")[2])

        source(f"INSERT INTO public.{TABLE} VALUES (3, 'must-not-be-skipped')")
        lost = invalidate_slot()
        check("PostgreSQL invalidates the inactive slot after WAL removal",
              lost, "lost:wal_removed")

        failed = command(
            [str(ROOT / "target/debug/lake-replication-supervisor")], env=env, timeout=10
        )
        (RESULTS / "after-loss.stdout").write_text(failed.stdout)
        (RESULTS / "after-loss.stderr").write_text(failed.stderr)
        check("supervisor fails before launching a receiver on the lost slot",
              failed.returncode != 0 and "lost WAL" in failed.stderr and
              "wal_removed" in failed.stderr, True)
        health = destination(f"""
            SELECT sub.state || ':' || ss.state || ':' || ss.source_check_failures || ':' ||
                   (ss.last_source_error LIKE '%lost WAL%')::text || ':' ||
                   (ss.last_source_error LIKE '%wal_removed%')::text
              FROM lake_replication.subscriptions sub
              JOIN lake_replication.service_status ss USING (subscription_id)
             WHERE sub.subscription_id={subscription_id}
        """).stdout.strip()
        check("slot loss is durably visible as a fail-closed source error",
              health, "error:error:1:true:true")
        unchanged = destination(f"""
            SELECT staged_lsn || ':' || published_lsn || ':' || acknowledged_lsn || ':' ||
                   (SELECT string_agg(id || ':' || note, ',' ORDER BY id)
                      FROM {RUN}.{TABLE})
              FROM lake_replication.checkpoints WHERE subscription_id={subscription_id}
        """).stdout.strip()
        check("lost WAL neither advances checkpoints nor silently skips the row",
              unchanged, f"{baseline_lsn}:1:snapshot,2:before-slot-loss")
        confirmed_after = value(SOURCE_PORT, "application", f"""
            SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{SLOT}'
        """)
        check("failed validation does not advance the invalidated slot",
              confirmed_after, slot_lsn)
        exit_code = 0
    except Exception as error:
        EVIDENCE["error"] = str(error)
        print(f"ERROR {error}", flush=True)
    finally:
        if supervisor is not None:
            if supervisor.poll() is None and subscription_id:
                destination(
                    f"SELECT lake_replication.request_service_stop({subscription_id})",
                    check_result=False,
                )
            try:
                stdout, stderr = supervisor.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                supervisor.terminate()
                stdout, stderr = supervisor.communicate(timeout=5)
            (RESULTS / "supervisor.stdout").write_text(stdout)
            (RESULTS / "supervisor.stderr").write_text(stderr)
        logs = command(SOURCE_COMPOSE + ["logs", "--no-color", "--tail", "300"])
        (RESULTS / "source.log").write_text(logs.stdout + logs.stderr)
        cleanup_destination(subscription_id)
        command(SOURCE_COMPOSE + ["down", "-v"])
        EVIDENCE["completed_at"] = datetime.now(timezone.utc).isoformat()
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"evidence: {RESULTS / 'results.json'}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Source identity, slot continuity, and fail-closed supervisor probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("source_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}


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


def names(label):
    stem = f"{RUN}_{label}"
    return stem, f"{stem}_pub", f"{stem}_slot", stem


def environment(subscription_id, publication, slot, schema):
    result = os.environ.copy()
    result.update({
        "LAKE_SLOT": slot,
        "LAKE_PUBLICATION": publication,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_DISCOVER_PUBLICATION": "1",
        "LAKE_DESTINATION_SCHEMA": schema,
        "LAKE_CREATE_TABLES_USING_ICEBERG": "1",
        "LAKE_TYPE_MAPPING_VERSION": "1",
        "LAKE_SUPERVISOR_POLL_MS": "100",
        "LAKE_RECEIVER_IDLE_TIMEOUT_MS": "200",
        "LAKE_SUPERVISOR_BASE_BACKOFF_MS": "200",
        "LAKE_SUPERVISOR_MAX_BACKOFF_MS": "800",
    })
    return result


def cleanup(subscription_id, table, publication, slot, schema):
    for _ in range(50):
        dropped = sql("application", f"""
            SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
             WHERE slot_name='{slot}'
        """, check_result=False)
        if dropped.returncode == 0:
            break
        time.sleep(0.1)
    sql("application", f"DROP PUBLICATION IF EXISTS {publication}", check_result=False)
    sql("application", f"DROP TABLE IF EXISTS public.{table}", check_result=False)
    sql("analytics", f"DROP SCHEMA IF EXISTS {schema} CASCADE", check_result=False)
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
            DELETE FROM lake_replication.sources src WHERE NOT EXISTS
                (SELECT 1 FROM lake_replication.subscriptions sub
                  WHERE sub.source_id=src.source_id);
        """, check_result=False)


def setup(label, identity, fake_identity=False):
    table, publication, slot, schema = names(label)
    value("application", f"""
        CREATE TABLE public.{table} (id bigint PRIMARY KEY, note text NOT NULL);
        INSERT INTO public.{table} VALUES (1, 'snapshot');
        CREATE PUBLICATION {publication} FOR TABLE public.{table}
    """)
    value("analytics", f"CREATE SCHEMA {schema}")
    system_identifier = int(identity["system_identifier"]) + (1 if fake_identity else 0)
    subscription_id = int(value("analytics", f"""
        SELECT lake_replication.create_subscription(
            {system_identifier}, {identity['database_oid']}, {identity['timeline']},
            '{table}', '{slot}', ARRAY['{publication}'])
    """))
    env = environment(subscription_id, publication, slot, schema)
    initial = command([str(ROOT / "target/debug/lake-replication-initial-sync")], env=env)
    if initial.returncode:
        raise RuntimeError(initial.stderr.strip())
    return subscription_id, table, publication, slot, schema, env


def healthy_case(identity):
    resources = setup("healthy", identity)
    subscription_id, table, publication, slot, schema, env = resources
    supervisor = None
    try:
        value("application", f"INSERT INTO public.{table} VALUES (2, 'healthy')")
        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")],
            cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        rows = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {schema}.{table}",
            lambda item: item == "1:snapshot,2:healthy")
        check("validated source continues replication", rows, "1:snapshot,2:healthy")
        observed = value("analytics", f"""
            SELECT (source_checked_at IS NOT NULL)::text || ':' ||
                   observed_system_identifier || ':' || observed_database_oid || ':' ||
                   observed_timeline || ':' || source_check_failures
              FROM lake_replication.service_health
             WHERE subscription_id={subscription_id}
        """)
        check("healthy source identity and timeline are durably observable", observed,
              f"true:{identity['system_identifier']}:{identity['database_oid']}:{identity['timeline']}:0")
        value("analytics", f"SELECT lake_replication.request_service_stop({subscription_id})")
        stdout, stderr = supervisor.communicate(timeout=10)
        (RESULTS / "healthy.stdout").write_text(stdout)
        (RESULTS / "healthy.stderr").write_text(stderr)
        check("validated supervisor stops cleanly", supervisor.returncode, 0)
    finally:
        if supervisor is not None and supervisor.poll() is None:
            supervisor.terminate()
            supervisor.communicate(timeout=5)
        cleanup(subscription_id, table, publication, slot, schema)


def fatal_case(label, identity, fake_identity=False, remove_slot=False):
    resources = setup(label, identity, fake_identity=fake_identity)
    subscription_id, table, publication, slot, schema, env = resources
    try:
        baseline = value("analytics", f"""
            SELECT staged_lsn FROM lake_replication.checkpoints
             WHERE subscription_id={subscription_id}
        """)
        if remove_slot:
            value("application", f"SELECT pg_drop_replication_slot('{slot}')")
        value("application", f"INSERT INTO public.{table} VALUES (2, 'must-not-apply')")
        supervisor = command([str(ROOT / "target/debug/lake-replication-supervisor")],
                             env=env, timeout=10)
        (RESULTS / f"{label}.stdout").write_text(supervisor.stdout)
        (RESULTS / f"{label}.stderr").write_text(supervisor.stderr)
        expected_error = "source identity mismatch" if fake_identity else "is missing"
        check(f"{label} fails before launching the receiver",
              supervisor.returncode != 0 and expected_error in supervisor.stderr, True)
        state = value("analytics", f"""
            SELECT sub.state || ':' || ss.state || ':' || ss.source_check_failures || ':' ||
                   (ss.last_source_error LIKE '%{expected_error}%')::text
              FROM lake_replication.subscriptions sub
              JOIN lake_replication.service_status ss USING (subscription_id)
             WHERE sub.subscription_id={subscription_id}
        """)
        check(f"{label} is recorded as a fail-closed source error",
              state, "error:error:1:true")
        unchanged = value("analytics", f"""
            SELECT c.staged_lsn || ':' ||
                   (SELECT string_agg(id || ':' || note, ',' ORDER BY id)
                      FROM {schema}.{table})
              FROM lake_replication.checkpoints c
             WHERE c.subscription_id={subscription_id}
        """)
        check(f"{label} does not stage or publish queued WAL",
              unchanged, f"{baseline}:1:snapshot")
        if not remove_slot:
            confirmed = value("application", f"""
                SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{slot}'
            """)
            check("identity mismatch does not advance source slot", confirmed, baseline)
    finally:
        cleanup(subscription_id, table, publication, slot, schema)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
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
        healthy_case(identity)
        fatal_case("identity_mismatch", identity, fake_identity=True)
        fatal_case("missing_slot", identity, remove_slot=True)
        exit_code = 0
    except Exception as error:
        EVIDENCE["error"] = str(error)
        print(f"ERROR {error}", flush=True)
    finally:
        EVIDENCE["completed_at"] = datetime.now(timezone.utc).isoformat()
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"evidence: {RESULTS / 'results.json'}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Milestone 0 characterization. Only mutates this Compose project's lab data."""
import json
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("probe_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": [], "native": {}}


def command(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=120, **kwargs)


def sql_args(database):
    return COMPOSE + ["exec", "-T", "postgres", "psql", "-XAt", "-v",
                      "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose",
                      "-U", "postgres", "-d", database]


def sql(database, statement):
    result = command(sql_args(database), input=statement)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({"name": name, "passed": passed,
                                "actual": actual, "expected": expected})
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(name)


def wait_for(database, statement, expected, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if sql(database, statement) == expected:
            return True
        time.sleep(0.2)
    return False


def inventory(label):
    # Keep metadata and current file inventory after each committed step.
    data = sql("analytics", f"""
        SELECT coalesce(jsonb_agg(jsonb_build_object(
            'table', t.table_name, 'metadata_location', t.metadata_location,
            'metadata', lake_iceberg.metadata(t.metadata_location),
            'files', CASE WHEN jsonb_array_length(coalesce(
                       lake_iceberg.metadata(t.metadata_location)::jsonb->'snapshots',
                       '[]'::jsonb)) > 0
                     THEN (SELECT coalesce(jsonb_agg(to_jsonb(f)), '[]'::jsonb)
                           FROM lake_iceberg.files(t.metadata_location) f)
                     ELSE '[]'::jsonb END)), '[]'::jsonb)
        FROM iceberg_tables t WHERE t.table_namespace = '{RUN}';
    """)
    (RESULTS / f"{label}.json").write_text(data + "\n")
    cleanup = sql("analytics", f"""
        SELECT jsonb_build_object(
          'in_progress_files', (SELECT coalesce(jsonb_agg(to_jsonb(f)), '[]'::jsonb)
             FROM lake_engine.in_progress_files f WHERE path LIKE '%{RUN}%'),
          'deletion_queue', (SELECT coalesce(jsonb_agg(to_jsonb(f)), '[]'::jsonb)
             FROM lake_engine.deletion_queue f WHERE table_name IN
               (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='{RUN}')));
    """)
    (RESULTS / f"{label}-cleanup.json").write_text(cleanup + "\n")


def direct_sql():
    table = f"{RUN}.direct"
    peer = f"{RUN}.peer"
    sql("analytics", f"CREATE SCHEMA {RUN}; CREATE TABLE {table} (id int, value text) USING iceberg;")
    sql("analytics", f"CREATE TABLE {peer} (id int, value text) USING iceberg;")
    inventory("00-empty")
    sql("analytics", f"INSERT INTO {table} SELECT i, 'row-' || i FROM generate_series(1, 100) i;")
    check("direct inserts", sql("analytics", f"SELECT count(*) FROM {table}"), "100")
    inventory("01-insert")
    sql("analytics", f"UPDATE {table} SET value = repeat('toast', 16000) WHERE id = 1;")
    check("direct large value", sql("analytics", f"SELECT length(value) FROM {table} WHERE id=1"), "80000")
    inventory("02-update")
    sql("analytics", f"DELETE FROM {table} WHERE id=2;")
    check("direct delete", sql("analytics", f"SELECT count(*) FROM {table}"), "99")
    inventory("03-delete")
    sql("analytics", f"BEGIN; INSERT INTO {table} VALUES (101, 'rollback'); ROLLBACK;")
    check("direct rollback", sql("analytics", f"SELECT count(*) FROM {table} WHERE id=101"), "0")

    # A separate connection observes the table while a writer pauses after its first INSERT.
    writer = subprocess.Popen(sql_args("analytics"), stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    writer.stdin.write(f"SET application_name='{RUN}'; BEGIN; INSERT INTO {table} VALUES (101,'a'); INSERT INTO {peer} VALUES (101,'a'); SELECT pg_sleep(8); INSERT INTO {table} VALUES (102,'b'); INSERT INTO {peer} VALUES (102,'b'); COMMIT;\n")
    writer.stdin.close()
    writer.stdin = None
    try:
        check("writer reached transaction barrier", wait_for("analytics",
              f"SELECT count(*) FROM pg_stat_activity WHERE application_name='{RUN}' AND wait_event='PgSleep'", "1"), True)
        visible = sql("analytics", f"SELECT (SELECT count(*) FROM {table} WHERE id>=101) || ':' || (SELECT count(*) FROM {peer})")
        # A blocked reader may resume after commit and see both rows.
        check("no partial cross-table transaction", visible in ("0:0", "2:2"), True)
        _, stderr = writer.communicate(timeout=30)
        if writer.returncode:
            raise RuntimeError(stderr)
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.communicate()
    check("complete direct transaction", sql("analytics", f"SELECT count(*) FROM {table} WHERE id>=101"), "2")
    check("complete peer transaction", sql("analytics", f"SELECT count(*) FROM {peer}"), "2")
    inventory("04-transaction")

    # Terminate the backend after an INSERT returned but before COMMIT.
    writer = subprocess.Popen(sql_args("analytics"), stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    writer.stdin.write(f"SET application_name='{RUN}_abort'; BEGIN; INSERT INTO {table} VALUES (103,'abort'); INSERT INTO {peer} VALUES (103,'abort'); SELECT pg_sleep(60); COMMIT;\n")
    writer.stdin.close()
    writer.stdin = None
    try:
        check("precommit failure barrier", wait_for("analytics",
              f"SELECT count(*) FROM pg_stat_activity WHERE application_name='{RUN}_abort' AND wait_event='PgSleep'", "1"), True)
        sql("analytics", f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='{RUN}_abort'")
        writer.communicate(timeout=30)
        check("terminated transaction rejected", writer.returncode != 0, True)
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.communicate()
    check("terminated transaction invisible", sql("analytics", f"SELECT count(*) FROM {table} WHERE id=103"), "0")
    check("terminated peer transaction invisible", sql("analytics", f"SELECT count(*) FROM {peer} WHERE id=103"), "0")
    inventory("05-aborted-backend")

    # Catalog publication and an ordinary heap checkpoint must recover together.
    sql("analytics", f"CREATE TABLE {RUN}.checkpoint (batch int PRIMARY KEY);")
    sql("analytics", f"BEGIN; INSERT INTO {table} VALUES (104,'durable'); INSERT INTO {peer} VALUES (104,'durable'); INSERT INTO {RUN}.checkpoint VALUES (1); COMMIT;")
    result = command(COMPOSE + ["kill", "-s", "SIGKILL", "postgres"])
    if result.returncode:
        raise RuntimeError(result.stderr)
    result = command(COMPOSE + ["start", "postgres"])
    if result.returncode:
        raise RuntimeError(result.stderr)
    deadline = time.monotonic() + 60
    while True:
        try:
            sql("analytics", "SELECT 1")
            break
        except RuntimeError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)
    check("post-crash catalogs and checkpoint", sql("analytics", f"SELECT (SELECT count(*) FROM {table} WHERE id=104) || ':' || (SELECT count(*) FROM {peer} WHERE id=104) || ':' || (SELECT count(*) FROM {RUN}.checkpoint WHERE batch=1)"), "1:1:1")
    check("post-crash aborted row absent", sql("analytics", f"SELECT count(*) FROM {table} WHERE id=103"), "0")
    check("post-crash aborted peer absent", sql("analytics", f"SELECT count(*) FROM {peer} WHERE id=103"), "0")
    inventory("06-recovered")
    sql("analytics", f"TRUNCATE {table};")
    check("direct truncate", sql("analytics", f"SELECT count(*) FROM {table}"), "0")
    inventory("07-truncate")


def amplification():
    for suffix in ('individual', 'batch'):
        sql("analytics", f"CREATE TABLE {RUN}.{suffix} (id int, value text) USING iceberg;")
    for key in range(10):
        sql("analytics", f"INSERT INTO {RUN}.individual VALUES ({key}, 'value');")
    sql("analytics", f"INSERT INTO {RUN}.batch SELECT i, 'value' FROM generate_series(0,9) i;")
    check("equivalent batch rows", sql("analytics", f"SELECT count(*) FROM ((SELECT * FROM {RUN}.individual EXCEPT ALL SELECT * FROM {RUN}.batch) UNION ALL (SELECT * FROM {RUN}.batch EXCEPT ALL SELECT * FROM {RUN}.individual)) d"), "0")
    inventory("08-amplification")
    tables = json.loads((RESULTS / "08-amplification.json").read_text())
    EVIDENCE["amplification"] = {
        t["table"]: {"snapshots": len(t["metadata"].get("snapshots", [])),
                     "current_files": len(t["files"])}
        for t in tables if t["table"] in ("individual", "batch")
    }


def native(copy_data, iceberg=True):
    suffix = ('copy' if copy_data else 'stream') + ('_iceberg' if iceberg else '_heap')
    name = f"{RUN}_{suffix}"
    table = f"{RUN}.{suffix}"
    sql("application", f"CREATE TABLE {table} (id int PRIMARY KEY, value text); ALTER TABLE {table} REPLICA IDENTITY FULL;")
    using = ' USING iceberg' if iceberg else ''
    sql("analytics", f"CREATE TABLE {table} (id int, value text){using};")
    if copy_data:
        sql("application", f"INSERT INTO {table} VALUES (1, 'initial');")
    sql("application", f"CREATE PUBLICATION {name} FOR TABLE {table};")
    # A separate source connection creates the slot before CREATE SUBSCRIPTION.
    sql("application", f"SELECT pg_create_logical_replication_slot('{name}', 'pgoutput');")
    created = False
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        sql("analytics", f"""CREATE SUBSCRIPTION {name}
            CONNECTION 'host=/var/run/postgresql dbname=application user=postgres'
            PUBLICATION {name} WITH (create_slot=false, slot_name='{name}',
                                    copy_data={'true' if copy_data else 'false'});
        """)
        created = True
        if not copy_data:
            sql("application", f"INSERT INTO {table} VALUES (1, 'streamed');")
        reached = wait_for("analytics", f"SELECT count(*) FROM {table}", "1", timeout=20)
        EVIDENCE["native"][name] = {"first_row_reached": reached,
            "subscription_state": sql("analytics", f"SELECT to_jsonb(r) FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid=r.srsubid WHERE s.subname='{name}'"),
            "errors": sql("analytics", f"SELECT to_jsonb(s) FROM pg_stat_subscription_stats s WHERE subname='{name}'")}
        if not reached:
            logs = command(COMPOSE + ["logs", "--no-color", "--since", started_at, "postgres"])
            diagnostic = logs.stdout + logs.stderr
            (RESULTS / f"{suffix}-postgres.log").write_text(diagnostic)
            rejected = any(f'cannot use relation "{relation}" as logical replication target'
                           in diagnostic for relation in (table, suffix))
            if logs.returncode or not rejected:
                raise RuntimeError(f"Native {suffix} timed out without a confirmed relation-kind rejection; inspect {suffix}-postgres.log")
            EVIDENCE["native"][name]["rejection"] = "foreign-table logical replication target"
            print(f"BLOCKED native {'initial copy' if copy_data else 'streaming apply'}; inspect postgres.log", flush=True)
            return False
        sql("application", f"BEGIN; UPDATE {table} SET value=repeat('toast',16000) WHERE id=1; INSERT INTO {table} VALUES (2,'second'); COMMIT;")
        check("native update and insert", wait_for("analytics", f"SELECT count(*) FROM {table} WHERE (id=1 AND length(value)=80000) OR id=2", "2"), True)
        sql("application", f"DELETE FROM {table} WHERE id=2;")
        check("native delete", wait_for("analytics", f"SELECT count(*) FROM {table}", "1"), True)
        sql("application", f"TRUNCATE {table};")
        check("native truncate", wait_for("analytics", f"SELECT count(*) FROM {table}", "0"), True)
        return True
    except RuntimeError as exc:
        if "42809" not in str(exc) or "logical replication target" not in str(exc):
            raise
        EVIDENCE["native"][name] = {"first_row_reached": False, "error": str(exc)}
        print(f"BLOCKED native subscription: {exc}", flush=True)
        return False
    finally:
        # Stop retries and release only the slot allocated by this probe.
        if created:
            sql("analytics", f"ALTER SUBSCRIPTION {name} DISABLE;")
            if not wait_for("application", f"SELECT active::text FROM pg_replication_slots WHERE slot_name='{name}'", "false"):
                raise RuntimeError(f"Slot {name} still active; cleanup needs investigation")
            sql("analytics", f"ALTER SUBSCRIPTION {name} SET (slot_name=NONE); DROP SUBSCRIPTION {name};")
        sql("application", f"SELECT pg_drop_replication_slot('{name}'); DROP PUBLICATION {name};")


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    exit_code = 1
    try:
        EVIDENCE["postgres_version"] = sql("analytics", "SELECT version()")
        EVIDENCE["extensions"] = sql("analytics", "SELECT jsonb_object_agg(extname, extversion) FROM pg_extension")
        EVIDENCE["source_identity"] = sql("application", "SELECT to_jsonb(s) FROM pg_control_system() s")
        direct_sql()
        amplification()
        sql("application", f"CREATE SCHEMA {RUN};")
        check("heap subscription control", native(True, iceberg=False), True)
        copy_ok = native(True)
        stream_ok = native(False)
        inventory("09-native")
        EVIDENCE["status"] = "native-basic-probes-passed" if copy_ok and stream_ok else "native-blocked"
        exit_code = 0 if copy_ok and stream_ok else 2
    except Exception as exc:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        logs = command(COMPOSE + ["logs", "--no-color", "postgres"])
        (RESULTS / "postgres.log").write_text(logs.stdout + logs.stderr)
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

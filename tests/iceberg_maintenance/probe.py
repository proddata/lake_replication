#!/usr/bin/env python3
"""Validates pg_lake's native Iceberg storage maintenance: manual VACUUM
compaction, per-table snapshot-retention overrides, autovacuum_enabled
options, and the deletion-queue PITR retention window. This project does not
implement Iceberg-side maintenance itself; it relies on pg_lake's built-in
VACUUM (ICEBERG) support and background autovacuum worker. See
docs/milestone-25.md.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("icebergvac_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}


def command(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=60, **kwargs)


def sql(statement, *, check_result=True):
    result = command(COMPOSE + ["exec", "-T", "postgres", "psql", "-XAt",
                     "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "analytics"],
                     input=statement)
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def value(statement):
    return sql(statement).stdout.strip()


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({"name": name, "passed": passed,
                               "actual": actual, "expected": expected})
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def file_count(table):
    return value(f"""
        SELECT count(*) FROM lake_iceberg.files(
            (SELECT metadata_location FROM iceberg_tables
              WHERE table_namespace='{RUN}' AND table_name='{table}'))
    """)


def snapshot_count(table):
    return value(f"""
        SELECT jsonb_array_length(coalesce(
            lake_iceberg.metadata(
                (SELECT metadata_location FROM iceberg_tables
                  WHERE table_namespace='{RUN}' AND table_name='{table}')
            )::jsonb->'snapshots', '[]'::jsonb))
    """)


def foreign_table_option(table, option):
    return value(f"""
        SELECT coalesce((
            SELECT split_part(opt, '=', 2)
              FROM pg_foreign_table ft
              JOIN pg_class c ON c.oid=ft.ftrelid
              JOIN pg_namespace n ON n.oid=c.relnamespace,
              unnest(ft.ftoptions) opt
             WHERE n.nspname='{RUN}' AND c.relname='{table}'
               AND opt LIKE '{option}=%'
        ), '')
    """)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    exit_code = 1
    try:
        value(f"CREATE SCHEMA {RUN}")

        # Manual VACUUM compaction: repeated single-row commits amplify into
        # one file each (see the Milestone 0 lab finding), and default table
        # options leave autovacuum eligible without any configuration from
        # this project's destination-table creation path.
        value(f"CREATE TABLE {RUN}.compact_target (id int, note text) USING iceberg")
        check("newly created destination has no autovacuum override",
              foreign_table_option("compact_target", "autovacuum_enabled"), "")
        for row_id in range(1, 6):
            value(f"INSERT INTO {RUN}.compact_target VALUES ({row_id}, 'row-{row_id}')")
        check("five separate commits amplify into five data files",
              file_count("compact_target"), "5")
        before_snapshots = int(snapshot_count("compact_target"))
        value(f"VACUUM {RUN}.compact_target")
        check("VACUUM compacts data files into one",
              file_count("compact_target"), "1")
        check("VACUUM's own compaction commits add snapshots rather than "
              "pruning history still within max_snapshot_age",
              int(snapshot_count("compact_target")) > before_snapshots, True)

        # Per-table max_snapshot_age=0 expires old snapshots automatically on
        # every write, without a separate VACUUM.
        value(f"""
            CREATE TABLE {RUN}.autoexpire_target (id int) USING iceberg
                WITH (max_snapshot_age = 0)
        """)
        for row_id in range(1, 4):
            value(f"INSERT INTO {RUN}.autoexpire_target VALUES ({row_id})")
        check("max_snapshot_age=0 keeps only the latest snapshot without VACUUM",
              snapshot_count("autoexpire_target"), "1")

        # autovacuum_enabled=false is honored as a per-table foreign-table
        # option (manual VACUUM still compacts it; only the background
        # autovacuum worker skips it).
        value(f"""
            CREATE TABLE {RUN}.novacuum_target (id int) USING iceberg
                WITH (autovacuum_enabled = false)
        """)
        check("autovacuum_enabled=false is recorded on the destination table",
              foreign_table_option("novacuum_target", "autovacuum_enabled"), "false")

        # Dropping a table queues its files for deletion but keeps them for
        # the default 10-day PITR window; VACUUM (ICEBERG) must not remove
        # them early.
        value(f"CREATE TABLE {RUN}.dropped_target (id int) USING iceberg")
        value(f"INSERT INTO {RUN}.dropped_target VALUES (1)")
        value(f"DROP TABLE {RUN}.dropped_target")

        def dropped_queue_entries():
            # DROP clears deletion_queue.table_name to NULL (it is a regclass
            # column and the relation is already gone), so matching by path
            # is the only way to find a dropped table's queued files.
            return int(value(f"""
                SELECT count(*) FROM lake_engine.deletion_queue
                 WHERE path LIKE '%/{RUN}/dropped_target/%'
            """))

        check("dropped table's files enter the deletion queue",
              dropped_queue_entries() > 0, True)
        value("VACUUM (ICEBERG)")
        check("VACUUM (ICEBERG) respects the 10-day deletion-queue retention",
              dropped_queue_entries() > 0, True)

        EVIDENCE["status"] = "passed"
        exit_code = 0
    except Exception as exc:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        sql(f"DROP SCHEMA IF EXISTS {RUN} CASCADE", check_result=False)
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

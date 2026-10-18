#!/usr/bin/env python3
"""Lab-only crash-window characterization for pg_lake Iceberg commits."""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("crash_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": [], "boundaries": {}}


def command(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=30, **kwargs)


def sql(statement, *, check=True):
    result = command(COMPOSE + ["exec", "-T", "-e", "PGCONNECT_TIMEOUT=2",
                     "postgres", "psql", "-XAt",
                     "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose",
                     "-U", "postgres", "-d", "analytics"], input=statement)
    if check and result.returncode:
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
        raise AssertionError(name)


def wait_ready():
    deadline = time.monotonic() + 90
    last_error = ""
    while time.monotonic() < deadline:
        try:
            if value("SELECT 1") == "1":
                return
        except (RuntimeError, subprocess.TimeoutExpired) as exc:
            last_error = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"PostgreSQL did not restart after crash: {last_error}")


def catalog_state(table):
    raw = value(f"""
        SELECT jsonb_build_object(
          'rows', (SELECT count(*) FROM {RUN}.{table}),
          'metadata_location', t.metadata_location,
          'snapshots', jsonb_array_length(coalesce(
              lake_iceberg.metadata(t.metadata_location)::jsonb->'snapshots',
              '[]'::jsonb)),
          'in_progress', (SELECT count(*) FROM lake_engine.in_progress_files f
                          WHERE f.path LIKE '%/{RUN}/{table}/%'),
          'in_progress_files', (SELECT coalesce(jsonb_agg(to_jsonb(f)), '[]'::jsonb)
                                FROM lake_engine.in_progress_files f
                                WHERE f.path LIKE '%/{RUN}/{table}/%'))
        FROM iceberg_tables t
        WHERE t.table_namespace='{RUN}' AND t.table_name='{table}';
    """)
    return json.loads(raw)


def in_progress_count(table):
    return int(value(f"""SELECT count(*) FROM lake_engine.in_progress_files
                         WHERE path LIKE '%/{RUN}/{table}/%'"""))


def exercise(point, expected_rows):
    table = point
    value(f"CREATE TABLE {RUN}.{table} (id bigint, marker text) USING iceberg;")
    before_start = value("SELECT pg_postmaster_start_time()")
    started_at = datetime.now(timezone.utc).isoformat()
    writer = sql(f"""
        SET pg_lake_table.lab_crash_point='{point}';
        INSERT INTO {RUN}.{table} VALUES (1, '{point}');
    """, check=False)
    check(f"{point}: client did not receive success", writer.returncode != 0, True)
    wait_ready()
    after_start = value("SELECT pg_postmaster_start_time()")
    state = catalog_state(table)
    check(f"{point}: recovered row count", state["rows"], expected_rows)
    logs = command(COMPOSE + ["logs", "--no-color", "--since", started_at,
                             "postgres"])
    diagnostic = logs.stdout + logs.stderr
    (RESULTS / f"{point}-postgres.log").write_text(diagnostic)
    check(f"{point}: backend killed at boundary",
          "was terminated by signal 9" in diagnostic, True)
    check(f"{point}: PostgreSQL performed crash recovery",
          "all server processes terminated; reinitializing" in diagnostic and
          "database system is ready to accept connections" in diagnostic, True)
    EVIDENCE["boundaries"][point] = {
        "writer_returncode": writer.returncode,
        "writer_stdout": writer.stdout,
        "writer_stderr": writer.stderr,
        "postmaster_start_before": before_start,
        "postmaster_start_after": after_start,
        "recovered": state,
    }
    if expected_rows == 0:
        check(f"{point}: abandoned objects are discoverable",
              state["in_progress"] > 0, True)
        value(f"VACUUM {RUN}.{table}")
        check(f"{point}: VACUUM cleared orphan records",
              in_progress_count(table), 0)
        EVIDENCE["boundaries"][point]["in_progress_after_vacuum"] = 0


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    exit_code = 1
    try:
        check("instrumentation defaults off",
              value("SHOW pg_lake_table.lab_crash_point"), "none")
        value(f"CREATE SCHEMA {RUN}")
        exercise("before_upload_completion", 0)
        exercise("after_upload_completion", 0)
        exercise("after_commit", 1)
        EVIDENCE["status"] = "passed"
        exit_code = 0
    except Exception as exc:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

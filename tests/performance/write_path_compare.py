#!/usr/bin/env python3
"""Compare journal-shaped SQL inserts with pg_lake's bulk COPY insert path.

This isolates destination insert application. It does not compare full CDC
pipelines or measure update/delete performance.
"""

import argparse
import json
from pathlib import Path
import subprocess
import time
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]


def psql(database, script, *, timeout=600):
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "postgres", "psql", "-XAtq",
                   "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database],
        input=script, text=True, capture_output=True, timeout=timeout, cwd=ROOT,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def measure(database, script):
    started = time.monotonic()
    psql(database, script)
    return round(time.monotonic() - started, 3)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--batch-rows", type=int, default=500)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--copy-first", action="store_true",
                        help="run COPY before SQL to expose ordering effects")
    args = parser.parse_args()
    if min(args.rows, args.batch_rows, args.payload_bytes) <= 0:
        parser.error("all sizes must be positive")
    if args.payload_bytes > 100000:
        parser.error("payload is too large for this diagnostic probe")

    run = datetime.now(timezone.utc).strftime("write_paths_%Y%m%d_%H%M%S")
    database = run
    output = ROOT / ".lab" / run
    output.mkdir(parents=True, exist_ok=False)
    evidence = {"run": run, "configuration": vars(args), "status": "running"}
    created = False
    try:
        psql("application", f"CREATE DATABASE {database};")
        created = True
        psql(database, "CREATE EXTENSION pg_lake CASCADE; "
                       "CREATE TABLE sql_target (id bigint, note text) USING iceberg; "
                       "CREATE TABLE copy_target (id bigint, note text) USING iceberg;")

        # One psql process per path. Each statement is a distinct Iceberg write;
        # the batch count is identical in the two paths.
        sql_batches = []
        copy_batches = []
        payload = "x" * args.payload_bytes
        for first in range(1, args.rows + 1, args.batch_rows):
            last = min(args.rows, first + args.batch_rows - 1)
            sql_batches.append(
                "INSERT INTO sql_target "
                "SELECT (jsonb_populate_record(NULL::sql_target, row)).* "
                "FROM unnest(ARRAY(SELECT jsonb_build_object('id', g, 'note', "
                f"repeat('x', {args.payload_bytes})) FROM generate_series({first}, {last}) g)) "
                "AS input(row);"
            )
            copy_batches.append("COPY copy_target (id, note) FROM STDIN;\n" +
                                "".join(f"{row}\t{payload}\n" for row in range(first, last + 1)) +
                                "\\.\n")
        evidence["batches"] = len(sql_batches)
        scripts = {"sql": "\n".join(sql_batches),
                   "copy": "\n".join(copy_batches)}
        order = ("copy", "sql") if args.copy_first else ("sql", "copy")
        evidence["path_order"] = order
        for name in order:
            evidence[f"{name}_seconds"] = measure(database, scripts[name])
        counts = psql(database, "SELECT (SELECT count(*) FROM sql_target), "
                                "(SELECT count(*) FROM copy_target);")
        evidence["destination_counts"] = counts
        if counts != f"{args.rows}|{args.rows}":
            raise RuntimeError(f"destination count mismatch: {counts}")
        for name in ("sql", "copy"):
            evidence[f"{name}_rows_per_second"] = round(
                args.rows / evidence[f"{name}_seconds"], 1)
        evidence["status"] = "passed"
        print(f"SQL: {evidence['sql_rows_per_second']} rows/s; "
              f"COPY: {evidence['copy_rows_per_second']} rows/s")
    except Exception as exc:
        evidence["status"] = "error"
        evidence["error"] = str(exc)
        raise
    finally:
        (output / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
        if created:
            psql("application", f"DROP DATABASE IF EXISTS {database} WITH (FORCE);")
        print(f"Evidence: {output / 'results.json'}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Exported-snapshot initial copy, crash, rebuild, and WAL catch-up probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("initial_%Y%m%d_%H%M%S")
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


def environment(subscription_id, **settings):
    result = os.environ.copy()
    result.update({
        "LAKE_SLOT": SLOT,
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_SOURCE_TABLE": f"public.{TABLE}",
        "LAKE_DESTINATION_TABLE": f"{RUN}.target",
    })
    result.update(settings)
    return result


def worker(binary, subscription_id, **settings):
    result = command([str(ROOT / "target/debug" / binary)],
                     env=environment(subscription_id, **settings))
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def initial_sync(subscription_id, **settings):
    return worker("lake-replication-initial-sync", subscription_id,
                  LAKE_OVERWRITE_DESTINATION="1",
                  LAKE_INITIAL_COPY_CHUNK_ROWS="25", **settings)


def receive(subscription_id):
    return worker("lake-replication-receiver", subscription_id,
                  LAKE_MAX_TRANSACTIONS="1")


def publish(subscription_id):
    return worker("lake-replication-publisher", subscription_id,
                  LAKE_BATCH_MAX_TRANSACTIONS="10",
                  LAKE_BATCH_MAX_ROWS="1000",
                  LAKE_BATCH_MAX_BYTES="8388608")


def wait_for(database, statement, expected, timeout=8):
    deadline = time.monotonic() + timeout
    actual = None
    while time.monotonic() < deadline:
        actual = value(database, statement)
        if actual == expected:
            return actual
        time.sleep(0.05)
    return actual


def digest(database, relation):
    return value(database, f"""
        SELECT count(*) || ':' || count(DISTINCT id) || ':' ||
               md5(string_agg(id::text || ':' || note, ',' ORDER BY id))
          FROM {relation}
    """)


def cleanup(subscription_id):
    sql("application", f"""
        SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
         WHERE slot_name='{SLOT}';
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS public.{TABLE};
    """, check_result=False)
    sql("analytics", f"DROP SCHEMA IF EXISTS {RUN} CASCADE", check_result=False)
    if subscription_id is not None:
        sql("analytics", f"""
            DELETE FROM lake_replication.changes WHERE transaction_id IN
                (SELECT transaction_id FROM lake_replication.transactions
                  WHERE subscription_id={subscription_id});
            DELETE FROM lake_replication.transactions WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.batches WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.initial_copies WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.relations WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.checkpoints WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.subscriptions WHERE subscription_id={subscription_id};
        """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscription_id = None
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
            INSERT INTO public.{TABLE}
            SELECT g, CASE WHEN g=50
                           THEN 'line' || chr(10) || 'slash\\quote''unicode-☃'
                           ELSE 'snapshot-' || g END
              FROM generate_series(1, 100) g;
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE}
        """)
        subscription_id = int(value("analytics", f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """))
        value("analytics", f"""
            CREATE SCHEMA {RUN};
            CREATE TABLE {RUN}.target (id bigint, note text) USING iceberg;
            INSERT INTO {RUN}.target VALUES (0, 'previous-readable-state')
        """)

        failed = command(
            [str(ROOT / "target/debug/lake-replication-initial-sync")],
            env=environment(subscription_id,
                            LAKE_OVERWRITE_DESTINATION="1",
                            LAKE_INITIAL_COPY_CHUNK_ROWS="25",
                            LAKE_INITIAL_COPY_FAIL_AFTER_ROWS="50"))
        check("injected mid-copy failure is reported", failed.returncode != 0, True)
        relation = value("analytics", f"""
            SELECT remote_relation_id || ':' || schema_version
              FROM lake_replication.relations WHERE subscription_id={subscription_id}
             ORDER BY schema_version DESC LIMIT 1
        """).split(":")
        check("partial snapshot rows and error state are durable",
              value("analytics", f"""
                SELECT state || ':' || rows_copied || ':' ||
                       (last_error LIKE '%injected failure%')::text
                  FROM lake_replication.initial_copies
                 WHERE subscription_id={subscription_id}
              """), "error:50:true")
        check("failed copy leaves previous Iceberg state readable",
              digest("analytics", f"{RUN}.target"),
              "1:1:306b5770bfac685cf5850a6b3f3d4398")
        check("failed copy does not establish a replication baseline",
              value("analytics", f"""
                SELECT received_lsn || ':' || staged_lsn || ':' ||
                       published_lsn || ':' || acknowledged_lsn
                  FROM lake_replication.checkpoints
                 WHERE subscription_id={subscription_id}
              """), "0/0:0/0:0/0:0/0")

        value("analytics", f"""
            SELECT lake_replication.reset_initial_copy(
                {subscription_id}, {relation[0]}, {relation[1]})
        """)
        value("application", f"SELECT pg_drop_replication_slot('{SLOT}')")

        process = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-initial-sync")],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=environment(subscription_id,
                            LAKE_OVERWRITE_DESTINATION="1",
                            LAKE_INITIAL_COPY_CHUNK_ROWS="25",
                            LAKE_INITIAL_COPY_PAUSE_MS="1500",
                            LAKE_INITIAL_COPY_CHUNK_PAUSE_MS="250"))
        state = wait_for("analytics", f"""
            SELECT state FROM lake_replication.initial_copies
             WHERE subscription_id={subscription_id}
        """, "copying")
        check("table exposes copying progress", state, "copying")
        check("previous Iceberg state remains visible while snapshot is copied",
              digest("analytics", f"{RUN}.target"),
              "1:1:306b5770bfac685cf5850a6b3f3d4398")

        value("application", f"""
            BEGIN;
            UPDATE public.{TABLE} SET note='caught-up-update' WHERE id=1;
            DELETE FROM public.{TABLE} WHERE id=2;
            INSERT INTO public.{TABLE} VALUES (101, 'caught-up-insert');
            COMMIT
        """)
        check("COPY streams durable progress before completion",
              wait_for("analytics", f"""
                SELECT rows_copied::text FROM lake_replication.initial_copies
                 WHERE subscription_id={subscription_id}
              """, "25"), "25")
        check("destination remains unchanged after the first streamed chunk",
              digest("analytics", f"{RUN}.target"),
              "1:1:306b5770bfac685cf5850a6b3f3d4398")
        stdout, stderr = process.communicate(timeout=30)
        if process.returncode:
            raise RuntimeError(stderr.strip())
        check("snapshot worker reports all pre-existing rows",
              "initial copy ready tables=1 rows=100/100" in stdout, True)
        check("atomic transition exposes only the exported snapshot",
              value("analytics", f"""
                SELECT count(*) || ':' ||
                       (SELECT note FROM {RUN}.target WHERE id=1) || ':' ||
                       (SELECT count(*) FROM {RUN}.target WHERE id IN (2,101))
                  FROM {RUN}.target
              """), "100:snapshot-1:1")
        check("COPY framing preserves newline, backslash, quote, and Unicode",
              value("analytics", f"""
                SELECT encode(convert_to(note, 'UTF8'), 'hex') FROM {RUN}.target WHERE id=50
              """),
              value("application", f"""
                SELECT encode(convert_to(note, 'UTF8'), 'hex') FROM public.{TABLE} WHERE id=50
              """))
        baseline = value("analytics", f"""
            SELECT consistent_lsn FROM lake_replication.initial_copies
             WHERE subscription_id={subscription_id}
        """)
        check("snapshot LSN atomically initializes every checkpoint",
              value("analytics", f"""
                SELECT received_lsn || ':' || staged_lsn || ':' ||
                       published_lsn || ':' || acknowledged_lsn
                  FROM lake_replication.checkpoints
                 WHERE subscription_id={subscription_id}
              """), f"{baseline}:{baseline}:{baseline}:{baseline}")
        check("copy progress reaches ready with exact row count",
              value("analytics", f"""
                SELECT state || ':' || rows_copied FROM lake_replication.initial_copies
                 WHERE subscription_id={subscription_id}
              """), "ready:100")

        receiver_output = receive(subscription_id)
        check("one concurrent source transaction is staged after the snapshot",
              '"event":"transaction_staged"' in receiver_output
              and "staged xid=" in receiver_output and "changes=3" in receiver_output,
              True)
        check("snapshot and pgoutput use one canonical schema version",
              value("analytics", f"""
                SELECT count(*) || ':' || max(schema_version)
                  FROM lake_replication.relations WHERE subscription_id={subscription_id}
              """), "1:1")
        publish(subscription_id)
        check("WAL catch-up has no missing or duplicate live rows",
              digest("analytics", f"{RUN}.target"),
              digest("application", f"public.{TABLE}"))
        check("final row identities reflect update, delete, and insert",
              value("analytics", f"""
                SELECT (SELECT note FROM {RUN}.target WHERE id=1) || ':' ||
                       (SELECT count(*) FROM {RUN}.target WHERE id=2) || ':' ||
                       (SELECT note FROM {RUN}.target WHERE id=101)
              """), "caught-up-update:0:caught-up-insert")
        check("relation and subscription enter continuous replication",
              value("analytics", f"""
                SELECT s.state || ':' || r.state
                  FROM lake_replication.subscriptions s
                  JOIN lake_replication.relations r USING (subscription_id)
                 WHERE s.subscription_id={subscription_id}
              """), "streaming:ready")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'initial_copy', (SELECT to_jsonb(i) FROM lake_replication.initial_copies i
                                  WHERE subscription_id={subscription_id}),
                'checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
                                WHERE subscription_id={subscription_id}),
                'transactions', (SELECT count(*) FROM lake_replication.transactions
                                  WHERE subscription_id={subscription_id}),
                'target_digest', (SELECT count(*) || ':' || count(DISTINCT id) || ':' ||
                                   md5(string_agg(id::text || ':' || note, ',' ORDER BY id))
                                    FROM {RUN}.target))
        """))
        EVIDENCE["status"] = "passed"
        exit_code = 0
    except Exception as exc:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        cleanup(subscription_id)
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""End-to-end insert replication and Iceberg micro-batch probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "pg-lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("publisher_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE = RUN
PUBLICATION = f"{RUN}_pub"
SLOT = f"{RUN}_slot"


def command(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True,
                          timeout=45, **kwargs)


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


def worker(binary, subscription_id, **settings):
    environment = os.environ.copy()
    environment["LAKE_SUBSCRIPTION_ID"] = str(subscription_id)
    environment.update(settings)
    result = command([str(ROOT / "target/debug" / binary)], env=environment)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def worker_result(binary, subscription_id, **settings):
    environment = os.environ.copy()
    environment["LAKE_SUBSCRIPTION_ID"] = str(subscription_id)
    environment.update(settings)
    return command([str(ROOT / "target/debug" / binary)], env=environment)


def wait_ready():
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if value("analytics", "SELECT 1") == "1":
                return
        except (RuntimeError, subprocess.TimeoutExpired):
            pass
        time.sleep(0.25)
    raise RuntimeError("PostgreSQL did not recover after the publisher commit crash")


def snapshots():
    return value("analytics", f"""
        SELECT jsonb_array_length(coalesce(
            lake_iceberg.metadata(metadata_location)::jsonb->'snapshots', '[]'::jsonb))
          FROM iceberg_tables
         WHERE table_namespace='{RUN}' AND table_name='target'
    """)


def cleanup(subscription_id):
    sql("application", f"""
        SELECT pg_drop_replication_slot(slot_name)
          FROM pg_replication_slots WHERE slot_name='{SLOT}';
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
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};
        """)
        value("application", f"""
            SELECT slot_name FROM pg_create_logical_replication_slot('{SLOT}', 'pgoutput')
        """)
        subscription_id = int(value("analytics", f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """))
        value("analytics", f"""
            CREATE SCHEMA {RUN};
            CREATE TABLE {RUN}.target (id bigint, note text) USING iceberg
        """)

        value("application", f"INSERT INTO public.{TABLE} VALUES (1, 'one')")
        value("application", f"""
            BEGIN;
            INSERT INTO public.{TABLE} VALUES (2, 'two'), (3, 'three');
            COMMIT
        """)
        value("application", f"INSERT INTO public.{TABLE} VALUES (4, 'four')")
        receiver = worker(
            "lake-replication-receiver", subscription_id,
            LAKE_SLOT=SLOT, LAKE_PUBLICATION=PUBLICATION,
            LAKE_MAX_TRANSACTIONS="3")
        check("three source transactions staged",
              receiver.count("staged xid="), 3)
        check("four inserts retained in transaction order",
              value("analytics", f"""
                SELECT count(DISTINCT t.transaction_id) || ':' || count(*) || ':' ||
                       string_agg(c.new_tuple->>'id', ',' ORDER BY t.end_lsn, c.change_index)
                  FROM lake_replication.changes c
                  JOIN lake_replication.transactions t USING (transaction_id)
                 WHERE t.subscription_id={subscription_id}
              """), "3:4:1,2,3,4")
        relation = value("analytics", f"""
            SELECT remote_relation_id || ':' || schema_version
              FROM lake_replication.relations WHERE subscription_id={subscription_id}
        """).split(":")
        value("analytics", f"""
            SELECT lake_replication.assign_destination(
                {subscription_id}, {relation[0]}, {relation[1]}, '{RUN}.target'::regclass)
        """)

        first = worker(
            "lake-replication-publisher", subscription_id,
            LAKE_BATCH_MAX_TRANSACTIONS="2", LAKE_BATCH_MAX_ROWS="100",
            LAKE_BATCH_MAX_BYTES="1048576", LAKE_MAX_BATCHES="1")
        check("first batch contains two complete transactions",
              "transactions=2" in first and "rows=3/3" in first, True)
        check("first batch makes three rows visible atomically",
              value("analytics", f"SELECT count(*) FROM {RUN}.target"), "3")
        check("first batch creates one Iceberg snapshot", snapshots(), "1")
        check("published checkpoint stops at the batch boundary",
              value("analytics", f"""
                SELECT (published_lsn < staged_lsn)::text || ':' ||
                       count(*) FILTER (WHERE state='published') || ':' ||
                       count(*) FILTER (WHERE state='staged')
                  FROM lake_replication.checkpoints c
                  JOIN lake_replication.transactions t USING (subscription_id)
                 WHERE c.subscription_id={subscription_id}
                 GROUP BY published_lsn, staged_lsn
              """), "true:2:1")

        first_batch = value("analytics", f"""
            SELECT batch_key || '|' || first_end_lsn || '|' || last_end_lsn
              FROM lake_replication.batches WHERE subscription_id={subscription_id}
        """).split("|")
        check("replaying a published batch performs no inserts",
              value("analytics", f"""
                SELECT inserted_rows FROM lake_replication.publish_insert_batch(
                    {subscription_id}, '{first_batch[0]}',
                    '{first_batch[1]}'::pg_lsn, '{first_batch[2]}'::pg_lsn)
              """), "0")
        check("published batch replay creates no snapshot", snapshots(), "1")

        second = worker_result(
            "lake-replication-publisher", subscription_id,
            LAKE_BATCH_MAX_TRANSACTIONS="2", LAKE_BATCH_MAX_ROWS="100",
            LAKE_BATCH_MAX_BYTES="1048576", LAKE_MAX_BATCHES="1",
            LAKE_LAB_CRASH_POINT="after_commit")
        check("post-commit publisher result is ambiguous", second.returncode != 0, True)
        wait_ready()
        check("ambiguous batch committed before connection loss",
              value("analytics", f"SELECT count(*) FROM {RUN}.target"), "4")
        check("destination equals the source after catch-up",
              value("analytics", f"""
                SELECT string_agg(id || '=' || note, ',' ORDER BY id) FROM {RUN}.target
              """), value("application", f"""
                SELECT string_agg(id || '=' || note, ',' ORDER BY id) FROM public.{TABLE}
              """))
        check("two micro-batches create two Iceberg snapshots", snapshots(), "2")
        check("published checkpoint catches staged progress",
              value("analytics", f"""
                SELECT (published_lsn = staged_lsn)::text || ':' ||
                       count(*) FILTER (WHERE state='published')
                  FROM lake_replication.checkpoints c
                  JOIN lake_replication.transactions t USING (subscription_id)
                 WHERE c.subscription_id={subscription_id}
                 GROUP BY published_lsn, staged_lsn
              """), "true:3")
        check("publisher restart reconciles as a successful no-op",
              worker("lake-replication-publisher", subscription_id),
              "no staged transactions")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'source_transactions', 3, 'target_rows', (SELECT count(*) FROM {RUN}.target),
                'batches', (SELECT count(*) FROM lake_replication.batches
                             WHERE subscription_id={subscription_id}),
                'snapshots', {snapshots()},
                'checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
                                WHERE subscription_id={subscription_id}))
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

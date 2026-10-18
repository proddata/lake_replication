#!/usr/bin/env python3
"""End-to-end unchanged-TOAST and large-value transition probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
DEST_DB = os.environ.get("LAKE_TEST_DESTINATION_DATABASE", "analytics")
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("toast_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"


def command(args, timeout=90, **kwargs):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True,
                          timeout=timeout, **kwargs)


def sql(database, statement, *, check_result=True):
    if database == "analytics":
        database = DEST_DB
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
    environment["LAKE_DESTINATION_DATABASE"] = DEST_DB
    environment.update(settings)
    result = command([str(ROOT / "target/debug" / binary)], env=environment)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def receive(subscription_id):
    return worker("lake-replication-receiver", subscription_id,
                  LAKE_SLOT=SLOT, LAKE_PUBLICATION=PUBLICATION,
                  LAKE_MAX_TRANSACTIONS="1")


def publish(subscription_id):
    return worker("lake-replication-publisher", subscription_id,
                  LAKE_BATCH_MAX_TRANSACTIONS="10",
                  LAKE_BATCH_MAX_ROWS="100", LAKE_BATCH_MAX_BYTES="8388608")


def checksum(database, relation):
    return value(database, f"""
        SELECT coalesce(length(payload)::text, 'NULL') || ':' ||
               coalesce(md5(payload), 'NULL') || ':' || marker
          FROM {relation} WHERE id=1
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
            CREATE TABLE public.{TABLE} (
                id bigint PRIMARY KEY, payload text, marker text NOT NULL);
            ALTER TABLE public.{TABLE} ALTER COLUMN payload SET STORAGE EXTERNAL;
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE}
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
            CREATE TABLE {RUN}.target (
                id bigint, payload text, marker text) USING iceberg
        """)

        value("application", f"""
            INSERT INTO public.{TABLE}
            SELECT 1, string_agg(md5(g::text), '' ORDER BY g), 'v1'
              FROM generate_series(1, 4000) g
        """)
        toast_relation = value("application", f"""
            SELECT reltoastrelid::regclass FROM pg_class
             WHERE oid='public.{TABLE}'::regclass
        """)
        check("source value is stored in external TOAST chunks",
              int(value("application", f"SELECT count(*) FROM {toast_relation}")) > 0, True)
        receive(subscription_id)
        relation = value("analytics", f"""
            SELECT remote_relation_id || ':' || schema_version FROM lake_replication.relations
             WHERE subscription_id={subscription_id} ORDER BY schema_version DESC LIMIT 1
        """).split(":")
        value("analytics", f"""
            SELECT lake_replication.assign_destination(
                {subscription_id}, {relation[0]}, {relation[1]}, '{RUN}.target'::regclass)
        """)
        publish(subscription_id)
        check("initial external value reaches Iceberg exactly",
              checksum("analytics", f"{RUN}.target"),
              checksum("application", f"public.{TABLE}"))

        value("application", f"UPDATE public.{TABLE} SET marker='v2' WHERE id=1")
        receive(subscription_id)
        check("unchanged TOAST field is absent from update post-image",
              value("analytics", f"""
                SELECT ((NOT (new_tuple ? 'payload')) AND new_tuple->>'marker'='v2')::text
                  FROM lake_replication.changes c JOIN lake_replication.transactions t
                    USING (transaction_id) WHERE t.subscription_id={subscription_id}
                    AND operation='update' ORDER BY t.end_lsn DESC LIMIT 1
              """), "true")
        publish(subscription_id)
        check("marker-only update preserves external Iceberg value",
              checksum("analytics", f"{RUN}.target"),
              checksum("application", f"public.{TABLE}"))

        value("application", f"""
            UPDATE public.{TABLE} SET payload=(
                SELECT string_agg(md5('new-' || g::text), '' ORDER BY g)
                  FROM generate_series(1, 4000) g), marker='v3' WHERE id=1
        """)
        receive(subscription_id)
        check("changed external value is present in update post-image",
              value("analytics", f"""
                SELECT (new_tuple ? 'payload')::text FROM lake_replication.changes c
                JOIN lake_replication.transactions t USING(transaction_id)
                WHERE t.subscription_id={subscription_id} AND operation='update'
                ORDER BY t.end_lsn DESC LIMIT 1
              """), "true")
        publish(subscription_id)
        check("external-to-external replacement reaches Iceberg exactly",
              checksum("analytics", f"{RUN}.target"),
              checksum("application", f"public.{TABLE}"))

        value("application", f"""
            UPDATE public.{TABLE} SET payload='inline', marker='v4' WHERE id=1
        """)
        receive(subscription_id)
        publish(subscription_id)
        check("external-to-inline transition reaches Iceberg",
              checksum("analytics", f"{RUN}.target"),
              checksum("application", f"public.{TABLE}"))

        value("application", f"UPDATE public.{TABLE} SET payload=NULL, marker='v5' WHERE id=1")
        receive(subscription_id)
        publish(subscription_id)
        check("inline-to-NULL transition reaches Iceberg",
              checksum("analytics", f"{RUN}.target"), "NULL:NULL:v5")

        value("application", f"""
            UPDATE public.{TABLE} SET payload=(
                SELECT string_agg(md5('restored-' || g::text), '' ORDER BY g)
                  FROM generate_series(1, 4000) g), marker='v6' WHERE id=1
        """)
        receive(subscription_id)
        publish(subscription_id)
        check("NULL-to-external transition reaches Iceberg exactly",
              checksum("analytics", f"{RUN}.target"),
              checksum("application", f"public.{TABLE}"))

        value("application", f"UPDATE public.{TABLE} SET payload=NULL, marker='v7' WHERE id=1")
        receive(subscription_id)
        publish(subscription_id)
        check("source and Iceberg final states match",
              checksum("analytics", f"{RUN}.target"),
              checksum("application", f"public.{TABLE}"))
        check("all seven source transactions are published",
              value("analytics", f"""
                SELECT count(*) || ':' || count(*) FILTER (WHERE state='published')
                  FROM lake_replication.transactions WHERE subscription_id={subscription_id}
              """), "7:7")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'transactions', (SELECT count(*) FROM lake_replication.transactions
                                  WHERE subscription_id={subscription_id}),
                'changes', (SELECT count(*) FROM lake_replication.changes c JOIN
                             lake_replication.transactions t USING(transaction_id)
                             WHERE t.subscription_id={subscription_id}),
                'target', (SELECT json_build_object('id',id,'payload',payload,'marker',marker)
                             FROM {RUN}.target),
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

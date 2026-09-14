#!/usr/bin/env python3
"""End-to-end UPDATE, DELETE, and TRUNCATE replication probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "pg-lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("mutations_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"


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


def receive(subscription_id, count="1"):
    return worker("lake-replication-receiver", subscription_id,
                  LAKE_SLOT=SLOT, LAKE_PUBLICATION=PUBLICATION,
                  LAKE_MAX_TRANSACTIONS=count)


def publish(subscription_id):
    return worker("lake-replication-publisher", subscription_id,
                  LAKE_BATCH_MAX_TRANSACTIONS="100",
                  LAKE_BATCH_MAX_ROWS="1000", LAKE_BATCH_MAX_BYTES="1048576")


def snapshot_count():
    return value("analytics", f"""
        SELECT jsonb_array_length(coalesce(
            lake_iceberg.metadata(metadata_location)::jsonb->'snapshots', '[]'::jsonb))
          FROM iceberg_tables WHERE table_namespace='{RUN}' AND table_name='target'
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
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
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
            CREATE TABLE {RUN}.target (id bigint, note text) USING iceberg
        """)

        value("application", f"""
            INSERT INTO public.{TABLE} VALUES (1, 'one'), (2, 'two')
        """)
        check("initial insert transaction decoded", "changes=2" in receive(subscription_id), True)
        relation = value("analytics", f"""
            SELECT remote_relation_id || ':' || schema_version
              FROM lake_replication.relations WHERE subscription_id={subscription_id}
        """).split(":")
        value("analytics", f"""
            SELECT lake_replication.assign_destination(
                {subscription_id}, {relation[0]}, {relation[1]}, '{RUN}.target'::regclass)
        """)
        check("initial rows published", "rows=2/2" in publish(subscription_id), True)

        value("application", f"""
            BEGIN;
            UPDATE public.{TABLE} SET note='one-updated' WHERE id=1;
            UPDATE public.{TABLE} SET id=20, note='twenty' WHERE id=2;
            DELETE FROM public.{TABLE} WHERE id=1;
            COMMIT
        """)
        mutation_output = receive(subscription_id)
        check("update and delete transaction decoded", "changes=3" in mutation_output, True)
        check("mutation identities are named and non-empty",
              value("analytics", f"""
                SELECT count(*) FILTER (WHERE operation='update') || ':' ||
                       count(*) FILTER (WHERE operation='delete') || ':' ||
                       bool_and(old_tuple <> '{{}}'::jsonb)
                  FROM lake_replication.changes c JOIN lake_replication.transactions t
                    USING (transaction_id) WHERE t.subscription_id={subscription_id}
                    AND operation IN ('update','delete')
              """), "2:1:true")
        mutation_publish = publish(subscription_id)
        check("three mutations applied in one batch", "rows=3/3" in mutation_publish, True)
        check("key-changing update and delete match source",
              value("analytics", f"""
                SELECT string_agg(id || '=' || note, ',' ORDER BY id) FROM {RUN}.target
              """), "20=twenty")
        check("mutation transaction creates one additional snapshot", snapshot_count(), "2")

        value("application", f"INSERT INTO public.{TABLE} VALUES (3, 'temporary')")
        value("application", f"TRUNCATE public.{TABLE}")
        check("insert and truncate transactions decoded",
              receive(subscription_id, "2").count("staged xid="), 2)
        check("truncate envelope has no row tuple",
              value("analytics", f"""
                SELECT (old_tuple IS NULL AND new_tuple IS NULL)::text
                  FROM lake_replication.changes c JOIN lake_replication.transactions t
                    USING (transaction_id) WHERE t.subscription_id={subscription_id}
                    AND operation='truncate'
              """), "true")
        final_publish = publish(subscription_id)
        check("ordered insert plus truncate share one batch",
              "transactions=2" in final_publish and "rows=1/2" in final_publish, True)
        check("truncate leaves destination equal to source",
              value("analytics", f"SELECT count(*) FROM {RUN}.target") + ":" +
              value("application", f"SELECT count(*) FROM public.{TABLE}"), "0:0")
        check("four source commits become three Iceberg snapshots", snapshot_count(), "3")
        check("all progress coordinates converge",
              value("analytics", f"""
                SELECT (received_lsn=staged_lsn AND staged_lsn=published_lsn
                        AND staged_lsn=acknowledged_lsn)::text
                  FROM lake_replication.checkpoints WHERE subscription_id={subscription_id}
              """), "true")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'transactions', (SELECT count(*) FROM lake_replication.transactions
                                  WHERE subscription_id={subscription_id}),
                'changes', (SELECT count(*) FROM lake_replication.changes c JOIN
                             lake_replication.transactions t USING(transaction_id)
                             WHERE t.subscription_id={subscription_id}),
                'batches', (SELECT count(*) FROM lake_replication.batches
                             WHERE subscription_id={subscription_id}),
                'snapshots', {snapshot_count()},
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

#!/usr/bin/env python3
"""Automatic Iceberg DDL and versioned type-contract validation probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "pg-lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("schema_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
GOOD, BAD = f"{RUN}_good", f"{RUN}_bad"
GOOD_PUB, BAD_PUB = f"{GOOD}_pub", f"{BAD}_pub"
GOOD_SLOT, BAD_SLOT = f"{GOOD}_slot", f"{BAD}_slot"


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


def worker(subscription_id, slot, publication, source_table, destination_table,
           create_missing):
    environment = os.environ.copy()
    environment.update({
        "LAKE_SLOT": slot,
        "LAKE_PUBLICATION": publication,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_SOURCE_TABLE": f"public.{source_table}",
        "LAKE_DESTINATION_TABLE": f"{RUN}.{destination_table}",
        "LAKE_CREATE_TABLES_USING_ICEBERG": "1" if create_missing else "0",
        "LAKE_TYPE_MAPPING_VERSION": "1",
    })
    return command([str(ROOT / "target/debug/lake-replication-initial-sync")],
                   env=environment)


def cleanup(subscriptions):
    sql("application", f"""
        SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
         WHERE slot_name IN ('{GOOD_SLOT}', '{BAD_SLOT}');
        DROP PUBLICATION IF EXISTS {GOOD_PUB};
        DROP PUBLICATION IF EXISTS {BAD_PUB};
        DROP TABLE IF EXISTS public.{GOOD}, public.{BAD};
    """, check_result=False)
    sql("analytics", f"DROP SCHEMA IF EXISTS {RUN} CASCADE", check_result=False)
    for subscription_id in subscriptions:
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
        """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscriptions = []
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
            CREATE TABLE public.{GOOD} (
                id bigint PRIMARY KEY,
                note text NOT NULL,
                enabled boolean,
                happened_at timestamp with time zone NOT NULL);
            INSERT INTO public.{GOOD} VALUES
                (1, 'automatic', true, '2026-09-14 12:34:56+00'),
                (2, 'nullable', NULL, '2026-09-14 13:00:00+00');
            CREATE PUBLICATION {GOOD_PUB} FOR TABLE public.{GOOD};
            CREATE TABLE public.{BAD} (id bigint PRIMARY KEY, note text NOT NULL);
            INSERT INTO public.{BAD} VALUES (1, 'must-not-copy');
            CREATE PUBLICATION {BAD_PUB} FOR TABLE public.{BAD};
        """)
        value("analytics", f"CREATE SCHEMA {RUN}")

        def subscription(name, slot, publication):
            result = int(value("analytics", f"""
                SELECT lake_replication.create_subscription(
                    {identity['system_identifier']}, {identity['database_oid']},
                    {identity['timeline']}, '{name}', '{slot}', ARRAY['{publication}'])
            """))
            subscriptions.append(result)
            return result

        good_subscription = subscription(GOOD, GOOD_SLOT, GOOD_PUB)
        good = worker(good_subscription, GOOD_SLOT, GOOD_PUB, GOOD, GOOD, True)
        if good.returncode:
            raise RuntimeError(good.stderr.strip())
        check("missing destination is created automatically",
              value("analytics", f"""
                SELECT c.relkind::text || ':' || s.srvname
                  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                  JOIN pg_foreign_table ft ON ft.ftrelid=c.oid
                  JOIN pg_foreign_server s ON s.oid=ft.ftserver
                 WHERE n.nspname='{RUN}' AND c.relname='{GOOD}'
              """), "f:pg_lake_iceberg")
        check("created schema uses canonical mapped types in source order",
              value("analytics", f"""
                SELECT string_agg(attname || ':' || format_type(atttypid,atttypmod),
                                  ',' ORDER BY attnum)
                  FROM pg_attribute WHERE attrelid='{RUN}.{GOOD}'::regclass
                   AND attnum > 0 AND NOT attisdropped
              """),
              "id:bigint,note:text,enabled:boolean,happened_at:timestamp with time zone")
        check("automatic DDL preserves source NOT NULL contracts",
              value("analytics", f"""
                SELECT string_agg(attname || ':' || attnotnull, ',' ORDER BY attnum)
                  FROM pg_attribute WHERE attrelid='{RUN}.{GOOD}'::regclass
                   AND attnum > 0 AND NOT attisdropped
              """), "id:true,note:true,enabled:false,happened_at:true")
        check("initial rows populate the automatically created Iceberg table",
              value("analytics", f"""
                SELECT string_agg(id || ':' || note || ':' || coalesce(enabled::text,'NULL') ||
                                  ':' || happened_at::text, ',' ORDER BY id)
                  FROM {RUN}.{GOOD}
              """),
              "1:automatic:true:2026-09-14 12:34:56+00,2:nullable:NULL:2026-09-14 13:00:00+00")
        check("relation records mapping version one and four contracts",
              value("analytics", f"""
                SELECT mapping_version || ':' || jsonb_array_length(columns)
                  FROM lake_replication.relation_type_contracts
                 WHERE subscription_id={good_subscription}
              """), "1:4")
        check("mapping catalog exposes the supported version",
              value("analytics", """
                SELECT count(*) || ':' || bool_and(mapping_version=1)
                  FROM lake_replication.type_mappings
              """), "13:true")

        value("analytics", f"""
            CREATE TABLE {RUN}.{BAD} (id text, note text) USING iceberg;
            INSERT INTO {RUN}.{BAD} VALUES ('old', 'still-readable')
        """)
        bad_subscription = subscription(BAD, BAD_SLOT, BAD_PUB)
        bad = worker(bad_subscription, BAD_SLOT, BAD_PUB, BAD, BAD, False)
        check("incompatible pre-created destination is rejected",
              bad.returncode != 0 and "destination schema mismatch" in bad.stderr, True)
        check("schema rejection occurs before snapshot staging or checkpoints",
              value("analytics", f"""
                SELECT (SELECT count(*) FROM lake_replication.initial_copies
                         WHERE subscription_id={bad_subscription}) || ':' ||
                       received_lsn || ':' || staged_lsn
                  FROM lake_replication.checkpoints
                 WHERE subscription_id={bad_subscription}
              """), "0:0/0:0/0")
        check("rejected destination retains its prior data",
              value("analytics", f"SELECT string_agg(id || ':' || note, ',') FROM {RUN}.{BAD}"),
              "old:still-readable")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'mapping_count', (SELECT count(*) FROM lake_replication.type_mappings),
                'good_contract', (SELECT to_jsonb(t) FROM lake_replication.relation_type_contracts t
                                   WHERE subscription_id={good_subscription}),
                'good_rows', (SELECT jsonb_agg(to_jsonb(g) ORDER BY id) FROM {RUN}.{GOOD} g),
                'bad_checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
                                   WHERE subscription_id={bad_subscription}),
                'bad_rows', (SELECT jsonb_agg(to_jsonb(b)) FROM {RUN}.{BAD} b))
        """))
        EVIDENCE["status"] = "passed"
        exit_code = 0
    except Exception as exc:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        cleanup(subscriptions)
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

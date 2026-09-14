#!/usr/bin/env python3
"""Additive schema evolution and incompatible-change pause probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "pg-lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("evolution_%Y%m%d_%H%M%S")
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
        "LAKE_TYPE_MAPPING_VERSION": "1",
    })
    result.update(settings)
    return result


def worker(binary, subscription_id, check_result=True, **settings):
    result = command([str(ROOT / "target/debug" / binary)],
                     env=environment(subscription_id, **settings))
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


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
            DELETE FROM lake_replication.relation_type_contracts
             WHERE subscription_id={subscription_id};
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
            INSERT INTO public.{TABLE} VALUES (1, '10'), (2, '20');
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE}
        """)
        subscription_id = int(value("analytics", f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """))
        value("analytics", f"CREATE SCHEMA {RUN}")
        worker("lake-replication-initial-sync", subscription_id,
               LAKE_CREATE_TABLES_USING_ICEBERG="1")
        check("initial schema starts at one assigned version",
              value("analytics", f"""
                SELECT count(*) || ':' || max(schema_version) || ':' ||
                       count(*) FILTER (WHERE destination_relation IS NOT NULL)
                  FROM lake_replication.relations WHERE subscription_id={subscription_id}
              """), "1:1:1")

        value("application", f"""
            ALTER TABLE public.{TABLE} ADD COLUMN extra text;
            BEGIN;
            UPDATE public.{TABLE} SET note='11', extra='added-online' WHERE id=1;
            INSERT INTO public.{TABLE} VALUES (3, '30', 'new-row');
            COMMIT
        """)
        receiver = worker("lake-replication-receiver", subscription_id,
                          LAKE_MAX_TRANSACTIONS="1")
        check("receiver stages the transaction after additive reconciliation",
              receiver.stdout.startswith("staged xid=") and "changes=2" in receiver.stdout,
              True)
        check("nullable appended column creates one ready assigned version",
              value("analytics", f"""
                SELECT count(*) || ':' || max(schema_version) || ':' ||
                       count(*) FILTER (WHERE schema_version=2 AND state='ready'
                                        AND destination_relation IS NOT NULL)
                  FROM lake_replication.relations WHERE subscription_id={subscription_id}
              """), "2:2:1")
        check("Iceberg destination gains the mapped nullable column",
              value("analytics", f"""
                SELECT attname || ':' || format_type(atttypid,atttypmod) || ':' || attnotnull
                  FROM pg_attribute WHERE attrelid='{RUN}.target'::regclass
                   AND attname='extra'
              """), "extra:text:false")
        worker("lake-replication-publisher", subscription_id,
               LAKE_BATCH_MAX_TRANSACTIONS="10", LAKE_BATCH_MAX_ROWS="100",
               LAKE_BATCH_MAX_BYTES="8388608")
        check("additive change and new rows publish with old rows preserved",
              value("analytics", f"""
                SELECT string_agg(id || ':' || note || ':' || coalesce(extra,'NULL'),
                                  ',' ORDER BY id) FROM {RUN}.target
              """), "1:11:added-online,2:20:NULL,3:30:new-row")
        safe_checkpoint = value("analytics", f"""
            SELECT staged_lsn FROM lake_replication.checkpoints
             WHERE subscription_id={subscription_id}
        """)

        value("application", f"""
            ALTER TABLE public.{TABLE} ALTER COLUMN note TYPE bigint USING note::bigint;
            INSERT INTO public.{TABLE} VALUES (4, 40, 'must-wait')
        """)
        rejected = worker("lake-replication-receiver", subscription_id,
                          check_result=False, LAKE_MAX_TRANSACTIONS="1")
        check("incompatible type change stops the receiver",
              rejected.returncode != 0 and "changed name, order, or type" in rejected.stderr,
              True)
        check("incompatible relation version is durably paused with diagnostics",
              value("analytics", f"""
                SELECT s.state || ':' || r.state || ':' ||
                       (s.last_error LIKE '%changed name, order, or type%')::text
                  FROM lake_replication.subscriptions s
                  JOIN lake_replication.relations r USING(subscription_id)
                 WHERE s.subscription_id={subscription_id} AND r.schema_version=3
              """), "paused:paused:true")
        check("unsafe transaction is neither staged nor checkpointed",
              value("analytics", f"""
                SELECT count(*) || ':' || staged_lsn
                  FROM lake_replication.transactions t
                  JOIN lake_replication.checkpoints c USING(subscription_id)
                 WHERE t.subscription_id={subscription_id}
                 GROUP BY staged_lsn
              """), f"1:{safe_checkpoint}")
        check("source slot remains at the last durable safe position",
              value("application", f"""
                SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{SLOT}'
              """), safe_checkpoint)
        check("prior Iceberg schema and data remain readable after pause",
              value("analytics", f"""
                SELECT format_type(a.atttypid,a.atttypmod) || ':' ||
                       (SELECT string_agg(id || ':' || note || ':' || coalesce(extra,'NULL'),
                                          ',' ORDER BY id) FROM {RUN}.target)
                  FROM pg_attribute a WHERE a.attrelid='{RUN}.target'::regclass
                   AND a.attname='note'
              """), "text:1:11:added-online,2:20:NULL,3:30:new-row")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'relations', (SELECT jsonb_agg(to_jsonb(r) ORDER BY schema_version)
                               FROM lake_replication.relations r
                              WHERE subscription_id={subscription_id}),
                'contracts', (SELECT jsonb_agg(to_jsonb(t) ORDER BY relation_schema_version)
                               FROM lake_replication.relation_type_contracts t
                              WHERE subscription_id={subscription_id}),
                'checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
                                WHERE subscription_id={subscription_id}),
                'subscription', (SELECT to_jsonb(s) FROM lake_replication.subscriptions s
                                  WHERE subscription_id={subscription_id}),
                'target', (SELECT jsonb_agg(to_jsonb(d) ORDER BY id) FROM {RUN}.target d))
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

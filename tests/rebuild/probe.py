#!/usr/bin/env python3
"""Paused-subscription replacement snapshot, catch-up, and promotion probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "pg-lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("rebuild_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION = RUN, f"{RUN}_pub"
OLD_SLOT, NEW_SLOT = f"{RUN}_old", f"{RUN}_new"
SHADOW, BACKUP = "__rebuild", "__backup"


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


def environment(subscription_id, slot, suffix="", **settings):
    result = os.environ.copy()
    result.update({
        "LAKE_SLOT": slot,
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_DISCOVER_PUBLICATION": "1",
        "LAKE_DESTINATION_SCHEMA": RUN,
        "LAKE_DESTINATION_TABLE_SUFFIX": suffix,
        "LAKE_CREATE_TABLES_USING_ICEBERG": "1",
        "LAKE_TYPE_MAPPING_VERSION": "1",
    })
    result.update(settings)
    return result


def worker(binary, subscription_id, slot, suffix="", check_result=True, **settings):
    result = command([str(ROOT / "target/debug" / binary)],
                     env=environment(subscription_id, slot, suffix, **settings))
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def publish(subscription_id, slot, suffix=""):
    return worker("lake-replication-publisher", subscription_id, slot, suffix,
                  LAKE_BATCH_MAX_TRANSACTIONS="10", LAKE_BATCH_MAX_ROWS="100",
                  LAKE_BATCH_MAX_BYTES="8388608")


def cleanup(subscriptions):
    sql("application", f"""
        SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
         WHERE slot_name IN ('{OLD_SLOT}', '{NEW_SLOT}');
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS public.{TABLE};
    """, check_result=False)
    sql("analytics", f"DROP SCHEMA IF EXISTS {RUN} CASCADE", check_result=False)
    if subscriptions:
        ids = ",".join(str(item) for item in subscriptions)
        sql("analytics", f"""
            DELETE FROM lake_replication.rebuild_promotions
             WHERE old_subscription_id IN ({ids}) OR replacement_subscription_id IN ({ids});
            DELETE FROM lake_replication.changes WHERE transaction_id IN
                (SELECT transaction_id FROM lake_replication.transactions
                  WHERE subscription_id IN ({ids}));
            DELETE FROM lake_replication.transactions WHERE subscription_id IN ({ids});
            DELETE FROM lake_replication.batches WHERE subscription_id IN ({ids});
            DELETE FROM lake_replication.initial_copies WHERE subscription_id IN ({ids});
            DELETE FROM lake_replication.relation_type_contracts WHERE subscription_id IN ({ids});
            DELETE FROM lake_replication.relations WHERE subscription_id IN ({ids});
            DELETE FROM lake_replication.checkpoints WHERE subscription_id IN ({ids});
            DELETE FROM lake_replication.subscriptions WHERE subscription_id IN ({ids});
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
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
            INSERT INTO public.{TABLE} VALUES (1, '10'), (2, '20');
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE}
        """)
        value("analytics", f"CREATE SCHEMA {RUN}")

        def subscription(name, slot):
            result = int(value("analytics", f"""
                SELECT lake_replication.create_subscription(
                    {identity['system_identifier']}, {identity['database_oid']},
                    {identity['timeline']}, '{name}', '{slot}', ARRAY['{PUBLICATION}'])
            """))
            subscriptions.append(result)
            return result

        old_subscription = subscription(f"{RUN}_old", OLD_SLOT)
        worker("lake-replication-initial-sync", old_subscription, OLD_SLOT)
        old_baseline = value("analytics", f"""
            SELECT staged_lsn FROM lake_replication.checkpoints
             WHERE subscription_id={old_subscription}
        """)
        value("application", f"""
            ALTER TABLE public.{TABLE} ALTER COLUMN note TYPE bigint USING note::bigint;
            INSERT INTO public.{TABLE} VALUES (3, 30)
        """)
        paused = worker("lake-replication-receiver", old_subscription, OLD_SLOT,
                        check_result=False, LAKE_MAX_TRANSACTIONS="1")
        check("incompatible source schema pauses the original subscription",
              paused.returncode != 0 and value("analytics", f"""
                SELECT state FROM lake_replication.subscriptions
                 WHERE subscription_id={old_subscription}
              """) == "paused", True)
        check("old slot retains the incompatible transaction",
              value("application", f"""
                SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{OLD_SLOT}'
              """), old_baseline)
        check("old Iceberg table remains readable during rebuild",
              value("analytics", f"""
                SELECT format_type(a.atttypid,a.atttypmod) || ':' ||
                       (SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE})
                  FROM pg_attribute a WHERE a.attrelid='{RUN}.{TABLE}'::regclass
                   AND a.attname='note'
              """), "text:1:10,2:20")

        replacement = subscription(f"{RUN}_replacement", NEW_SLOT)
        worker("lake-replication-initial-sync", replacement, NEW_SLOT, SHADOW)
        check("replacement snapshot contains the formerly unsafe source row",
              value("analytics", f"""
                SELECT format_type(a.atttypid,a.atttypmod) || ':' ||
                       (SELECT string_agg(id || ':' || note, ',' ORDER BY id)
                          FROM {RUN}.{TABLE}{SHADOW})
                  FROM pg_attribute a
                 WHERE a.attrelid='{RUN}.{TABLE}{SHADOW}'::regclass AND a.attname='note'
              """), "bigint:1:10,2:20,3:30")
        check("original table is unchanged after replacement snapshot completes",
              value("analytics", f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}"),
              "1:10,2:20")

        value("application", f"INSERT INTO public.{TABLE} VALUES (4, 40)")
        worker("lake-replication-receiver", replacement, NEW_SLOT, SHADOW,
               LAKE_MAX_TRANSACTIONS="1")
        publish(replacement, NEW_SLOT, SHADOW)
        check("replacement slot catches up before promotion",
              value("analytics", f"""
                SELECT string_agg(id || ':' || note, ',' ORDER BY id)
                  FROM {RUN}.{TABLE}{SHADOW}
              """), "1:10,2:20,3:30,4:40")

        promoted = value("analytics", f"""
            SELECT lake_replication.promote_subscription_rebuild(
                {old_subscription}, {replacement}, '{RUN}', '{SHADOW}', '{BACKUP}')
        """)
        check("one rebuilt relation is promoted atomically", promoted, "1")
        check("canonical table now exposes rebuilt schema and caught-up rows",
              value("analytics", f"""
                SELECT format_type(a.atttypid,a.atttypmod) || ':' ||
                       (SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE})
                  FROM pg_attribute a WHERE a.attrelid='{RUN}.{TABLE}'::regclass
                   AND a.attname='note'
              """), "bigint:1:10,2:20,3:30,4:40")
        check("previous Iceberg state is retained under the backup name",
              value("analytics", f"""
                SELECT format_type(a.atttypid,a.atttypmod) || ':' ||
                       (SELECT string_agg(id || ':' || note, ',' ORDER BY id)
                          FROM {RUN}.{TABLE}{BACKUP})
                  FROM pg_attribute a
                 WHERE a.attrelid='{RUN}.{TABLE}{BACKUP}'::regclass AND a.attname='note'
              """), "text:1:10,2:20")
        check("promotion records slot handoff and disables only the old subscription",
              value("analytics", f"""
                SELECT o.state || ':' || n.state || ':' || p.old_slot_name || ':' ||
                       p.replacement_slot_name
                  FROM lake_replication.rebuild_promotions p
                  JOIN lake_replication.subscriptions o
                    ON o.subscription_id=p.old_subscription_id
                  JOIN lake_replication.subscriptions n
                    ON n.subscription_id=p.replacement_subscription_id
                 WHERE p.old_subscription_id={old_subscription}
              """), f"disabled:streaming:{OLD_SLOT}:{NEW_SLOT}")

        value("application", f"INSERT INTO public.{TABLE} VALUES (5, 50)")
        worker("lake-replication-receiver", replacement, NEW_SLOT,
               LAKE_MAX_TRANSACTIONS="1")
        publish(replacement, NEW_SLOT)
        check("replacement subscription continues against the promoted table",
              value("analytics", f"""
                SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}
              """), "1:10,2:20,3:30,4:40,5:50")
        check("old retained slot is explicitly removable after promotion",
              value("application", f"""
                SELECT pg_drop_replication_slot('{OLD_SLOT}');
                SELECT count(*) FROM pg_replication_slots WHERE slot_name='{OLD_SLOT}'
              """), "0")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'promotion', (SELECT to_jsonb(p) FROM lake_replication.rebuild_promotions p
                               WHERE old_subscription_id={old_subscription}),
                'old_subscription', (SELECT to_jsonb(s) FROM lake_replication.subscriptions s
                                     WHERE subscription_id={old_subscription}),
                'replacement_subscription', (SELECT to_jsonb(s) FROM lake_replication.subscriptions s
                                             WHERE subscription_id={replacement}),
                'replacement_checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
                                           WHERE subscription_id={replacement}),
                'target', (SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {RUN}.{TABLE} t),
                'backup', (SELECT jsonb_agg(to_jsonb(b) ORDER BY id)
                            FROM {RUN}.{TABLE}{BACKUP} b))
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

#!/usr/bin/env python3
"""Live pgoutput transport, durable feedback, and restart probe."""
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
RUN = datetime.now(timezone.utc).strftime("transport_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE = RUN
PUBLICATION = f"{RUN}_pub"
SLOT = f"{RUN}_slot"


def command(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True,
                          timeout=30, **kwargs)


def sql(database, statement, *, check_result=True):
    result = command(COMPOSE + ["exec", "-T", "-e", "PGCONNECT_TIMEOUT=2",
                     "postgres", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                     "-U", "postgres", "-d", database], input=statement)
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


def receive(subscription_id):
    environment = os.environ.copy()
    environment.update({
        "LAKE_SLOT": SLOT,
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_MAX_TRANSACTIONS": "1",
    })
    result = command([str(ROOT / "target/debug/lake-replication-receiver")],
                     env=environment)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def wait_for_slot_lsn(expected):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        actual = value("application", f"""
            SELECT confirmed_flush_lsn FROM pg_replication_slots
             WHERE slot_name='{SLOT}'
        """)
        if actual == expected:
            return actual
        time.sleep(0.05)
    return actual


def cleanup(subscription_id):
    sql("application", f"""
        SELECT pg_drop_replication_slot(slot_name)
          FROM pg_replication_slots WHERE slot_name='{SLOT}';
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS public.{TABLE};
    """, check_result=False)
    if subscription_id is not None:
        sql("analytics", f"""
            DELETE FROM lake_replication.changes WHERE transaction_id IN
                (SELECT transaction_id FROM lake_replication.transactions
                  WHERE subscription_id={subscription_id});
            DELETE FROM lake_replication.transactions WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.relations WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.checkpoints WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.subscriptions WHERE subscription_id={subscription_id};
        """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscription_id = None
    exit_code = 1
    try:
        build = command(["cargo", "build", "--quiet", "--bin",
                         "lake-replication-receiver"])
        if build.returncode:
            raise RuntimeError(build.stderr.strip())
        identity = json.loads(value("application", """
            SELECT json_build_object(
                'system_identifier', (SELECT system_identifier FROM pg_control_system()),
                'database_oid', (SELECT oid FROM pg_database WHERE datname=current_database()),
                'timeline', timeline_id)
            FROM pg_control_checkpoint();
        """))
        value("application", f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};
        """)
        value("application", f"""
            SELECT slot_name FROM pg_create_logical_replication_slot('{SLOT}', 'pgoutput');
        """)
        subscription_id = int(value("analytics", f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}']);
        """))

        value("application", f"""
            INSERT INTO public.{TABLE} VALUES (1, 'first O''Reilly \\ value');
        """)
        first_output = receive(subscription_id)
        check("first live transaction decoded and staged",
              first_output.startswith("staged xid=") and "changes=1" in first_output,
              True)
        first_checkpoint = value("analytics", f"""
            SELECT staged_lsn FROM lake_replication.checkpoints
             WHERE subscription_id={subscription_id}
        """)
        check("source flush feedback follows durable staging",
              wait_for_slot_lsn(first_checkpoint), first_checkpoint)
        check("received, staged, and acknowledged positions converge",
              value("analytics", f"""
                SELECT received_lsn || ':' || staged_lsn || ':' || acknowledged_lsn
                  FROM lake_replication.checkpoints WHERE subscription_id={subscription_id}
              """), f"{first_checkpoint}:{first_checkpoint}:{first_checkpoint}")
        check("live relation metadata registered",
              value("analytics", f"""
                SELECT count(*) || ':' || max(schema_version)
                  FROM lake_replication.relations WHERE subscription_id={subscription_id}
              """), "1:1")
        check("text tuple survives JSON and SQL escaping",
              value("analytics", f"""
                SELECT payload->0->'new_tuple'->>'note'
                  FROM lake_replication.transactions WHERE subscription_id={subscription_id}
              """), "first O'Reilly \\ value")

        value("application", f"""
            BEGIN;
            INSERT INTO public.{TABLE} VALUES (2, 'restart-a');
            INSERT INTO public.{TABLE} VALUES (3, 'restart-b');
            COMMIT;
        """)
        second_output = receive(subscription_id)
        check("fresh process resumes with a complete source transaction",
              second_output.startswith("staged xid=") and "changes=2" in second_output,
              True)
        final_checkpoint = value("analytics", f"""
            SELECT staged_lsn FROM lake_replication.checkpoints
             WHERE subscription_id={subscription_id}
        """)
        check("restart feedback advances the source slot",
              wait_for_slot_lsn(final_checkpoint), final_checkpoint)
        check("restart creates no duplicate relation schema version",
              value("analytics", f"""
                SELECT count(*) || ':' || max(schema_version)
                  FROM lake_replication.relations WHERE subscription_id={subscription_id}
              """), "1:1")
        check("transaction boundaries and insert order retained",
              value("analytics", f"""
                SELECT (SELECT count(*) FROM lake_replication.transactions
                         WHERE subscription_id={subscription_id}) || ':' ||
                       (SELECT count(*) FROM lake_replication.changes c
                         JOIN lake_replication.transactions t USING (transaction_id)
                        WHERE t.subscription_id={subscription_id}) || ':' ||
                       string_agg(c.new_tuple->>'id', ',' ORDER BY t.end_lsn, c.change_index)
                  FROM lake_replication.changes c
                  JOIN lake_replication.transactions t USING (transaction_id)
                 WHERE t.subscription_id={subscription_id}
              """), "2:3:1,2,3")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'transactions', (SELECT count(*) FROM lake_replication.transactions
                                  WHERE subscription_id={subscription_id}),
                'changes', (SELECT count(*) FROM lake_replication.changes c JOIN
                             lake_replication.transactions t USING (transaction_id)
                             WHERE t.subscription_id={subscription_id}),
                'checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
                                WHERE subscription_id={subscription_id}),
                'slot_flush_lsn', (SELECT staged_lsn FROM lake_replication.checkpoints
                                   WHERE subscription_id={subscription_id}));
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

#!/usr/bin/env python3
"""Protocol-v4 streamed commit and abort integration probe."""
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
RUN = datetime.now(timezone.utc).strftime("streaming_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"


def command(args, timeout=60, **kwargs):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True,
                          timeout=timeout, **kwargs)


def psql_args(database):
    return COMPOSE + ["exec", "-T", "postgres", "psql", "-XAt",
                      "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database]


def sql(database, statement, *, check_result=True):
    result = command(psql_args(database), input=statement)
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


def worker_environment(subscription_id):
    environment = os.environ.copy()
    environment.update({
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_SLOT": SLOT,
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_MAX_TRANSACTIONS": "1",
        "LAKE_DEBUG_STREAMING_IMMEDIATE": "1",
    })
    return environment


def start_receiver(subscription_id):
    return subprocess.Popen(
        [str(ROOT / "target/debug/lake-replication-receiver")], cwd=ROOT,
        env=worker_environment(subscription_id), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def finish(process, timeout=60):
    stdout, stderr = process.communicate(timeout=timeout)
    if process.returncode:
        raise RuntimeError(stderr.strip())
    return stdout.strip()


def publish(subscription_id):
    environment = os.environ.copy()
    environment.update({
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_BATCH_MAX_TRANSACTIONS": "10",
        "LAKE_BATCH_MAX_ROWS": "1000",
        "LAKE_BATCH_MAX_BYTES": "8388608",
    })
    result = command([str(ROOT / "target/debug/lake-replication-publisher")],
                     timeout=120, env=environment)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


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
    receiver = None
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
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, payload text NOT NULL);
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
            CREATE TABLE {RUN}.target (id bigint, payload text) USING iceberg
        """)

        receiver = start_receiver(subscription_id)
        time.sleep(0.5)
        source = subprocess.Popen(psql_args("application") + ["-c", f"""
            BEGIN;
            INSERT INTO public.{TABLE}
                SELECT g, repeat('x', 2000) || g FROM generate_series(1, 60) g;
            SELECT pg_sleep(1);
            INSERT INTO public.{TABLE}
                SELECT g, repeat('y', 2000) || g FROM generate_series(61, 120) g;
            SELECT pg_sleep(2);
            COMMIT;
        """], cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(1.25)
        check("stream fragments are not staged before source commit",
              value("analytics", f"""
                SELECT count(t.transaction_id) || ':' || staged_lsn || ':' ||
                       ((SELECT count(*) FROM lake_replication.streamed_changes
                          WHERE subscription_id={subscription_id}) > 0)::text
                  FROM lake_replication.transactions t
                RIGHT JOIN lake_replication.checkpoints c USING (subscription_id)
                WHERE c.subscription_id={subscription_id} GROUP BY staged_lsn
              """), "0:0/0:true")
        receiver.terminate()
        receiver.communicate(timeout=5)
        receiver = None
        check("durable fragments survive receiver loss",
              int(value("analytics", f"""
                SELECT count(*) FROM lake_replication.streamed_changes
                 WHERE subscription_id={subscription_id}
              """)) > 0, True)
        source_stdout, source_stderr = source.communicate(timeout=30)
        if source.returncode:
            raise RuntimeError(source_stderr.strip() or source_stdout.strip())
        receiver = start_receiver(subscription_id)
        committed_output = finish(receiver)
        receiver = None
        EVIDENCE["stream_segment_count"] = committed_output.count("stream segment xid=")
        check("large transaction used streamed delivery",
              committed_output.count("stream segment xid=") >= 1, True)
        check("stream commit releases one complete transaction",
              "staged streamed xid=" in committed_output and "changes=120" in committed_output,
              True)
        check("committed stream stored all changes atomically",
              value("analytics", f"""
                SELECT count(*) || ':' || count(DISTINCT t.transaction_id)
                  FROM lake_replication.changes c JOIN lake_replication.transactions t
                    USING (transaction_id) WHERE t.subscription_id={subscription_id}
              """), "120:1")
        check("committed stream removes durable fragments",
              value("analytics", f"""
                SELECT count(*) FROM lake_replication.streamed_transactions
                 WHERE subscription_id={subscription_id}
              """), "0")

        receiver = start_receiver(subscription_id)
        time.sleep(0.5)
        value("application", f"""
            BEGIN;
            INSERT INTO public.{TABLE}
                SELECT g, repeat('aborted', 400) FROM generate_series(1001, 1100) g;
            SELECT pg_sleep(1);
            ROLLBACK
        """)
        value("application", f"INSERT INTO public.{TABLE} VALUES (121, 'after-abort')")
        aborted_output = finish(receiver)
        receiver = None
        check("stream abort is observed and discarded",
              "discarded streamed xid=" in aborted_output, True)
        check("stream abort removes durable fragments",
              value("analytics", f"""
                SELECT count(*) FROM lake_replication.streamed_transactions
                 WHERE subscription_id={subscription_id}
              """), "0")
        check("aborted rows never enter the journal",
              value("analytics", f"""
                SELECT count(*) FILTER (WHERE (new_tuple->>'id')::bigint >= 1000) || ':' ||
                       count(*) FILTER (WHERE new_tuple->>'id'='121')
                  FROM lake_replication.changes c JOIN lake_replication.transactions t
                    USING (transaction_id) WHERE t.subscription_id={subscription_id}
              """), "0:1")

        relation = value("analytics", f"""
            SELECT remote_relation_id || ':' || schema_version FROM lake_replication.relations
             WHERE subscription_id={subscription_id} ORDER BY schema_version DESC LIMIT 1
        """).split(":")
        value("analytics", f"""
            SELECT lake_replication.assign_destination(
                {subscription_id}, {relation[0]}, {relation[1]}, '{RUN}.target'::regclass)
        """)
        publisher_output = publish(subscription_id)
        check("streamed and post-abort transactions publish together",
              "transactions=2" in publisher_output and "rows=121/121" in publisher_output,
              True)
        check("Iceberg contains committed rows only",
              value("analytics", f"""
                SELECT count(*) || ':' || count(*) FILTER (WHERE id>=1000) || ':' ||
                       count(*) FILTER (WHERE id=121 AND payload='after-abort')
                  FROM {RUN}.target
              """), "121:0:1")
        check("feedback and publication converge after stream commit",
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
                'target_rows', (SELECT count(*) FROM {RUN}.target),
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
        if receiver is not None and receiver.poll() is None:
            receiver.terminate()
            receiver.communicate(timeout=5)
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        cleanup(subscription_id)
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

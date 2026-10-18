#!/usr/bin/env python3
"""Publication discovery, filters, columns, and atomic multi-table cutover."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
DEST_DB = os.environ.get("LAKE_TEST_DESTINATION_DATABASE", "analytics")
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("multi_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
ACCOUNTS, EVENTS = f"{RUN}_accounts", f"{RUN}_events"
PUBLICATION, SLOT = f"{RUN}_pub", f"{RUN}_slot"


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


def environment(subscription_id, **settings):
    result = os.environ.copy()
    result.update({
        "LAKE_SLOT": SLOT,
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_DISCOVER_PUBLICATION": "1",
        "LAKE_DESTINATION_SCHEMA": RUN,
        "LAKE_DESTINATION_DATABASE": DEST_DB,
    })
    result.update(settings)
    return result


def worker(binary, subscription_id, **settings):
    result = command([str(ROOT / "target/debug" / binary)],
                     env=environment(subscription_id, **settings))
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def wait_for(statement, expected, timeout=8):
    deadline = time.monotonic() + timeout
    actual = None
    while time.monotonic() < deadline:
        actual = value("analytics", statement)
        if actual == expected:
            return actual
        time.sleep(0.05)
    return actual


def cleanup(subscription_id):
    sql("application", f"""
        SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
         WHERE slot_name='{SLOT}';
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS public.{ACCOUNTS}, public.{EVENTS};
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
            CREATE TABLE public.{ACCOUNTS} (
                id bigint PRIMARY KEY, tenant_id bigint NOT NULL,
                note text NOT NULL, secret text NOT NULL);
            CREATE TABLE public.{EVENTS} (id bigint PRIMARY KEY, note text NOT NULL);
            INSERT INTO public.{ACCOUNTS} VALUES
                (1, 1, 'visible-a', 'hidden-a'),
                (2, 2, 'filtered', 'hidden-b'),
                (3, 1, 'visible-c', 'hidden-c');
            INSERT INTO public.{EVENTS} VALUES (10, 'event-a'), (11, 'event-b');
            CREATE PUBLICATION {PUBLICATION}
              FOR TABLE public.{ACCOUNTS} (id, note) WHERE (tenant_id = 1),
                        public.{EVENTS}
              WITH (publish = 'insert')
        """)
        check("PostgreSQL exposes two effective publication tables",
              value("application", f"""
                SELECT count(*) || ':' || count(*) FILTER (WHERE rowfilter IS NOT NULL) || ':' ||
                       count(*) FILTER (WHERE cardinality(attnames)=2)
                  FROM pg_publication_tables WHERE pubname='{PUBLICATION}'
              """), "2:1:2")
        subscription_id = int(value("analytics", f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """))
        value("analytics", f"""
            CREATE SCHEMA {RUN};
            CREATE TABLE {RUN}.{ACCOUNTS} (id bigint, note text) USING iceberg;
            CREATE TABLE {RUN}.{EVENTS} (id bigint, note text) USING iceberg;
            INSERT INTO {RUN}.{ACCOUNTS} VALUES (-1, 'old-account');
            INSERT INTO {RUN}.{EVENTS} VALUES (-1, 'old-event')
        """)

        process = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-initial-sync")],
            cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=environment(subscription_id,
                            LAKE_OVERWRITE_DESTINATION="1",
                            LAKE_INITIAL_COPY_CHUNK_ROWS="1",
                            LAKE_INITIAL_COPY_FAIL_ONCE_AFTER_ROWS="1",
                            LAKE_INITIAL_COPY_CHUNK_PAUSE_MS="250",
                            LAKE_INITIAL_COPY_PAUSE_MS="1200"))
        check("both discovered tables enter copying under one snapshot",
              wait_for(f"""
                SELECT count(*) || ':' || count(DISTINCT snapshot_name) || ':' ||
                       count(DISTINCT consistent_lsn)
                  FROM lake_replication.initial_copies
                 WHERE subscription_id={subscription_id} AND state='copying'
              """, "2:1:1"), "2:1:1")
        check("both previous Iceberg tables remain readable before cutover",
              value("analytics", f"""
                SELECT (SELECT string_agg(id::text || ':' || note, ',')
                          FROM {RUN}.{ACCOUNTS}) || '|' ||
                       (SELECT string_agg(id::text || ':' || note, ',')
                          FROM {RUN}.{EVENTS})
              """), "-1:old-account|-1:old-event")

        value("application", f"""
            BEGIN;
            INSERT INTO public.{ACCOUNTS} VALUES
                (4, 1, 'caught-account', 'hidden-d'),
                (5, 2, 'not-published', 'hidden-e');
            INSERT INTO public.{EVENTS} VALUES (12, 'caught-event');
            COMMIT
        """)
        stdout, stderr = process.communicate(timeout=30)
        if process.returncode:
            raise RuntimeError(stderr.strip())
        check("one worker discovers and copies both tables",
              "initial copy ready tables=2 rows=4/4" in stdout, True)
        check("one table retry preserves the shared snapshot and completes both copies",
              '"event":"table_copy_retry"' in stderr, True)
        check("row filter and column list shape the snapshot",
              value("analytics", f"""
                SELECT (SELECT string_agg(id::text || ':' || note, ',' ORDER BY id)
                          FROM {RUN}.{ACCOUNTS}) || '|' ||
                       (SELECT string_agg(id::text || ':' || note, ',' ORDER BY id)
                          FROM {RUN}.{EVENTS})
              """), "1:visible-a,3:visible-c|10:event-a,11:event-b")
        check("both copies become ready at one checkpoint boundary",
              value("analytics", f"""
                SELECT count(*) || ':' || count(*) FILTER (WHERE state='ready') || ':' ||
                       count(DISTINCT consistent_lsn) || ':' ||
                       ((SELECT staged_lsn FROM lake_replication.checkpoints
                          WHERE subscription_id={subscription_id}) = min(consistent_lsn))::text
                  FROM lake_replication.initial_copies
                 WHERE subscription_id={subscription_id}
              """), "2:2:1:true")
        check("discovered relation metadata contains only published columns",
              value("analytics", f"""
                SELECT count(*) || ':' ||
                       count(*) FILTER (WHERE jsonb_array_length(columns)=2) || ':' ||
                       count(*) FILTER (WHERE columns::text LIKE '%tenant_id%'
                                             OR columns::text LIKE '%secret%')
                  FROM lake_replication.relations WHERE subscription_id={subscription_id}
              """), "2:2:0")

        receiver = worker("lake-replication-receiver", subscription_id,
                          LAKE_MAX_TRANSACTIONS="1")
        check("one concurrent transaction emits only the two eligible changes",
              '"event":"transaction_staged"' in receiver
              and "staged xid=" in receiver and "changes=2" in receiver, True)
        worker("lake-replication-publisher", subscription_id,
               LAKE_BATCH_MAX_TRANSACTIONS="10", LAKE_BATCH_MAX_ROWS="100",
               LAKE_BATCH_MAX_BYTES="8388608")
        check("filtered/projected WAL catch-up matches effective publication data",
              value("analytics", f"""
                SELECT (SELECT string_agg(id::text || ':' || note, ',' ORDER BY id)
                          FROM {RUN}.{ACCOUNTS}) || '|' ||
                       (SELECT string_agg(id::text || ':' || note, ',' ORDER BY id)
                          FROM {RUN}.{EVENTS})
              """),
              "1:visible-a,3:visible-c,4:caught-account|10:event-a,11:event-b,12:caught-event")
        check("filtered-out source identities never reach Iceberg",
              value("analytics", f"""
                SELECT count(*) FROM {RUN}.{ACCOUNTS} WHERE id IN (2,5)
              """), "0")
        check("multi-relation source transaction remains one journal transaction",
              value("analytics", f"""
                SELECT count(*) || ':' ||
                       (SELECT count(*) FROM lake_replication.changes c
                         JOIN lake_replication.transactions t USING(transaction_id)
                        WHERE t.subscription_id={subscription_id})
                  FROM lake_replication.transactions
                 WHERE subscription_id={subscription_id}
              """), "1:2")

        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'copies', (SELECT jsonb_agg(to_jsonb(i) ORDER BY remote_relation_id)
                             FROM lake_replication.initial_copies i
                            WHERE subscription_id={subscription_id}),
                'checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
                                WHERE subscription_id={subscription_id}),
                'accounts', (SELECT jsonb_agg(to_jsonb(a) ORDER BY id)
                              FROM {RUN}.{ACCOUNTS} a),
                'events', (SELECT jsonb_agg(to_jsonb(e) ORDER BY id)
                            FROM {RUN}.{EVENTS} e))
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

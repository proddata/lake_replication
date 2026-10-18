#!/usr/bin/env python3
"""Milestone 1 journal and atomic local-catalog publication probe."""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("journal_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}


def command(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=30, **kwargs)


def sql(database, statement, *, check_result=True):
    result = command(COMPOSE + ["exec", "-T", "-e", "PGCONNECT_TIMEOUT=2",
                     "postgres", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                     "-v", "VERBOSITY=verbose", "-U", "postgres", "-d", database],
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


def wait_ready():
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if value("analytics", "SELECT 1") == "1":
                return
        except (RuntimeError, subprocess.TimeoutExpired):
            pass
        time.sleep(0.25)
    raise RuntimeError("PostgreSQL did not recover after the journal commit crash")


def payload(row_id):
    return json.dumps([{
        "operation": "insert",
        "remote_relation_id": 1,
        "relation_schema_version": 1,
        "new_tuple": {"id": str(row_id), "value": f"row-{row_id}"},
    }], separators=(",", ":"))


def stage(subscription_id, xid, begin_lsn, commit_lsn, end_lsn, row_id,
          commit_time="2026-01-01 00:00:00+00", *, check_result=True):
    statement = f"""
        SELECT lake_replication.stage_transaction(
            {subscription_id}, {xid}, '{begin_lsn}', '{commit_lsn}', '{end_lsn}',
            '{commit_time}', '{payload(row_id)}'::jsonb);
    """
    return sql("analytics", statement, check_result=check_result)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    exit_code = 1
    try:
        identity = json.loads(value("application", """
            SELECT json_build_object(
                'system_identifier', (SELECT system_identifier FROM pg_control_system()),
                'database_oid', (SELECT oid FROM pg_database WHERE datname=current_database()),
                'timeline', timeline_id)
            FROM pg_control_checkpoint();
        """))
        subscription_id = int(value("analytics", f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{RUN}_slot', ARRAY['{RUN}_publication']);
        """))
        EVIDENCE["subscription_id"] = subscription_id
        value("analytics", fr"""
            CREATE TABLE {RUN}_target (id bigint, value text) USING iceberg;
            INSERT INTO lake_replication.relations (
                subscription_id, remote_relation_id, schema_version,
                source_namespace, source_relation, replica_identity, columns,
                destination_relation
            ) VALUES (
                {subscription_id}, 1, 1, 'public', 'source', 'd',
                '[{{"name":"id","type_oid":20}},{{"name":"value","type_oid":25}}]',
                '{RUN}_target'::regclass);
        """)

        check("first transaction staged",
              stage(subscription_id, 101, "0/100", "0/110", "0/120", 1).stdout.strip(),
              "t")
        check("identical replay is idempotent",
              stage(subscription_id, 101, "0/100", "0/110", "0/120", 1).stdout.strip(),
              "f")
        conflict = stage(subscription_id, 999, "0/100", "0/110", "0/120", 9,
                         check_result=False)
        check("conflicting replay fails closed",
              conflict.returncode != 0 and "23505" in conflict.stderr, True)
        unknown_relation = sql("analytics", f"""
            SELECT lake_replication.stage_transaction(
                {subscription_id}, 102, '0/121', '0/122', '0/123',
                '2026-01-01 00:00:01+00',
                '[{{"operation":"insert","remote_relation_id":99,
                    "relation_schema_version":1,"new_tuple":{{}}}}]'::jsonb);
        """, check_result=False)
        check("unknown relation version fails closed",
              unknown_relation.returncode != 0 and "22023" in unknown_relation.stderr,
              True)
        wrong_width = sql("analytics", f"""
            SELECT lake_replication.stage_transaction(
                {subscription_id}, 102, '0/121', '0/122', '0/123',
                '2026-01-01 00:00:01+00',
                '[{{"operation":"insert","remote_relation_id":1,
                    "relation_schema_version":1,"new_tuple":{{"unknown":"x"}}}}]'::jsonb);
        """, check_result=False)
        check("unknown tuple column fails closed",
              wrong_width.returncode != 0 and "22023" in wrong_width.stderr,
              True)
        check("one transaction and one change stored",
              value("analytics", f"""
                SELECT (SELECT count(*) FROM lake_replication.transactions
                        WHERE subscription_id={subscription_id}) || ':' ||
                       (SELECT count(*) FROM lake_replication.changes c
                        JOIN lake_replication.transactions t USING (transaction_id)
                        WHERE t.subscription_id={subscription_id});
              """), "1:1")
        check("staging advances received and staged only",
              value("analytics", f"""
                SELECT received_lsn || ':' || staged_lsn || ':' ||
                       published_lsn || ':' || acknowledged_lsn
                  FROM lake_replication.checkpoints
                 WHERE subscription_id={subscription_id};
              """), "0/120:0/120:0/0:0/0")

        value("analytics", f"SELECT lake_replication.mark_acknowledged({subscription_id}, '0/120')")
        invalid_ack = sql("analytics", f"SELECT lake_replication.mark_acknowledged({subscription_id}, '0/121')",
                          check_result=False)
        check("acknowledgement cannot exceed durable staging",
              invalid_ack.returncode != 0 and "22023" in invalid_ack.stderr, True)

        rollback = stage(subscription_id, 102, "0/121", "0/125", "0/130", 2)
        check("second transaction staged", rollback.stdout.strip(), "t")
        value("analytics", fr"""
            BEGIN;
            SELECT lake_replication.prepare_batch(
                {subscription_id}, '{RUN}:0/120-0/130', '0/120', '0/130');
            ROLLBACK;
        """)
        # A planned batch is not durable unless the caller commits it.
        check("uncommitted batch preparation rolled back",
              value("analytics", f"""SELECT count(*) FROM lake_replication.batches
                                      WHERE subscription_id={subscription_id}
                                        AND batch_key='{RUN}:0/120-0/130'"""),
              "0")

        value("analytics", fr"""
            BEGIN;
            SELECT lake_replication.prepare_batch(
                {subscription_id}, '{RUN}:0/120-0/130', '0/120', '0/130') \gset
            INSERT INTO {RUN}_target VALUES (1, 'row-1'), (2, 'row-2');
            SELECT lake_replication.finish_batch(:prepare_batch);
            COMMIT;
        """)
        check("batch rows and checkpoint commit together",
              value("analytics", f"""
                SELECT (SELECT count(*) FROM {RUN}_target) || ':' ||
                       (SELECT count(*) FROM lake_replication.transactions
                         WHERE subscription_id={subscription_id} AND state='published') || ':' ||
                       (SELECT published_lsn FROM lake_replication.checkpoints
                         WHERE subscription_id={subscription_id});
              """), "2:2:0/130")

        check("third transaction staged",
              stage(subscription_id, 103, "0/131", "0/135", "0/140", 3).stdout.strip(),
              "t")
        started_at = datetime.now(timezone.utc).isoformat()
        ambiguous = sql("analytics", fr"""
            SET pg_lake_table.lab_crash_point='after_commit';
            BEGIN;
            SELECT lake_replication.prepare_batch(
                {subscription_id}, '{RUN}:0/140', '0/140', '0/140') \gset
            INSERT INTO {RUN}_target VALUES (3, 'row-3');
            SELECT lake_replication.finish_batch(:prepare_batch);
            COMMIT;
        """, check_result=False)
        check("post-commit client result is ambiguous", ambiguous.returncode != 0, True)
        wait_ready()
        check("ambiguous batch reconciles as published",
              value("analytics", f"""
                SELECT (SELECT count(*) FROM {RUN}_target WHERE id=3) || ':' ||
                       (SELECT state FROM lake_replication.batches
                         WHERE subscription_id={subscription_id}
                           AND batch_key='{RUN}:0/140') || ':' ||
                       (SELECT published_lsn FROM lake_replication.checkpoints
                         WHERE subscription_id={subscription_id});
              """), "1:published:0/140")
        check("deterministic batch lookup avoids replay",
              value("analytics", f"""
                SELECT lake_replication.prepare_batch(
                    {subscription_id}, '{RUN}:0/140', '0/140', '0/140');
              """).isdigit(), True)
        value("analytics", f"SELECT lake_replication.mark_acknowledged({subscription_id}, '0/140')")
        check("all four checkpoints converge",
              value("analytics", f"""
                SELECT received_lsn || ':' || staged_lsn || ':' ||
                       published_lsn || ':' || acknowledged_lsn
                  FROM lake_replication.checkpoints
                 WHERE subscription_id={subscription_id};
              """), "0/140:0/140:0/140:0/140")

        logs = command(COMPOSE + ["logs", "--no-color", "--since", started_at, "postgres"])
        (RESULTS / "ambiguous-commit-postgres.log").write_text(logs.stdout + logs.stderr)
        EVIDENCE["final_state"] = json.loads(value("analytics", f"""
            SELECT json_build_object(
                'target_rows', (SELECT count(*) FROM {RUN}_target),
                'transactions', (SELECT count(*) FROM lake_replication.transactions
                                  WHERE subscription_id={subscription_id}),
                'batches', (SELECT count(*) FROM lake_replication.batches
                             WHERE subscription_id={subscription_id}),
                'checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
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
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Bounded journal-retention and maintenance-observability probe."""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("maintenance_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}


def command(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=30, **kwargs)


def sql(statement, *, check_result=True):
    result = command(COMPOSE + ["exec", "-T", "postgres", "psql", "-XAt",
                     "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "analytics"],
                     input=statement)
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def value(statement):
    return sql(statement).stdout.strip()


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({"name": name, "passed": passed,
                               "actual": actual, "expected": expected})
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def install_migration():
    current = value("""
        SELECT to_regprocedure(
                   'lake_replication.run_journal_maintenance(bigint,boolean)'
               ) IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM information_schema.columns
                WHERE table_schema='lake_replication'
                  AND table_name='journal_maintenance_status'
                  AND column_name='failures'
           );
    """)
    if current == "f":
        migration = (ROOT / "migrations/0015_journal_maintenance.sql").read_text()
        sql(migration)


def payload(row_id):
    return json.dumps([{
        "operation": "insert",
        "remote_relation_id": 1,
        "relation_schema_version": 1,
        "new_tuple": {"id": str(row_id)},
    }], separators=(",", ":"))


def stage(subscription_id, row_id, end_lsn):
    commit_lsn = end_lsn - 1
    begin_lsn = end_lsn - 2
    value(f"""
        SELECT lake_replication.stage_transaction(
            {subscription_id}, {1000 + row_id}, '0/{begin_lsn:X}',
            '0/{commit_lsn:X}', '0/{end_lsn:X}', clock_timestamp(),
            '{payload(row_id)}'::jsonb);
    """)


def finish(subscription_id, key, first_lsn, last_lsn):
    value(f"""
        SELECT lake_replication.finish_batch(
            lake_replication.prepare_batch(
                {subscription_id}, '{key}', '0/{first_lsn:X}', '0/{last_lsn:X}'
            )
        );
    """)


def configure(subscription_id, enabled, retention, interval="1 second", maximum=1):
    value(f"""
        SELECT lake_replication.configure_journal_maintenance(
            {subscription_id}, {str(enabled).lower()}, INTERVAL '{retention}',
            INTERVAL '{interval}', {maximum});
    """)


def cleanup(subscription_id, force=True):
    return value(f"""
        SELECT did_run || ':' || safe_cleanup_lsn || ':' ||
               transactions_deleted || ':' || changes_deleted || ':' ||
               batches_deleted
          FROM lake_replication.run_journal_maintenance(
              {subscription_id}, {str(force).lower()});
    """)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    exit_code = 1
    try:
        install_migration()
        subscription_id = int(value(f"""
            SELECT lake_replication.create_subscription(
                9900000000000000000, 4242, 1, '{RUN}', '{RUN}_slot',
                ARRAY['{RUN}_publication']);
        """))
        EVIDENCE["subscription_id"] = subscription_id
        value(f"""
            INSERT INTO lake_replication.relations (
                subscription_id, remote_relation_id, schema_version,
                source_namespace, source_relation, replica_identity, columns
            ) VALUES (
                {subscription_id}, 1, 1, 'public', 'source', 'd',
                '[{{"name":"id","type_oid":20}}]'::jsonb
            );
        """)
        check("new subscription receives default policy",
              value(f"""
                  SELECT enabled || ':' || retention_interval || ':' ||
                         max_transactions_per_run
                    FROM lake_replication.journal_maintenance_policies
                   WHERE subscription_id={subscription_id};
              """), "true:1 day:1000")

        configure(subscription_id, True, "0 seconds")
        stage(subscription_id, 0, 0x100)
        finish(subscription_id, f"{RUN}:automatic", 0x100, 0x100)
        value(f"SELECT lake_replication.mark_acknowledged({subscription_id}, '0/100');")
        value(f"SELECT lake_replication.start_service({subscription_id}, 1);")
        value(f"""
            SELECT lake_replication.report_service_cycle(
                {subscription_id}, 'running', 1, 1);
        """)
        check("successful service cycle runs due cleanup automatically",
              value(f"""
                  SELECT count(*) || ':' ||
                         (SELECT count(*) FROM lake_replication.batches
                           WHERE subscription_id={subscription_id}) || ':' ||
                         (SELECT runs FROM lake_replication.journal_maintenance_status
                           WHERE subscription_id={subscription_id})
                    FROM lake_replication.transactions
                   WHERE subscription_id={subscription_id};
              """), "0:0:1")
        value(f"""
            DELETE FROM lake_replication.journal_maintenance_policies
             WHERE subscription_id={subscription_id};
            SELECT lake_replication.report_service_cycle(
                {subscription_id}, 'running', 1, 0);
        """)
        check("maintenance failure does not roll back service progress",
              value(f"""
                  SELECT ss.receiver_cycles || ':' || jm.failures || ':' ||
                         (jm.last_error IS NOT NULL)::text
                    FROM lake_replication.service_status ss
                    JOIN lake_replication.journal_maintenance_status jm
                      USING (subscription_id)
                   WHERE subscription_id={subscription_id};
              """), "2:1:true")
        configure(subscription_id, True, "0 seconds")

        stage(subscription_id, 1, 0x110)
        stage(subscription_id, 2, 0x120)
        stage(subscription_id, 3, 0x130)
        finish(subscription_id, f"{RUN}:first-two", 0x110, 0x120)
        value(f"SELECT lake_replication.mark_acknowledged({subscription_id}, '0/110');")

        check("cleanup is capped and bounded by both checkpoints",
              cleanup(subscription_id), "true:0/110:1:1:0")
        check("newer published and staged recovery evidence remains",
              value(f"""
                  SELECT string_agg(end_lsn::text || '=' || state, ',' ORDER BY end_lsn)
                         || ':' || (SELECT count(*) FROM lake_replication.batches
                                    WHERE subscription_id={subscription_id})
                    FROM lake_replication.transactions
                   WHERE subscription_id={subscription_id};
              """), "0/120=published,0/130=staged:1")
        check("repeat cleanup is idempotent at the same watermark",
              cleanup(subscription_id), "true:0/110:0:0:0")

        value(f"SELECT lake_replication.mark_acknowledged({subscription_id}, '0/130');")
        check("publication remains the limiting cleanup checkpoint",
              cleanup(subscription_id), "true:0/120:1:1:1")
        batch_id = value(f"""
            SELECT lake_replication.prepare_batch(
                {subscription_id}, '{RUN}:ambiguous', '0/130', '0/130');
        """)
        check("publishing transaction survives maintenance",
              cleanup(subscription_id), "true:0/120:0:0:0")
        check("ambiguous batch and transaction are retained",
              value(f"""
                  SELECT t.state || ':' || b.state
                    FROM lake_replication.transactions t
                    JOIN lake_replication.batches b USING (batch_id)
                   WHERE t.subscription_id={subscription_id};
              """), "publishing:publishing")

        value(f"SELECT lake_replication.finish_batch({batch_id});")
        check("completed ambiguous work becomes reclaimable",
              cleanup(subscription_id), "true:0/130:1:1:1")

        stage(subscription_id, 4, 0x140)
        finish(subscription_id, f"{RUN}:retained", 0x140, 0x140)
        value(f"SELECT lake_replication.mark_acknowledged({subscription_id}, '0/140');")
        configure(subscription_id, True, "1 day", "1 hour")
        check("retention window protects recent published evidence",
              cleanup(subscription_id), "true:0/140:0:0:0")
        configure(subscription_id, True, "0 seconds", "1 hour")
        check("forced cleanup bypasses schedule but honors policy",
              cleanup(subscription_id), "true:0/140:1:1:1")
        check("ordinary run is rate limited by cleanup interval",
              cleanup(subscription_id, force=False), "false:0/140:0:0:0")

        configure(subscription_id, False, "0 seconds")
        check("disabled policy also blocks forced cleanup",
              cleanup(subscription_id), "false:0/140:0:0:0")
        check("health view reports bounded storage and cumulative work",
              value(f"""
                  SELECT safe_cleanup_lsn || ':' || staged_transactions || ':' ||
                         publishing_transactions || ':' || published_transactions || ':' ||
                         retained_changes || ':' || (retained_bytes = 0)::text || ':' ||
                         total_transactions_deleted || ':' || total_changes_deleted || ':' ||
                         total_batches_deleted || ':' || (total_bytes_deleted > 0)::text
                    FROM lake_replication.journal_maintenance_health
                   WHERE subscription_id={subscription_id};
              """), "0/140:0:0:0:0:true:5:5:4:true")

        EVIDENCE["final_state"] = json.loads(value(f"""
            SELECT json_build_object(
                'checkpoint', (SELECT to_jsonb(c) FROM lake_replication.checkpoints c
                                WHERE subscription_id={subscription_id}),
                'maintenance', (SELECT to_jsonb(h)
                                  FROM lake_replication.journal_maintenance_health h
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

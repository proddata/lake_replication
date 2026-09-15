#!/usr/bin/env python3
"""SCRAM authentication and least-privilege logical replication probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
SOURCE_COMPOSE = [
    "docker", "compose", "-p", "pg-lake-replication-scram-lab",
    "-f", str(ROOT / "deploy/scram/compose.yml"),
]
RUN = datetime.now(timezone.utc).strftime("scram_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"
SOURCE_PORT, DESTINATION_PORT = 55440, 55434
ADMIN_PASSWORD = "admin-scram-lab-only"
SOURCE_PASSWORD = "source-scram-lab-only"
WRONG_PASSWORD = "wrong-password-must-stay-secret"
DESTINATION_PASSWORD = "distinct-destination-secret"
LAB_ENV = os.environ | {
    "AUTH_LAB_ADMIN_PASSWORD": ADMIN_PASSWORD,
    "AUTH_LAB_SOURCE_PASSWORD": SOURCE_PASSWORD,
}


def command(args, timeout=120, **kwargs):
    return subprocess.run(
        args, cwd=ROOT, text=True, capture_output=True, timeout=timeout, **kwargs
    )


def sql(port, database, statement, *, user="postgres", password=None,
        check_result=True):
    env = os.environ.copy()
    if password is not None:
        env["PGPASSWORD"] = password
    result = command([
        "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
        "-p", str(port), "-U", user, "-d", database,
    ], input=statement, env=env)
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def source(statement, *, user="postgres", password=ADMIN_PASSWORD,
           check_result=True):
    return sql(SOURCE_PORT, "application", statement, user=user,
               password=password, check_result=check_result)


def destination(statement, *, check_result=True):
    return sql(DESTINATION_PORT, "analytics", statement,
               check_result=check_result)


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({
        "name": name, "passed": passed, "actual": actual, "expected": expected,
    })
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(name)


def wait_for(query, predicate, *, timeout=30):
    deadline = time.monotonic() + timeout
    latest = ""
    while time.monotonic() < deadline:
        result = destination(query, check_result=False)
        if result.returncode == 0:
            latest = result.stdout.strip()
            if predicate(latest):
                return latest
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for query; latest={latest!r}")


def environment(subscription_id, source_password=SOURCE_PASSWORD):
    result = os.environ.copy()
    result.update({
        "LAKE_SOURCE_ADDRESS": f"127.0.0.1:{SOURCE_PORT}",
        "LAKE_DESTINATION_ADDRESS": f"127.0.0.1:{DESTINATION_PORT}",
        "LAKE_SOURCE_USER": "lake_source",
        "LAKE_SOURCE_PASSWORD": source_password,
        "LAKE_DESTINATION_USER": "postgres",
        "LAKE_DESTINATION_PASSWORD": DESTINATION_PASSWORD,
        "LAKE_SOURCE_DATABASE": "application",
        "LAKE_DESTINATION_DATABASE": "analytics",
        "LAKE_SLOT": SLOT,
        "LAKE_SLOT_FAILOVER": "0",
        "LAKE_PUBLICATION": PUBLICATION,
        "LAKE_SUBSCRIPTION_ID": str(subscription_id),
        "LAKE_DISCOVER_PUBLICATION": "1",
        "LAKE_DESTINATION_SCHEMA": RUN,
        "LAKE_CREATE_TABLES_USING_ICEBERG": "1",
        "LAKE_TYPE_MAPPING_VERSION": "1",
        "LAKE_SUPERVISOR_POLL_MS": "100",
        "LAKE_RECEIVER_IDLE_TIMEOUT_MS": "200",
        "LAKE_SUPERVISOR_BASE_BACKOFF_MS": "200",
        "LAKE_SUPERVISOR_MAX_BACKOFF_MS": "800",
    })
    return result


def cleanup_destination(subscription_id):
    destination(f"DROP SCHEMA IF EXISTS {RUN} CASCADE", check_result=False)
    if subscription_id:
        destination(f"""
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
            DELETE FROM lake_replication.sources src WHERE NOT EXISTS
                (SELECT 1 FROM lake_replication.subscriptions sub
                  WHERE sub.source_id=src.source_id);
        """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscription_id = None
    supervisor = None
    exit_code = 1
    command(SOURCE_COMPOSE + ["down", "-v"], env=LAB_ENV)
    try:
        started = command(
            SOURCE_COMPOSE + ["up", "-d", "--wait", "--wait-timeout", "180"],
            timeout=210, env=LAB_ENV,
        )
        if started.returncode:
            raise RuntimeError(started.stderr.strip())
        build = command(["cargo", "build", "--quiet", "--bins"])
        if build.returncode:
            raise RuntimeError(build.stderr.strip())

        role = source("""
            SELECT rolsuper::text || ':' || rolreplication::text || ':' ||
                   rolcreatedb::text || ':' || rolcreaterole::text
              FROM pg_roles WHERE rolname='lake_source'
        """).stdout.strip()
        check("source identity is a non-superuser replication role",
              role, "false:true:false:false")
        identity = json.loads(source("""
            SELECT json_build_object(
                'system_identifier', (SELECT system_identifier FROM pg_control_system()),
                'database_oid', (SELECT oid FROM pg_database WHERE datname=current_database()),
                'timeline', (('x' || substr(
                    pg_walfile_name(pg_current_wal_flush_lsn()), 1, 8
                ))::bit(32)::bigint))
        """).stdout.strip())
        source(f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
            INSERT INTO public.{TABLE} VALUES (1, 'snapshot');
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};
            GRANT SELECT ON public.{TABLE} TO lake_source;
        """)
        selected = source(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM public.{TABLE}",
            user="lake_source", password=SOURCE_PASSWORD,
        ).stdout.strip()
        check("least-privilege role can read the published table", selected, "1:snapshot")
        denied = source(
            f"INSERT INTO public.{TABLE} VALUES (99, 'denied')", user="lake_source",
            password=SOURCE_PASSWORD, check_result=False,
        )
        check("least-privilege role cannot write source data", denied.returncode != 0, True)

        destination(f"CREATE SCHEMA {RUN}")
        subscription_id = int(destination(f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """).stdout.strip())

        rejected = command(
            [str(ROOT / "target/debug/lake-replication-initial-sync")],
            env=environment(subscription_id, WRONG_PASSWORD),
        )
        combined_rejection = rejected.stdout + rejected.stderr
        check("incorrect SCRAM credentials are rejected",
              rejected.returncode != 0 and
              "password authentication failed" in combined_rejection, True)
        check("authentication errors do not disclose the attempted password",
              WRONG_PASSWORD not in combined_rejection, True)

        initial = command(
            [str(ROOT / "target/debug/lake-replication-initial-sync")],
            env=environment(subscription_id),
        )
        if initial.returncode:
            raise RuntimeError(initial.stderr.strip())
        check("SCRAM-authenticated initial copy completes",
              destination(
                  f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}"
              ).stdout.strip(), "1:snapshot")

        source(f"INSERT INTO public.{TABLE} VALUES (2, 'scram-stream')")
        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")], cwd=ROOT,
            env=environment(subscription_id), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        rows = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item == "1:snapshot,2:scram-stream",
        )
        check("SCRAM-authenticated logical stream publishes exactly", rows,
              "1:snapshot,2:scram-stream")
        destination(f"SELECT lake_replication.request_service_stop({subscription_id})")
        stdout, stderr = supervisor.communicate(timeout=10)
        (RESULTS / "supervisor.stdout").write_text(stdout)
        (RESULTS / "supervisor.stderr").write_text(stderr)
        check("authenticated supervisor stops cleanly", supervisor.returncode, 0)
        all_output = initial.stdout + initial.stderr + stdout + stderr
        check("successful worker output discloses no configured password",
              all(secret not in all_output for secret in
                  (SOURCE_PASSWORD, DESTINATION_PASSWORD)), True)
        live_logs = command(
            SOURCE_COMPOSE + ["logs", "--no-color"], env=LAB_ENV
        ).stdout
        check("source logs disclose no configured password",
              all(secret not in live_logs for secret in
                  (ADMIN_PASSWORD, SOURCE_PASSWORD, WRONG_PASSWORD)), True)
        supervisor = None
        exit_code = 0
    except Exception as error:
        EVIDENCE["error"] = str(error)
        print(f"ERROR {error}", flush=True)
    finally:
        if supervisor is not None:
            if supervisor.poll() is None and subscription_id:
                destination(
                    f"SELECT lake_replication.request_service_stop({subscription_id})",
                    check_result=False,
                )
            try:
                stdout, stderr = supervisor.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                supervisor.terminate()
                stdout, stderr = supervisor.communicate(timeout=5)
            (RESULTS / "supervisor.stdout").write_text(stdout)
            (RESULTS / "supervisor.stderr").write_text(stderr)
        logs = command(
            SOURCE_COMPOSE + ["logs", "--no-color", "--tail", "300"], env=LAB_ENV
        )
        (RESULTS / "source.log").write_text(logs.stdout + logs.stderr)
        cleanup_destination(subscription_id)
        command(SOURCE_COMPOSE + ["down", "-v"], env=LAB_ENV)
        EVIDENCE["completed_at"] = datetime.now(timezone.utc).isoformat()
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"evidence: {RESULTS / 'results.json'}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

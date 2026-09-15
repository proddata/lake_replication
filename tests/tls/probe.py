#!/usr/bin/env python3
"""Certificate-verified TLS plus SCRAM logical replication probe."""
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parents[2]
SOURCE_COMPOSE = [
    "docker", "compose", "-p", "pg-lake-replication-tls-lab",
    "-f", str(ROOT / "deploy/tls/compose.yml"),
]
RUN = datetime.now(timezone.utc).strftime("tls_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE, PUBLICATION, SLOT = RUN, f"{RUN}_pub", f"{RUN}_slot"
SOURCE_PORT, DESTINATION_PORT = 55441, 55434
ADMIN_PASSWORD = "admin-tls-lab-only"
SOURCE_PASSWORD = "source-tls-lab-only"
LAB_ENV = os.environ | {
    "TLS_LAB_ADMIN_PASSWORD": ADMIN_PASSWORD,
    "TLS_LAB_SOURCE_PASSWORD": SOURCE_PASSWORD,
}


def command(args, timeout=120, **kwargs):
    return subprocess.run(
        args, cwd=ROOT, text=True, capture_output=True, timeout=timeout, **kwargs
    )


def sql(port, database, statement, *, ssl=True, check_result=True):
    env = os.environ | {"PGPASSWORD": ADMIN_PASSWORD}
    if ssl:
        env.update({
            "PGSSLMODE": "verify-full",
            "PGSSLROOTCERT": str(RESULTS / "ca.crt"),
        })
    else:
        env["PGSSLMODE"] = "disable"
    result = command([
        "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-h", "localhost",
        "-p", str(port), "-U", "postgres", "-d", database,
    ], input=statement, env=env)
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def source(statement, *, ssl=True, check_result=True):
    return sql(SOURCE_PORT, "application", statement, ssl=ssl,
               check_result=check_result)


def destination(statement, *, check_result=True):
    result = command([
        "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
        "-p", str(DESTINATION_PORT), "-U", "postgres", "-d", "analytics",
    ], input=statement)
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({
        "name": name, "passed": passed, "actual": actual, "expected": expected,
    })
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(name)


def wait_for(query, predicate, *, source_query=False, timeout=30):
    deadline = time.monotonic() + timeout
    latest = ""
    while time.monotonic() < deadline:
        result = source(query, check_result=False) if source_query else destination(
            query, check_result=False
        )
        if result.returncode == 0:
            latest = result.stdout.strip()
            if predicate(latest):
                return latest
        time.sleep(0.1)
    raise TimeoutError(f"timed out waiting for query; latest={latest!r}")


def environment(subscription_id, *, root="ca.crt", server_name=None):
    result = os.environ.copy()
    result.update({
        "LAKE_SOURCE_ADDRESS": f"127.0.0.1:{SOURCE_PORT}",
        "LAKE_DESTINATION_ADDRESS": f"127.0.0.1:{DESTINATION_PORT}",
        "LAKE_SOURCE_USER": "lake_source",
        "LAKE_SOURCE_PASSWORD": SOURCE_PASSWORD,
        "LAKE_SOURCE_TLS_MODE": "verify-full",
        "LAKE_SOURCE_TLS_ROOT_CERT": str(RESULTS / root),
        "LAKE_DESTINATION_USER": "postgres",
        "LAKE_DESTINATION_TLS_MODE": "disable",
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
        "LAKE_RECEIVER_IDLE_TIMEOUT_MS": "2000",
        "LAKE_SUPERVISOR_BASE_BACKOFF_MS": "200",
        "LAKE_SUPERVISOR_MAX_BACKOFF_MS": "800",
    })
    if server_name:
        result["LAKE_SOURCE_TLS_SERVER_NAME"] = server_name
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
        for certificate in ("ca.crt", "untrusted-ca.crt"):
            copied = command(
                SOURCE_COMPOSE + ["cp", f"source:/certs/{certificate}",
                                  str(RESULTS / certificate)], env=LAB_ENV,
            )
            if copied.returncode:
                raise RuntimeError(copied.stderr.strip())
        build = command(["cargo", "build", "--quiet", "--bins"])
        if build.returncode:
            raise RuntimeError(build.stderr.strip())

        plaintext = source("SELECT 1", ssl=False, check_result=False)
        check("source rejects plaintext TCP connections", plaintext.returncode != 0, True)
        tls_session = source("SELECT ssl::text FROM pg_stat_ssl WHERE pid=pg_backend_pid()")
        check("administrative control observes an encrypted TLS session",
              tls_session.stdout.strip(), "true")
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
        destination(f"CREATE SCHEMA {RUN}")
        subscription_id = int(destination(f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """).stdout.strip())

        untrusted = command(
            [str(ROOT / "target/debug/lake-replication-initial-sync")],
            env=environment(subscription_id, root="untrusted-ca.crt"),
        )
        check("worker rejects a certificate from an untrusted CA",
              untrusted.returncode != 0 and "UnknownIssuer" in untrusted.stderr, True)
        mismatch = command(
            [str(ROOT / "target/debug/lake-replication-initial-sync")],
            env=environment(subscription_id, server_name="wrong.example"),
        )
        mismatch_error = mismatch.stderr.lower()
        check("worker rejects a mismatched server identity",
              mismatch.returncode != 0 and "certificate" in mismatch_error and
              "wrong.example" in mismatch_error, True)

        initial = command(
            [str(ROOT / "target/debug/lake-replication-initial-sync")],
            env=environment(subscription_id),
        )
        if initial.returncode:
            raise RuntimeError(initial.stderr.strip())
        check("verify-full TLS initial copy completes",
              destination(
                  f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}"
              ).stdout.strip(), "1:snapshot")

        source(f"INSERT INTO public.{TABLE} VALUES (2, 'tls-stream')")
        supervisor = subprocess.Popen(
            [str(ROOT / "target/debug/lake-replication-supervisor")], cwd=ROOT,
            env=environment(subscription_id), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        encrypted_worker = wait_for("""
            SELECT bool_and(ssl)::text FROM pg_stat_activity
              JOIN pg_stat_ssl USING (pid)
             WHERE application_name LIKE 'lake-replication-%'
        """, lambda item: item == "true", source_query=True)
        check("source observes worker connections only over TLS",
              encrypted_worker, "true")
        rows = wait_for(
            f"SELECT string_agg(id || ':' || note, ',' ORDER BY id) FROM {RUN}.{TABLE}",
            lambda item: item == "1:snapshot,2:tls-stream",
        )
        check("TLS logical stream publishes exactly", rows,
              "1:snapshot,2:tls-stream")
        destination(f"SELECT lake_replication.request_service_stop({subscription_id})")
        stdout, stderr = supervisor.communicate(timeout=10)
        (RESULTS / "supervisor.stdout").write_text(stdout)
        (RESULTS / "supervisor.stderr").write_text(stderr)
        check("TLS supervisor stops cleanly", supervisor.returncode, 0)
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

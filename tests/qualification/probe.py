#!/usr/bin/env python3
"""Qualify one native release archive on a disposable pg_lake host.

The destination image receives only the exported archive. The repository is
used to build the pg_lake prerequisite image and orchestration tooling, never
mounted into the running source or destination containers.
"""

import hashlib
import json
import os
import platform
import secrets
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
VERSION = os.environ.get("VERSION", "0.1.0-beta.1")
RUN = datetime.now(timezone.utc).strftime("qualification_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".qualification" / RUN
COMPOSE_FILE = ROOT / "deploy/qualification/compose.yml"
PG_LAKE_REVISION = "8180647d4535fdb1ed223f92f301ba6094a8ec3e"
PROJECT = f"pg-lake-replication-{RUN.replace('_', '-')}"
SOURCE_IMAGE = f"pg-lake-replication-qualification-source:{RUN}"
DESTINATION_IMAGE = f"pg-lake-replication-qualification-destination:{RUN}"
BASE_IMAGE = f"pg-lake-qualification-base:{PG_LAKE_REVISION[:12]}"
ADMIN_PASSWORD = secrets.token_urlsafe(24)
SOURCE_PASSWORD = secrets.token_urlsafe(24)
ENV = {
    **os.environ,
    "QUALIFICATION_SOURCE_IMAGE": SOURCE_IMAGE,
    "QUALIFICATION_DESTINATION_IMAGE": DESTINATION_IMAGE,
    "QUAL_ADMIN_PASSWORD": ADMIN_PASSWORD,
    "QUAL_SOURCE_PASSWORD": SOURCE_PASSWORD,
}
EVIDENCE = {
    "run": RUN,
    "pg_lake_revision": PG_LAKE_REVISION,
    "checks": [],
}


def command(args, *, check_result=True, timeout=1200, cwd=ROOT, **kwargs):
    result = subprocess.run(
        args,
        cwd=cwd,
        env=ENV,
        text=True,
        capture_output=True,
        timeout=timeout,
        **kwargs,
    )
    if check_result and result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result


def compose(*args, **kwargs):
    return command(
        ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), *args],
        **kwargs,
    )


def sql(service, database, statement, *, check_result=True):
    return compose(
        "exec",
        "-T",
        service,
        "psql",
        "-XAt",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "postgres",
        "-d",
        database,
        input=statement,
        check_result=check_result,
    ).stdout.strip()


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append(
        {"name": name, "passed": passed, "actual": actual, "expected": expected}
    )
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def wait_for(service, database, statement, expected, timeout=60):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        result = compose(
            "exec",
            "-T",
            service,
            "psql",
            "-XAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            database,
            "-c",
            statement,
            check_result=False,
        )
        if result.returncode == 0:
            latest = result.stdout.strip()
            if latest == expected:
                return latest
        time.sleep(0.5)
    return latest


def native_architecture():
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        return "amd64"
    if machine in {"arm64", "aarch64"}:
        return "arm64"
    raise RuntimeError(f"unsupported qualification host architecture: {machine}")


def archive_path():
    configured = os.environ.get("QUALIFICATION_ARCHIVE")
    if configured:
        return Path(configured).expanduser().resolve()
    return ROOT / "dist/release" / (
        f"pg_lake_replication-{VERSION}-linux-{native_architecture()}.tar.gz"
    )


def verify_archive(archive):
    if not archive.is_file():
        raise RuntimeError(
            f"release archive not found: {archive}; build it with "
            "`make install-bundles` or set QUALIFICATION_ARCHIVE"
        )
    checksum_file = Path(f"{archive}.sha256")
    if not checksum_file.is_file():
        raise RuntimeError(f"release checksum not found: {checksum_file}")
    fields = checksum_file.read_text().strip().split()
    if len(fields) < 2 or fields[1].lstrip("*") != archive.name:
        raise RuntimeError(f"invalid checksum manifest: {checksum_file}")
    actual = hashlib.sha256(archive.read_bytes()).hexdigest()
    check("release archive checksum", actual, fields[0])
    with tarfile.open(archive, "r:gz") as bundle:
        metadata = {}
        for field in ("TARGETARCH", "SOURCE_REVISION"):
            member = next(
                (item for item in bundle.getmembers() if item.name.endswith(f"/{field}")),
                None,
            )
            if member is None:
                raise RuntimeError(f"release archive does not contain {field}")
            stream = bundle.extractfile(member)
            if stream is None:
                raise RuntimeError(f"release archive {field} is not a regular file")
            metadata[field] = stream.read().decode().strip()
    check("archive architecture matches qualification host", metadata["TARGETARCH"], native_architecture())
    EVIDENCE["archive"] = archive.name
    EVIDENCE["archive_sha256"] = actual
    EVIDENCE["architecture"] = metadata["TARGETARCH"]
    EVIDENCE["source_revision"] = metadata["SOURCE_REVISION"]


def build_images(archive, context):
    base_exists = command(
        ["docker", "image", "inspect", BASE_IMAGE], check_result=False
    ).returncode == 0
    if not base_exists or os.environ.get("QUALIFICATION_REBUILD_BASE") == "1":
        command(
            [
                "docker",
                "build",
                "--target",
                "pg-lake-base",
                "--build-arg",
                f"PG_LAKE_REV={PG_LAKE_REVISION}",
                "--tag",
                BASE_IMAGE,
                "--file",
                str(ROOT / "deploy/poc/Dockerfile"),
                str(ROOT),
            ],
            timeout=7200,
        )
    for name in (
        "Dockerfile.destination",
        "Dockerfile.source",
        "destination-init.sql",
        "source-init.sh",
        "pgduck.sh",
        "pgduck.sql",
    ):
        shutil.copy2(ROOT / "deploy/qualification" / name, context / name)
    shutil.copy2(archive, context / "bundle.tar.gz")
    command(
        [
            "docker",
            "build",
            "--build-arg",
            f"BASE_IMAGE={BASE_IMAGE}",
            "--tag",
            DESTINATION_IMAGE,
            "--file",
            str(context / "Dockerfile.destination"),
            str(context),
        ],
        timeout=1800,
    )
    command(
        [
            "docker",
            "build",
            "--tag",
            SOURCE_IMAGE,
            "--file",
            str(context / "Dockerfile.source"),
            str(context),
        ],
        timeout=900,
    )


def worker_logs():
    return compose(
        "exec",
        "-T",
        "destination",
        "sh",
        "-ec",
        "find /worker-logs -maxdepth 1 -type f -name '*.log' -exec cat {} +",
    ).stdout


def verify_structured_logs(logs, required_events=()):
    records = []
    for line in logs.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise AssertionError(f"worker log is not JSON: {line[:120]}") from error
        if not {"timestamp_unix_ms", "level", "component", "event", "message"} <= record.keys():
            raise AssertionError(f"worker log lacks required fields: {record}")
        records.append(record)
    check("structured worker logs emitted", bool(records), True)
    observed_events = {record["event"] for record in records}
    check(
        "structured failure and recovery events emitted",
        set(required_events).issubset(observed_events),
        True,
    )
    check(
        "credential values absent from worker logs",
        ADMIN_PASSWORD not in logs and SOURCE_PASSWORD not in logs,
        True,
    )
    EVIDENCE["worker_log_records"] = len(records)
    EVIDENCE["worker_log_events"] = sorted(observed_events)
    (RESULTS / "worker.jsonl").write_text(logs)


def verify_uninstaller():
    result = compose(
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "/bin/sh",
        "destination",
        "-ec",
        """
test -x /usr/local/bin/lake-replication-supervisor
test -f "$(pg_config --pkglibdir)/lake_subscription_sugar.so"
/qualification/uninstall.sh
test ! -e /usr/local/bin/lake-replication-supervisor
test ! -e "$(pg_config --pkglibdir)/lake_subscription_sugar.so"
test ! -e "$(pg_config --sharedir)/extension/lake_replication.control"
test ! -e "$(pg_config --sharedir)/extension/lake_replication--0.1.0-beta.1.sql"
test ! -e "$(pg_config --sharedir)/extension/lake_replication--0.1.0--0.1.0-beta.1.sql"
test -e "$(pg_config --sharedir)/extension/pg_lake.control"
printf 'uninstall-verified\\n'
""",
    )
    check("uninstaller removes only project program files", "uninstall-verified" in result.stdout, True)


def verify_upgrade_contract():
    database = "upgrade_contract"
    restore_database = "upgrade_restore"
    dump_path = "/tmp/lake-upgrade-contract.dump"
    compose("exec", "-T", "destination", "createdb", "-U", "postgres", database)
    try:
        sql(
            "destination",
            database,
            "CREATE EXTENSION pg_lake CASCADE; "
            "CREATE EXTENSION lake_replication VERSION '0.1.0'; "
            "CREATE EXTENSION lake_subscription_sugar VERSION '1.0' CASCADE;",
        )
        subscription_id = sql(
            "destination",
            database,
            "SELECT lake_replication.create_subscription("
            "9001, 1234, 1, 'upgrade_fixture', 'upgrade_slot', "
            "ARRAY['upgrade_pub']);",
        )
        sql(
            "destination",
            database,
            f"SELECT lake_replication.start_service({subscription_id}, 4242); "
            "UPDATE lake_replication.subscriptions SET state='streaming' "
            f"WHERE subscription_id={subscription_id}; "
            "UPDATE lake_replication.checkpoints "
            "SET received_lsn='0/10', staged_lsn='0/10', "
            "published_lsn='0/10', acknowledged_lsn='0/10' "
            f"WHERE subscription_id={subscription_id};",
        )
        sql(
            "destination",
            database,
            f"INSERT INTO lake_subscription_sugar.pending_cleanup "
            "(subscription_id, source_conninfo, source_slot_name) VALUES "
            f"({subscription_id}, 'host=source dbname=application', 'upgrade_slot'); "
            "INSERT INTO lake_subscription_sugar.worker_launch_config "
            "(subscription_id, slot_name, publication_name, source_database, "
            "destination_database, source_address, destination_address, "
            "destination_schema) VALUES "
            f"({subscription_id}, 'upgrade_slot', 'upgrade_pub', 'application', "
            "'upgrade_contract', 'source:5432', '/var/run/postgresql:5432', 'public');",
        )
        check(
            "development baseline excludes beta observability schema",
            sql(
                "destination",
                database,
                "SELECT extversion || ':' || "
                "(to_regclass('lake_replication.subscription_status') IS NULL) "
                "FROM pg_extension WHERE extname='lake_replication'",
            ),
            "0.1.0:true",
        )
        sql(
            "destination",
            database,
            "ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.1'; "
            "ALTER EXTENSION lake_subscription_sugar UPDATE TO '1.1';",
        )
        check(
            "extension upgrades in place and preserves control-plane state",
            sql(
                "destination",
                database,
                "SELECT e.extversion || ':' || s.subscription_name || ':' || "
                "c.published_lsn || ':' || st.health_state "
                "FROM pg_extension e "
                "JOIN lake_replication.subscriptions s ON true "
                "JOIN lake_replication.checkpoints c USING (subscription_id) "
                "JOIN lake_replication.subscription_status st USING (subscription_id) "
                "WHERE e.extname='lake_replication' "
                "AND s.subscription_name='upgrade_fixture'",
            ),
            "0.1.0-beta.1:upgrade_fixture:0/10:degraded",
        )
        check(
            "upgrade registers durable extension state for logical backup",
            sql(
                "destination",
                database,
                "SELECT "
                "(SELECT cardinality(extconfig) FROM pg_extension "
                "WHERE extname='lake_replication') || ':' || "
                "(SELECT extversion || ':' || cardinality(extconfig) "
                "FROM pg_extension WHERE extname='lake_subscription_sugar') || ':' || "
                "(SELECT count(*) FROM lake_subscription_sugar.worker_launch_config)",
            ),
            "16:1.1:2:1",
        )
        logical_dump = compose(
            "exec",
            "-T",
            "destination",
            "pg_dump",
            "-U",
            "postgres",
            "--data-only",
            database,
        ).stdout
        check(
            "logical backup contains durable state but not regenerated seed mappings",
            "COPY lake_replication.subscriptions" in logical_dump
            and "COPY lake_subscription_sugar.worker_launch_config" in logical_dump
            and "COPY lake_replication.type_mappings" not in logical_dump,
            True,
        )
        compose(
            "exec",
            "-T",
            "destination",
            "pg_dump",
            "-U",
            "postgres",
            "--format=custom",
            f"--file={dump_path}",
            database,
        )
        compose(
            "exec", "-T", "destination", "createdb", "-U", "postgres", restore_database
        )
        for section in ("pre-data", "data", "post-data"):
            restore_args = [
                "exec",
                "-T",
                "destination",
                "pg_restore",
                "--exit-on-error",
                f"--section={section}",
            ]
            if section == "data":
                restore_args.append("--disable-triggers")
            restore_args.extend(
                ["-U", "postgres", "--dbname", restore_database, dump_path]
            )
            compose(*restore_args)
        check(
            "logical backup restores extension versions and durable control state",
            sql(
                "destination",
                restore_database,
                "SELECT "
                "(SELECT extversion FROM pg_extension "
                "WHERE extname='lake_replication') || ':' || "
                "(SELECT extversion FROM pg_extension "
                "WHERE extname='lake_subscription_sugar') || ':' || "
                "(SELECT count(*) FROM lake_replication.subscriptions) || ':' || "
                "(SELECT count(*) FROM lake_subscription_sugar.worker_launch_config)",
            ),
            "0.1.0-beta.1:1.1:1:1",
        )
        check(
            "no unsupported SQL downgrade path is advertised",
            sql(
                "destination",
                database,
                "SELECT count(*) FROM pg_extension_update_paths('lake_replication') "
                "WHERE source='0.1.0-beta.1' AND target='0.1.0' "
                "AND path IS NOT NULL",
            ),
            "0",
        )
    finally:
        for disposable_database in (restore_database, database):
            compose(
                "exec",
                "-T",
                "destination",
                "dropdb",
                "--if-exists",
                "-U",
                "postgres",
                disposable_database,
                check_result=False,
            )
        compose(
            "exec",
            "-T",
            "destination",
            "rm",
            "-f",
            dump_path,
            check_result=False,
        )


def exercise_replication():
    connection = (
        "host=source port=5432 dbname=application "
        f"user=lake_source password={SOURCE_PASSWORD}"
    )
    sql(
        "destination",
        "analytics",
        f"""
CREATE SUBSCRIPTION orders_to_lake
  CONNECTION '{connection}'
  PUBLICATION orders_pub
  WITH (lake_target = 'iceberg');
""",
    )
    check(
        "initial copy reaches the artifact-installed destination",
        wait_for(
            "destination",
            "analytics",
            "SELECT note FROM public.orders WHERE id=1",
            "initial",
            timeout=90,
        ),
        "initial",
    )
    check(
        "subscription becomes healthy",
        wait_for(
            "destination",
            "analytics",
            "SELECT health_state || ':' || subscription_state "
            "FROM lake_replication.subscription_status "
            "WHERE subscription_name='orders_to_lake'",
            "healthy:streaming",
            timeout=90,
        ),
        "healthy:streaming",
    )
    check(
        "all documented metrics are exported",
        sql(
            "destination",
            "analytics",
            "SELECT count(*) FROM lake_replication.metrics "
            "WHERE subscription_name='orders_to_lake'",
        ),
        "12",
    )
    sql(
        "source",
        "application",
        "INSERT INTO public.orders (id, note) VALUES (2, 'streamed'); "
        "UPDATE public.orders SET note='initial-updated' WHERE id=1;",
    )
    check(
        "live insert and update replicate",
        wait_for(
            "destination",
            "analytics",
            "SELECT string_agg(id || '=' || note, ',' ORDER BY id) FROM public.orders",
            "1=initial-updated,2=streamed",
            timeout=60,
        ),
        "1=initial-updated,2=streamed",
    )
    sql("destination", "analytics", "ALTER SUBSCRIPTION orders_to_lake DISABLE")
    check(
        "disable reaches stopped state",
        wait_for(
            "destination",
            "analytics",
            "SELECT health_state FROM lake_replication.subscription_status "
            "WHERE subscription_name='orders_to_lake'",
            "stopped",
            timeout=30,
        ),
        "stopped",
    )
    sql(
        "source",
        "application",
        "INSERT INTO public.orders (id, note) VALUES (3, 'while-disabled')",
    )
    sql("destination", "analytics", "ALTER SUBSCRIPTION orders_to_lake ENABLE")
    check(
        "enable catches up changes made while stopped",
        wait_for(
            "destination",
            "analytics",
            "SELECT note FROM public.orders WHERE id=3",
            "while-disabled",
            timeout=60,
        ),
        "while-disabled",
    )
    compose("restart", "destination", timeout=180)
    check(
        "destination is ready after container restart",
        wait_for("destination", "analytics", "SELECT 1", "1", timeout=90),
        "1",
    )
    sql(
        "source",
        "application",
        "INSERT INTO public.orders (id, note) VALUES (4, 'after-restart')",
    )
    check(
        "launcher resumes replication after restart",
        wait_for(
            "destination",
            "analytics",
            "SELECT note FROM public.orders WHERE id=4",
            "after-restart",
            timeout=90,
        ),
        "after-restart",
    )
    compose("stop", "source", timeout=120)
    check(
        "source outage becomes observable retry state",
        wait_for(
            "destination",
            "analytics",
            "SELECT health_state || ':' || service_state || ':' || "
            "(consecutive_failures > 0)::text || ':' || "
            "(total_failures > 0)::text || ':' || "
            "(next_retry_at IS NOT NULL)::text || ':' || "
            "(last_failure_at IS NOT NULL)::text || ':' || "
            "coalesce(last_error LIKE 'no safe writable source endpoint:%', false)::text "
            "FROM lake_replication.subscription_status "
            "WHERE subscription_name='orders_to_lake'",
            "degraded:backoff:true:true:true:true:true",
            timeout=60,
        ),
        "degraded:backoff:true:true:true:true:true",
    )
    check(
        "retry metrics mark the subscription unhealthy",
        sql(
            "destination",
            "analytics",
            "SELECT "
            "(max(value) FILTER (WHERE metric='lake_replication_subscription_healthy'))::bigint "
            "|| ':' || "
            "(max(value) FILTER (WHERE metric='lake_replication_consecutive_failures') > 0)::text "
            "|| ':' || "
            "(max(value) FILTER (WHERE metric='lake_replication_failures_total') > 0)::text "
            "FROM lake_replication.metrics WHERE subscription_name='orders_to_lake'",
        ),
        "0:true:true",
    )
    compose("start", "source", timeout=120)
    check(
        "source is ready after outage",
        wait_for("source", "application", "SELECT 1", "1", timeout=60),
        "1",
    )
    check(
        "subscription recovers while retaining last-failure evidence",
        wait_for(
            "destination",
            "analytics",
            "SELECT health_state || ':' || service_state || ':' || "
            "consecutive_failures || ':' || "
            "(total_failures > 0)::text || ':' || "
            "(last_failure_at IS NOT NULL)::text || ':' || "
            "coalesce(last_error LIKE 'no safe writable source endpoint:%', false)::text "
            "FROM lake_replication.subscription_status "
            "WHERE subscription_name='orders_to_lake'",
            "healthy:running:0:true:true:true",
            timeout=90,
        ),
        "healthy:running:0:true:true:true",
    )
    sql(
        "source",
        "application",
        "INSERT INTO public.orders (id, note) VALUES (5, 'after-source-outage')",
    )
    check(
        "replication resumes after source outage",
        wait_for(
            "destination",
            "analytics",
            "SELECT note FROM public.orders WHERE id=5",
            "after-source-outage",
            timeout=90,
        ),
        "after-source-outage",
    )
    verify_structured_logs(worker_logs(), required_events=("cycle_failed",))
    sql("destination", "analytics", "DROP SUBSCRIPTION orders_to_lake")
    check(
        "drop removes the source replication slot",
        wait_for(
            "source",
            "application",
            "SELECT count(*) FROM pg_replication_slots "
            "WHERE slot_name='orders_to_lake_slot'",
            "0",
            timeout=30,
        ),
        "0",
    )


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    exit_code = 1
    started = False
    try:
        archive = archive_path()
        verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix="pg-lake-qualification-") as temp:
            build_images(archive, Path(temp))
        compose("config", "--quiet")
        compose("up", "-d", "--wait", "--wait-timeout", "300", timeout=600)
        started = True
        check(
            "archive-installed extensions load",
            sql(
                "destination",
                "analytics",
                "SELECT string_agg(extname || ':' || extversion, ',' ORDER BY extname) "
                "FROM pg_extension WHERE extname IN "
                "('lake_replication', 'lake_subscription_sugar')",
            ),
            "lake_replication:0.1.0-beta.1,lake_subscription_sugar:1.1",
        )
        verify_upgrade_contract()
        exercise_replication()
        verify_uninstaller()
        EVIDENCE["status"] = "passed"
        exit_code = 0
    except Exception as error:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(error)
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
    finally:
        if started:
            logs = compose("logs", "--no-color", check_result=False)
            (RESULTS / "compose.log").write_text(logs.stdout + logs.stderr)
        compose("down", "-v", "--remove-orphans", check_result=False, timeout=300)
        EVIDENCE["passed"] = exit_code == 0
        (RESULTS / "results.json").write_text(
            json.dumps(EVIDENCE, indent=2, sort_keys=True) + "\n"
        )
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

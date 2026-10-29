#!/usr/bin/env python3
"""Qualify one native release archive on a disposable pg_lake host.

The destination image receives only the exported archive. The repository is
used to build the pg_lake prerequisite image and orchestration tooling, never
mounted into the running source or destination containers.
"""

import hashlib
import importlib.util
import json
import os
import platform
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[2]
VERSION = os.environ.get("VERSION", "0.1.0-beta.4")
RUN = datetime.now(timezone.utc).strftime("qualification_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".qualification" / RUN
COMPOSE_FILE = ROOT / "deploy/qualification/compose.yml"
PG_LAKE_REVISION = os.environ.get(
    "QUALIFICATION_PG_LAKE_REVISION",
    "8180647d4535fdb1ed223f92f301ba6094a8ec3e",
)
if not re.fullmatch(r"[0-9a-f]{40}", PG_LAKE_REVISION):
    raise RuntimeError(
        "QUALIFICATION_PG_LAKE_REVISION must be a full lowercase commit SHA"
    )
PROJECT = f"lake-replication-{RUN.replace('_', '-')}"
SOURCE_IMAGE = f"lake-replication-qualification-source:{RUN}"
DESTINATION_IMAGE = f"lake-replication-qualification-destination:{RUN}"
BASE_IMAGE = f"pg-lake-qualification-base:{PG_LAKE_REVISION[:12]}"
ADMIN_PASSWORD = secrets.token_urlsafe(24)
SOURCE_PASSWORD = secrets.token_urlsafe(24) + "@:/%"
ROTATED_SOURCE_PASSWORD = secrets.token_urlsafe(24) + "?#@"
ENV = {
    **os.environ,
    "QUALIFICATION_SOURCE_IMAGE": SOURCE_IMAGE,
    "QUALIFICATION_DESTINATION_IMAGE": DESTINATION_IMAGE,
    "QUAL_ADMIN_PASSWORD": ADMIN_PASSWORD,
    "QUAL_SOURCE_PASSWORD": SOURCE_PASSWORD,
    "QUAL_ROTATED_SOURCE_PASSWORD": ROTATED_SOURCE_PASSWORD,
}
EVIDENCE = {
    "run": RUN,
    "pg_lake_revision": PG_LAKE_REVISION,
    "checks": [],
}


def command(args, *, check_result=True, timeout=1200, cwd=ROOT, capture_output=True, **kwargs):
    result = subprocess.run(
        args,
        cwd=cwd,
        env=ENV,
        text=True,
        capture_output=capture_output,
        timeout=timeout,
        **kwargs,
    )
    if check_result and result.returncode:
        raise RuntimeError((result.stderr or "").strip() or (result.stdout or "").strip()
                           or f"{args[0]} exited with status {result.returncode}")
    return result


def compose(*args, **kwargs):
    return command(
        ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), *args],
        **kwargs,
    )


def sql_result(service, database, statement, *, check_result=True):
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
    )


def sql(service, database, statement, *, check_result=True):
    return sql_result(
        service, database, statement, check_result=check_result
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
        f"lake_replication-{VERSION}-linux-{native_architecture()}.tar.gz"
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


def build_storage_images():
    spec=importlib.util.spec_from_file_location('qualification_storage',ROOT/'tests/qualification/storage.py')
    storage=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(storage)
    return storage.ensure(command,ENV,EVIDENCE,ROOT,RESULTS,native_architecture())


def build_images(archive, context):
    # Fail before the expensive prerequisite compilation if S3 fixtures cannot
    # be prepared. Reuse only locally verified source/recipe/architecture IDs.
    build_storage_images()
    spec=importlib.util.spec_from_file_location('qualification_prerequisite',ROOT/'tests/qualification/prerequisite.py')
    prerequisite=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(prerequisite)
    prerequisite.ensure(command,ROOT,PG_LAKE_REVISION,BASE_IMAGE)
    expected=prerequisite.identity(ROOT,PG_LAKE_REVISION,native_architecture())
    EVIDENCE['prerequisite']={'identity':expected,'image_id':prerequisite.image(command,BASE_IMAGE,expected)}
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
        ADMIN_PASSWORD not in logs
        and SOURCE_PASSWORD not in logs
        and quote(SOURCE_PASSWORD, safe="") not in logs
        and ROTATED_SOURCE_PASSWORD not in logs
        and quote(ROTATED_SOURCE_PASSWORD, safe="") not in logs,
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
test -f "$(pg_config --pkglibdir)/lake_replication.so"
/qualification/uninstall.sh
test ! -e /usr/local/bin/lake-replication-supervisor
test ! -e "$(pg_config --pkglibdir)/lake_replication.so"
test ! -e "$(pg_config --sharedir)/extension/lake_replication.control"
test ! -e "$(pg_config --sharedir)/extension/lake_replication--0.1.0-beta.4.sql"
test ! -e "$(pg_config --sharedir)/extension/lake_replication--0.1.0-beta.3--0.1.0-beta.4.sql"
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
            "CREATE EXTENSION dblink; "
            "CREATE EXTENSION lake_replication VERSION '0.1.0';",
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
            "ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.4';",
        )
        sql(
            "destination",
            database,
            f"INSERT INTO lake_replication.pending_cleanup "
            "(subscription_id, source_slot_name) VALUES "
            f"({subscription_id}, 'upgrade_slot'); "
            "INSERT INTO lake_replication.source_connections "
            "(subscription_id, conninfo) VALUES "
            f"({subscription_id}, 'host=source dbname=application'); "
            "INSERT INTO lake_replication.worker_launch_config "
            "(subscription_id, slot_name, publication_name, source_database, "
            "destination_database, source_address, destination_address, "
            "destination_schema, desired_state, origin_system_identifier, origin_database_oid) VALUES "
            f"({subscription_id}, 'upgrade_slot', 'upgrade_pub', 'application', "
            "'upgrade_contract', 'source:5432', '/var/run/postgresql:5432', 'public', 'stopped', (SELECT system_identifier FROM pg_control_system()), (SELECT oid FROM pg_database WHERE datname=current_database()));",
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
            "0.1.0-beta.4:upgrade_fixture:0/10:degraded",
        )
        check(
            "extension upgrade installs common parameterized type resolution",
            sql(
                "destination",
                database,
                "SELECT lake_replication.resolve_destination_type(1, 'numeric(12,2)') "
                "|| ':' || lake_replication.resolve_destination_type("
                "1, 'character varying(32)')",
            ),
            "numeric(12,2):character varying(32)",
        )
        check(
            "extension upgrade adds per-subscription credential metadata",
            sql(
                "destination",
                database,
                "SELECT (credential_file IS NULL)::text || ':' || config_generation "
                "FROM lake_replication.worker_launch_config "
                f"WHERE subscription_id={subscription_id}",
            ),
            "true:1",
        )
        check(
            "upgrade registers durable extension state for logical backup",
            sql(
                "destination",
                database,
                "SELECT "
                "(SELECT cardinality(extconfig) FROM pg_extension "
                "WHERE extname='lake_replication') || ':' || "
                "(SELECT count(*) FROM lake_replication.worker_launch_config)",
            ),
            "20:1",
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
            and "COPY lake_replication.worker_launch_config" in logical_dump
            and "COPY lake_replication.source_connections" in logical_dump
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
                "(SELECT count(*) FROM lake_replication.subscriptions) || ':' || "
                "(SELECT count(*) FROM lake_replication.worker_launch_config)",
            ),
            "0.1.0-beta.4:1:1",
        )
        check(
            "no unsupported SQL downgrade path is advertised",
            sql(
                "destination",
                database,
                "SELECT count(*) FROM pg_extension_update_paths('lake_replication') "
                "WHERE source='0.1.0-beta.4' AND target='0.1.0' "
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
        f"postgres://lake_source:{quote(SOURCE_PASSWORD, safe='')}"
        "@source:5432/application"
    )
    for option_name, option in (
        ("enabled", "enabled = false"),
        ("copy_data", "copy_data = false"),
    ):
        unsupported = sql_result(
            "destination", "analytics",
            "CREATE SUBSCRIPTION unsupported_lake "
            f"CONNECTION '{connection}' PUBLICATION orders_pub "
            f"WITH (lake_target = 'iceberg', {option})",
            check_result=False,
        )
        check(
            f"unsupported {option_name} fails instead of being ignored",
            unsupported.returncode != 0
            and f'option "{option_name}" is not supported'
            in unsupported.stdout + unsupported.stderr,
            True,
        )
    for transaction in (
        "BEGIN; CREATE SUBSCRIPTION rolled_back_lake "
        f"CONNECTION '{connection}' PUBLICATION orders_pub "
        "WITH (lake_target = 'iceberg'); ROLLBACK;",
        "BEGIN; SAVEPOINT before_subscription; "
        "CREATE SUBSCRIPTION savepoint_lake "
        f"CONNECTION '{connection}' PUBLICATION orders_pub "
        "WITH (lake_target = 'iceberg'); "
        "ROLLBACK TO SAVEPOINT before_subscription; COMMIT;",
    ):
        sql("destination", "analytics", transaction)
    check(
        "rolled-back subscriptions leave no source slots or control rows",
        sql(
            "source", "application",
            "SELECT count(*) FROM pg_replication_slots "
            "WHERE slot_name IN ('rolled_back_lake', 'savepoint_lake')",
        ) + ":" + sql(
            "destination", "analytics",
            "SELECT count(*) FROM lake_replication.subscriptions "
            "WHERE subscription_name IN ('rolled_back_lake', 'savepoint_lake')",
        ),
        "0:0",
    )
    create_result = sql_result(
        "destination",
        "analytics",
        f"""
CREATE SUBSCRIPTION orders_to_lake
  CONNECTION '{connection}'
  PUBLICATION orders_pub
  WITH (lake_target = 'iceberg',
        lake_credential_file = '/credentials/destination-only.env');
""",
    )
    check(
        "subscription notice does not expose the source password",
        SOURCE_PASSWORD not in create_result.stdout + create_result.stderr
        and quote(SOURCE_PASSWORD, safe="") not in create_result.stdout + create_result.stderr,
        True,
    )
    check(
        "subscription records its selected credential file",
        sql(
            "destination",
            "analytics",
            "SELECT credential_file || ':' || config_generation "
            "FROM lake_replication.worker_launch_config c "
            "JOIN lake_replication.subscriptions s USING (subscription_id) "
            "WHERE s.subscription_name='orders_to_lake'",
        ),
        "/credentials/destination-only.env:1",
    )
    check(
        "postgres URI credentials are stored for workers without a source password file",
        sql(
            "destination", "analytics",
            "SELECT source_user || ':' || (source_password IS NOT NULL)::text "
            "FROM lake_replication.source_connections p "
            "JOIN lake_replication.subscriptions s USING (subscription_id) "
            "WHERE s.subscription_name='orders_to_lake'",
        ),
        "lake_source:true",
    )
    check(
        "pending cleanup contains no connection or password columns",
        sql(
            "destination", "analytics",
            "SELECT count(*) FROM information_schema.columns "
            "WHERE table_schema='lake_replication' "
            "AND table_name='pending_cleanup' "
            "AND column_name IN ('source_conninfo', 'source_password', 'conninfo')",
        ),
        "0",
    )
    sql(
        "destination", "analytics",
        "CREATE ROLE lake_secret_reader LOGIN; "
        "GRANT pg_read_all_data TO lake_secret_reader",
    )
    check(
        "pg_read_all_data cannot read stored connection secrets or invoke the worker getter",
        sql(
            "destination", "analytics",
            "SET ROLE lake_secret_reader; "
            "SELECT (SELECT count(*) FROM lake_replication.source_connections)::text "
            "|| ':' || has_function_privilege(current_user, "
            "'lake_replication.source_credentials(bigint)', 'EXECUTE')::text",
        ).endswith("0:false"),
        True,
    )
    relative_path = sql_result(
        "destination",
        "analytics",
        "ALTER SUBSCRIPTION orders_to_lake SET "
        "(lake_credential_file = 'relative.env')",
        check_result=False,
    )
    check(
        "relative per-subscription credential paths are rejected without mutation",
        relative_path.returncode != 0
        and "must be an absolute path" in (relative_path.stdout + relative_path.stderr)
        and sql(
            "destination",
            "analytics",
            "SELECT credential_file || ':' || config_generation "
            "FROM lake_replication.worker_launch_config c "
            "JOIN lake_replication.subscriptions s USING (subscription_id) "
            "WHERE s.subscription_name='orders_to_lake'",
        )
        == "/credentials/destination-only.env:1",
        True,
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
        "initial sync automatically creates a persistent source pgoutput slot",
        sql(
            "source", "application",
            "SELECT plugin || ':' || slot_type || ':' || temporary::text "
            "|| ':' || failover::text FROM pg_replication_slots "
            "WHERE slot_name='orders_to_lake'",
        ),
        "pgoutput:logical:false:true",
    )
    check(
        "common parameterized types retain schema and values",
        sql(
            "destination",
            "analytics",
            "SELECT (SELECT string_agg(attname || ':' || "
            "format_type(atttypid,atttypmod), ',' ORDER BY attnum) "
            "FROM pg_attribute WHERE attrelid='public.orders'::regclass "
            "AND attname IN ('amount','code','region','label')) || ':' || "
            "amount::text || ':' || code || ':' || region || ':' || label "
            "FROM public.orders WHERE id=1",
        ),
        "amount:numeric(12,2),code:character varying(32),region:character(2),"
        "label:character varying:42.50:initial-sku:DE:initial-label",
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
        "INSERT INTO public.orders (id, note, amount, code, region, label) "
        "VALUES (2, 'streamed', 99.99, 'streamed-sku', 'US', 'streamed-label'); "
        "UPDATE public.orders SET note='initial-updated', amount=43.75, "
        "code='updated-sku', label='updated-label' WHERE id=1;",
    )
    check(
        "live insert and update replicate",
        wait_for(
            "destination",
            "analytics",
            "SELECT string_agg(id || '=' || note || ':' || amount::text || ':' || "
            "code || ':' || region || ':' || label, ',' ORDER BY id) FROM public.orders",
            "1=initial-updated:43.75:updated-sku:DE:updated-label,"
            "2=streamed:99.99:streamed-sku:US:streamed-label",
            timeout=60,
        ),
        "1=initial-updated:43.75:updated-sku:DE:updated-label,"
        "2=streamed:99.99:streamed-sku:US:streamed-label",
    )
    sql(
        "source",
        "application",
        "ALTER TABLE public.orders ADD COLUMN discount numeric(6,2); "
        "UPDATE public.orders SET discount=1.25 WHERE id=1;",
    )
    check(
        "nullable parameterized column is added and replicated online",
        wait_for(
            "destination",
            "analytics",
            "SELECT format_type(a.atttypid,a.atttypmod) || ':' || "
            "(SELECT string_agg(id || '=' || coalesce(discount::text,'NULL'), "
            "',' ORDER BY id) FROM public.orders) "
            "FROM pg_attribute a WHERE a.attrelid='public.orders'::regclass "
            "AND a.attname='discount'",
            "numeric(6,2):1=1.25,2=NULL",
            timeout=60,
        ),
        "numeric(6,2):1=1.25,2=NULL",
    )
    previous_supervisor = sql(
        "destination",
        "analytics",
        "SELECT supervisor_pid FROM lake_replication.service_status ss "
        "JOIN lake_replication.subscriptions s USING (subscription_id) "
        "WHERE s.subscription_name='orders_to_lake'",
    )
    sql("destination", "analytics", "ALTER SUBSCRIPTION orders_to_lake DISABLE")
    check(
        "supervisor stops before the source password changes",
        wait_for(
            "destination", "analytics",
            "SELECT health_state FROM lake_replication.subscription_status "
            "WHERE subscription_name='orders_to_lake'",
            "stopped", timeout=30,
        ),
        "stopped",
    )
    sql(
        "source",
        "application",
        f"ALTER ROLE lake_source PASSWORD '{ROTATED_SOURCE_PASSWORD}'",
    )
    rotated_connection = (
        f"postgresql://lake_source:{quote(ROTATED_SOURCE_PASSWORD, safe='')}"
        "@source:5432/application"
    )
    sql(
        "destination", "analytics",
        f"ALTER SUBSCRIPTION orders_to_lake CONNECTION '{rotated_connection}'",
    )
    # Connection replacement and re-enable each advance the managed generation.
    sql("destination", "analytics", "ALTER SUBSCRIPTION orders_to_lake ENABLE")
    check(
        "postgresql URI credential rotation replaces the subscription supervisor",
        wait_for(
            "destination",
            "analytics",
            "SELECT c.credential_file || ':' || c.config_generation || ':' || "
            f"(ss.supervisor_pid <> {previous_supervisor})::text || ':' || ss.state "
            "FROM lake_replication.worker_launch_config c "
            "JOIN lake_replication.subscriptions s USING (subscription_id) "
            "JOIN lake_replication.service_status ss USING (subscription_id) "
            "WHERE s.subscription_name='orders_to_lake'",
            "/credentials/destination-only.env:3:true:running",
            timeout=60,
        ),
        "/credentials/destination-only.env:3:true:running",
    )
    sql(
        "source",
        "application",
        "INSERT INTO public.orders (id, note) VALUES (20, 'rotated-credential')",
    )
    check(
        "replication authenticates with the rotated credential",
        wait_for(
            "destination",
            "analytics",
            "SELECT note FROM public.orders WHERE id=20",
            "rotated-credential",
            timeout=60,
        ),
        "rotated-credential",
    )

    rotated_supervisor = sql(
        "destination",
        "analytics",
        "SELECT supervisor_pid FROM lake_replication.service_status ss "
        "JOIN lake_replication.subscriptions s USING (subscription_id) "
        "WHERE s.subscription_name='orders_to_lake'",
    )
    sql("destination", "analytics", "ALTER SUBSCRIPTION orders_to_lake DISABLE")
    check(
        "supervisor stops before restoring the source password",
        wait_for(
            "destination", "analytics",
            "SELECT health_state FROM lake_replication.subscription_status "
            "WHERE subscription_name='orders_to_lake'",
            "stopped", timeout=30,
        ),
        "stopped",
    )
    sql(
        "source",
        "application",
        f"ALTER ROLE lake_source PASSWORD '{SOURCE_PASSWORD}'",
    )
    sql(
        "destination", "analytics",
        f"ALTER SUBSCRIPTION orders_to_lake CONNECTION '{connection}'",
    )
    sql("destination", "analytics", "ALTER SUBSCRIPTION orders_to_lake ENABLE")
    check(
        "CONNECTION can return to the original password",
        wait_for(
            "destination",
            "analytics",
            "SELECT c.credential_file || ':' || c.config_generation || ':' || "
            f"(ss.supervisor_pid <> {rotated_supervisor})::text || ':' || ss.state "
            "FROM lake_replication.worker_launch_config c "
            "JOIN lake_replication.subscriptions s USING (subscription_id) "
            "JOIN lake_replication.service_status ss USING (subscription_id) "
            "WHERE s.subscription_name='orders_to_lake'",
            "/credentials/destination-only.env:5:true:running",
            timeout=60,
        ),
        "/credentials/destination-only.env:5:true:running",
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
            "WHERE slot_name='orders_to_lake'",
            "0",
            timeout=30,
        ),
        "0",
    )


    sql(
        "source", "application",
        "CREATE TABLE public.native_slot_items (id bigint PRIMARY KEY, note text NOT NULL); "
        "INSERT INTO public.native_slot_items VALUES (1, 'snapshot'); "
        "CREATE PUBLICATION native_slot_pub FOR TABLE public.native_slot_items; "
        "GRANT SELECT ON public.native_slot_items TO lake_source; "
        "SELECT pg_create_logical_replication_slot('native_slot_existing', 'pgoutput')",
    )
    sql(
        "destination", "analytics",
        "CREATE SUBSCRIPTION native_slot_lake "
        f"CONNECTION '{connection}' PUBLICATION native_slot_pub "
        "WITH (lake_target = 'iceberg', create_slot = false, "
        "slot_name = 'native_slot_existing')",
    )
    check(
        "pre-created slot supplies a snapshot copy",
        wait_for(
            "destination", "analytics",
            "SELECT note FROM public.native_slot_items WHERE id=1",
            "snapshot", timeout=90,
        ),
        "snapshot",
    )
    sql("source", "application", "INSERT INTO public.native_slot_items VALUES (2, 'streamed')")
    check(
        "pre-created slot streams later changes",
        wait_for(
            "destination", "analytics",
            "SELECT note FROM public.native_slot_items WHERE id=2",
            "streamed", timeout=90,
        ),
        "streamed",
    )
    check(
        "temporary snapshot slot is released",
        sql(
            "source", "application",
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'lake_sync_%'",
        ),
        "0",
    )
    sql("destination", "analytics", "DROP SUBSCRIPTION native_slot_lake")
    check(
        "drop removes the pre-created slot",
        sql(
            "source", "application",
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name='native_slot_existing'",
        ),
        "0",
    )


def exercise_interrupted_initial_copy():
    sql(
        "source", "application",
        "CREATE TABLE public.recovery_items (id bigint PRIMARY KEY, note text NOT NULL); "
        "INSERT INTO public.recovery_items VALUES (1, 'one'), (2, 'two'), (3, 'three'); "
        "CREATE PUBLICATION recovery_pub FOR TABLE public.recovery_items; "
        "GRANT SELECT ON public.recovery_items TO lake_source",
    )
    identity = sql(
        "source", "application",
        "SELECT (pg_control_system()).system_identifier || ':' || "
        "(SELECT oid FROM pg_database WHERE datname=current_database()) || ':' || "
        "(pg_control_checkpoint()).timeline_id",
    ).split(":")
    subscription_id = sql(
        "destination", "analytics",
        "SELECT lake_replication.create_subscription("
        f"{identity[0]}, {identity[1]}, {identity[2]}, "
        "'recovery_lake', 'recovery_lake', ARRAY['recovery_pub'])",
    )
    worker_env = [
        "LAKE_CREDENTIAL_FILE=/credentials/worker.env",
        "LAKE_SOURCE_ADDRESS=source:5432",
        "LAKE_DESTINATION_ADDRESS=127.0.0.1:5432",
        "LAKE_SOURCE_DATABASE=application",
        "LAKE_DESTINATION_DATABASE=analytics",
        "LAKE_SLOT=recovery_lake",
        "LAKE_PUBLICATION=recovery_pub",
        f"LAKE_SUBSCRIPTION_ID={subscription_id}",
        "LAKE_DISCOVER_PUBLICATION=1",
        "LAKE_DESTINATION_SCHEMA=public",
        "LAKE_CREATE_TABLES_USING_ICEBERG=1",
        "LAKE_INITIAL_COPY_CHUNK_ROWS=1",
    ]

    def initial_copy(*extra):
        return compose(
            "exec", "-T", "-u", "postgres", "destination", "env",
            *worker_env, *extra, "/usr/local/bin/lake-replication-initial-sync",
            check_result=False, timeout=180,
        )

    interrupted = initial_copy(
        "LAKE_INITIAL_COPY_FAIL_AFTER_ROWS=1",
        "LAKE_INITIAL_COPY_TABLE_RETRIES=0",
    )
    staged = sql(
        "destination", "analytics",
        "SELECT count(*) FROM lake_replication.initial_copy_rows "
        f"WHERE subscription_id={subscription_id}",
    )
    check("interrupted initial copy keeps staged rows", interrupted.returncode != 0 and staged == "1", True)
    resumed = initial_copy()
    check("initial copy restarts from a fresh snapshot", resumed.returncode, 0)
    check(
        "restarted copy publishes each source row once",
        sql("destination", "analytics", "SELECT string_agg(note, ',' ORDER BY id) FROM public.recovery_items"),
        "one,two,three",
    )
    check(
        "restarted copy advances the checkpoint and retains one source slot",
        sql(
            "destination", "analytics",
            "SELECT (received_lsn > '0/0')::text FROM lake_replication.checkpoints "
            f"WHERE subscription_id={subscription_id}",
        ) + ":" + sql(
            "source", "application",
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name='recovery_lake'",
        ),
        "true:1",
    )
    sql("source", "application", "INSERT INTO public.recovery_items VALUES (4, 'after-restart')")
    received = compose(
        "exec", "-T", "-u", "postgres", "destination", "env",
        *worker_env, "LAKE_MAX_TRANSACTIONS=1",
        "LAKE_RECEIVER_IDLE_TIMEOUT_MS=5000",
        "/usr/local/bin/lake-replication-receiver",
        check_result=False, timeout=30,
    )
    check("persistent slot streams after restarted copy", received.returncode, 0)
    published = compose(
        "exec", "-T", "-u", "postgres", "destination", "env",
        *worker_env, "/usr/local/bin/lake-replication-publisher",
        check_result=False, timeout=30,
    )
    check("recovered stream publishes new changes", published.returncode, 0)
    check(
        "change after interrupted copy appears at destination",
        sql("destination", "analytics", "SELECT note FROM public.recovery_items WHERE id=4"),
        "after-restart",
    )
    sql("source", "application", "SELECT pg_drop_replication_slot('recovery_lake')")


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
            "archive-installed extension loads",
            sql(
                "destination",
                "analytics",
                "SELECT string_agg(extname || ':' || extversion, ',' ORDER BY extname) "
                "FROM pg_extension WHERE extname IN "
                "('lake_replication')",
            ),
            "lake_replication:0.1.0-beta.4",
        )
        verify_upgrade_contract()
        exercise_replication()
        exercise_interrupted_initial_copy()
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

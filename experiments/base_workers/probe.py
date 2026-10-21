#!/usr/bin/env python3
"""Exercise the real replication launcher through pg_extension_base, in isolation."""

import importlib.util
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[2]
HARDENED = "--hardened" in sys.argv[1:]
STATIC = "--static" in sys.argv[1:]
if STATIC:
    HARDENED = True
spec = importlib.util.spec_from_file_location("qualification", ROOT / "tests/qualification/probe.py")
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
q.EVIDENCE["experiment"] = "pg_extension_base adapter for the existing replication launcher"
q.EVIDENCE["production_launcher_changed"] = False
q.EVIDENCE["hardened_adapter"] = HARDENED
q.EVIDENCE["launcher_variant"] = "static" if STATIC else "base"
q.EVIDENCE["findings"] = []


def worker_pid(database):
    return q.sql("destination", database, "SELECT coalesce(max(w.pid),0) FROM extension_base.list_base_workers() w JOIN pg_extension e ON e.oid=w.extension_id WHERE e.extname='lake_replication' AND w.database_id=(SELECT oid FROM pg_database WHERE datname=current_database())")


def await_worker(database):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        pid = worker_pid(database)
        if pid != "0":
            return pid
        time.sleep(0.25)
    raise AssertionError(f"no base worker in {database}")


def install_database(database):
    q.sql("destination", "analytics", f"CREATE DATABASE {database}")
    q.sql("destination", database, "CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication")
    return await_worker(database)


def observe(name, operation):
    try:
        operation()
    except Exception as error:
        q.EVIDENCE["findings"].append({"case": name, "error": str(error)})
        print(f"FINDING {name}: {error}", flush=True)


def lifecycle_without_subscriptions():
    first = await_worker("analytics")
    second = install_database("prototype_second")
    q.check("different databases get different launcher processes", first != second, True)
    q.check("only pg_extension_base is explicitly preloaded", q.sql("destination", "analytics", "SHOW shared_preload_libraries"), "pg_extension_base")
    q.check("no session preload is needed", q.sql("destination", "analytics", "SHOW session_preload_libraries"), "")
    q.check("no configured launcher database is needed", q.sql("destination", "analytics", "SHOW lake_replication.database_name"), "")
    q.check("legacy static launcher is absent", q.sql("destination", "analytics", "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication launcher'"), "0")

    q.sql("destination", "analytics", "CREATE DATABASE prototype_aborted")
    q.sql("destination", "prototype_aborted", "CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; BEGIN; CREATE EXTENSION lake_replication; ROLLBACK")
    q.check("rolled-back extension creation leaves no registration", q.sql("destination", "prototype_aborted", "SELECT count(*) FROM extension_base.workers WHERE extension_name='lake_replication'"), "0")
    q.sql("destination", "analytics", "DROP DATABASE prototype_aborted")

    q.sql("destination", "prototype_second", "BEGIN; DROP EXTENSION lake_replication CASCADE; ROLLBACK")
    q.check("rolled-back extension drop resumes the worker", int(await_worker("prototype_second")) > 0, True)
    pid = worker_pid("prototype_second")
    q.sql("destination", "prototype_second", "DROP EXTENSION lake_replication CASCADE")
    q.check("committed extension drop stops the launcher", q.wait_for("destination", "analytics", f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid}", "0"), "0")
    q.sql("destination", "prototype_second", "CREATE EXTENSION lake_replication")
    await_worker("prototype_second")
    q.check("extension recreation registers exactly one worker", q.sql("destination", "prototype_second", "SELECT count(*) FROM extension_base.workers WHERE extension_name='lake_replication'"), "1")


def replica_note(database):
    return q.sql("destination", database, "SELECT string_agg(note,',' ORDER BY id) FROM public.prototype_items")


def wait_note(database, expected):
    q.check(f"{database} replication reaches {expected}", q.wait_for("destination", database, "SELECT string_agg(note,',' ORDER BY id) FROM public.prototype_items", expected, timeout=60), expected)


def live_lifecycle():
    q.sql("source", "application", "CREATE TABLE public.prototype_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO public.prototype_items VALUES(1,'one'); CREATE PUBLICATION prototype_pub FOR TABLE public.prototype_items; GRANT SELECT ON public.prototype_items TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    for database, subscription in (("analytics", "prototype_a"), ("prototype_second", "prototype_b")):
        q.sql("destination", database, f"CREATE SUBSCRIPTION {subscription} CONNECTION '{connection}' PUBLICATION prototype_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        wait_note(database, "one")
    q.sql("source", "application", "INSERT INTO public.prototype_items VALUES(2,'two')")
    for database in ("analytics", "prototype_second"):
        wait_note(database, "one,two")

    before = q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()")
    pid = worker_pid("prototype_second")
    q.sql("destination", "analytics", f"SELECT pg_terminate_backend({pid})")
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline and worker_pid("prototype_second") in ("0", pid):
        time.sleep(0.25)
    q.check("terminated base launcher is replaced", worker_pid("prototype_second") not in ("0", pid), True)
    q.check("launcher termination does not restart PostgreSQL", q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()"), before)
    q.sql("destination", "prototype_second", "ALTER SUBSCRIPTION prototype_b DISABLE")
    q.check("disable works after launcher replacement", q.wait_for("destination", "prototype_second", "SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='prototype_b'", "stopped", timeout=30), "stopped")
    q.sql("destination", "prototype_second", "ALTER SUBSCRIPTION prototype_b ENABLE")

    q.compose("restart", "destination", timeout=180)
    q.wait_for("destination", "analytics", "SELECT 1", "1", timeout=90)
    await_worker("analytics")
    await_worker("prototype_second")
    q.sql("source", "application", "INSERT INTO public.prototype_items VALUES(3,'three')")
    for database in ("analytics", "prototype_second"):
        wait_note(database, "one,two,three")
    q.sql("destination", "analytics", "DROP SUBSCRIPTION prototype_a")
    q.sql("destination", "prototype_second", "DROP SUBSCRIPTION prototype_b")


def template_and_failed_drop():
    q.sql("destination", "analytics", "CREATE DATABASE prototype_template IS_TEMPLATE true")
    q.sql("destination", "prototype_template", "CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication")
    q.check("registered workers do not start in a template database", q.sql("destination", "analytics", "SELECT count(*) FROM pg_stat_activity WHERE datname='prototype_template'"), "0")
    q.sql("destination", "analytics", "CREATE DATABASE prototype_clone TEMPLATE prototype_template")
    q.check("template clone discovers its registered launcher", int(await_worker("prototype_clone")) > 0, True)

    # Hold an ordinary client connection so DROP DATABASE must fail.
    held = subprocess.Popen(["docker", "compose", "-p", q.PROJECT, "-f", str(q.COMPOSE_FILE), "exec", "-T", "destination", "psql", "-XAt", "-U", "postgres", "-d", "prototype_clone", "-c", "SELECT pg_sleep(30)"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=q.ENV)
    try:
        q.wait_for("destination", "analytics", "SELECT count(*) FROM pg_stat_activity WHERE datname='prototype_clone' AND query='SELECT pg_sleep(30)'", "1", timeout=10)
        failed = q.sql_result("destination", "analytics", "DROP DATABASE prototype_clone", check_result=False)
        q.check("drop database fails while an ordinary client is connected", failed.returncode != 0, True)
        q.check("failed database drop resumes its launcher", int(await_worker("prototype_clone")) > 0, True)
    finally:
        q.sql("destination", "analytics", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='prototype_clone' AND query='SELECT pg_sleep(30)'")
        held.wait(timeout=15)
    q.sql("destination", "analytics", "DROP DATABASE prototype_clone")
    q.check("committed database drop completes", q.sql("destination", "analytics", "SELECT count(*) FROM pg_database WHERE datname='prototype_clone'"), "0")


def active_extension_drop():
    install_database("prototype_drop")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    q.sql("destination", "prototype_drop", f"CREATE SUBSCRIPTION prototype_drop_sub CONNECTION '{connection}' PUBLICATION prototype_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    wait_note("prototype_drop", "one,two,three")
    child = q.sql("destination", "prototype_drop", "SELECT supervisor_pid FROM lake_replication.service_status")
    assert child.isdigit()
    launcher = worker_pid("prototype_drop")
    before = q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()")
    q.compose("exec", "-T", "destination", "kill", "-STOP", child)
    try:
        q.sql("destination", "prototype_drop", "SET statement_timeout='12s'; DROP EXTENSION lake_replication CASCADE")
        remaining = q.wait_for("destination", "analytics", f"SELECT count(*) FROM pg_stat_activity WHERE datname='prototype_drop' AND (backend_type='lake_replication supervisor' OR pid={launcher})", "0", timeout=10)
        alive = q.compose("exec", "-T", "destination", "sh", "-c", f"kill -0 {child} 2>/dev/null", check_result=False).returncode == 0
        # Record both independent cleanup observations before reporting failure.
        for name, actual, expected in (
            ("extension drop removes its replication worker connections", remaining, "0"),
            ("extension drop explicitly terminates a paused external supervisor", alive, False),
            ("active extension drop does not restart PostgreSQL", q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()"), before),
        ):
            observe(name, lambda n=name, a=actual, e=expected: q.check(n, a, e))
    finally:
        q.compose("exec", "-T", "destination", "sh", "-c", f"kill -CONT {child} 2>/dev/null || true", check_result=False)
        q.compose("exec", "-T", "destination", "sh", "-c", f"kill -TERM {child} 2>/dev/null || true", check_result=False)
    q.sql("destination", "analytics", "DROP DATABASE prototype_drop WITH (FORCE)")


def cloned_configuration():
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    q.sql("destination", "prototype_template", f"CREATE SUBSCRIPTION prototype_seed CONNECTION '{connection}' PUBLICATION prototype_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env'); ALTER SUBSCRIPTION prototype_seed DISABLE")
    q.sql("destination", "analytics", "CREATE DATABASE prototype_config_clone TEMPLATE prototype_template")
    await_worker("prototype_config_clone")
    # Keep the copied subscription stopped: inspect ownership without allowing
    # it to write into a table/catalog cloned from another destination.
    q.check("cloned subscription stays stopped", q.sql("destination", "prototype_config_clone", "SELECT desired_state FROM lake_replication.worker_launch_config"), "stopped")
    if HARDENED:
        q.check("cloned subscription is rejected with a durable diagnostic", q.wait_for("destination", "prototype_config_clone", "SELECT (state='error' AND last_error LIKE '%inherited destination binding%')::text FROM lake_replication.subscriptions WHERE subscription_name='prototype_seed'", "true"), "true")
        q.sql("destination", "prototype_config_clone", "UPDATE lake_replication.worker_launch_config SET desired_state='running'")
        q.check("attempted enable of inherited binding is fenced", q.wait_for("destination", "prototype_config_clone", "SELECT desired_state FROM lake_replication.worker_launch_config", "stopped"), "stopped")
        version = q.sql("destination", "prototype_config_clone", "SELECT xmin::text FROM lake_replication.subscriptions WHERE subscription_name='prototype_seed'")
        time.sleep(3)
        q.check("unchanged binding errors do not rewrite subscription rows", q.sql("destination", "prototype_config_clone", "SELECT xmin::text FROM lake_replication.subscriptions WHERE subscription_name='prototype_seed'"), version)
    else:
        q.check("cloned launch configuration binds to the new database", q.sql("destination", "prototype_config_clone", "SELECT count(*) FROM lake_replication.worker_launch_config WHERE destination_database <> current_database()"), "0")


def worker_churn():
    """Exercise handle retirement, not merely a successful one-time launch."""
    launcher = (q.sql("destination", "analytics", "SELECT pid FROM pg_stat_activity WHERE backend_type='lake_replication launcher'")
                if STATIC else await_worker("analytics"))
    pattern = re.compile(r"lake_replication worker handles: (\d+) total .*; (\d+) used")

    def handle_memory():
        def contexts():
            logs = q.compose("logs", "--no-color", "destination")
            return pattern.findall(logs.stdout + logs.stderr)
        before = len(contexts())
        q.sql("destination", "analytics", f"SELECT pg_log_backend_memory_contexts({launcher})")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            found = contexts()
            if len(found) > before:
                total, used = found[-1]
                return {"total_bytes": int(total), "used_bytes": int(used)}
            time.sleep(0.2)
        raise AssertionError("launcher did not report its handle memory context")

    q.sql("source", "application", "CREATE TABLE public.prototype_churn(id bigint PRIMARY KEY); CREATE PUBLICATION prototype_churn_pub FOR TABLE public.prototype_churn; GRANT SELECT ON public.prototype_churn TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    q.sql("destination", "analytics", f"CREATE SUBSCRIPTION prototype_churn_sub CONNECTION '{connection}' PUBLICATION prototype_churn_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    try:
        q.check("churn subscription finishes its initial copy", q.wait_for("destination", "analytics", "SELECT state FROM lake_replication.subscriptions WHERE subscription_name='prototype_churn_sub'", "streaming"), "streaming")
        child = q.sql("destination", "analytics", "SELECT supervisor_pid FROM lake_replication.service_status ss JOIN lake_replication.subscriptions s USING(subscription_id) WHERE s.subscription_name='prototype_churn_sub'")
        before = q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()")
        q.compose("exec", "-T", "destination", "kill", "-STOP", child)
        q.sql("destination", "analytics", "ALTER SUBSCRIPTION prototype_churn_sub DISABLE")
        count = "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'"
        q.check("churn starts with all subscription wrappers retired", q.wait_for("destination", "analytics", count, "0"), "0")
        alive = q.compose("exec", "-T", "destination", "sh", "-c", f"kill -0 {child} 2>/dev/null", check_result=False).returncode == 0
        q.check("disable explicitly reaps a paused supervisor in the shared runtime", alive, False)
        q.check("paused supervisor disable does not restart PostgreSQL", q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()"), before)
        baseline = handle_memory()
        for cycle in range(10):
            q.sql("destination", "analytics", "ALTER SUBSCRIPTION prototype_churn_sub ENABLE")
            q.check(f"churn cycle {cycle + 1} starts exactly one wrapper", q.wait_for("destination", "analytics", count, "1"), "1")
            q.sql("destination", "analytics", "ALTER SUBSCRIPTION prototype_churn_sub DISABLE")
            q.check(f"churn cycle {cycle + 1} retires its wrapper", q.wait_for("destination", "analytics", count, "0"), "0")
        final = handle_memory()
        q.EVIDENCE["handle_churn"] = {"cycles": 10, "before": baseline, "after": final}
        q.check("retired handles return to the original memory usage", final, baseline)
    finally:
        q.sql("destination", "analytics", "DROP SUBSCRIPTION prototype_churn_sub", check_result=False)


def main():
    q.RESULTS.mkdir(parents=True, exist_ok=False)
    started = False
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        revision = q.EVIDENCE["source_revision"]
        q.command(["git", "diff", "--exit-code", revision, "--", "worker", "migrations", "extensions/lake_replication"])
        q.EVIDENCE["adapter_sha256"] = hashlib.sha256((ROOT / "experiments/base_workers/adapt.py").read_bytes()).hexdigest()
        q.EVIDENCE["experiment_fingerprints"] = {
            name: hashlib.sha256((ROOT / "experiments/base_workers" / name).read_bytes()).hexdigest()
            for name in ("adapt.py", "runtime.c.inc", "probe.py", "Dockerfile", "register.sql", "destination-init.sql")
        }
        with tempfile.TemporaryDirectory(prefix="lake-base-worker-") as temporary:
            q.build_images(archive, Path(temporary))
        image = f"lake-replication-base-worker-prototype:{q.RUN}"
        built = q.command(["docker", "build", "--build-arg", f"QUAL_IMAGE={q.DESTINATION_IMAGE}", "--build-arg", f"PROTOTYPE_HARDENED={int(HARDENED)}", "--build-arg", f"PROTOTYPE_VARIANT={'static' if STATIC else 'base'}", "--tag", image, "--file", str(ROOT / "experiments/base_workers/Dockerfile"), str(ROOT)], timeout=600)
        (q.RESULTS / "prototype-build.log").write_text(built.stdout + built.stderr)
        q.ENV["QUALIFICATION_DESTINATION_IMAGE"] = image
        text = q.COMPOSE_FILE.read_text()
        if not STATIC:
            text = text.replace("shared_preload_libraries=pg_extension_base,lake_replication", "shared_preload_libraries=pg_extension_base")
            text = text.replace("      - -c\n      - lake_replication.database_name=analytics\n", "")
        q.COMPOSE_FILE = q.RESULTS / "compose.yml"
        q.COMPOSE_FILE.write_text(text)
        q.compose("up", "-d", "--wait", "--wait-timeout", "300", timeout=600)
        started = True
        if STATIC:
            q.check("static baseline has exactly one launcher", q.wait_for("destination", "analytics", "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication launcher'", "1"), "1")
        else:
            observe("extension and database lifecycle", lifecycle_without_subscriptions)
        observe("packaged lifecycle through the adapter", q.verify_upgrade_contract)
        observe("replication, credentials, disable/enable, restart, outage", q.exercise_replication)
        observe("interrupted initial copy", q.exercise_interrupted_initial_copy)
        if HARDENED:
            observe("repeated worker retirement and handle memory", worker_churn)
        if not STATIC:
            observe("two live destinations and launcher replacement", live_lifecycle)
            observe("templates and failed database drop", template_and_failed_drop)
            observe("cloned subscription configuration", cloned_configuration)
            observe("dropping an extension with live supervisors", active_extension_drop)
        q.EVIDENCE["passed"] = not q.EVIDENCE["findings"]
        q.EVIDENCE["status"] = "passed" if q.EVIDENCE["passed"] else "findings"
    except Exception as error:
        q.EVIDENCE.update(passed=False, status="error", error=str(error))
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
    finally:
        if started:
            logs = q.compose("logs", "--no-color", check_result=False)
            (q.RESULTS / "compose.log").write_text(logs.stdout + logs.stderr)
        q.compose("down", "-v", "--remove-orphans", check_result=False, timeout=300)
        (q.RESULTS / "results.json").write_text(json.dumps(q.EVIDENCE, indent=2, sort_keys=True) + "\n")
        print(f"Evidence: {q.RESULTS}", flush=True)
    return 0 if q.EVIDENCE.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())

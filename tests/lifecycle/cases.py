"""Lifecycle regression cases promoted from the evaluated adapter fixture."""
import re
import subprocess
import time
from urllib.parse import quote

q = None  # Inject the private qualification fixture, never the development lab.
HARDENED = True
STATIC = False

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
    second = install_database("lifecycle_second")
    q.check("different databases get different launcher processes", first != second, True)
    q.check("only pg_extension_base is explicitly preloaded", q.sql("destination", "analytics", "SHOW shared_preload_libraries"), "pg_extension_base")
    q.check("no session preload is needed", q.sql("destination", "analytics", "SHOW session_preload_libraries"), "")
    q.check("no configured launcher database is needed", q.sql("destination", "analytics", "SHOW lake_replication.database_name"), "")
    q.check("legacy static launcher is absent", q.sql("destination", "analytics", "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication launcher'"), "0")

    q.sql("destination", "analytics", "CREATE DATABASE lifecycle_aborted")
    q.sql("destination", "lifecycle_aborted", "CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; BEGIN; CREATE EXTENSION lake_replication; ROLLBACK")
    q.check("rolled-back extension creation leaves no registration", q.sql("destination", "lifecycle_aborted", "SELECT count(*) FROM extension_base.workers WHERE extension_name='lake_replication'"), "0")
    q.sql("destination", "analytics", "DROP DATABASE lifecycle_aborted")

    q.sql("destination", "lifecycle_second", "BEGIN; DROP EXTENSION lake_replication CASCADE; ROLLBACK")
    q.check("rolled-back extension drop resumes the worker", int(await_worker("lifecycle_second")) > 0, True)
    pid = worker_pid("lifecycle_second")
    q.sql("destination", "lifecycle_second", "DROP EXTENSION lake_replication CASCADE")
    q.check("committed extension drop stops the launcher", q.wait_for("destination", "analytics", f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid}", "0"), "0")
    q.sql("destination", "lifecycle_second", "CREATE EXTENSION lake_replication")
    await_worker("lifecycle_second")
    q.check("extension recreation registers exactly one worker", q.sql("destination", "lifecycle_second", "SELECT count(*) FROM extension_base.workers WHERE extension_name='lake_replication'"), "1")


def replica_note(database):
    return q.sql("destination", database, "SELECT string_agg(note,',' ORDER BY id) FROM public.lifecycle_items")


def wait_note(database, expected):
    q.check(f"{database} replication reaches {expected}", q.wait_for("destination", database, "SELECT string_agg(note,',' ORDER BY id) FROM public.lifecycle_items", expected, timeout=60), expected)


def live_lifecycle():
    q.sql("source", "application", "CREATE TABLE public.lifecycle_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO public.lifecycle_items VALUES(1,'one'); CREATE PUBLICATION lifecycle_pub FOR TABLE public.lifecycle_items; GRANT SELECT ON public.lifecycle_items TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    for database, subscription in (("analytics", "lifecycle_a"), ("lifecycle_second", "lifecycle_b")):
        q.sql("destination", database, f"CREATE SUBSCRIPTION {subscription} CONNECTION '{connection}' PUBLICATION lifecycle_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        wait_note(database, "one")
    q.sql("source", "application", "INSERT INTO public.lifecycle_items VALUES(2,'two')")
    for database in ("analytics", "lifecycle_second"):
        wait_note(database, "one,two")

    before = q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()")
    pid = worker_pid("lifecycle_second")
    q.sql("destination", "analytics", f"SELECT pg_terminate_backend({pid})")
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline and worker_pid("lifecycle_second") in ("0", pid):
        time.sleep(0.25)
    q.check("terminated base launcher is replaced", worker_pid("lifecycle_second") not in ("0", pid), True)
    q.check("launcher termination does not restart PostgreSQL", q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()"), before)
    q.sql("destination", "lifecycle_second", "ALTER SUBSCRIPTION lifecycle_b DISABLE")
    q.check("disable works after launcher replacement", q.wait_for("destination", "lifecycle_second", "SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='lifecycle_b'", "stopped", timeout=30), "stopped")
    q.sql("destination", "lifecycle_second", "ALTER SUBSCRIPTION lifecycle_b ENABLE")

    q.compose("restart", "destination", timeout=180)
    q.wait_for("destination", "analytics", "SELECT 1", "1", timeout=90)
    await_worker("analytics")
    await_worker("lifecycle_second")
    q.sql("source", "application", "INSERT INTO public.lifecycle_items VALUES(3,'three')")
    for database in ("analytics", "lifecycle_second"):
        wait_note(database, "one,two,three")
    q.sql("destination", "analytics", "DROP SUBSCRIPTION lifecycle_a")
    q.sql("destination", "lifecycle_second", "DROP SUBSCRIPTION lifecycle_b")


def template_and_failed_drop():
    q.sql("destination", "analytics", "CREATE DATABASE lifecycle_template IS_TEMPLATE true")
    q.sql("destination", "lifecycle_template", "CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication")
    q.check("registered workers do not start in a template database", q.sql("destination", "analytics", "SELECT count(*) FROM pg_stat_activity WHERE datname='lifecycle_template'"), "0")
    q.sql("destination", "analytics", "CREATE DATABASE lifecycle_clone TEMPLATE lifecycle_template")
    q.check("template clone discovers its registered launcher", int(await_worker("lifecycle_clone")) > 0, True)

    # Hold an ordinary client connection so DROP DATABASE must fail.
    held = subprocess.Popen(["docker", "compose", "-p", q.PROJECT, "-f", str(q.COMPOSE_FILE), "exec", "-T", "destination", "psql", "-XAt", "-U", "postgres", "-d", "lifecycle_clone", "-c", "SELECT pg_sleep(30)"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=q.ENV)
    try:
        q.wait_for("destination", "analytics", "SELECT count(*) FROM pg_stat_activity WHERE datname='lifecycle_clone' AND query='SELECT pg_sleep(30)'", "1", timeout=10)
        failed = q.sql_result("destination", "analytics", "DROP DATABASE lifecycle_clone", check_result=False)
        q.check("drop database fails while an ordinary client is connected", failed.returncode != 0, True)
        q.check("failed database drop resumes its launcher", int(await_worker("lifecycle_clone")) > 0, True)
    finally:
        q.sql("destination", "analytics", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='lifecycle_clone' AND query='SELECT pg_sleep(30)'")
        held.wait(timeout=15)
    q.sql("destination", "analytics", "DROP DATABASE lifecycle_clone")
    q.check("committed database drop completes", q.sql("destination", "analytics", "SELECT count(*) FROM pg_database WHERE datname='lifecycle_clone'"), "0")


def active_extension_drop():
    install_database("lifecycle_drop")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    q.sql("destination", "lifecycle_drop", f"CREATE SUBSCRIPTION lifecycle_drop_sub CONNECTION '{connection}' PUBLICATION lifecycle_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    wait_note("lifecycle_drop", "one,two,three")
    child = q.sql("destination", "lifecycle_drop", "SELECT supervisor_pid FROM lake_replication.service_status")
    assert child.isdigit()
    launcher = worker_pid("lifecycle_drop")
    before = q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()")
    q.compose("exec", "-T", "destination", "kill", "-STOP", child)
    try:
        q.sql("destination", "lifecycle_drop", "SET statement_timeout='12s'; DROP EXTENSION lake_replication CASCADE")
        remaining = q.wait_for("destination", "analytics", f"SELECT count(*) FROM pg_stat_activity WHERE datname='lifecycle_drop' AND (backend_type='lake_replication supervisor' OR pid={launcher})", "0", timeout=10)
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
    q.sql("destination", "analytics", "DROP DATABASE lifecycle_drop WITH (FORCE)")


def cloned_configuration():
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    q.sql("destination", "lifecycle_template", f"CREATE SUBSCRIPTION lifecycle_seed CONNECTION '{connection}' PUBLICATION lifecycle_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env'); ALTER SUBSCRIPTION lifecycle_seed DISABLE")
    q.sql("destination", "analytics", "CREATE DATABASE lifecycle_config_clone TEMPLATE lifecycle_template")
    await_worker("lifecycle_config_clone")
    # Keep the copied subscription stopped: inspect ownership without allowing
    # it to write into a table/catalog cloned from another destination.
    q.check("cloned subscription stays stopped", q.sql("destination", "lifecycle_config_clone", "SELECT desired_state FROM lake_replication.worker_launch_config"), "stopped")
    if HARDENED:
        q.check("cloned subscription is rejected with a durable diagnostic", q.wait_for("destination", "lifecycle_config_clone", "SELECT (state='error' AND last_error LIKE '%inherited destination binding%')::text FROM lake_replication.subscriptions WHERE subscription_name='lifecycle_seed'", "true"), "true")
        q.sql("destination", "lifecycle_config_clone", "UPDATE lake_replication.worker_launch_config SET desired_state='running'")
        q.check("attempted enable of inherited binding is fenced", q.wait_for("destination", "lifecycle_config_clone", "SELECT desired_state FROM lake_replication.worker_launch_config", "stopped"), "stopped")
        version = q.sql("destination", "lifecycle_config_clone", "SELECT xmin::text FROM lake_replication.subscriptions WHERE subscription_name='lifecycle_seed'")
        time.sleep(3)
        q.check("unchanged binding errors do not rewrite subscription rows", q.sql("destination", "lifecycle_config_clone", "SELECT xmin::text FROM lake_replication.subscriptions WHERE subscription_name='lifecycle_seed'"), version)
    else:
        q.check("cloned launch configuration binds to the new database", q.sql("destination", "lifecycle_config_clone", "SELECT count(*) FROM lake_replication.worker_launch_config WHERE destination_database <> current_database()"), "0")


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

    q.sql("source", "application", "CREATE TABLE public.lifecycle_churn(id bigint PRIMARY KEY); CREATE PUBLICATION lifecycle_churn_pub FOR TABLE public.lifecycle_churn; GRANT SELECT ON public.lifecycle_churn TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    q.sql("destination", "analytics", f"CREATE SUBSCRIPTION lifecycle_churn_sub CONNECTION '{connection}' PUBLICATION lifecycle_churn_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    try:
        q.check("churn subscription finishes its initial copy", q.wait_for("destination", "analytics", "SELECT state FROM lake_replication.subscriptions WHERE subscription_name='lifecycle_churn_sub'", "streaming"), "streaming")
        child = q.sql("destination", "analytics", "SELECT supervisor_pid FROM lake_replication.service_status ss JOIN lake_replication.subscriptions s USING(subscription_id) WHERE s.subscription_name='lifecycle_churn_sub'")
        before = q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()")
        q.compose("exec", "-T", "destination", "kill", "-STOP", child)
        q.sql("destination", "analytics", "ALTER SUBSCRIPTION lifecycle_churn_sub DISABLE")
        count = "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'"
        q.check("churn starts with all subscription wrappers retired", q.wait_for("destination", "analytics", count, "0"), "0")
        alive = q.compose("exec", "-T", "destination", "sh", "-c", f"kill -0 {child} 2>/dev/null", check_result=False).returncode == 0
        q.check("disable explicitly reaps a paused supervisor in the shared runtime", alive, False)
        q.check("paused supervisor disable does not restart PostgreSQL", q.sql("destination", "analytics", "SELECT pg_postmaster_start_time()"), before)
        baseline = handle_memory()
        for cycle in range(10):
            q.sql("destination", "analytics", "ALTER SUBSCRIPTION lifecycle_churn_sub ENABLE")
            q.check(f"churn cycle {cycle + 1} starts exactly one wrapper", q.wait_for("destination", "analytics", count, "1"), "1")
            q.sql("destination", "analytics", "ALTER SUBSCRIPTION lifecycle_churn_sub DISABLE")
            q.check(f"churn cycle {cycle + 1} retires its wrapper", q.wait_for("destination", "analytics", count, "0"), "0")
        final = handle_memory()
        q.EVIDENCE["handle_churn"] = {"cycles": 10, "before": baseline, "after": final}
        q.check("retired handles return to the original memory usage", final, baseline)
    finally:
        q.sql("destination", "analytics", "DROP SUBSCRIPTION lifecycle_churn_sub", check_result=False)

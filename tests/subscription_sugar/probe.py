#!/usr/bin/env python3
"""Architecture spike: builds and loads extensions/lake_subscription_sugar
inside the running lab's PostgreSQL container, then proves that
`CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg')`,
`ALTER SUBSCRIPTION ... DISABLE`, and `DROP SUBSCRIPTION` are all
intercepted and routed to this project's own lake_replication.* control
plane -- with real source-identity discovery via dblink, not a stub --
instead of PostgreSQL's real subscription/slot/apply-worker machinery ever
running. Also installs and checks the crunchy_iceberg-shaped monitoring
views. See docs/native-subscription-spike.md.

The second half of this probe goes further: it builds the four Rust worker
binaries, copies them into the lab container, and proves that a single
`CREATE SUBSCRIPTION` genuinely starts replication end to end (initial copy
of pre-existing rows, then live INSERT/UPDATE streaming into a real Iceberg
table) with no separate manual step, and that `DROP SUBSCRIPTION` stops the
supervisor process cleanly without crashing the cluster -- the fix for a
real crash this probe caught during development, where SIGTERM-ing a
detached worker in this container (PostgreSQL runs as PID 1 here) made the
postmaster treat it as a crash and restart the whole cluster.

Not part of the shipped replication path. Requires the lab's PostgreSQL
image (it ships pg_config/gcc/make and the matching server headers to
compile a PGXS extension in place; see the Dockerfile), and either a Linux
host or a working `docker run rust:bookworm` fallback to cross-build the
worker binaries for the container's architecture.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
EXT_DIR = ROOT / "extensions" / "lake_subscription_sugar"
COMPOSE = ["docker", "compose", "-p", "pg-lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("subsug_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE = RUN
PUBLICATION = f"{RUN}_pub"
SUBSCRIPTION = f"{RUN}_sub"
WORKER_BIN_DIR = "/usr/local/bin"
WORKER_BINARIES = ["lake-replication-receiver", "lake-replication-publisher",
                   "lake-replication-initial-sync", "lake-replication-supervisor"]


def command(args, timeout=60, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout, **kwargs)


def sql(database, statement, *, check_result=True):
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
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def container_id():
    result = command(COMPOSE + ["ps", "-q", "postgres"])
    if result.returncode or not result.stdout.strip():
        raise RuntimeError("lab postgres container is not running (run `make lab-up` first)")
    return result.stdout.strip()


def build_and_install_extension(cid):
    command(["docker", "exec", cid, "mkdir", "-p", "/tmp/lake_subscription_sugar"])
    for name in ("lake_subscription_sugar.c", "Makefile", "lake_subscription_sugar.control",
                 "lake_subscription_sugar--1.0.sql"):
        result = command(["docker", "cp", str(EXT_DIR / name),
                          f"{cid}:/tmp/lake_subscription_sugar/{name}"])
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
    for target in ("all", "install"):
        result = command(["docker", "exec", "-w", "/tmp/lake_subscription_sugar", cid,
                          "make", target])
        if result.returncode:
            raise RuntimeError(result.stderr.strip())

    # Installed and activated the way pg_lake itself ships: CREATE
    # EXTENSION once (no shared_preload_libraries, no manual LOAD per
    # statement), with session_preload_libraries making the
    # ProcessUtility_hook active for every future session to this database.
    sql("analytics", "DROP EXTENSION IF EXISTS lake_subscription_sugar CASCADE")
    sql("analytics", "CREATE EXTENSION lake_subscription_sugar")
    sql("analytics", "ALTER DATABASE analytics SET session_preload_libraries "
                     "= 'lake_subscription_sugar'")


def wait_for(database, statement, expected, timeout=15):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = value(database, statement)
        if latest == expected:
            return latest
        time.sleep(0.2)
    return latest


def is_linux_elf(path):
    if not path.exists():
        return False
    with open(path, "rb") as handle:
        return handle.read(4) == b"\x7fELF"


def build_worker_binaries(cid):
    """Builds the four Rust worker binaries and copies them into the lab
    container at WORKER_BIN_DIR. `cargo build` targets the host by default;
    when the host isn't Linux (e.g. this is run from a macOS/Windows dev
    machine, unlike the Linux container), that produces a binary the
    container can't run, so this falls back to cross-building for Linux
    inside a throwaway `rust:bookworm` container against the same source,
    matching the container's own Debian bookworm base.
    """
    supervisor_path = ROOT / "target/debug/lake-replication-supervisor"
    try:
        build = command(["cargo", "build", "--quiet", "--bins"], cwd=ROOT, timeout=180)
        host_build_failed = build.returncode != 0
    except FileNotFoundError:
        host_build_failed = True  # no cargo on this host at all

    if host_build_failed or not is_linux_elf(supervisor_path):
        cross = command([
            "docker", "run", "--rm", "-v", f"{ROOT}:/src", "-w", "/src",
            "-e", "CARGO_HOME=/src/.cargo-linux-build", "rust:bookworm",
            "cargo", "build", "--quiet", "--bins",
        ], timeout=600)
        if cross.returncode:
            raise RuntimeError(cross.stderr.strip())
        if not is_linux_elf(supervisor_path):
            raise RuntimeError("cross-built worker binary is still not a Linux ELF")

    for name in WORKER_BINARIES:
        result = command(["docker", "cp", str(ROOT / "target/debug" / name),
                          f"{cid}:{WORKER_BIN_DIR}/{name}"])
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
        command(["docker", "exec", cid, "chmod", "755", f"{WORKER_BIN_DIR}/{name}"])


def install_migrations():
    installed = value("analytics", """
        SELECT to_regprocedure('lake_replication.create_subscription'
            '(numeric,oid,bigint,name,name,text[])') IS NOT NULL;
    """)
    if installed != "t":
        for migration in sorted((ROOT / "migrations").glob("*.sql")):
            sql("analytics", migration.read_text())
    sql("analytics", (EXT_DIR / "monitoring_views.sql").read_text())


def force_delete_subscription(subscription_id):
    sql("analytics", f"""
        DELETE FROM lake_replication.changes WHERE transaction_id IN
            (SELECT transaction_id FROM lake_replication.transactions
              WHERE subscription_id={subscription_id});
        DELETE FROM lake_replication.transactions WHERE subscription_id={subscription_id};
        DELETE FROM lake_replication.batches WHERE subscription_id={subscription_id};
        DELETE FROM lake_replication.initial_copies WHERE subscription_id={subscription_id};
        DELETE FROM lake_replication.relations WHERE subscription_id={subscription_id};
        DELETE FROM lake_replication.checkpoints WHERE subscription_id={subscription_id};
        DELETE FROM lake_replication.journal_maintenance_policies WHERE subscription_id={subscription_id};
        DELETE FROM lake_replication.journal_maintenance_status WHERE subscription_id={subscription_id};
        DELETE FROM lake_replication.service_status WHERE subscription_id={subscription_id};
        DELETE FROM lake_replication.subscriptions WHERE subscription_id={subscription_id};
    """, check_result=False)


def kill_leftover_worker(cid, subscription_id):
    if subscription_id is None or cid is None:
        return
    pid = value("analytics", f"""
        SELECT coalesce(supervisor_pid::text, '')
          FROM lake_replication.service_status WHERE subscription_id={subscription_id}
    """)
    if pid:
        command(["docker", "exec", cid, "kill", "-9", pid], timeout=10)


def cleanup(cid, subscription_id):
    kill_leftover_worker(cid, subscription_id)
    sql("analytics", f"DROP TABLE IF EXISTS public.{TABLE}", check_result=False)
    if subscription_id is not None:
        force_delete_subscription(subscription_id)
    sql("analytics", "DELETE FROM lake_replication.sources", check_result=False)
    sql("application", f"""
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS public.{TABLE};
    """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscription_id = None
    cid = None
    exit_code = 1
    try:
        cid = container_id()
        build_and_install_extension(cid)
        build_worker_binaries(cid)
        install_migrations()
        postmaster_start = value("analytics", "SELECT pg_postmaster_start_time()")

        # One real subscription, start to finish: CREATE SUBSCRIPTION alone
        # both registers it in this project's own control plane (not
        # PostgreSQL's) and starts real replication -- initial copy of the
        # pre-existing row, then live INSERT/UPDATE streaming -- with no
        # separate manual step, the way native CREATE SUBSCRIPTION would
        # spawn its own walreceiver/apply worker.
        value("application", f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text);
            INSERT INTO public.{TABLE} VALUES (1, 'one');
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};
        """)
        real_identity = value("application", """
            SELECT (SELECT system_identifier FROM pg_control_system()) || ':' ||
                   (SELECT oid FROM pg_database WHERE datname=current_database()) || ':' ||
                   (SELECT timeline_id FROM pg_control_checkpoint())
        """)

        result = sql("analytics", f"""
            CREATE SUBSCRIPTION {SUBSCRIPTION}
              CONNECTION 'host=127.0.0.1 port=5432 dbname=application user=postgres'
              PUBLICATION {PUBLICATION}
              WITH (lake_target = 'iceberg');
        """)
        check("hook logs interception before routing to the control plane",
              "lake_subscription_sugar: intercepted" in (result.stdout + result.stderr), True)
        check("CREATE SUBSCRIPTION reports that it started the workers",
              "starting initial sync and the supervisor" in (result.stdout + result.stderr),
              True)

        subscription_id = int(value("analytics", f"""
            SELECT subscription_id FROM lake_replication.subscriptions
             WHERE subscription_name='{SUBSCRIPTION}'
        """))
        check("our own control plane received a real subscription row",
              value("analytics", f"""
                  SELECT slot_name || ':' || publication_names::text
                    FROM lake_replication.subscriptions
                   WHERE subscription_id={subscription_id}
              """), f"{SUBSCRIPTION}_slot:{{{PUBLICATION}}}")
        check("real source identity was discovered via dblink, not stubbed",
              value("analytics", f"""
                  SELECT system_identifier || ':' || database_oid || ':' || timeline
                    FROM lake_replication.sources s
                    JOIN lake_replication.subscriptions sub USING (source_id)
                   WHERE sub.subscription_id={subscription_id}
              """), real_identity)
        check("PostgreSQL's native subscription catalog was never touched",
              value("analytics", f"""
                  SELECT count(*) FROM pg_subscription WHERE subname='{SUBSCRIPTION}'
              """), "0")

        check("subscription reaches streaming on its own (initial sync ran unattended)",
              wait_for("analytics", f"""
                  SELECT state FROM lake_replication.subscriptions
                   WHERE subscription_id={subscription_id}
              """, "streaming", timeout=20), "streaming")
        check("the pre-existing row was copied without any manual step",
              wait_for("analytics", f"SELECT note FROM public.{TABLE} WHERE id=1",
                       "one", timeout=10), "one")

        supervisor_pid = value("analytics", f"""
            SELECT supervisor_pid FROM lake_replication.service_status
             WHERE subscription_id={subscription_id}
        """)
        check("the supervisor is a real, running OS process inside the container",
              command(["docker", "exec", cid, "kill", "-0", supervisor_pid]).returncode, 0)

        value("application", f"""
            INSERT INTO public.{TABLE} VALUES (2, 'two');
            UPDATE public.{TABLE} SET note='ONE-updated' WHERE id=1;
        """)
        check("ongoing INSERT/UPDATE replicate live, with no manual worker invocation",
              wait_for("analytics", f"""
                  SELECT string_agg(id || '=' || note, ',' ORDER BY id)
                    FROM public.{TABLE}
              """, "1=ONE-updated,2=two", timeout=10), "1=ONE-updated,2=two")

        check("monitoring view catches up to zero unflushed rows once the "
              "supervisor's next cycle publishes them",
              wait_for("analytics", f"""
                  SELECT unflushed_inserts || ':' || unflushed_updates
                    FROM crunchy_iceberg.stat_subscription_rel
                   WHERE subscription_id={subscription_id}
              """, "0:0", timeout=10), "0:0")
        check("monitoring view reports a real flush timestamp, duration, and lsn",
              value("analytics", f"""
                  SELECT (last_flush_time IS NOT NULL) || ':' ||
                         (last_flush_duration IS NOT NULL) || ':' ||
                         (last_flush_lsn IS NOT NULL)
                    FROM crunchy_iceberg.stat_subscription
                   WHERE subscription_id={subscription_id}
              """), "true:true:true")

        result = sql("analytics", f"""
            ALTER SUBSCRIPTION {SUBSCRIPTION} DISABLE;
        """)
        check("ALTER SUBSCRIPTION DISABLE is intercepted and routed to a graceful stop",
              "intercepted ALTER SUBSCRIPTION" in (result.stdout + result.stderr), True)

        result = sql("analytics", f"""
            DROP SUBSCRIPTION {SUBSCRIPTION};
        """)
        check("DROP SUBSCRIPTION is intercepted and completes without error",
              "intercepted DROP SUBSCRIPTION" in (result.stdout + result.stderr)
              and result.returncode == 0, True)
        check("DROP SUBSCRIPTION did not crash the cluster "
              "(postmaster start time is unchanged)",
              value("analytics", "SELECT pg_postmaster_start_time()"), postmaster_start)
        check("the supervisor process is actually gone after DROP",
              command(["docker", "exec", cid, "kill", "-0", supervisor_pid]).returncode != 0,
              True)
        check("the control-plane row is actually gone after DROP",
              value("analytics", f"""
                  SELECT count(*) FROM lake_replication.subscriptions
                   WHERE subscription_id={subscription_id}
              """), "0")
        check("the source-side replication slot was dropped for real, not left behind",
              value("application", f"""
                  SELECT count(*) FROM pg_replication_slots
                   WHERE slot_name='{SUBSCRIPTION}_slot'
              """), "0")
        check("this extension's own pending_cleanup bookkeeping row is gone too",
              value("analytics", f"""
                  SELECT count(*) FROM lake_subscription_sugar.pending_cleanup
                   WHERE subscription_id={subscription_id}
              """), "0")
        subscription_id = None

        EVIDENCE["status"] = "passed"
        exit_code = 0
    except Exception as exc:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        cleanup(cid, subscription_id)
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

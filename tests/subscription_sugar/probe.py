#!/usr/bin/env python3
"""Architecture spike: builds and loads extensions/lake_subscription_sugar
inside the running lab's PostgreSQL container, then proves that
`CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg')` is intercepted and
routed to this project's own lake_replication.create_subscription() control
plane, instead of PostgreSQL's real subscription/slot/apply-worker
machinery ever running. See docs/native-subscription-spike.md.

Not part of the shipped replication path. Requires the lab's PostgreSQL
image (it ships pg_config/gcc/make and the matching server headers to
compile a PGXS extension in place; see the Dockerfile).
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys


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


def command(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=60, **kwargs)


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
    for name in ("lake_subscription_sugar.c", "Makefile"):
        result = command(["docker", "cp", str(EXT_DIR / name),
                          f"{cid}:/tmp/lake_subscription_sugar/{name}"])
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
    for target in ("all", "install"):
        result = command(["docker", "exec", "-w", "/tmp/lake_subscription_sugar", cid,
                          "make", target])
        if result.returncode:
            raise RuntimeError(result.stderr.strip())


def install_migrations():
    installed = value("analytics", """
        SELECT to_regprocedure('lake_replication.create_subscription'
            '(numeric,oid,bigint,name,name,text[])') IS NOT NULL;
    """)
    if installed != "t":
        for migration in sorted((ROOT / "migrations").glob("*.sql")):
            sql("analytics", migration.read_text())


def cleanup(subscription_id):
    sql("analytics", f"DROP TABLE IF EXISTS public.{TABLE}", check_result=False)
    if subscription_id is not None:
        sql("analytics", f"""
            DELETE FROM lake_replication.checkpoints WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.journal_maintenance_policies WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.journal_maintenance_status WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.subscriptions WHERE subscription_id={subscription_id};
        """, check_result=False)
    sql("application", f"""
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS public.{TABLE};
    """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    subscription_id = None
    exit_code = 1
    try:
        cid = container_id()
        build_and_install_extension(cid)
        install_migrations()

        value("application", f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text);
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};
        """)

        result = sql("analytics", f"""
            LOAD 'lake_subscription_sugar';
            CREATE SUBSCRIPTION {SUBSCRIPTION}
              CONNECTION 'host=127.0.0.1 port=5432 dbname=application user=postgres'
              PUBLICATION {PUBLICATION}
              WITH (lake_target = 'iceberg');
        """)
        check("hook logs interception before routing to the control plane",
              "lake_subscription_sugar: intercepted" in (result.stdout + result.stderr), True)

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
        check("PostgreSQL's native subscription catalog was never touched",
              value("analytics", f"""
                  SELECT count(*) FROM pg_subscription WHERE subname='{SUBSCRIPTION}'
              """), "0")
        check("no real replication slot was created on the source",
              value("application", "SELECT count(*) FROM pg_replication_slots"), "0")

        EVIDENCE["status"] = "passed"
        exit_code = 0
    except Exception as exc:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        cleanup(subscription_id)
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

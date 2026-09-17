#!/usr/bin/env python3
"""Architecture spike: does native PostgreSQL logical replication (CREATE
SUBSCRIPTION) accept a destination table that uses a non-default table
access method, and does the built-in apply worker correctly stream INSERT/
UPDATE/DELETE into it?

This is not part of this project's shipped replication path. pg_lake's own
"iceberg" access method is an internal placeholder over a foreign table
(relkind='f'), which PostgreSQL's CheckSubscriptionRelkind rejects with
SQLSTATE 42809 -- the reason this project builds its own external receiver/
publisher instead of using CREATE SUBSCRIPTION. This probe uses the
built-in, already-compiled heap_tableam_handler under a second access
method name ("heap2", the same trick PostgreSQL's own create_am regression
test uses) to check whether a *complete* table access method -- unlike
pg_lake's placeholder -- would let native subscriptions work, informing
whether a from-scratch Iceberg table access method is a viable alternative
to today's external-worker design. See docs/native-subscription-spike.md.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "pg-lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("natsub_%Y%m%d_%H%M%S")
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "checks": []}
TABLE = RUN
PUBLICATION = f"{RUN}_pub"
SLOT = f"{RUN}_slot"
SUBSCRIPTION = f"{RUN}_sub"


def command(args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, timeout=30, **kwargs)


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


def wait_for(database, statement, expected, timeout=15):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = value(database, statement)
        if last == expected:
            return last
        time.sleep(0.2)
    return last


def cleanup():
    sql("analytics", f"""
        ALTER SUBSCRIPTION {SUBSCRIPTION} DISABLE;
        ALTER SUBSCRIPTION {SUBSCRIPTION} SET (slot_name = NONE);
        DROP SUBSCRIPTION IF EXISTS {SUBSCRIPTION};
        DROP TABLE IF EXISTS public.{TABLE};
    """, check_result=False)
    sql("application", f"""
        SELECT pg_drop_replication_slot(slot_name)
          FROM pg_replication_slots WHERE slot_name='{SLOT}';
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS public.{TABLE};
    """, check_result=False)


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    exit_code = 1
    try:
        # Source: an ordinary heap table and publication, nothing unusual.
        value("application", f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text);
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};
        """)

        # Destination: register the built-in heap handler under a second,
        # non-default access method name (PostgreSQL's own create_am
        # regression test does exactly this), then create the target table
        # with it. No C code compiled -- heap_tableam_handler already ships
        # in every PostgreSQL build.
        value("analytics", """
            DO $$ BEGIN
              IF NOT EXISTS (SELECT 1 FROM pg_am WHERE amname='heap2') THEN
                CREATE ACCESS METHOD heap2 TYPE TABLE HANDLER heap_tableam_handler;
              END IF;
            END $$;
        """)
        value("analytics", f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text)
                USING heap2;
        """)
        check("destination table uses a non-default table access method",
              value("analytics", f"""
                  SELECT c.relkind::text || ':' || a.amname
                    FROM pg_class c JOIN pg_am a ON a.oid=c.relam
                   WHERE c.relname='{TABLE}' AND c.relnamespace='public'::regnamespace
              """), "r:heap2")

        # Create the slot as its own step. CREATE SUBSCRIPTION's default
        # create_slot=true deadlocks here: the calling backend holds an open
        # transaction while it opens a second connection back into the same
        # cluster (source and destination are two databases on one
        # postmaster in this lab) to negotiate the slot's synchronized
        # snapshot, which then waits on that same transaction's xid.
        value("application", f"""
            SELECT slot_name FROM pg_create_logical_replication_slot('{SLOT}', 'pgoutput')
        """)
        value("analytics", f"""
            CREATE SUBSCRIPTION {SUBSCRIPTION}
              CONNECTION 'host=127.0.0.1 port=5432 dbname=application user=postgres'
              PUBLICATION {PUBLICATION}
              WITH (create_slot = false, slot_name = '{SLOT}')
        """)
        check("native CREATE SUBSCRIPTION accepts the table as a target "
              "(no SQLSTATE 42809)",
              wait_for("analytics", f"""
                  SELECT srsubstate FROM pg_subscription_rel
                   WHERE srrelid='public.{TABLE}'::regclass
              """, "r"), "r")

        value("application", f"INSERT INTO public.{TABLE} VALUES (1,'one'),(2,'two'),(3,'three')")
        check("apply worker streams INSERT into the heap2-backed table",
              wait_for("analytics", f"""
                  SELECT string_agg(id || '=' || note, ',' ORDER BY id)
                    FROM public.{TABLE}
              """, "1=one,2=two,3=three"), "1=one,2=two,3=three")

        value("application", f"""
            UPDATE public.{TABLE} SET note='TWO-updated' WHERE id=2;
            DELETE FROM public.{TABLE} WHERE id=3;
        """)
        check("apply worker streams UPDATE and DELETE into the heap2-backed table",
              wait_for("analytics", f"""
                  SELECT string_agg(id || '=' || note, ',' ORDER BY id)
                    FROM public.{TABLE}
              """, "1=one,2=TWO-updated"), "1=one,2=TWO-updated")

        check("destination table was never rewritten off the custom access method",
              value("analytics", f"""
                  SELECT c.relkind::text || ':' || a.amname
                    FROM pg_class c JOIN pg_am a ON a.oid=c.relam
                   WHERE c.relname='{TABLE}' AND c.relnamespace='public'::regnamespace
              """), "r:heap2")

        EVIDENCE["status"] = "passed"
        exit_code = 0
    except Exception as exc:
        EVIDENCE["status"] = "error"
        EVIDENCE["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
    finally:
        cleanup()
        (RESULTS / "results.json").write_text(json.dumps(EVIDENCE, indent=2) + "\n")
        print(f"Evidence: {RESULTS}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

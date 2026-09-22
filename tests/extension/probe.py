#!/usr/bin/env python3
"""Build and validate the packaged lake_replication SQL extension."""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]
EXTENSION = ROOT / "extensions/lake_replication"
COMPOSE = [
    "docker", "compose", "-p", "pg-lake-replication-lab",
    "-f", str(ROOT / "deploy/docker-compose/compose.yml"),
]
RUN = datetime.now(timezone.utc).strftime("extension_%Y%m%d_%H%M%S")
DATABASE = f"lake_extension_{RUN.removeprefix('extension_')}"
RESULTS = ROOT / ".lab" / RUN
EVIDENCE = {"run": RUN, "database": DATABASE, "checks": []}


def command(args, *, check=True, timeout=120, **kwargs):
    result = subprocess.run(
        args, cwd=ROOT, text=True, capture_output=True, timeout=timeout, **kwargs
    )
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result


def compose(*args, **kwargs):
    return command(COMPOSE + list(args), **kwargs)


def sql(statement):
    return compose(
        "exec", "-T", "postgres", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
        "-U", "postgres", "-d", DATABASE, input=statement,
    ).stdout.strip()


def check(name, actual, expected):
    passed = actual == expected
    EVIDENCE["checks"].append({
        "name": name, "passed": passed, "actual": actual, "expected": expected,
    })
    print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
    if not passed:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def main():
    RESULTS.mkdir(parents=True, exist_ok=False)
    container = None
    exit_code = 1
    try:
        command(["make", "-C", str(EXTENSION)])
        container = compose("ps", "-q", "postgres").stdout.strip()
        if not container:
            raise RuntimeError("the main PostgreSQL lab container is not running")

        shared = "/usr/share/postgresql/18/extension"
        for filename in [
            "lake_replication.control",
            "lake_replication--0.1.0.sql",
            "lake_replication--0.1.0-beta.1.sql",
            "lake_replication--0.1.0--0.1.0-beta.1.sql",
        ]:
            command([
                "docker", "cp", str(EXTENSION / filename),
                f"{container}:{shared}/{filename}",
            ])

        compose("exec", "-T", "postgres", "createdb", "-U", "postgres", DATABASE)
        sql("CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION lake_replication;")
        check(
            "extension version",
            sql("SELECT extversion FROM pg_extension WHERE extname='lake_replication'"),
            "0.1.0-beta.1",
        )
        check(
            "all type mappings installed",
            sql("SELECT count(*) FROM lake_replication.type_mappings"),
            "13",
        )
        check(
            "operator status and metrics views installed",
            sql("""
                SELECT to_regclass('lake_replication.subscription_status') IS NOT NULL
                       AND to_regclass('lake_replication.metrics') IS NOT NULL
                       AND EXISTS (
                           SELECT 1 FROM information_schema.columns
                            WHERE table_schema='lake_replication'
                              AND table_name='subscription_status'
                              AND column_name='source_lag_bytes'
                       )
            """),
            "t",
        )
        member_count = int(sql("""
            SELECT count(*)
              FROM pg_depend d
              JOIN pg_extension e ON e.oid=d.refobjid
             WHERE e.extname='lake_replication' AND d.deptype='e'
        """))
        check("extension owns control objects", member_count > 90, True)

        sql("DROP EXTENSION lake_replication")
        check(
            "drop removes control schema",
            sql("SELECT to_regnamespace('lake_replication') IS NULL"),
            "t",
        )
        check(
            "drop preserves pg_lake",
            sql("SELECT count(*) FROM pg_extension WHERE extname='pg_lake'"),
            "1",
        )
        sql("CREATE EXTENSION lake_replication")
        check(
            "extension recreates cleanly",
            sql("SELECT count(*) FROM lake_replication.type_mappings"),
            "13",
        )
        exit_code = 0
    except Exception as error:
        EVIDENCE["error"] = str(error)
        print(f"FAIL {error}", flush=True)
    finally:
        compose(
            "exec", "-T", "postgres", "dropdb", "--if-exists", "-U", "postgres",
            DATABASE, check=False,
        )
        if container:
            command([
                "docker", "exec", "-u", "root", container, "rm", "-f",
                "/usr/share/postgresql/18/extension/lake_replication.control",
                "/usr/share/postgresql/18/extension/lake_replication--0.1.0.sql",
                "/usr/share/postgresql/18/extension/lake_replication--0.1.0-beta.1.sql",
                "/usr/share/postgresql/18/extension/lake_replication--0.1.0--0.1.0-beta.1.sql",
            ], check=False)
        EVIDENCE["passed"] = exit_code == 0
        (RESULTS / "results.json").write_text(
            json.dumps(EVIDENCE, indent=2, sort_keys=True) + "\n"
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

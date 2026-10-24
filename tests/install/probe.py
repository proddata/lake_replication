#!/usr/bin/env python3
"""Build and smoke-test the exact exported installation bundle."""
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = [
    "docker", "compose", "-p", "lake-replication-lab",
    "-f", str(ROOT / "deploy/docker-compose/compose.yml"),
]
RUN = datetime.now(timezone.utc).strftime("install_%Y%m%d_%H%M%S")
DATABASE = f"lake_bundle_{RUN.removeprefix('install_')}"
RESULTS = ROOT / ".lab" / RUN
BUNDLE = RESULTS / "dist/lake_replication-0.1.0-beta.3"
CONTAINER_BUNDLE = f"/tmp/{RUN}"
EVIDENCE = {"run": RUN, "database": DATABASE, "checks": []}


def command(args, *, check_result=True, timeout=1200, **kwargs):
    result = subprocess.run(
        args, cwd=ROOT, text=True, capture_output=True, timeout=timeout, **kwargs
    )
    if check_result and result.returncode:
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
        command([str(ROOT / "deploy/install/build-bundle.sh"), str(RESULTS / "dist")])
        archive = RESULTS / "dist/lake_replication-0.1.0-beta.3.tar.gz"
        check("release archive produced", archive.is_file(), True)
        compatibility = (BUNDLE / "COMPATIBILITY.md").read_text()
        check(
            "compatibility matrix shipped",
            "8180647d4535fdb1ed223f92f301ba6094a8ec3e" in compatibility
            and "REST-catalog-backed destination tables are unsupported" in compatibility,
            True,
        )
        expected_revision = command(["git", "rev-parse", "HEAD"]).stdout.strip()
        if command(["git", "status", "--porcelain"]).stdout.strip():
            expected_revision += "-dirty"
        check(
            "source revision recorded",
            (BUNDLE / "SOURCE_REVISION").read_text().strip(),
            expected_revision,
        )
        container = compose("ps", "-q", "postgres").stdout.strip()
        if not container:
            raise RuntimeError("the main PostgreSQL lab container is not running")

        command(["docker", "cp", str(BUNDLE), f"{container}:{CONTAINER_BUNDLE}"])
        checksum = command([
            "docker", "exec", "-w", CONTAINER_BUNDLE, container,
            "sha256sum", "-c", "SHA256SUMS",
        ])
        check(
            "all bundle checksums",
            all(line.endswith(": OK") for line in checksum.stdout.splitlines()),
            True,
        )
        installed = command([
            "docker", "exec", "-u", "root", "-w", CONTAINER_BUNDLE,
            container, "./install.sh",
        ])
        check(
            "installer selected PostgreSQL 18",
            "for PostgreSQL 18." in installed.stdout or "for PostgreSQL 18 " in installed.stdout,
            True,
        )
        check(
            "installer reports validated pg_lake revision",
            "8180647d4535fdb1ed223f92f301ba6094a8ec3e" in installed.stdout,
            True,
        )

        compose("exec", "-T", "postgres", "createdb", "-U", "postgres", DATABASE)
        sql("""
            CREATE EXTENSION pg_lake CASCADE;
            CREATE EXTENSION dblink;
            CREATE EXTENSION lake_replication;
        """)
        check(
            "packaged extension loads",
            sql("""
                SELECT string_agg(extname || ':' || extversion, ',' ORDER BY extname)
                  FROM pg_extension
                 WHERE extname = 'lake_replication'
            """),
            "lake_replication:0.1.0-beta.3",
        )
        links = command([
            "docker", "exec", container, "sh", "-ec",
            "for binary in /usr/local/bin/lake-replication-*; do "
            "test -x \"$binary\"; ldd \"$binary\" >/dev/null; done; "
            "ldd /usr/lib/postgresql/18/lib/lake_replication.so >/dev/null",
        ])
        check("packaged native files link", links.returncode, 0)
        exit_code = 0
    except Exception as error:
        EVIDENCE["error"] = str(error)
        print(f"FAIL {error}", flush=True)
    finally:
        compose(
            "exec", "-T", "postgres", "dropdb", "--if-exists", "-U", "postgres",
            DATABASE, check_result=False,
        )
        if container:
            command([
                "docker", "exec", "-u", "root", container, "rm", "-rf",
                CONTAINER_BUNDLE,
                "/usr/share/postgresql/18/extension/lake_replication.control",
                "/usr/share/postgresql/18/extension/lake_replication--0.1.0.sql",
                "/usr/share/postgresql/18/extension/lake_replication--0.1.0-beta.3.sql",
                "/usr/share/postgresql/18/extension/lake_replication--0.1.0-beta.2--0.1.0-beta.3.sql",
            ], check_result=False)
        EVIDENCE["passed"] = exit_code == 0
        (RESULTS / "results.json").write_text(
            json.dumps(EVIDENCE, indent=2, sort_keys=True) + "\n"
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

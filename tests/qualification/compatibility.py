#!/usr/bin/env python3
"""Run the artifact-only qualification against exact pg_lake revisions."""

import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PROBE = ROOT / "tests/qualification/probe.py"
DEFAULT_REVISIONS = (
    "8180647d4535fdb1ed223f92f301ba6094a8ec3e",
    "f609ecb17891383f021aebb0e249fe2994ed57bb",
)


def revisions():
    configured = os.environ.get("COMPATIBILITY_PG_LAKE_REVISIONS")
    selected = configured.split() if configured else list(DEFAULT_REVISIONS)
    if len(selected) < 2:
        raise RuntimeError("the compatibility lane requires at least two revisions")
    if len(set(selected)) != len(selected):
        raise RuntimeError("the compatibility revisions must be distinct")
    invalid = [revision for revision in selected if not re.fullmatch(r"[0-9a-f]{40}", revision)]
    if invalid:
        raise RuntimeError(f"invalid full lowercase commit SHA: {invalid[0]}")
    return selected


def main():
    if not os.environ.get("QUALIFICATION_ARCHIVE"):
        raise RuntimeError(
            "set QUALIFICATION_ARCHIVE to the exact native archive and keep its .sha256 file adjacent"
        )
    for revision in revisions():
        print(f"QUALIFY pg_lake {revision}", flush=True)
        environment = {
            **os.environ,
            "QUALIFICATION_PG_LAKE_REVISION": revision,
        }
        subprocess.run(
            [sys.executable, str(PROBE)],
            cwd=ROOT,
            env=environment,
            check=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

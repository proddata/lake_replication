#!/usr/bin/env python3
"""Export locally available pg_lake Git history for Docker builds; never fetch."""

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "vendor/pg_lake"


def git(*args):
    return subprocess.check_output(
        ["git", "-C", str(SOURCE), *args], text=True
    ).strip()


def main():
    if not (SOURCE / ".git").exists():
        raise RuntimeError("initialize upstream first: git submodule update --init vendor/pg_lake")
    revision = sys.argv[1] if len(sys.argv) > 1 else git("rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise RuntimeError("expected a full lowercase pg_lake commit SHA")
    git("cat-file", "-e", f"{revision}^{{commit}}")
    if revision not in git("rev-list", "--all", "HEAD").splitlines():
        raise RuntimeError("requested revision must be reachable from a local branch, tag, or HEAD")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise RuntimeError("commit or stash upstream changes before exporting a reproducible build")
    destination = ROOT / ".upstream/pg_lake.bundle"
    destination.parent.mkdir(exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=destination.parent, suffix=".bundle")
    os.close(fd)
    try:
        # Include other locally fetched branches/tags for compatibility builds,
        # as well as the detached submodule HEAD. No upstream checkout changes.
        git("bundle", "create", temporary, "--all", "HEAD")
        os.replace(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)
    print(f"Prepared local pg_lake bundle for {revision}: {destination}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(str(error))

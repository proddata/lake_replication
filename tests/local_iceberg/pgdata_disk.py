#!/usr/bin/env python3
"""Physical ENOSPC across a private shared PostgreSQL/engine data filesystem."""
import sys
from wal_disk import LocalWalDiskProbe


if __name__=='__main__':sys.exit(LocalWalDiskProbe(scope='pgdata').run())

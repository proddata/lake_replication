#!/usr/bin/env python3
"""Add transaction-scoped, instance-wide admission to a disposable C copy."""
from pathlib import Path
import sys
from adapt import replace_once


def adapt(root):
    path = root / 'extensions/lake_replication/lake_replication.c'
    text = replace_once(path.read_text(), '#include "storage/latch.h"',
                        '#include "storage/latch.h"\n#include "storage/lock.h"')
    text += '\n' + Path(__file__).with_name('admission.c.inc').read_text()
    path.write_text(text)


if __name__ == '__main__':
    adapt(Path(sys.argv[1]))

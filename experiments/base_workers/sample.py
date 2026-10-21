#!/usr/bin/env python3
"""Read Linux resource counters inside the disposable destination/pgduck container."""
import json
import os
import sys
import time
from pathlib import Path


def sample(launcher_pids=()):
    cpu = dict(line.split() for line in Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())
    result = {
        'monotonic_seconds': time.monotonic(),
        'cpu_usage_usec': int(cpu['usage_usec']),
        'memory_current_bytes': int(Path('/sys/fs/cgroup/memory.current').read_text()),
        'clock_ticks': os.sysconf('SC_CLK_TCK'),
        'processes': [],
    }
    for directory in Path('/proc').iterdir():
        if not directory.name.isdigit():
            continue
        try:
            cmdline = (directory / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
            is_launcher = int(directory.name) in launcher_pids or 'lake_replication launcher' in cmdline
            if not (is_launcher or 'lake_replication supervisor' in cmdline
                    or '/usr/local/bin/lake-replication-' in cmdline
                    or 'pg base extension' in cmdline):
                continue
            # comm may contain spaces; fields after the closing ')' begin at field 3.
            fields = (directory / 'stat').read_text().rsplit(')', 1)[1].split()
            status = dict(line.split(':', 1) for line in (directory / 'status').read_text().splitlines())
            pss = next(line.split()[1] for line in (directory / 'smaps_rollup').read_text().splitlines()
                       if line.startswith('Pss:'))
            result['processes'].append({
                'pid': int(directory.name),
                'kind': 'wrapper' if 'lake_replication supervisor' in cmdline else 'launcher' if is_launcher else 'framework' if 'pg base extension' in cmdline else 'rust',
                'start_ticks': int(fields[19]),
                'cpu_ticks': int(fields[11]) + int(fields[12]),
                'rss_bytes': int(fields[21]) * os.sysconf('SC_PAGE_SIZE'),
                'pss_bytes': int(pss) * 1024,
                'voluntary_switches': int(status['voluntary_ctxt_switches']),
                'involuntary_switches': int(status['nonvoluntary_ctxt_switches']),
            })
        except (FileNotFoundError, ProcessLookupError, PermissionError, StopIteration):
            continue
    return result


if __name__ == '__main__':
    pids = {int(pid) for pid in sys.argv[1].split(',') if pid} if len(sys.argv) > 1 else set()
    print(json.dumps(sample(pids), sort_keys=True))

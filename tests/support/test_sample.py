"""Resource sampler must exclude its python -c source and identify the engine."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('resource_sample', Path(__file__).with_name('sample.py'))
sampler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sampler)


class ResourceSampleTests(unittest.TestCase):
    def test_self_exclusion_engine_and_throttling(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            proc, cgroup = root / 'proc', root / 'cgroup'
            proc.mkdir(); cgroup.mkdir()
            (cgroup / 'cpu.stat').write_text('usage_usec 100\nnr_throttled 3\nthrottled_usec 25\n')
            (cgroup / 'memory.current').write_text('2048\n')
            (cgroup / 'cpu.max').write_text('200000 100000\n')
            (cgroup / 'io.pressure').write_text('some avg10=1.0 total=123\n')
            for pid, command in [(os.getpid(), 'python3\0-c\0lake_replication supervisor'),
                                 (999999, '/usr/local/bin/pgduck_server\0--port\0' + '5332'),
                                 (999998, 'postgres: checkpointer\0')]:
                directory = proc / str(pid)
                directory.mkdir()
                (directory / 'cmdline').write_bytes(command.encode())
                fields = ['0'] * 22
                fields[11], fields[12], fields[19], fields[21] = '12', '4', '30', '8'
                (directory / 'stat').write_text(f'{pid} (name with spaces) ' + ' '.join(fields))
                (directory / 'status').write_text('voluntary_ctxt_switches: 4\nnonvoluntary_ctxt_switches: 2\n')
                (directory / 'smaps_rollup').write_text('Pss: 16 kB\n')
            with patch.object(sampler.os, 'sched_getaffinity', return_value={0, 1}, create=True):
                result = sampler.sample(proc_root=proc, cgroup_root=cgroup)
            self.assertEqual([p['kind'] for p in sorted(result['processes'], key=lambda p:p['pid'])], ['postgres','engine'])
            self.assertNotIn(os.getpid(), [p['pid'] for p in result['processes']])
            self.assertEqual(result['cpu_stat']['throttled_usec'], 25)
            self.assertEqual(result['cgroup']['cpu.max'], '200000 100000')
            self.assertEqual(result['cpu_affinity'], [0, 1])
            self.assertEqual(result['processes'][0]['cpu_ticks'], 16)

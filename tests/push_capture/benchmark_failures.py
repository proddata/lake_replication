#!/usr/bin/env python3
"""Verify benchmark failures retain bounded diagnostic snapshots and clean up."""
import json
import os
import sys
from benchmark import BenchmarkProbe


class FailedDrainProbe(BenchmarkProbe):
    def measure(self,mode,consume):
        def failed_drain():raise RuntimeError('injected benchmark drain failure')
        return super().measure(mode,failed_drain)


def main():
    os.environ.update(PUSH_BENCH_ORDER='pull_first',PUSH_BENCH_CONCURRENT='1',PUSH_BENCH_TRANSACTIONS='4',
        PUSH_BENCH_ROWS='2',PUSH_BENCH_INTERVAL='0',PUSH_BENCH_OBJECT_TRACE='0')
    probe=FailedDrainProbe()
    code=probe.run()
    saved=json.loads((probe.output/'results.json').read_text())
    failure=saved['measurements']['logical_pull']
    assert code==1 and saved['cleanup_returncode']==0
    assert 'injected benchmark drain failure' in failure['error']
    assert failure['observed_application_commits']==4
    assert failure['diagnostics']['source']['rows_a']==8 and failure['diagnostics']['source']['rows_b']==8
    assert failure['diagnostics']['destination']['journal_transactions']==0
    assert failure['diagnostics']['destination']['mirror_rows_a']==0
    assert failure['wall_elapsed_seconds']>=0 and failure['elapsed_seconds']>=0
    print('PASS expected failure retains writer/replication diagnostics and removes its fixture',flush=True)
    return 0


if __name__=='__main__':sys.exit(main())

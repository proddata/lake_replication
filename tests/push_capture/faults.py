#!/usr/bin/env python3
"""Crash-boundary source SQL qualification; test-only instrumented runtime."""
from datetime import datetime, timezone
import hashlib
import json
import subprocess
import sys
import time

from probe import Probe, ROOT


class FaultProbe(Probe):
    def __init__(self):
        super().__init__()
        self.evidence['scope'] = 'instrumented synthetic source SQL crash boundaries'
        self.evidence['boundaries'] = {}

    def library_hashes(self, runtime):
        marker = json.loads(self.command(self.compose + ['exec', '-T', 'source', 'sh', '-ec',
            'cat "$(pg_config --sharedir)/pg_lake/push-faults.json"']).stdout)
        self.evidence['fault_instrumentation'] = marker
        self.check('instrumented runtime explicitly marked test only', marker['test_only'], True)
        patch = ROOT / 'deploy/docker-compose/patches/pg-lake-lab-crash-points.patch'
        self.check('fault instrumentation matches retained patch', marker['patch_sha256'],
                   hashlib.sha256(patch.read_bytes()).hexdigest())
        self.evidence['files_sha256'][str(patch.relative_to(ROOT))] = marker['patch_sha256']
        return dict(runtime['libraries'], pg_lake_table=marker['library_sha256'])

    def wait_ready(self):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if self.value('SELECT 1') == '1':
                    return
            except (RuntimeError, subprocess.TimeoutExpired):
                pass
            time.sleep(0.2)
        raise RuntimeError('source did not recover after injected crash')

    def boundary(self, point, number, previous_count, committed):
        started = datetime.now(timezone.utc).isoformat()
        writer = self.sql(f"SET pg_lake_table.lab_crash_point='{point}'; BEGIN;"
                          + self.append(number) + 'COMMIT;', required=False)
        self.check(f'{point}: no successful client response', writer.returncode != 0, True)
        self.wait_ready()
        expected = previous_count + int(committed)
        self.check(f'{point}: all logs and local control state agree after recovery',
                   self.state(), [expected] * 5)
        logs = self.command(self.compose + ['logs', '--no-color', '--since', started, 'source'])
        diagnostic = logs.stdout + logs.stderr
        (self.output / f'{point}.log').write_text(diagnostic)
        self.check(f'{point}: injected SIGKILL caused recovery',
                   'was terminated by signal 9' in diagnostic and
                   'all server processes terminated; reinitializing' in diagnostic and
                   'database system is ready to accept connections' in diagnostic, True)
        identity = self.value("SELECT count(*) FROM capture_probe.receipts WHERE "
                              f"batch_id='00000000-0000-0000-0000-{number:012d}'")
        self.check(f'{point}: reconnect resolves durable identity', identity, str(int(committed)))
        # Retry only after the fresh durable-identity check. The postcommit crash
        # occurs inside XACT_EVENT_COMMIT, before PostgreSQL can send success.
        self.check(f'{point}: retry reconciles rather than blindly appending',
                   self.value(self.append(number)), 'f' if committed else 't')
        self.check(f'{point}: exactly one live batch after retry',
                   self.state(), [previous_count + 1] * 5)
        catalog = json.loads(self.value('''SELECT jsonb_agg(jsonb_build_object(
            'table', table_name, 'metadata_location', metadata_location))
            FROM iceberg_tables WHERE table_namespace='capture_probe';'''))
        self.evidence['boundaries'][point] = dict(
            writer_returncode=writer.returncode, writer_stderr=writer.stderr,
            committed_before_retry=committed, recovered_catalog=catalog,
            state_after_retry=self.state())

    def exercise(self):
        super().exercise()
        self.check('fault injection defaults off',
                   self.value('SHOW pg_lake_table.lab_crash_point'), 'none')
        self.boundary('before_upload_completion', 7, 4, False)
        self.boundary('after_upload_completion', 8, 5, False)
        self.boundary('after_commit', 9, 6, True)
        self.evidence['unqualified'].remove('upload/catalog crash boundaries')


if __name__ == '__main__':
    sys.exit(FaultProbe().run())

#!/usr/bin/env python3
"""Source-side SQL feasibility only; provisions and removes an isolated fixture."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from session import Session

ROOT = Path(__file__).resolve().parents[2]


class Probe:
    def __init__(self):
        stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
        self.project = f'lake-push-probe-{stamp}-{uuid.uuid4().hex[:8]}'
        self.output = ROOT / '.qualification' / self.project
        self.compose = ['docker', 'compose', '-p', self.project, '-f',
                        str(ROOT / 'deploy/push/compose.yml')]
        self.evidence = {'project': self.project, 'checks': [],
                         'scope': 'synthetic source SQL; no decoder or importer',
                         'unqualified': ['upload/catalog crash boundaries',
                                         'concurrent consistent log reads',
                                         'source decoding and slot advancement',
                                         'logical integration/recovery baseline']}

    @staticmethod
    def command(args, *, input=None, timeout=60, required=True):
        result = subprocess.run(args, input=input, text=True,
                                capture_output=True, timeout=timeout)
        if required and result.returncode:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        return result

    def sql_args(self):
        return self.compose + ['exec', '-T', 'source', 'psql', '-XqAt',
                               '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=verbose',
                               '-U', 'postgres', '-d', 'capture']

    def sql(self, statement, *, required=True):
        return self.command(self.sql_args(), input=statement, required=required)

    def value(self, statement):
        return self.sql(statement).stdout.strip()

    def check(self, name, actual, expected):
        passed = actual == expected
        self.evidence['checks'].append(dict(name=name, actual=actual,
                                            expected=expected, passed=passed))
        print(f"{'PASS' if passed else 'FAIL'} {name}", flush=True)
        if not passed:
            raise AssertionError(f'{name}: {actual!r} != {expected!r}')

    @staticmethod
    def append(number, *, changed=False):
        # Synthetic source boundaries, deliberately with unrelated-WAL gaps.
        body = '[{"id":99}]' if changed else '[{"id":1,"note":null}]'
        return ("SELECT capture_probe.append_batch("
                f"'00000000-0000-0000-0000-{number:012d}', '0/{number * 256:X}',"
                f"'{body}'::jsonb, '[{{\"id\":2,\"note\":\"b\"}}]'::jsonb);")

    def state(self):
        # First importer read path is explicitly source PostgreSQL/pg_lake.
        return json.loads(self.value('''
            BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
            SELECT jsonb_build_array(
                (SELECT count(*) FROM capture_probe.log_a),
                (SELECT count(*) FROM capture_probe.log_b),
                (SELECT count(*) FROM capture_probe.manifests),
                (SELECT count(*) FROM capture_probe.receipts),
                (SELECT sequence FROM capture_probe.progress));
            COMMIT;
        '''))

    def provenance(self):
        self.evidence['source_commit'] = self.command(
            ['git', '-C', str(ROOT), 'rev-parse', 'HEAD']).stdout.strip()
        self.evidence['worktree_status'] = self.command(
            ['git', '-C', str(ROOT), 'status', '--short']).stdout.splitlines()
        diff = self.command(['git', '-C', str(ROOT), 'diff', '--binary']).stdout
        self.evidence['tracked_worktree_diff_sha256'] = hashlib.sha256(
            diff.encode()).hexdigest()
        self.evidence['pg_lake_revision'] = self.command(
            ['git', '-C', str(ROOT / 'vendor/pg_lake'), 'rev-parse', 'HEAD']).stdout.strip()
        self.evidence['files_sha256'] = {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for directory in ['tests/push_capture', 'deploy/push']
            for path in sorted((ROOT / directory).rglob('*'))
            if path.is_file() and '__pycache__' not in path.parts
        }
        self.evidence['images'] = {}
        for key in ['PUSH_PG_LAKE_IMAGE', 'PUSH_STORAGE_IMAGE', 'PUSH_STORAGE_INIT_IMAGE']:
            reference = os.environ[key]
            info = json.loads(self.command(['docker', 'image', 'inspect', reference]).stdout)[0]
            self.evidence['images'][key] = {
                'reference': reference, 'id': info['Id'],
                'architecture': info['Architecture'], 'digests': info.get('RepoDigests', [])}

    def backend_termination(self):
        writer = subprocess.Popen(self.sql_args(), text=True, stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            writer.stdin.write("SET application_name='push_probe_before_commit'; BEGIN;"
                               + self.append(3) + 'SELECT pg_sleep(45); COMMIT;\n')
            writer.stdin.close()
            writer.stdin = None
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                pid = self.value("SELECT pid FROM pg_stat_activity WHERE "
                                 "application_name='push_probe_before_commit' "
                                 "AND wait_event='PgSleep'")
                if pid:
                    break
                if writer.poll() is not None:
                    raise RuntimeError('writer exited before precommit barrier')
                time.sleep(0.1)
            else:
                raise RuntimeError('writer did not reach precommit barrier')
            self.check('terminate only the fixture writer', self.value(
                f'SELECT pg_terminate_backend({int(pid)}, 5000)'), 't')
            _, stderr = writer.communicate(timeout=15)
            self.check('terminated writer reports failure', writer.returncode != 0, True)
            self.evidence['precommit_termination_stderr'] = stderr
        finally:
            if writer.poll() is None:
                writer.kill()
                writer.communicate(timeout=10)

    def library_hashes(self, runtime):
        return runtime['libraries']

    def consistent_read(self):
        reader = Session(self.sql_args())
        try:
            # Pin the transaction snapshot through ordinary state first. This
            # deliberately leaves Iceberg tables unopened until AFTER capture
            # commits, detecting a per-table latest-snapshot read path.
            self.check('reader pins pre-capture source snapshot', reader.query(
                'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; '
                'SELECT sequence FROM capture_probe.progress'), ['3'])
            self.check('capture commits while old reader snapshot stays open',
                       self.value(self.append(6)), 't')
            for table in ['manifests', 'log_a', 'log_b', 'receipts']:
                self.check(f'old snapshot reads consistent {table}', reader.query(
                    f'SELECT count(*) FROM capture_probe.{table}'), ['3'])
            self.check('reader snapshot keeps its original checkpoint', reader.query(
                'SELECT sequence FROM capture_probe.progress'), ['3'])
            reader.query('COMMIT')
            self.check('fresh snapshot sees the complete new batch', self.state(), [4] * 5)
            self.evidence['unqualified'].remove('concurrent consistent log reads')
        finally:
            reader.close()

    def exercise(self):
        info = json.loads(self.value('''SELECT jsonb_build_object(
            'version', version(), 'server_version_num', current_setting('server_version_num'),
            'system_identifier', (SELECT system_identifier::text FROM pg_control_system()),
            'database_oid', (SELECT oid FROM pg_database WHERE datname=current_database()),
            'extensions', (SELECT jsonb_object_agg(extname, extversion) FROM pg_extension),
            'wal_level', current_setting('wal_level'),
            'object_prefix', current_setting('pg_lake_iceberg.default_location_prefix'));'''))
        self.evidence['source'] = info
        self.check('source is PostgreSQL 18', int(info['server_version_num']) // 10000, 18)
        self.check('source has no lake_replication installation requirement',
                   'lake_replication' in info['extensions'], False)
        marker = self.command(self.compose + ['exec', '-T', 'source', 'sh', '-ec',
            'cat "$(pg_config --sharedir)/pg_lake/lake-replication-patchset.json"'])
        self.evidence['runtime_patchset'] = json.loads(marker.stdout)
        runtime = self.evidence['runtime_patchset']
        self.check('runtime uses the pinned pg_lake revision',
                   runtime['upstream_revision'], self.evidence['pg_lake_revision'])
        manifest = json.loads((ROOT / 'deploy/pg_lake/patchset.json').read_text())
        self.check('runtime uses the expected patchset', runtime['patchset'], manifest['patchset'])
        self.check('runtime patch matches repository inputs',
                   runtime['patch_sha256'], manifest['patch_sha256'])
        expected_hashes = self.library_hashes(runtime)
        for library in ['pg_lake_iceberg', 'pg_lake_table']:
            installed = self.command(self.compose + ['exec', '-T', 'source', 'sh', '-ec',
                f'sha256sum "$(pg_config --pkglibdir)/{library}.so"']).stdout.split()[0]
            self.check(f'{library} installed library matches runtime marker',
                       installed, expected_hashes[library])
        self.sql((ROOT / 'tests/push_capture/fixture.sql').read_text())
        self.check('new stream is empty', self.state(), [0, 0, 0, 0, 0])
        self.check('commit accepts a two-table batch', self.value(self.append(1)), 't')
        self.check('logs manifest receipt and checkpoint commit together',
                   self.state(), [1, 1, 1, 1, 1])
        self.sql('BEGIN;' + self.append(2) + 'ROLLBACK;')
        self.check('rollback leaves no batch or checkpoint', self.state(), [1, 1, 1, 1, 1])
        failed = self.sql('BEGIN;' + self.append(2) + 'SELECT 1/0; COMMIT;', required=False)
        self.check('error after appends fails the transaction', failed.returncode != 0, True)
        self.check('transaction error rolls back every artifact', self.state(), [1, 1, 1, 1, 1])
        self.backend_termination()
        self.check('backend death before commit exposes nothing', self.state(), [1, 1, 1, 1, 1])
        # COMMIT succeeds, then the session dies. Do not consume the append result;
        # recover by durable identity on a fresh connection before any retry.
        lost = self.sql('BEGIN;' + self.append(4) +
                        'COMMIT; SELECT pg_terminate_backend(pg_backend_pid());', required=False)
        self.check('postcommit session loss reports failure', lost.returncode != 0, True)
        self.check('reconnect resolves committed batch by durable identity', self.value(
            "SELECT sequence FROM capture_probe.receipts WHERE "
            "batch_id='00000000-0000-0000-0000-000000000004'"), '2')
        self.check('identical retry does not append', self.value(self.append(4)), 'f')
        self.check('lost-session replay leaves exactly one live batch',
                   self.state(), [2, 2, 2, 2, 2])
        conflict = self.sql(self.append(4, changed=True), required=False)
        self.check('same identity with different payload fails closed',
                   conflict.returncode != 0 and '23505' in conflict.stderr, True)
        stale = self.sql(self.append(2), required=False)
        self.check('new identity cannot move frontier backwards',
                   stale.returncode != 0 and 'frontier must increase' in stale.stderr, True)
        self.check('rejected replays leave capture state unchanged',
                   self.state(), [2, 2, 2, 2, 2])
        self.check('manifests have contiguous sequence despite source LSN gaps', self.value(
            'SELECT string_agg(sequence::text,\',\' ORDER BY sequence) '
            'FROM capture_probe.manifests'), '1,2')
        self.check('manifest counts and payload digests match both logs', self.value('''
            WITH bodies AS (
                SELECT m.batch_id, m.digest, m.count_a, m.count_b,
                    (SELECT jsonb_agg(payload::jsonb ORDER BY ordinal)
                     FROM capture_probe.log_a a WHERE a.batch_id=m.batch_id) AS a,
                    (SELECT jsonb_agg(payload::jsonb ORDER BY ordinal)
                     FROM capture_probe.log_b b WHERE b.batch_id=m.batch_id) AS b
                FROM capture_probe.manifests m)
            SELECT bool_and(count_a=jsonb_array_length(a) AND count_b=jsonb_array_length(b)
                AND digest=encode(sha256(convert_to(jsonb_build_array(a,b)::text,'UTF8')),'hex'))
            FROM bodies;
        '''), 't')
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(self.value, [self.append(5), self.append(5)]))
        self.check('concurrent identical retries append once', sorted(results), ['f', 't'])
        self.check('concurrent retry keeps all artifact counts aligned',
                   self.state(), [3, 3, 3, 3, 3])
        self.consistent_read()

    def run(self):
        self.output.mkdir(parents=True, exist_ok=False)
        exit_code = 1
        try:
            self.provenance()
            # Retain the exact uncommitted harness/source inputs, not just
            # hashes pointing at files that later implementation may change.
            for name,digest in self.evidence['files_sha256'].items():
                data=(ROOT/name).read_bytes()
                if hashlib.sha256(data).hexdigest()!=digest:
                    raise RuntimeError(f'probe input changed during provenance capture: {name}')
                target=self.output/'sources'/name
                target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes(data)
            self.command(self.compose + ['config', '--quiet'])
            print(f'Starting isolated source fixture {self.project}', flush=True)
            self.command(self.compose + ['up', '-d', '--pull', 'never', '--wait',
                                         '--wait-timeout', '180'], timeout=240)
            self.exercise()
            self.evidence['status'] = 'passed'
            exit_code = 0
        except Exception as exc:
            self.evidence.update(status='error', error=str(exc))
            print(f'ERROR: {exc}', file=sys.stderr)
        finally:
            try:
                logs = self.command(self.compose + ['logs', '--no-color'], required=False)
                (self.output / 'containers.log').write_text(logs.stdout + logs.stderr)
                cleanup = self.command(self.compose + ['down', '-v', '--remove-orphans'],
                                       timeout=90, required=False)
                self.evidence['cleanup_returncode'] = cleanup.returncode
                if cleanup.returncode:
                    self.evidence['status'] = 'error'
                    self.evidence['cleanup_error'] = cleanup.stderr
                    exit_code = 1
            finally:
                (self.output / 'results.json').write_text(json.dumps(self.evidence, indent=2) + '\n')
                print(f'Evidence: {self.output}', flush=True)
        return exit_code


if __name__ == '__main__':
    sys.exit(Probe().run())

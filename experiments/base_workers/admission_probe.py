#!/usr/bin/env python3
"""Qualify instance-wide transaction admission in disposable Docker fixtures."""
import concurrent.futures
import hashlib
import importlib.util
import json
import shutil
import tempfile
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('scale', Path(__file__).with_name('scale.py'))
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)
c, q = s.c, s.q
VIEW = 'lake_replication.prototype_writer_admission'
HELD = f"SELECT count(*) FROM {VIEW} WHERE admission_state='admitted'"
WAITING = f"SELECT count(*) FROM {VIEW} WHERE admission_state='waiting_for_writer'"


def count(statement, expected):
    q.check(statement, q.wait_for('destination', 'analytics', statement, str(expected)), str(expected))


def rejected(future, label):
    try:
        future.result(timeout=15)
    except Exception as error:
        q.check(label, any(t in str(error) for t in ('canceling statement', 'terminating connection', 'ownership rejected')), True)
    else:
        raise AssertionError(label + ': unexpectedly completed')


def transaction_checks(image, text, directory):
    directory.mkdir()
    q.COMPOSE_FILE = directory / 'compose.yml'
    q.COMPOSE_FILE.write_text(text)
    q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = image
    clients = []
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    try:
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        q.sql('destination', 'analytics', 'CREATE DATABASE admission_other')
        q.sql('destination', 'admission_other', 'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
        owner = c.Psql('destination', 'analytics')
        first = c.Psql('destination', 'admission_other')
        second = c.Psql('destination', 'analytics')
        clients.extend((owner, first, second))
        owner.query('BEGIN; SELECT lake_replication.prototype_admit_writer(); SELECT lake_replication.prototype_admit_writer()')
        count(HELD, 1)
        first.query('BEGIN')
        first_wait = pool.submit(first.query, 'SELECT lake_replication.prototype_admit_writer()')
        count(WAITING, 1)
        q.check('waiter from another database is visible', q.sql('destination', 'analytics',
            f"SELECT database_name FROM {VIEW} WHERE admission_state='waiting_for_writer'"), 'admission_other')
        second.query('BEGIN')
        second_pid = second.query('SELECT pg_backend_pid()')[0]
        second_wait = pool.submit(second.query, 'SELECT lake_replication.prototype_admit_writer()')
        count(WAITING, 2)
        q.check('ordinary SQL still runs while writers queue', q.sql('destination', 'admission_other', 'SELECT 1'), '1')
        owner.query('COMMIT')
        first_wait.result(timeout=15)
        q.check('first queued writer is admitted before second', second_wait.done(), False)
        count(HELD, 1)
        q.sql('destination', 'analytics', f'SELECT pg_cancel_backend({second_pid})')
        rejected(second_wait, 'queued writer cancellation is interruptible')
        count(WAITING, 0)
        first.query('ROLLBACK')
        count(HELD, 0)
        owner.query('BEGIN; SAVEPOINT inner_work; SELECT lake_replication.prototype_admit_writer(); ROLLBACK TO inner_work')
        count(HELD, 0)
        owner.query('SELECT lake_replication.prototype_admit_writer(); ROLLBACK')
        count(HELD, 0)
        owner.query('BEGIN; SELECT lake_replication.prototype_admit_writer()')
        timed = q.sql_result('destination', 'admission_other',
            "SET lock_timeout='250ms'; SELECT lake_replication.prototype_admit_writer()", check_result=False)
        q.check('lock timeout leaves no orphan permit request', timed.returncode != 0 and 'lock timeout' in timed.stderr, True)
        count(WAITING, 0)
        # The ownership guard queues before touching its configuration row.
        invalid = c.Psql('destination', 'admission_other')
        clients.append(invalid)
        guard = pool.submit(invalid.query, 'SELECT lake_replication.prototype_assert_writer(999999)')
        count(WAITING, 1)
        owner.query('COMMIT')
        rejected(guard, 'ownership rejection still applies after admission')
        count(HELD, 0)
        doomed = c.Psql('destination', 'admission_other')
        clients.append(doomed)
        pid = doomed.query('SELECT pg_backend_pid()')[0]
        doomed.query('BEGIN; SELECT lake_replication.prototype_admit_writer()')
        count(HELD, 1)
        q.sql('destination', 'analytics', f'SELECT pg_terminate_backend({pid})')
        count(HELD, 0)
        owner.query('SELECT lake_replication.prototype_admit_writer()')
        count(HELD, 0)
        q.check('all ten native guards remain installed', q.sql('destination', 'analytics',
            "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname='lake_replication' AND p.prosrc LIKE '%PERFORM lake_replication.prototype_assert_writer(p_subscription_id);%'"), '10')
        # Exercise an actual queued worker, not only synthetic permit calls.
        q.sql('source', 'application', "CREATE TABLE admission_items(id bigint PRIMARY KEY); INSERT INTO admission_items VALUES(1); CREATE PUBLICATION admission_pub FOR TABLE admission_items; GRANT SELECT ON admission_items TO lake_source")
        connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
        q.sql('destination', 'admission_other', f"CREATE SUBSCRIPTION admission_sub CONNECTION '{connection}' PUBLICATION admission_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        q.check('real initial copy completes under admission', q.wait_for('destination', 'admission_other',
            "SELECT state FROM lake_replication.subscriptions WHERE subscription_name='admission_sub'", 'streaming'), 'streaming')
        q.sql('destination', 'admission_other', 'ALTER SUBSCRIPTION admission_sub DISABLE')
        count(s.WRAPPERS, 0)
        q.wait_for('source', 'application', "SELECT active::text FROM pg_replication_slots WHERE slot_name='admission_sub'", 'false')
        owner.query('BEGIN; SELECT lake_replication.prototype_admit_writer()')
        q.sql('source', 'application', 'INSERT INTO admission_items VALUES(2)')
        q.sql('destination', 'admission_other', 'ALTER SUBSCRIPTION admission_sub ENABLE')
        count(f'SELECT (count(*)>0)::text FROM {VIEW} WHERE admission_state=\'waiting_for_writer\'', 'true')
        stopped = q.sql_result('destination', 'admission_other', "SET statement_timeout='5s'; ALTER SUBSCRIPTION admission_sub DISABLE", check_result=False)
        q.check('queued worker holds no ownership row lock that blocks disable', stopped.returncode, 0)
        count(s.WRAPPERS, 0)
        owner.query('COMMIT')
        count(WAITING, 0)
        count(HELD, 0)
        q.check('disabled queued worker did not publish its pending change', q.sql('destination', 'admission_other', 'SELECT count(*) FROM admission_items'), '1')
        q.sql('destination', 'admission_other', 'ALTER SUBSCRIPTION admission_sub ENABLE')
        q.check('queued subscription resumes and makes progress', q.wait_for('destination', 'admission_other', 'SELECT count(*) FROM admission_items', '2'), '2')
        q.sql('destination', 'admission_other', 'ALTER SUBSCRIPTION admission_sub DISABLE')
        count(s.WRAPPERS, 0)
        q.wait_for('source', 'application', "SELECT active::text FROM pg_replication_slots WHERE slot_name='admission_sub'", 'false')
        q.sql('destination', 'admission_other', 'DROP SUBSCRIPTION admission_sub')
        # SIGKILL of a PostgreSQL backend triggers expected internal crash
        # recovery. With no replication supervisors left, isolate permit reset.
        crash = c.Psql('destination', 'admission_other')
        clients.append(crash)
        pid = crash.query('SELECT pg_backend_pid()')[0]
        crash.query('BEGIN; SELECT lake_replication.prototype_admit_writer()')
        count(HELD, 1)
        q.compose('exec', '-T', '--user', 'postgres', 'destination', 'kill', '-KILL', pid)
        q.check('PostgreSQL recovers after forced holder crash', q.wait_for('destination', 'analytics', 'SELECT 1', '1', timeout=90), '1')
        count(HELD, 0)
        q.sql('destination', 'admission_other', 'SELECT lake_replication.prototype_admit_writer()')
        count(HELD, 0)
        logs = q.compose('logs', '--no-color', 'destination').stdout
        q.check('forced backend crash actually caused PostgreSQL recovery', 'was terminated by signal 9' in logs and 'reinitializing' in logs, True)
    finally:
        # Cancel SQL waits before closing clients so a failed check cannot leave
        # the thread pool blocked behind a transaction held by another client.
        q.sql_result('destination', 'analytics',
            f'SELECT pg_terminate_backend(pid) FROM {VIEW} WHERE pid<>pg_backend_pid()', check_result=False)
        for client in clients:
            client.close()
        pool.shutdown(wait=True, cancel_futures=True)
        try:
            logs = q.compose('logs', '--no-color', check_result=False)
            (directory / 'compose.log').write_text(logs.stdout + logs.stderr)
        finally:
            q.compose('down', '-v', '--remove-orphans', timeout=300)


def main():
    q.RESULTS = ROOT / '.qualification' / (q.RUN + '_admission')
    q.RESULTS.mkdir(parents=True, exist_ok=False)
    root = q.RESULTS
    result = dict(status='running', production_changed=False,
        admission='one worker write transaction per PostgreSQL instance, across all databases',
        limits=['Experimental fresh-install overlay; PostgreSQL 18/Linux arm64.',
                'Does not reduce subscription wrapper count or bound ordinary SQL/independent maintenance.',
                'No multi-instance engine coordination, distributed ownership, adjustable pool or throughput qualification.'])
    original = q.COMPOSE_FILE.read_text()
    sources = root / 'sources'
    sources.mkdir()
    for name in ('admission_probe.py', 'admission.py', 'admission.c.inc', 'admission.sql', 'Dockerfile.admission',
                 'scale.py', 'compare.py', 'adapt.py', 'runtime.c.inc', 'binding.py', 'binding.sql', 'publication.py', 'publication.sql', 'register.sql'):
        shutil.copy2(Path(__file__).with_name(name), sources / name)
    result['source_fingerprints'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources.iterdir()}
    parent_file = ROOT / 'docs/evidence/2026-10-20-base-current-scaling-performance-results.json'
    parent = json.loads(parent_file.read_text())
    result['parent_evidence_sha256'] = hashlib.sha256(parent_file.read_bytes()).hexdigest()
    try:
        image = parent['image_id']
        q.check('exact current parent image', q.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', image]).stdout.strip(), image)
        for name in ('adapt.py', 'runtime.c.inc', 'binding.py', 'binding.sql', 'publication.py', 'publication.sql', 'register.sql', 'scale.py', 'compare.py'):
            q.check('qualified parent source ' + name, result['source_fingerprints'][name], parent['source_fingerprints'][name])
        archive = q.archive_path()
        q.verify_archive(archive)
        q.command(['git', 'diff', '--exit-code', q.EVIDENCE['source_revision'], '--', 'worker', 'migrations', 'extensions/lake_replication'])
        tag = 'lake-replication-admission-parent:' + q.RUN
        new = 'lake-replication-admission:' + q.RUN
        q.command(['docker', 'tag', image, tag])
        built = q.command(['docker', 'build', '--build-arg', f'QUAL_IMAGE={tag}', '--tag', new,
            '--file', str(Path(__file__).with_name('Dockerfile.admission')), str(ROOT)], timeout=600)
        (root / 'build.log').write_text(built.stdout + built.stderr)
        result['image_id'] = q.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', new]).stdout.strip()
        with tempfile.TemporaryDirectory(prefix='lake-admission-') as temporary:
            q.build_images(archive, Path(temporary))
        text = original.replace('  destination:\n', '  destination:\n    init: true\n')
        text = text.replace('shared_preload_libraries=pg_extension_base,lake_replication', 'shared_preload_libraries=pg_extension_base')
        text = text.replace('      - -c\n      - lake_replication.database_name=analytics\n', '')
        text = text.replace('      - max_worker_processes=24\n', '      - max_worker_processes=24\n      - -c\n      - lake_replication.prototype_instance_id=admission-owner\n')
        text = text.replace('max_replication_slots=8', 'max_replication_slots=32').replace('max_wal_senders=8', 'max_wal_senders=32')
        engine = "printf 'SET threads=1;\\n' > /tmp/admission-pgduck.sql; cat /qualification/pgduck.sql >> /tmp/admission-pgduck.sql; exec pgduck_server --unix_socket_directory /var/run/pgduck --port 5332 --memory_limit 2GB --cache_dir /tmp/pgduck-cache --init_file_path /tmp/admission-pgduck.sql"
        text = text.replace('    entrypoint: [/qualification/pgduck.sh]', '    entrypoint: ' + json.dumps(['bash', '-ec', engine]))
        transaction_checks(new, text, root / 'transactions')
        result['scale_case'] = s.scale_case(4, 16, 64, new, text, root / 'scale-4db-16sub-64slots')
        if result['scale_case']['status'] != 'passed':
            raise RuntimeError('bounded scaling case failed: ' + result['scale_case']['error'])
        result['status'] = 'passed'
    except Exception as error:
        result.update(status='error', error=str(error))
        print('ERROR: ' + str(error), flush=True)
    finally:
        result['checks'] = q.EVIDENCE
        s.write_json(root / 'results.json', result)
        print('Evidence: ' + str(root), flush=True)
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())

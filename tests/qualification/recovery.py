#!/usr/bin/env python3
"""Rehearse coordinated offline PG/object/credential restore on disposable volumes.

Same-cluster recovery only: preserve source identity and slots, restore physical
destination identity, then test safe WAL catch-up and refusal of an advanced slot.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import tarfile
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', Path(__file__).with_name('probe.py'))
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)


def sql(statement):
    return q.sql('destination', 'analytics', statement)


def rows():
    return sql("SELECT string_agg(id||':'||note,',' ORDER BY id) FROM recovery_items")


def frontiers():
    return sql("SELECT jsonb_build_array(received_lsn,staged_lsn,published_lsn,acknowledged_lsn)::text FROM lake_replication.checkpoints")


def stopped():
    q.sql('destination', 'analytics', 'ALTER SUBSCRIPTION recovery_sub DISABLE')
    q.check('recovery subscription drains before offline snapshot', q.wait_for(
        'destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'", '0'), '0')
    q.compose('stop', 'destination', 'pgduck', 'storage', timeout=180)


def volume_names():
    config = json.loads(q.compose('config', '--format', 'json').stdout)
    names = {key: config['volumes'][key]['name'] for key in ['destination-data', 'object-data', 'credentials']}
    # Never remove data outside this unique, disposable qualification project.
    assert q.PROJECT.startswith('lake-replication-qualification-') and q.PROJECT.endswith('-physical-recovery')
    for name in names.values():
        info = json.loads(q.command(['docker', 'volume', 'inspect', name]).stdout)[0]
        assert info['Labels']['com.docker.compose.project'] == q.PROJECT
    return names


def offline(snapshot, volumes, restore=False):
    # Destination and engine are stopped by the caller. Use only our verified
    # fixture volumes and the installed archive's image; never mount source data.
    assert volumes == volume_names()
    if restore:
        logs = q.compose('logs', '--no-color', check_result=False)
        (q.RESULTS / ('before-restore-' + str(len(list(q.RESULTS.glob('before-restore-*')))) + '.log')).write_text(logs.stdout + logs.stderr)
        # Clear process-local and container-layer caches, retaining only the
        # named volumes we deliberately restore. Source remains untouched.
        q.compose('rm', '-f', 'destination', 'pgduck', 'storage')
    args = ['run', '--rm', '--no-deps', '-T', '--user', 'root', '--entrypoint', '/bin/sh',
            '--volume', f'{snapshot}:/snapshot',
            '--volume', volumes['object-data'] + ':/recovery-objects']
    if restore:
        command = """set -eu
test -s /snapshot/postgres.tar.gz && test -s /snapshot/objects.tar.gz && test -s /snapshot/credentials.tar.gz
find /var/lib/postgresql/18/data -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
find /recovery-objects -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
tar -xzf /snapshot/postgres.tar.gz -C /var/lib/postgresql/18/data
tar -xzf /snapshot/objects.tar.gz -C /recovery-objects
"""
        # The service normally mounts credentials read-only. A separate explicit
        # mount provides write access only to this project's own credential volume.
        args.extend(['--volume', volumes['credentials'] + ':/recovery-credentials'])
        command += "find /recovery-credentials -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +\ntar -xzf /snapshot/credentials.tar.gz -C /recovery-credentials\n"
    else:
        command = """set -eu
umask 077
tar -czf /snapshot/postgres.tar.gz -C /var/lib/postgresql/18/data .
tar -czf /snapshot/objects.tar.gz -C /recovery-objects .
tar -czf /snapshot/credentials.tar.gz -C /credentials .
"""
    q.compose(*args, 'destination', '-ec', command, timeout=300)
    digest = q.compose('run', '--rm', '--no-deps', '-T', '--user', 'root',
                       '--entrypoint', '/bin/sh', 'destination', '-ec',
                       'sha256sum /credentials/worker.env /credentials/destination-only.env').stdout.strip()
    if restore:
        q.check('offline restore preserves credential file bytes before startup', digest, q.EVIDENCE['credential_sha256'])
    else:
        q.EVIDENCE['credential_sha256'] = digest


def start_restored(identity, checkpoint):
    q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
    q.check('physical rollback restores the original extension version', sql(
        "SELECT extversion FROM pg_extension WHERE extname='lake_replication'"), q.RESTORE_VERSION)
    q.check('physical restore preserves destination cluster and database identity', sql(
        "SELECT (SELECT system_identifier FROM pg_control_system())||':'||(SELECT oid FROM pg_database WHERE datname=current_database())"), identity)
    q.check('physical restore preserves all checkpoint frontiers', frontiers(), checkpoint)
    q.check('physical restore keeps subscription disabled', sql('SELECT desired_state FROM lake_replication.worker_launch_config'), 'stopped')
    if sql("SELECT to_regprocedure('lake_replication.validate_runtime_state()') IS NOT NULL") == 't':
        sql('SELECT lake_replication.validate_runtime_state()')
    q.check('physical restore returns matching Iceberg objects and catalog', rows(), '1:one,2:two')


def exercise(snapshot):
    q.sql('source', 'application', "CREATE TABLE recovery_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO recovery_items VALUES(1,'one'),(2,'two'); CREATE PUBLICATION recovery_pub FOR TABLE recovery_items; GRANT SELECT ON recovery_items TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD, safe='')}@source:5432/application"
    sql(f"CREATE SUBSCRIPTION recovery_sub CONNECTION '{connection}' PUBLICATION recovery_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    q.check('recovery fixture publishes initial snapshot', q.wait_for('destination', 'analytics', "SELECT string_agg(id||':'||note,',' ORDER BY id) FROM recovery_items", '1:one,2:two'), '1:one,2:two')
    identity = sql("SELECT (SELECT system_identifier FROM pg_control_system())||':'||(SELECT oid FROM pg_database WHERE datname=current_database())")
    sql('ALTER SUBSCRIPTION recovery_sub DISABLE')
    q.check('snapshot has no running subscription wrapper', q.wait_for('destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'", '0'), '0')
    volumes = volume_names()
    checkpoint = frontiers()
    staged = sql('SELECT staged_lsn FROM lake_replication.checkpoints')
    stopped()
    offline(snapshot, volumes)
    q.EVIDENCE['snapshots'] = {p.name: {'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in snapshot.iterdir()}
    if q.UPGRADE_IMAGE:
        q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = q.UPGRADE_IMAGE
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        sql(f"ALTER EXTENSION lake_replication UPDATE TO '{q.VERSION}'")
        q.check('candidate SQL migration commits before physical rollback', sql(
            "SELECT extversion FROM pg_extension WHERE extname='lake_replication'"), q.VERSION)
        sql('SELECT lake_replication.validate_runtime_state()')
        q.check('committed migration preserves checkpoint before rollback', frontiers(), checkpoint)
        q.compose('stop', 'destination', 'pgduck', 'storage', timeout=180)
        # Reinstall the exact original image as well as restoring its old data.
        q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = q.RESTORE_IMAGE
    q.sql('source', 'application', "UPDATE recovery_items SET note='updated' WHERE id=1; DELETE FROM recovery_items WHERE id=2; INSERT INTO recovery_items VALUES(3,'after-backup')")
    offline(snapshot, volumes, restore=True)
    start_restored(identity, checkpoint)
    sql('ALTER SUBSCRIPTION recovery_sub ENABLE')
    query = "SELECT string_agg(id||':'||note,',' ORDER BY id) FROM recovery_items"
    expected = '1:updated,3:after-backup'
    q.check('safe restored slot catches up with exact inserts updates and deletes', q.wait_for('destination', 'analytics', query, expected), expected)
    q.check('source slot advances beyond the saved checkpoint', q.wait_for('source', 'application', f"SELECT (confirmed_flush_lsn>'{staged}')::text FROM pg_replication_slots WHERE slot_name='recovery_sub'", 'true'), 'true')
    stopped()
    offline(snapshot, volumes, restore=True)
    start_restored(identity, checkpoint)
    sql('ALTER SUBSCRIPTION recovery_sub ENABLE')
    q.check('stale restore refuses a source slot ahead of durable staging', q.wait_for(
        'destination', 'analytics', "SELECT coalesce(bool_or((coalesce(last_error,'')||coalesce(last_source_error,'')) LIKE '%ahead of durable staged LSN%'),false)::text FROM lake_replication.service_status", 'true'), 'true')
    q.check('refused continuity does not advance restored checkpoint', frontiers(), checkpoint)
    q.check('refused continuity leaves restored Iceberg data unchanged', rows(), '1:one,2:two')
    sql('ALTER SUBSCRIPTION recovery_sub DISABLE')
    q.wait_for('destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'", '0')
    # Explicit offline destination rebuild after retaining the coordinated backup.
    # This is not an automatic takeover or a forced checkpoint advancement.
    sql('DROP SUBSCRIPTION recovery_sub; DROP TABLE recovery_items')
    sql(f"CREATE SUBSCRIPTION recovery_rebuilt CONNECTION '{connection}' PUBLICATION recovery_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    q.check('explicit fresh-snapshot rebuild recovers advanced-slot restore', q.wait_for('destination', 'analytics', query, expected), expected)
    q.sql('source', 'application', "INSERT INTO recovery_items VALUES(4,'live-after-rebuild')")
    q.check('rebuilt destination continues live replication exactly', q.wait_for('destination', 'analytics', query, expected + ',4:live-after-rebuild'), expected + ',4:live-after-rebuild')
    sql('DROP SUBSCRIPTION recovery_rebuilt')
    q.check('recovery drill cleans up both original and replacement slots', q.sql('source', 'application', "SELECT count(*) FROM pg_replication_slots WHERE slot_name IN ('recovery_sub','recovery_rebuilt')"), '0')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--old-archive', type=Path, help='Also commit the upgrade and restore the pre-upgrade binary/schema backup')
    args = parser.parse_args()
    q.RESULTS = ROOT / '.qualification' / (q.RUN + '_physical_recovery')
    q.PROJECT += '-physical-recovery'
    q.RESULTS.mkdir(parents=True)
    q.COMPOSE_FILE = q.RESULTS / 'compose.yml'
    q.COMPOSE_FILE.write_text((ROOT / 'deploy/qualification/compose.yml').read_text())
    snapshot = q.RESULTS / 'private-snapshot'
    snapshot.mkdir(mode=0o700)
    q.EVIDENCE['scope'] = __doc__
    q.EVIDENCE['inputs'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__), Path(__file__).with_name('probe.py')]}
    exit_code = 1
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        q.EVIDENCE['candidate_archive'] = {k:q.EVIDENCE[k] for k in ['archive','archive_sha256','architecture','source_revision']}
        with tempfile.TemporaryDirectory(prefix='lake-recovery-artifact-') as temp:
            q.build_images(archive, Path(temp))
        q.UPGRADE_IMAGE = None
        q.RESTORE_IMAGE = q.DESTINATION_IMAGE
        q.RESTORE_VERSION = q.VERSION
        if args.old_archive:
            q.UPGRADE_IMAGE = 'lake-recovery-candidate:' + q.RUN
            q.command(['docker', 'tag', q.DESTINATION_IMAGE, q.UPGRADE_IMAGE])
            q.verify_archive(args.old_archive)
            q.EVIDENCE['old_archive'] = {k:q.EVIDENCE[k] for k in ['archive','archive_sha256','architecture','source_revision']}
            with tarfile.open(args.old_archive) as bundle:
                version = next(m for m in bundle if m.name.endswith('/VERSION'))
                q.RESTORE_VERSION = bundle.extractfile(version).read().decode().strip()
            with tempfile.TemporaryDirectory(prefix='lake-recovery-original-') as temp:
                q.build_images(args.old_archive, Path(temp))
            q.RESTORE_IMAGE = 'lake-recovery-original:' + q.RUN
            q.command(['docker', 'tag', q.DESTINATION_IMAGE, q.RESTORE_IMAGE])
            q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = q.RESTORE_IMAGE
        q.EVIDENCE['recovery_images'] = {}
        for label, image in [('original', q.RESTORE_IMAGE), ('candidate', q.UPGRADE_IMAGE or q.RESTORE_IMAGE)]:
            q.EVIDENCE['recovery_images'][label] = q.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', image]).stdout.strip()
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        exercise(snapshot)
        q.EVIDENCE['status'] = 'passed'
        exit_code = 0
    except Exception as error:
        q.EVIDENCE.update(status='error', error=str(error))
        print('ERROR:', error, flush=True)
    finally:
        logs = q.compose('logs', '--no-color', check_result=False)
        (q.RESULTS / 'compose.log').write_text(logs.stdout + logs.stderr)
        workers = q.compose('exec', '-T', 'destination', 'sh', '-ec',
                            "find /worker-logs -maxdepth 1 -type f -name '*.log' -exec cat {} +", check_result=False)
        (q.RESULTS / 'workers.jsonl').write_text(workers.stdout + workers.stderr)
        diagnostics = q.sql_result('destination', 'analytics',
            "SELECT jsonb_agg(jsonb_build_object('name',s.subscription_name,'state',s.state,'error',s.last_error,'service_error',ss.last_error,'source_error',ss.last_source_error)) FROM lake_replication.subscriptions s JOIN lake_replication.service_status ss USING(subscription_id)", check_result=False)
        (q.RESULTS / 'service-diagnostics.json').write_text(diagnostics.stdout)
        cleanup = q.compose('down', '-v', '--remove-orphans', check_result=False, timeout=300)
        q.EVIDENCE['cleanup_returncode'] = cleanup.returncode
        if cleanup.returncode:
            q.EVIDENCE['status'] = 'error'
            exit_code = 1
        (q.RESULTS / 'results.json').write_text(json.dumps(q.EVIDENCE, indent=2, sort_keys=True) + '\n')
        print('Evidence:', q.RESULTS, flush=True)
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())

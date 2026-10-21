#!/usr/bin/env python3
"""Qualify a fresh rebuild/cutover rehearsal; never rebind copied objects."""
import hashlib
import importlib.util
import json
import shutil
import tempfile
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', ROOT / 'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
OLD = 'analytics'
NEW = 'rebuild_candidate'
CLONE = 'rebuild_inherited'
OLD_SUB = 'rebuild_original_sub'
NEW_SUB = 'rebuild_candidate_sub'
PREFIX = 's3://lake-qualification/iceberg/fresh-' + q.RUN
ROWS = "SELECT coalesce(string_agg(id::text||':'||note,',' ORDER BY id),'') FROM public.rebuild_items"
WRAPPERS = "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'"
LOCATION = "SELECT option_value FROM pg_foreign_table f CROSS JOIN LATERAL pg_options_to_table(f.ftoptions) o WHERE f.ftrelid='public.rebuild_items'::regclass AND option_name='location'"
METADATA = "SELECT metadata_location FROM lake_iceberg.tables WHERE catalog_name=current_database() AND table_namespace='public' AND table_name='rebuild_items'"


def sql(db, statement):
    return q.sql('destination', db, statement)


def source(statement):
    return q.sql('source', 'application', statement)


def check(name, actual, expected):
    q.check(name, actual, expected)


def rows(db, expected, label):
    check(label, q.wait_for('destination', db, ROWS, expected, timeout=60), expected)
    check(label + ' contains unique primary keys', sql(db, 'SELECT (count(*)=count(DISTINCT id))::text FROM public.rebuild_items'), 'true')


def slot_count(slot):
    return source(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{slot}'")


def pause(db, subscription):
    sql(db, f'ALTER SUBSCRIPTION {subscription} DISABLE')
    check(db + ' wrapper drains', q.wait_for('destination', db, WRAPPERS, '0'), '0')
    check(subscription + ' slot becomes inactive', q.wait_for('source', 'application', f"SELECT active::text FROM pg_replication_slots WHERE slot_name='{subscription}'", 'false'), 'false')


def create(db, subscription, connection):
    sql(db, f"CREATE SUBSCRIPTION {subscription} CONNECTION '{connection}' PUBLICATION rebuild_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")


def assert_bound(db):
    check(db + ' validates its recorded origin', sql(db, 'SELECT lake_replication.prototype_binding_matches(subscription_id)::text FROM lake_replication.worker_launch_config'), 'true')


def exercise():
    source("CREATE TABLE public.rebuild_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO public.rebuild_items VALUES(1,'one'),(2,'two'); CREATE PUBLICATION rebuild_pub FOR TABLE public.rebuild_items; GRANT SELECT ON public.rebuild_items TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    create(OLD, OLD_SUB, connection)
    rows(OLD, '1:one,2:two', 'original completes initial copy')
    assert_bound(OLD)
    pause(OLD, OLD_SUB)
    old_location = sql(OLD, LOCATION)
    old_metadata = sql(OLD, METADATA)
    old_checkpoint = sql(OLD, 'SELECT published_lsn FROM lake_replication.checkpoints')
    check('original Iceberg location is recorded', bool(old_location), True)
    check('original Iceberg metadata snapshot is recorded', bool(old_metadata), True)
    q.EVIDENCE['original'] = dict(database_oid=sql(OLD, 'SELECT oid FROM pg_database WHERE datname=current_database()'),
        slot=OLD_SUB, location=old_location, metadata=old_metadata, checkpoint=old_checkpoint)

    # Retain a real inherited configuration to prove it is neither blessed nor
    # used as the starting point of the rebuild.
    sql('postgres', f'ALTER DATABASE {OLD} ALLOW_CONNECTIONS false')
    sql('postgres', f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{OLD}'")
    check('original has no connections during template copy', q.wait_for('destination', 'postgres', f"SELECT count(*) FROM pg_stat_activity WHERE datname='{OLD}'", '0'), '0')
    try:
        sql('postgres', f'CREATE DATABASE {CLONE} TEMPLATE {OLD}')
    finally:
        sql('postgres', f'ALTER DATABASE {OLD} ALLOW_CONNECTIONS true')
    check('inherited copy gets a binding diagnostic', q.wait_for('destination', CLONE, "SELECT (state='error' AND last_error LIKE '%inherited destination binding%')::text FROM lake_replication.subscriptions", 'true'), 'true')
    inherited_config = sql(CLONE, 'SELECT row_to_json(c) FROM lake_replication.worker_launch_config c')
    inherited_checkpoint = sql(CLONE, 'SELECT published_lsn FROM lake_replication.checkpoints')

    source("BEGIN; UPDATE public.rebuild_items SET note='one-updated' WHERE id=1; DELETE FROM public.rebuild_items WHERE id=2; INSERT INTO public.rebuild_items VALUES(3,'three'); COMMIT")
    check('paused original keeps its old snapshot', sql(OLD, ROWS), '1:one,2:two')

    # Explicit allocation checks: no inherited rows, no existing source slot,
    # and a separate prefix. This script is a disposable rehearsal, not a generic
    # operator command that accepts arbitrary existing destinations.
    check('candidate slot is absent before CREATE', slot_count(NEW_SUB), '0')
    check('original slot is retained before candidate CREATE', slot_count(OLD_SUB), '1')
    sql('postgres', f'CREATE DATABASE {NEW} TEMPLATE template0')
    sql('postgres', f"ALTER DATABASE {NEW} SET pg_lake_iceberg.default_location_prefix='{PREFIX}'")
    sql(NEW, 'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    check('candidate has no subscriptions before rebuild', sql(NEW, 'SELECT count(*) FROM lake_replication.subscriptions'), '0')
    check('candidate has no inherited launch config', sql(NEW, 'SELECT count(*) FROM lake_replication.worker_launch_config'), '0')
    check('candidate has no inherited checkpoint', sql(NEW, 'SELECT count(*) FROM lake_replication.checkpoints'), '0')
    check('candidate has no inherited initial copy', sql(NEW, 'SELECT count(*) FROM lake_replication.initial_copies'), '0')
    check('candidate has no existing destination table', sql(NEW, "SELECT (to_regclass('public.rebuild_items') IS NULL)::text"), 'true')
    check('candidate uses explicit fresh prefix', sql(NEW, 'SHOW pg_lake_iceberg.default_location_prefix'), PREFIX)
    check('candidate storage prefix is empty before CREATE', sql(NEW, f"SELECT count(*) FROM lake_file.list('{PREFIX}/**')"), '0')
    create(NEW, NEW_SUB, connection)
    rows(NEW, '1:one-updated,3:three', 'fresh rebuild copies current source state')
    assert_bound(NEW)
    new_location = sql(NEW, LOCATION)
    check('candidate location is within fresh prefix', new_location.startswith(PREFIX + '/'), True)
    check('candidate location differs from original', new_location != old_location, True)
    check('neither data location contains the other', not new_location.startswith(old_location.rstrip('/') + '/') and not old_location.startswith(new_location.rstrip('/') + '/'), True)
    check('candidate uses its own slot', sql(NEW, 'SELECT slot_name FROM lake_replication.worker_launch_config'), NEW_SUB)
    check('old and candidate source slots both exist', source(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name IN ('{OLD_SUB}','{NEW_SUB}')"), '2')
    check('candidate completed one new initial copy', sql(NEW, 'SELECT count(*) FROM lake_replication.initial_copies'), '1')
    q.EVIDENCE['candidate'] = dict(database_oid=sql(NEW, 'SELECT oid FROM pg_database WHERE datname=current_database()'),
        slot=NEW_SUB, location=new_location, prefix=PREFIX, checkpoint_after_copy=sql(NEW, 'SELECT published_lsn FROM lake_replication.checkpoints'))
    check('rebuild did not alter old checkpoint', sql(OLD, 'SELECT published_lsn FROM lake_replication.checkpoints'), old_checkpoint)
    check('rebuild did not rewrite old Iceberg metadata', sql(OLD, METADATA), old_metadata)
    check('rebuild did not overwrite old table data', sql(OLD, ROWS), '1:one,2:two')
    check('rebuild did not rewrite inherited launch configuration', sql(CLONE, 'SELECT row_to_json(c) FROM lake_replication.worker_launch_config c'), inherited_config)
    check('rebuild did not copy over inherited checkpoint', sql(CLONE, 'SELECT published_lsn FROM lake_replication.checkpoints'), inherited_checkpoint)

    source("BEGIN; UPDATE public.rebuild_items SET note='three-new' WHERE id=3; INSERT INTO public.rebuild_items VALUES(4,'four'),(5,'five'); COMMIT")
    rows(NEW, '1:one-updated,3:three-new,4:four,5:five', 'candidate streams after initial rebuild')
    check('candidate streaming leaves old data untouched', sql(OLD, ROWS), '1:one,2:two')
    check('candidate streaming leaves old metadata untouched', sql(OLD, METADATA), old_metadata)
    result = q.sql_result('destination', CLONE, f'DROP SUBSCRIPTION {OLD_SUB}', check_result=False)
    check('inherited cleanup remains explicitly blocked', result.returncode != 0 and 'cannot authorize source slot cleanup' in result.stderr, True)
    check('blocked inherited cleanup preserves old slot', slot_count(OLD_SUB), '1')
    check('blocked inherited cleanup preserves candidate slot', slot_count(NEW_SUB), '1')

    q.compose('restart', 'destination', timeout=180)
    check('destination returns after restart', q.wait_for('destination', 'postgres', 'SELECT 1', '1', timeout=90), '1')
    source("INSERT INTO public.rebuild_items VALUES(6,'six')")
    candidate_snapshot = '1:one-updated,3:three-new,4:four,5:five,6:six'
    rows(NEW, candidate_snapshot, 'candidate resumes after restart')
    check('restart keeps old owner stopped', sql(OLD, 'SELECT desired_state FROM lake_replication.worker_launch_config'), 'stopped')
    check('restart keeps inherited copy stopped', sql(CLONE, 'SELECT desired_state FROM lake_replication.worker_launch_config'), 'stopped')
    check('restart does not repeat candidate initial copy', sql(NEW, 'SELECT count(*) FROM lake_replication.initial_copies'), '1')

    # Consumer cutover is outside this test. Rehearse replication rollback:
    # drain the candidate, then resume the original using its retained slot.
    pause(NEW, NEW_SUB)
    candidate_metadata = sql(NEW, METADATA)
    source("BEGIN; UPDATE public.rebuild_items SET note='one-v2' WHERE id=1; DELETE FROM public.rebuild_items WHERE id=4; INSERT INTO public.rebuild_items VALUES(7,'seven'); COMMIT")
    sql(OLD, f'ALTER SUBSCRIPTION {OLD_SUB} ENABLE')
    expected = '1:one-v2,3:three-new,5:five,6:six,7:seven'
    rows(OLD, expected, 'rollback original catches up through retained source slot')
    check('rollback uses one original initial copy', sql(OLD, 'SELECT count(*) FROM lake_replication.initial_copies'), '1')
    check('rollback preserves original object location', sql(OLD, LOCATION), old_location)
    check('rollback leaves paused candidate snapshot unchanged', sql(NEW, ROWS), candidate_snapshot)
    check('rollback leaves paused candidate metadata unchanged', sql(NEW, METADATA), candidate_metadata)
    sql(NEW, f'ALTER SUBSCRIPTION {NEW_SUB} ENABLE')
    rows(NEW, expected, 'candidate can catch up again without copying old journal state')
    check('candidate catchup still has one initial copy', sql(NEW, 'SELECT count(*) FROM lake_replication.initial_copies'), '1')
    pause(NEW, NEW_SUB)
    sql(NEW, f'DROP SUBSCRIPTION {NEW_SUB}')
    check('candidate cleanup removes only its own slot', slot_count(NEW_SUB), '0')
    check('candidate cleanup retains original slot', slot_count(OLD_SUB), '1')
    check('candidate cleanup retains inherited metadata', sql(CLONE, 'SELECT count(*) FROM lake_replication.subscriptions'), '1')
    check('candidate cleanup retains its Iceberg table for separate retention policy', sql(NEW, ROWS), expected)
    source("INSERT INTO public.rebuild_items VALUES(8,'eight')")
    rows(OLD, expected + ',8:eight', 'original still streams after candidate cleanup')
    check('candidate cleanup did not rebind old storage', sql(OLD, LOCATION), old_location)


def main():
    q.RESULTS = ROOT / '.qualification' / (q.RUN + '_rebuild')
    q.RESULTS.mkdir(parents=True, exist_ok=False)
    q.EVIDENCE.update(experiment='fresh destination rebuild and replication rollback rehearsal', production_changed=False,
        limits=['Linux arm64, same source/destination cluster with Docker init.',
                'Not a writable Iceberg object takeover, in-place rebind or full logical data restore.',
                'No consumer/application traffic switch is performed; replication rollback only.',
                'Retained source slot WAL and source reachability are required; source outages/WAL loss are not qualified here.',
                'Physical-copy exclusivity, publication-side leases, cross-cluster restore and packaged upgrades remain unqualified.'])
    parent_file = ROOT / 'docs/evidence/2026-10-19-base-identity-origin-results.json'
    parent = json.loads(parent_file.read_text())
    q.EVIDENCE['parent_evidence_sha256'] = hashlib.sha256(parent_file.read_bytes()).hexdigest()
    q.EVIDENCE['runtime_source_fingerprints'] = parent['runtime_source_fingerprints']
    q.EVIDENCE['binding_fingerprints'] = parent['binding_fingerprints']
    q.EVIDENCE['harness_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    q.EVIDENCE['qualification_helper_sha256'] = hashlib.sha256(Path(q.__file__).read_bytes()).hexdigest()
    shutil.copy2(__file__, q.RESULTS / 'rebuild.py')
    started = False
    try:
        for group, names in ((parent['runtime_source_fingerprints'], ('adapt.py','runtime.c.inc','recovery.py','recovery.rs.inc','register.sql','Dockerfile','Dockerfile.recovery')),
                             (parent['binding_fingerprints'], ('binding.py','binding.sql','Dockerfile.binding'))):
            for name in names:
                if hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() != group[name]:
                    raise RuntimeError('runtime source differs from verified image: ' + name)
        image = parent['image_id']
        check('use exact identity-qualified local image', q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip(), image)
        q.EVIDENCE['image_id'] = image
        archive = q.archive_path()
        q.verify_archive(archive)
        q.command(['git','diff','--exit-code',q.EVIDENCE['source_revision'],'--','worker','migrations','extensions/lake_replication'])
        with tempfile.TemporaryDirectory(prefix='lake-rebuild-') as temporary:
            q.build_images(archive, Path(temporary))
        q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = image
        text = q.COMPOSE_FILE.read_text().replace('shared_preload_libraries=pg_extension_base,lake_replication','shared_preload_libraries=pg_extension_base').replace('      - -c\n      - lake_replication.database_name=analytics\n','').replace('  destination:\n','  destination:\n    init: true\n')
        q.COMPOSE_FILE = q.RESULTS / 'compose.yml'
        q.COMPOSE_FILE.write_text(text)
        started = True
        q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
        exercise()
        q.EVIDENCE.update(passed=True,status='passed')
    except Exception as error:
        q.EVIDENCE.update(passed=False,status='error',error=str(error))
        print(f'ERROR: {error}',flush=True)
    finally:
        try:
            if started:
                logs = q.compose('logs','--no-color',check_result=False)
                (q.RESULTS / 'compose.log').write_text(logs.stdout + logs.stderr)
                worker = q.compose('exec','-T','destination','sh','-ec',"find /worker-logs -maxdepth 1 -type f -name '*.log' -exec cat {} +",check_result=False)
                (q.RESULTS / 'worker.jsonl').write_text(worker.stdout)
        finally:
            cleanup = q.compose('down','-v','--remove-orphans',check_result=False,timeout=180)
            if cleanup.returncode:
                q.EVIDENCE.update(passed=False,status='cleanup_error',cleanup_error=cleanup.stderr)
            (q.RESULTS / 'results.json').write_text(json.dumps(q.EVIDENCE,indent=2,sort_keys=True)+'\n')
            print(f'Evidence: {q.RESULTS}',flush=True)
    return 0 if q.EVIDENCE.get('passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Probe inherited destination bindings without enabling same-name copies."""
import argparse
import hashlib
import importlib.util
import json
import shutil
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', ROOT / 'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
BINDING = False


def sql(db, statement):
    return q.sql('destination', db, statement)


def check(name, actual, expected):
    # Preserve all findings so a failed safety assertion does not mask later cases.
    passed = actual == expected
    q.EVIDENCE['checks'].append(dict(name=name, actual=actual, expected=expected, passed=passed))
    print(f"{'PASS' if passed else 'FINDING'} {name}", flush=True)


def observe(name, operation):
    try:
        operation()
    except Exception as error:
        q.EVIDENCE['errors'].append(dict(case=name, error=str(error)))
        print(f'ERROR {name}: {error}', flush=True)


def stop(db):
    sql(db, 'ALTER SUBSCRIPTION identity_sub DISABLE')
    q.check(db + ' wrappers drain', q.wait_for('destination', db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'", '0'), '0')
    q.check(db + ' releases the source slot', q.wait_for('source', 'application', "SELECT active::text FROM pg_replication_slots WHERE slot_name='identity_sub'", 'false'), 'false')


def disconnect(db):
    sql('postgres', f'ALTER DATABASE {db} ALLOW_CONNECTIONS false')
    sql('postgres', f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{db}'")
    q.check(db + ' has no remaining database connections', q.wait_for('destination', 'postgres', f"SELECT count(*) FROM pg_stat_activity WHERE datname='{db}'", '0'), '0')


def dump():
    q.compose('exec', '-T', 'destination', 'pg_dump', '-U', 'postgres', '--format=custom', '--file=/tmp/identity.dump', 'analytics')


def restore(db):
    sql('postgres', f'CREATE DATABASE {db}')
    for section in ('pre-data', 'data', 'post-data'):
        args = ['exec', '-T', 'destination', 'pg_restore', '--exit-on-error', f'--section={section}']
        if section == 'data':
            args.append('--disable-triggers')
        q.compose(*args, '-U', 'postgres', '--dbname', db, '/tmp/identity.dump')



def dump_metadata():
    tables = sql('analytics', "SELECT unnest(extconfig)::regclass::text FROM pg_extension WHERE extname='lake_replication'").splitlines()
    q.EVIDENCE['metadata_dump_tables'] = tables
    q.compose('exec', '-T', 'destination', 'pg_dump', '-U', 'postgres', '--format=custom', '--data-only',
              '--file=/tmp/identity-metadata.dump', *['--table=' + table for table in tables], 'analytics')


def restore_metadata(db):
    sql('postgres', f'CREATE DATABASE {db}')
    sql(db, 'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    # Control-plane-only fixture: regclass COPY values require the named
    # relation to exist. This placeholder is never enabled and does not import
    # or qualify the original Iceberg objects.
    sql(db, 'CREATE TABLE public.identity_items(id bigint PRIMARY KEY,note text NOT NULL)')
    q.compose('exec', '-T', 'destination', 'pg_restore', '--exit-on-error', '--single-transaction', '--data-only',
              '--disable-triggers', '-U', 'postgres', '--dbname', db, '/tmp/identity-metadata.dump')


def name_fence(db):
    expected = "SELECT (state='error' AND last_error LIKE '%inherited destination binding%')::text FROM lake_replication.subscriptions"
    check(db + ' inherited binding gets a durable error', q.wait_for('destination', db, expected, 'true', timeout=15), 'true')
    sql(db, 'ALTER SUBSCRIPTION identity_sub ENABLE')
    check(db + ' refuses enable of inherited binding', q.wait_for('destination', db, 'SELECT desired_state FROM lake_replication.worker_launch_config', 'stopped', timeout=10), 'stopped')
    check(db + ' has no subscription wrapper', sql(db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'"), '0')
    check(db + ' leaves the original source slot intact', q.sql('source', 'application', "SELECT count(*) FROM pg_replication_slots WHERE slot_name='identity_sub'"), '1')
    before = sql(db, 'SELECT xmin::text FROM lake_replication.subscriptions')
    time.sleep(2)
    check(db + ' unchanged diagnostic avoids tuple churn', sql(db, 'SELECT xmin::text FROM lake_replication.subscriptions'), before)


def same_name(db, label, original_oid):
    observed_oid = sql(db, 'SELECT oid FROM pg_database WHERE datname=current_database()')
    check(label + ' has a different database OID', observed_oid != original_oid, True)
    check(label + ' retains the old matching name', sql(db, 'SELECT (destination_database=current_database())::text FROM lake_replication.worker_launch_config'), 'true')
    # Never dispatch this copy: it has inherited slot and Iceberg bindings.
    check(label + ' is kept stopped during investigation', sql(db, 'SELECT desired_state FROM lake_replication.worker_launch_config'), 'stopped')
    # A template of the renamed original may already contain its diagnostic.
    # Clear only that diagnostic while launch intent remains stopped, to test
    # whether the current launcher detects this copy independently.
    sql(db, "UPDATE lake_replication.subscriptions SET state='streaming', last_error=NULL")
    time.sleep(2)
    check(label + ' is fenced by an inherited-binding error', sql(db, "SELECT (state='error' AND coalesce(last_error,'') LIKE '%inherited destination binding%')::text FROM lake_replication.subscriptions"), 'true')
    if BINDING:
        check(label + ' fails the origin tuple check', sql(db, 'SELECT lake_replication.prototype_binding_matches(subscription_id)::text FROM lake_replication.worker_launch_config'), 'false')
        sql(db, 'ALTER SUBSCRIPTION identity_sub ENABLE')
        check(label + ' refuses explicit enable', q.wait_for('destination', db, 'SELECT desired_state FROM lake_replication.worker_launch_config', 'stopped'), 'stopped')
        check(label + ' has no subscription wrapper', sql(db, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'"), '0')
    q.EVIDENCE['observations'].append(dict(case=label, original_database_oid=original_oid, copied_database_oid=observed_oid,
        same_name_dispatch_attempted=False, status=json.loads(sql(db, 'SELECT row_to_json(s) FROM lake_replication.subscription_status s'))))


def exercise():
    q.sql('source', 'application', "CREATE TABLE public.identity_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO public.identity_items VALUES(1,'one'); CREATE PUBLICATION identity_pub FOR TABLE public.identity_items; GRANT SELECT ON public.identity_items TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    sql('analytics', f"CREATE SUBSCRIPTION identity_sub CONNECTION '{connection}' PUBLICATION identity_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    q.check('identity fixture completes its initial copy', q.wait_for('destination', 'analytics', 'SELECT count(*) FROM public.identity_items', '1'), '1')
    if BINDING:
        check('new subscription records its actual destination origin', sql('analytics', 'SELECT lake_replication.prototype_binding_matches(subscription_id)::text FROM lake_replication.worker_launch_config'), 'true')
    original_oid = sql('analytics', 'SELECT oid FROM pg_database WHERE datname=current_database()')
    q.EVIDENCE['original_database_oid'] = original_oid
    q.EVIDENCE['cluster_system_identifier'] = sql('analytics', 'SELECT system_identifier FROM pg_control_system()')
    stop('analytics')
    disconnect('analytics')
    try:
        sql('postgres', 'CREATE DATABASE identity_clone TEMPLATE analytics')
    finally:
        sql('postgres', 'ALTER DATABASE analytics ALLOW_CONNECTIONS true')
    observe('template clone with another name', lambda: name_fence('identity_clone'))

    sql('analytics', 'ALTER SUBSCRIPTION identity_sub ENABLE')
    q.sql('source', 'application', "INSERT INTO public.identity_items VALUES(2,'two')")
    check('original still streams correct rows after clone rejection', q.wait_for('destination', 'analytics', "SELECT count(*)::text||':'||count(DISTINCT id)::text FROM public.identity_items", '2:2'), '2:2')
    stop('analytics')
    dump()
    dump_metadata()
    try:
        restore('identity_restore')
        check('full logical restore of existing Iceberg location succeeds', True, True)
    except Exception as error:
        q.EVIDENCE['observations'].append(dict(case='full logical restore', error=str(error)))
        check('full logical restore of existing Iceberg location succeeds', False, True)
    observe('metadata restore with another name', lambda: (restore_metadata('identity_metadata'), name_fence('identity_metadata')))

    disconnect('analytics')
    sql('postgres', 'ALTER DATABASE analytics RENAME TO identity_original')
    sql('postgres', 'ALTER DATABASE identity_original ALLOW_CONNECTIONS true')
    observe('database rename', lambda: name_fence('identity_original'))
    # Clone the stopped original back to the same old name. The name-only fence
    # has no signal of the changed database OID. Keep it stopped throughout.
    disconnect('identity_original')
    try:
        sql('postgres', 'CREATE DATABASE analytics TEMPLATE identity_original')
    finally:
        sql('postgres', 'ALTER DATABASE identity_original ALLOW_CONNECTIONS true')
    observe('same-name database copy', lambda: same_name('analytics', 'same-name template copy', original_oid))
    disconnect('analytics')
    sql('postgres', 'DROP DATABASE analytics WITH (FORCE)')
    observe('same-name metadata restore', lambda: (restore_metadata('analytics'), same_name('analytics', 'same-name metadata restore', original_oid)))

    # Deliberately exercise cleanup last, on this private fixture only. The
    # original is stopped, so an incorrectly authorized slot deletion is observable.
    check('original source slot exists before cloned DROP', q.sql('source', 'application', "SELECT count(*) FROM pg_replication_slots WHERE slot_name='identity_sub'"), '1')
    result = q.sql_result('destination', 'identity_clone', 'DROP SUBSCRIPTION identity_sub', check_result=False)
    q.EVIDENCE['cloned_drop'] = dict(returncode=result.returncode, stderr=result.stderr)
    check('cloned DROP preserves original source slot', q.sql('source', 'application', "SELECT count(*) FROM pg_replication_slots WHERE slot_name='identity_sub'"), '1')
    if BINDING:
        check('cloned DROP explicitly refuses unauthorized cleanup', result.returncode != 0 and 'cannot authorize source slot cleanup' in result.stderr, True)
        check('refused cloned DROP preserves local subscription metadata', sql('identity_clone', 'SELECT count(*) FROM lake_replication.subscriptions'), '1')
        sql('postgres', 'CREATE DATABASE identity_legacy')
        sql('identity_legacy', 'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
        sql('identity_legacy', "SELECT lake_replication.create_subscription(9001,1234,1,'legacy_fixture','legacy_slot',ARRAY['legacy_pub']); INSERT INTO lake_replication.worker_launch_config(subscription_id,slot_name,publication_name,source_database,destination_database,source_address,destination_address,destination_schema) SELECT subscription_id,'legacy_slot','legacy_pub','application',current_database(),'source:5432','127.0.0.1:5432','public' FROM lake_replication.subscriptions")
        check('legacy origin is not automatically blessed', sql('identity_legacy', 'SELECT (origin_system_identifier IS NULL AND origin_database_oid IS NULL)::text FROM lake_replication.worker_launch_config'), 'true')
        check('legacy row with matching name stays stopped', q.wait_for('destination', 'identity_legacy', 'SELECT desired_state FROM lake_replication.worker_launch_config', 'stopped'), 'stopped')
        check('legacy row has a durable diagnostic', sql('identity_legacy', "SELECT (state='error' AND last_error LIKE '%inherited destination binding%')::text FROM lake_replication.subscriptions"), 'true')
        disconnect('analytics')
        sql('postgres', 'DROP DATABASE analytics WITH (FORCE)')
        disconnect('identity_original')
        sql('postgres', 'ALTER DATABASE identity_original RENAME TO analytics; ALTER DATABASE analytics ALLOW_CONNECTIONS true')
        check('original database regains its original tuple', sql('analytics', 'SELECT lake_replication.prototype_binding_matches(subscription_id)::text FROM lake_replication.worker_launch_config'), 'true')
        sql('analytics', 'DROP SUBSCRIPTION identity_sub')
        check('valid original owner still cleans its own source slot', q.sql('source', 'application', "SELECT count(*) FROM pg_replication_slots WHERE slot_name='identity_sub'"), '0')


def main():
    global BINDING
    parser = argparse.ArgumentParser()
    parser.add_argument('--binding', action='store_true', help='Build the experimental origin dispatch/cleanup fence.')
    args = parser.parse_args()
    BINDING = args.binding
    q.RESULTS = ROOT / '.qualification' / (q.RUN + '_identity')
    q.RESULTS.mkdir(parents=True, exist_ok=False)
    q.EVIDENCE.update(experiment='clone, logical restore, rename and inherited cleanup ownership', production_changed=False,
                      errors=[], observations=[], binding_adapter=BINDING, same_name_copies_kept_stopped=True, metadata_restore_uses_non_iceberg_placeholder=True,
                      limits=['Same cluster, Linux arm64; physical-copy exclusivity and cross-cluster restore are unqualified.',
                              'Name-only matching is inspected without enabling a copied destination.',
                              'Rebind/takeover API and publication-side binding checks are not implemented.',
                              'Metadata-only restore does not recreate or qualify existing Iceberg data/table bindings.'])
    evidence_path = ROOT / 'docs/evidence/2026-10-19-base-recovery-final-results.json'
    inherited = json.loads(evidence_path.read_text())
    q.EVIDENCE['runtime_evidence_sha256'] = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
    q.EVIDENCE['runtime_source_fingerprints'] = inherited['source_fingerprints']
    q.EVIDENCE['harness_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    shutil.copy2(__file__, q.RESULTS / 'identity.py')
    started = False
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        q.command(['git', 'diff', '--exit-code', q.EVIDENCE['source_revision'], '--', 'worker', 'migrations', 'extensions/lake_replication'])
        for name in ('adapt.py', 'runtime.c.inc', 'recovery.py', 'recovery.rs.inc', 'register.sql', 'Dockerfile', 'Dockerfile.recovery'):
            actual = hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
            if actual != inherited['source_fingerprints'][name]:
                raise RuntimeError('runtime source differs from verified image: ' + name)
        image = inherited['image_id']
        q.check('use exact qualified recovery image', q.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', image]).stdout.strip(), image)
        q.EVIDENCE['parent_image_id'] = image
        if BINDING:
            names = ('binding.py','binding.sql','Dockerfile.binding')
            q.EVIDENCE['binding_fingerprints'] = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() for name in names}
            for name in names:
                shutil.copy2(Path(__file__).with_name(name), q.RESULTS / name)
            parent_tag = f'lake-replication-identity-parent:{q.RUN}'
            q.command(['docker','tag',image,parent_tag])
            q.check('binding build uses verified local parent', q.command(['docker','image','inspect','--format','{{.Id}}',parent_tag]).stdout.strip(), image)
            new_image = f'lake-replication-binding-prototype:{q.RUN}'
            build = q.command(['docker','build','--build-arg',f'QUAL_IMAGE={parent_tag}','--tag',new_image,'--file',str(Path(__file__).with_name('Dockerfile.binding')),str(ROOT)],timeout=600)
            (q.RESULTS / 'binding-build.log').write_text(build.stdout + build.stderr)
            image = q.command(['docker','image','inspect','--format','{{.Id}}',new_image]).stdout.strip()
        q.EVIDENCE['image_id'] = image
        with tempfile.TemporaryDirectory(prefix='lake-identity-') as temporary:
            q.build_images(archive, Path(temporary))
        q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = image
        text = q.COMPOSE_FILE.read_text().replace('shared_preload_libraries=pg_extension_base,lake_replication', 'shared_preload_libraries=pg_extension_base').replace('      - -c\n      - lake_replication.database_name=analytics\n', '').replace('  destination:\n', '  destination:\n    init: true\n')
        q.COMPOSE_FILE = q.RESULTS / 'compose.yml'
        q.COMPOSE_FILE.write_text(text)
        started = True
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        exercise()
        passed = not q.EVIDENCE['errors'] and all(c['passed'] for c in q.EVIDENCE['checks'])
        safety_checks = [c for c in q.EVIDENCE['checks'] if c['name'] != 'full logical restore of existing Iceberg location succeeds']
        q.EVIDENCE.update(passed=passed, status='passed' if passed else 'findings', binding_safety_passed=not q.EVIDENCE['errors'] and all(c['passed'] for c in safety_checks))
    except Exception as error:
        q.EVIDENCE.update(passed=False, status='error', error=str(error))
        print(f'ERROR: {error}', flush=True)
    finally:
        try:
            if started:
                logs = q.compose('logs', '--no-color', check_result=False)
                (q.RESULTS / 'compose.log').write_text(logs.stdout + logs.stderr)
                worker = q.compose('exec', '-T', 'destination', 'sh', '-ec', "find /worker-logs -maxdepth 1 -type f -name '*.log' -exec cat {} +", check_result=False)
                (q.RESULTS / 'worker.jsonl').write_text(worker.stdout)
        finally:
            cleanup = q.compose('down', '-v', '--remove-orphans', check_result=False, timeout=180)
            if cleanup.returncode:
                q.EVIDENCE.update(passed=False, status='cleanup_error', cleanup_error=cleanup.stderr)
            (q.RESULTS / 'results.json').write_text(json.dumps(q.EVIDENCE, indent=2, sort_keys=True) + '\n')
            print(f'Evidence: {q.RESULTS}', flush=True)
    return 0 if q.EVIDENCE.get('passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())

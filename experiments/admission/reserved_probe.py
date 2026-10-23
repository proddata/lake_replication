#!/usr/bin/env python3
"""Evaluate engine-only global/reserved admission on the current base runtime."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


m = module('admission_measure', Path(__file__).with_name('admission_measure.py'))
s, c, q = m.s, m.c, m.q
VIEW = 'lake_replication.prototype_writer_admission'


def lock_cases(image, reserved, text, directory, bulk_only=False):
    directory.mkdir()
    q.COMPOSE_FILE = directory / 'compose.yml'
    q.COMPOSE_FILE.write_text(text)
    q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = image
    clients = []
    pool = ThreadPoolExecutor(max_workers=2)
    result = {'status': 'running'}

    def client(database='analytics'):
        connection = c.Psql('destination', database)
        clients.append(connection)
        return connection

    def count(state, expected):
        statement = f"SELECT count(*) FROM {VIEW} WHERE admission_state='{state}'"
        q.check(f'{state} count reaches {expected}', q.wait_for('destination', 'analytics', statement, str(expected)), str(expected))

    try:
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        q.sql('destination', 'analytics', 'CREATE DATABASE admission_other')
        q.sql('destination', 'admission_other', 'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
        q.sql('source', 'application', 'CREATE TABLE meta_items(id bigint PRIMARY KEY); INSERT INTO meta_items VALUES(1); CREATE PUBLICATION meta_pub FOR TABLE meta_items; GRANT SELECT ON meta_items TO lake_source')
        connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
        q.sql('destination', 'analytics', f"CREATE SUBSCRIPTION meta_sub CONNECTION '{connection}' PUBLICATION meta_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        q.check('metadata fixture reaches streaming', q.wait_for('destination', 'analytics', "SELECT state FROM lake_replication.subscriptions WHERE subscription_name='meta_sub'", 'streaming'), 'streaming')
        q.sql('destination', 'analytics', 'ALTER SUBSCRIPTION meta_sub DISABLE')
        q.check('metadata fixture stops before permit checks', q.wait_for('destination', 'analytics', s.WRAPPERS, '0'), '0')
        identity = q.sql('destination', 'analytics', 'SELECT subscription_id,remote_relation_id,schema_version FROM lake_replication.relations').split('|')
        subscription, relation, version = identity
        owner, stream, waiter = client(), client('admission_other'), client('admission_other')
        owner.query('BEGIN; SELECT lake_replication.experiment_admit_engine(true); SELECT lake_replication.experiment_admit_engine(true)')
        count('admitted', 1)
        stream.query('BEGIN')
        pending = pool.submit(stream.query, 'SELECT lake_replication.experiment_admit_engine(false)')
        if reserved or bulk_only:
            pending.result(timeout=10)
            count('admitted', 1 if bulk_only else 2)
            q.check('streaming preserves its independent capacity across databases', q.sql('destination', 'analytics', f"SELECT string_agg(lane,',' ORDER BY lane) FROM {VIEW} WHERE admission_state='admitted'"), 'bulk' if bulk_only else 'bulk,stream')
        else:
            count('waiting_for_writer', 1)
            q.check('global engine gate queues streaming behind bulk', pending.done(), False)
            owner.query('COMMIT')
            pending.result(timeout=10)
            count('admitted', 1)
        # A control/journal operation must remain independent of either permit.
        staged = q.sql('destination', 'analytics', "SET statement_timeout='2s'; SELECT lake_replication.create_subscription(1,1,1,'control_only','control_only',ARRAY['pub'])>0")
        q.check('PostgreSQL-only control work bypasses engine admission', 't' in staged.splitlines(), True)
        ready = q.sql('destination', 'analytics', f"SET statement_timeout='2s'; SELECT lake_replication.ensure_iceberg_destination({subscription},{relation},{version},'public','meta_items',true)::text; SELECT lake_replication.reconcile_relation_schema({subscription},{relation},{version})")
        q.check('existing-table and ready-schema paths bypass engine admission', ready.splitlines()[-2:], ['meta_items', 'ready'])
        waiter.query('BEGIN')
        queued = pool.submit(waiter.query, f'SELECT lake_replication.experiment_admit_engine({str(bulk_only).lower()})')
        count('waiting_for_writer', 1)
        if bulk_only:
            owner.query('COMMIT')
        else:
            stream.query('ROLLBACK')
        queued.result(timeout=10)
        waiter.query('ROLLBACK')
        owner.query('ROLLBACK')
        if bulk_only:
            stream.query('ROLLBACK')
        count('admitted', 0)
        # Savepoint, lock timeout, cancellation and backend death rely on native locks.
        owner.query('BEGIN; SAVEPOINT work; SELECT lake_replication.experiment_admit_engine(true); ROLLBACK TO work')
        count('admitted', 0)
        owner.query('SELECT lake_replication.experiment_admit_engine(true)')
        rejected = q.sql_result('destination', 'admission_other', "SET lock_timeout='250ms'; SELECT lake_replication.experiment_admit_engine(true)", check_result=False)
        q.check('same-lane timeout is interruptible', rejected.returncode != 0 and 'lock timeout' in rejected.stderr, True)
        count('waiting_for_writer', 0)
        if reserved:
            mixed = q.sql_result('destination', 'admission_other', 'BEGIN; SELECT lake_replication.experiment_admit_engine(false); SELECT lake_replication.experiment_admit_engine(true)', check_result=False)
            q.check('cross-lane transaction fails before waiting', mixed.returncode != 0 and 'cannot mix engine admission lanes' in mixed.stderr, True)
        pid = waiter.query('SELECT pg_backend_pid()')[0]
        waiter.query('BEGIN')
        queued = pool.submit(waiter.query, 'SELECT lake_replication.experiment_admit_engine(true)')
        count('waiting_for_writer', 1)
        q.sql('destination', 'analytics', f'SELECT pg_cancel_backend({pid})')
        try:
            queued.result(timeout=10)
        except RuntimeError as error:
            q.check('queued cancellation returns promptly', 'canceling statement' in str(error), True)
        else:
            raise AssertionError('queued cancellation unexpectedly succeeded')
        owner.query('COMMIT')
        count('admitted', 0)
        doomed = client()
        pid = doomed.query('SELECT pg_backend_pid()')[0]
        doomed.query('BEGIN; SELECT lake_replication.experiment_admit_engine(true)')
        q.sql('destination', 'admission_other', f'SELECT pg_terminate_backend({pid})')
        count('admitted', 0)
        q.check('only seven engine functions acquire permits', q.sql('destination', 'analytics', "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='lake_replication' AND p.prosrc LIKE '%PERFORM lake_replication.experiment_admit_engine(%'"), '7')
        result['status'] = 'passed'
    except Exception as error:
        result.update(status='error', error=str(error))
    finally:
        # Roll back the owner before joining any waiter after an assertion failure.
        for connection in clients:
            connection.close()
        pool.shutdown(wait=True, cancel_futures=True)
        logs = q.compose('logs', '--no-color', check_result=False)
        (directory / 'compose.log').write_text(logs.stdout + logs.stderr)
        cleanup = q.compose('down', '-v', '--remove-orphans', check_result=False, timeout=300)
        result['cleanup_returncode'] = cleanup.returncode
        if cleanup.returncode:
            result['status'] = 'error'
        s.write_json(directory / 'results.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--variants', nargs='+', choices=('unbounded', 'bounded', 'reserved', 'bulk_only'), default=['unbounded', 'reserved'])
    parser.add_argument('--cases', nargs='+', choices=('locks', 'scale', 'fairness'), default=['locks', 'scale', 'fairness'])
    parser.add_argument('--copy-rows', type=int, default=2000000)
    parser.add_argument('--stream-transactions', type=int, default=120)
    parser.add_argument('--bulk-copies', type=int, default=1)
    args = parser.parse_args()
    if args.copy_rows < 1 or args.stream_transactions < 20 or not 1 <= args.bulk_copies <= 4:
        parser.error('positive copy size, 1..4 bulk copies and at least 20 streaming transactions required')
    q.RESULTS = ROOT / '.qualification' / (q.RUN + '_reserved_admission')
    q.RESULTS.mkdir(parents=True)
    root = q.RESULTS
    q.PROJECT += '-reserved-admission'
    q.SOURCE_IMAGE += '-reserved-admission'
    q.DESTINATION_IMAGE += '-reserved-admission'
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE, QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    result = dict(status='running', scope='Experimental engine-only admission on the migrated runtime; not release qualification', parameters=vars(args), cases=[], production_changed=False)
    paths = [*Path(__file__).parent.glob('*'), ROOT/'extensions/lake_replication/lake_replication.c']
    result['inputs'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths if p.is_file()}
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-reserved-admission-') as temp:
            q.build_images(archive, Path(temp))
        base = q.DESTINATION_IMAGE
        images = {'unbounded': base}
        for variant in args.variants:
            if variant != 'unbounded':
                image = f'lake-replication-engine-{variant}:{q.RUN}'
                build = q.command(['docker', 'build', '--build-arg', f'QUAL_IMAGE={base}', '--build-arg', f'RESERVED={int(variant!="bounded")}', '--build-arg', f'BULK_ONLY={int(variant=="bulk_only")}', '-t', image, '-f', str(Path(__file__).with_name('Dockerfile.reserved')), str(ROOT)], timeout=600)
                (root / f'build-{variant}.log').write_text(build.stdout + build.stderr)
                images[variant] = image
        result['images'] = {name: q.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', image]).stdout.strip() for name, image in images.items()}
        text = m.compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n', '')
        for variant in args.variants:
            for case in args.cases:
                if case == 'locks' and variant == 'unbounded':
                    continue
                print(f'CASE {variant} {case}', flush=True)
                directory = root / f'{variant}-{case}'
                if case == 'locks':
                    measured = lock_cases(images[variant], variant == 'reserved', text, directory, bulk_only=variant=='bulk_only')
                elif case == 'scale':
                    measured = s.scale_case(4, 16, 64, images[variant], text, directory)
                else:
                    fairness_args = argparse.Namespace(copy_rows=args.copy_rows, stream_transactions=args.stream_transactions, bulk_copies=args.bulk_copies, seed_after_subscriptions=False)
                    measured = m.fairness(fairness_args, variant, images[variant], text, directory)
                    measured['timing_validation'] = m.fairness_timing_validation(measured) if measured.get('trace') else {'valid': False}
                    if not measured['timing_validation']['valid']:
                        measured['status'] = 'finding'
                result['cases'].append({**measured, 'variant': variant, 'case': case})
                s.write_json(root / 'results.json', result)
                print(f"RESULT {variant} {case}: {measured['status']}", flush=True)
                if measured['status'] == 'error':
                    raise RuntimeError(measured.get('error', 'case failed'))
        result['status'] = 'passed' if all(row['status']=='passed' for row in result['cases']) else 'completed_with_findings'
    except Exception as error:
        result.update(status='error', error=str(error))
        print('ERROR:', error, flush=True)
    finally:
        result['archive_provenance'] = q.EVIDENCE
        s.write_json(root / 'results.json', result)
        print('Evidence:', root, flush=True)
    return 1 if result['status']=='error' else 0


if __name__ == '__main__':
    raise SystemExit(main())

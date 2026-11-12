#!/usr/bin/env python3
"""Upgrade a real pre-TLS beta.5 archive using only the new archive's refresh."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', ROOT / 'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)


def sql(statement):
    return q.sql('destination', 'analytics', statement)


def exercise(new_image, refresh):
    q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
    q.check('old archive uses beta.5 SQL', sql("SELECT extversion FROM pg_extension WHERE extname='lake_replication'"), '0.1.0-beta.5')
    absent = "SELECT (to_regprocedure('lake_replication.source_tls_settings(bigint)') IS NULL)::text"
    q.check('old archive predates TLS policy helper', sql(absent), 'true')
    q.sql('source', 'application', "CREATE TABLE tls_upgrade_items(id bigint PRIMARY KEY,note text); INSERT INTO tls_upgrade_items VALUES(1,'before'); CREATE PUBLICATION tls_upgrade_pub FOR TABLE tls_upgrade_items; GRANT SELECT ON tls_upgrade_items TO lake_source")
    from urllib.parse import quote
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application?sslmode=disable"
    sql(f"CREATE SUBSCRIPTION tls_upgrade_sub CONNECTION '{connection}' PUBLICATION tls_upgrade_pub WITH(lake_target='iceberg')")
    rows = 'SELECT string_agg(id::text||\':\'||note,\',\' ORDER BY id) FROM public.tls_upgrade_items'
    q.check('old archive completes snapshot', q.wait_for('destination', 'analytics', rows, '1:before'), '1:before')
    sql('ALTER SUBSCRIPTION tls_upgrade_sub DISABLE')
    q.check('old subscription drains before replacement', q.wait_for('destination','analytics',"SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='tls_upgrade_sub'",'stopped'), 'stopped')
    checkpoint = sql('SELECT published_lsn FROM lake_replication.checkpoints')
    copies = sql('SELECT count(*) FROM lake_replication.initial_copies')
    q.sql('source', 'application', "INSERT INTO tls_upgrade_items VALUES(2,'during')")
    # Earlier log files were 0644. Apply the documented permission migration.
    q.compose('exec', '-T', '--user', 'root', 'destination', 'sh', '-ec',
              "chmod 0700 /worker-logs; find /worker-logs -type f -name '*.log' -exec chmod 0600 {} +")
    q.compose('stop', 'destination', 'pgduck', timeout=180)
    q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = new_image
    q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
    q.check('binary replacement does not silently install same-version SQL', sql(absent), 'true')
    sql('ALTER EXTENSION lake_replication UPDATE')
    q.check('same-version UPDATE does not install helper', sql(absent), 'true')
    q.compose('cp', str(refresh), 'destination:/tmp/source-tls-refresh.sql')
    body = refresh.read_text().strip().removeprefix('BEGIN;').removesuffix('COMMIT;')
    rejected = q.sql_result('destination', 'analytics',
        "BEGIN; UPDATE lake_replication.worker_launch_config SET desired_state='running'; " + body + '; ROLLBACK', check_result=False)
    q.check('refresh refuses enabled subscriptions', rejected.returncode != 0 and 'stop subscriptions' in rejected.stderr, True)
    q.check('refused refresh leaves no helper', sql(absent), 'true')
    sql('BEGIN; ' + body + '; ROLLBACK')
    q.check('refresh rollback removes new helper and registration', sql(absent), 'true')
    q.compose('exec', '-T', 'destination', 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'analytics', '-f', '/tmp/source-tls-refresh.sql')
    q.check('refresh installs helper without SQL version change', sql("SELECT extversion||':'||(to_regprocedure('lake_replication.source_tls_settings(bigint)') IS NOT NULL)::text FROM pg_extension WHERE extname='lake_replication'"), '0.1.0-beta.5:true')
    q.check('new helper belongs to existing extension', sql("SELECT count(*) FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid WHERE d.classid='pg_proc'::regclass AND d.objid='lake_replication.source_tls_settings(bigint)'::regprocedure AND d.refclassid='pg_extension'::regclass AND e.extname='lake_replication' AND d.deptype='e'"), '1')
    q.check('helper has restricted execution and fixed definer context', sql("SELECT (p.prosecdef AND p.proconfig @> ARRAY['search_path=pg_catalog, lake_replication, pg_temp'] AND NOT has_function_privilege('pg_read_all_data',p.oid,'EXECUTE') AND has_function_privilege('lake_replication_worker',p.oid,'EXECUTE'))::text FROM pg_proc p WHERE p.oid='lake_replication.source_tls_settings(bigint)'::regprocedure"), 'true')
    subscription = sql("SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='tls_upgrade_sub'")
    worker_policy = sql(f"SET ROLE lake_replication_worker; SELECT (lake_replication.source_tls_settings({subscription}))[1]; RESET ROLE")
    q.check('worker role can retrieve stored source TLS policy', 'disable' in worker_policy.splitlines(), True)
    q.check('refresh preserves publication checkpoint', sql('SELECT published_lsn FROM lake_replication.checkpoints'), checkpoint)
    q.check('refresh preserves initial-copy state', sql('SELECT count(*) FROM lake_replication.initial_copies'), copies)
    sql('ALTER SUBSCRIPTION tls_upgrade_sub ENABLE')
    q.check('new workers replay changes retained during replacement', q.wait_for('destination','analytics',rows,'1:before,2:during'), '1:before,2:during')
    q.sql('source','application',"INSERT INTO tls_upgrade_items VALUES(3,'after')")
    q.check('new workers stream after refresh', q.wait_for('destination','analytics',rows,'1:before,2:during,3:after'), '1:before,2:during,3:after')
    q.check('handoff does not repeat completed copy', sql('SELECT count(*) FROM lake_replication.initial_copies'), copies)
    q.compose('restart','destination',timeout=180)
    q.check('upgraded destination restarts',q.wait_for('destination','analytics','SELECT 1','1',timeout=90),'1')
    q.sql('source','application',"INSERT INTO tls_upgrade_items VALUES(4,'restart')")
    q.check('upgraded workers resume after restart',q.wait_for('destination','analytics',rows,'1:before,2:during,3:after,4:restart'),'1:before,2:during,3:after,4:restart')
    sql('DROP SUBSCRIPTION tls_upgrade_sub')
    q.check('upgraded cleanup removes retained source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='tls_upgrade_sub'"),'0')
    sql('DROP EXTENSION lake_replication CASCADE')
    q.check('extension removal also removes manually refreshed helper',sql(absent),'true')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--old-archive', type=Path, required=True)
    args = parser.parse_args()
    q.RESULTS = q.RESULTS.with_name(q.RUN + '_source_tls_upgrade')
    q.PROJECT += '-source-tls-upgrade'
    q.SOURCE_IMAGE += '-source-tls-upgrade'
    q.DESTINATION_IMAGE += '-source-tls-upgrade'
    q.ENV['QUALIFICATION_SOURCE_IMAGE'] = q.SOURCE_IMAGE
    q.RESULTS.mkdir(parents=True)
    q.EVIDENCE['scope'] = __doc__
    passed = False
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        keys = ('archive', 'archive_sha256', 'architecture', 'source_revision')
        new = {key:q.EVIDENCE[key] for key in keys}
        with tempfile.TemporaryDirectory(prefix='lake-tls-upgrade-') as temp:
            temp = Path(temp)
            with tarfile.open(archive) as bundle:
                item = next(m for m in bundle.getmembers() if m.name.endswith('/DEVELOPMENT_UPDATES/0029_source_tls_policy.sql'))
                refresh = temp / '0029_source_tls_policy.sql'
                refresh.write_bytes(bundle.extractfile(item).read())
            q.EVIDENCE['refresh_sha256'] = hashlib.sha256(refresh.read_bytes()).hexdigest()
            (temp/'new').mkdir()
            q.build_images(archive,temp/'new')
            new_image = 'lake-tls-upgrade-candidate:' + q.RUN
            q.command(['docker','tag',q.DESTINATION_IMAGE,new_image])
            q.verify_archive(args.old_archive.resolve())
            q.EVIDENCE['old_archive'] = {key:q.EVIDENCE[key] for key in keys}
            q.EVIDENCE.update(new)
            (temp/'old').mkdir()
            q.build_images(args.old_archive.resolve(),temp/'old')
            q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = q.DESTINATION_IMAGE
            exercise(new_image, refresh)
        passed = True
        q.EVIDENCE.update(status='passed',passed=True)
    except Exception as error:
        q.EVIDENCE.update(status='error',passed=False,error=str(error))
        print('ERROR:',error,flush=True)
    finally:
        logs = q.compose('logs','--no-color',check_result=False)
        (q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        workers = q.compose('exec','-T','destination','sh','-ec',"find /worker-logs -type f -name '*.log' -exec cat {} +",check_result=False)
        (q.RESULTS/'workers.jsonl').write_text(workers.stdout)
        cleanup = q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
        q.EVIDENCE['cleanup_returncode'] = cleanup.returncode
        (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2)+'\n')
        print('Evidence:',q.RESULTS,flush=True)
    return 0 if passed and cleanup.returncode == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())

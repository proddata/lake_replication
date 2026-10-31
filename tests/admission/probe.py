#!/usr/bin/env python3
"""Qualify exported production admission packages; no SQL or binary overlay."""
import argparse
import hashlib
import importlib.util
import json
import re
from pathlib import Path
import shutil
import tempfile
import tarfile
from urllib.parse import quote

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('operational_cases',ROOT/'experiments/admission/operational_probe.py')
o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o)
q,c,m,s=o.q,o.c,o.m,o.p.s
o.ADMIT='lake_replication.admit_bulk()'
o.p.VIEW='lake_replication.bulk_admission'
VIEW=o.p.VIEW
if q.VERSION=='0.1.0-beta.5':q.STAGING_ROWS='initial_copy_chunks'


def contract():
    q.check('candidate SQL version',o.sql("SELECT extversion FROM pg_extension WHERE extname='lake_replication'"),q.VERSION)
    q.check('exactly five bulk entry points',o.sql("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='lake_replication' AND p.prosrc LIKE '%PERFORM lake_replication.admit_bulk();%'"),'5')
    q.check('streaming entry points do not acquire bulk admission',o.sql("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='lake_replication' AND p.proname IN ('publish_insert_batch','apply_change_run') AND p.prosrc LIKE '%admit_bulk%'"),'0')
    q.check('both streaming entry points acquire stream admission',o.sql("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='lake_replication' AND p.proname IN ('publish_insert_batch','apply_change_run') AND p.prosrc LIKE '%PERFORM lake_replication.admit_stream();%'"),'2')
    q.check('no prototype functions installed',o.sql("SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='lake_replication' AND p.proname LIKE 'experiment_%'"),'0')
    q.check('admin may invoke and inspect bulk admission',o.sql("SELECT has_function_privilege('lake_replication_admin','lake_replication.admit_bulk()','EXECUTE') AND has_table_privilege('lake_replication_admin','lake_replication.bulk_admission','SELECT')"),'t')
    denied=q.sql_result('destination','analytics','SET ROLE pg_read_all_data; SELECT lake_replication.admit_bulk()',check_result=False)
    q.check('read-all role cannot hold bulk admission',denied.returncode!=0 and 'permission denied' in denied.stderr,True)
    q.check('admin may invoke and inspect stream admission',o.sql("SELECT has_function_privilege('lake_replication_admin','lake_replication.admit_stream()','EXECUTE') AND has_table_privilege('lake_replication_admin','lake_replication.stream_admission','SELECT')"),'t')
    denied=q.sql_result('destination','analytics','SET ROLE pg_read_all_data; SELECT lake_replication.admit_stream()',check_result=False)
    q.check('read-all role cannot hold stream admission',denied.returncode!=0 and 'permission denied' in denied.stderr,True)


def stream_pool(result):
    spec=importlib.util.spec_from_file_location('production_pool_cases',Path(__file__).with_name('pool_cases.py'))
    cases=importlib.util.module_from_spec(spec);spec.loader.exec_module(cases)
    cases.q,cases.c=q,c
    try:
        cases.aborts(result)
        cases.progress(result)
        cases.mixed(result)
    finally:
        for connection in cases.CLIENTS:connection.close()
        cases.CLIENTS.clear()


def locks(result):
    contract()
    q.sql('destination','analytics','CREATE DATABASE admission_other')
    q.sql('destination','admission_other','CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    owner=o.client();owner.query('BEGIN; SAVEPOINT work; SELECT lake_replication.admit_bulk(); SELECT lake_replication.admit_bulk()')
    q.check('nested bulk calls own one permit',o.sql(f'SELECT count(*) FROM {VIEW}'),'1')
    blocked=q.sql_result('destination','admission_other',"SET lock_timeout='250ms'; SELECT lake_replication.admit_bulk()",check_result=False)
    q.check('bulk gate excludes another database and supports timeout',blocked.returncode!=0 and 'lock timeout' in blocked.stderr,True)
    o.sql("SET statement_timeout='2s'; SELECT lake_replication.create_subscription(42,1,1,'unblocked_control','control_slot',ARRAY['pub'])")
    owner.query('ROLLBACK TO work')
    q.check('savepoint rollback releases bulk permit',o.sql(f'SELECT count(*) FROM {VIEW}'),'0')
    owner.query('SELECT lake_replication.admit_bulk()');pid=owner.query('SELECT pg_backend_pid()')[0]
    q.sql('destination','admission_other',f'SELECT pg_terminate_backend({pid})')
    q.check('backend death releases bulk permit',q.wait_for('destination','analytics',f'SELECT count(*) FROM {VIEW}','0'),'0')


def upgrade(result,new_image,old_archive):
    q.verify_archive(old_archive)
    with tarfile.open(old_archive) as bundle:
        version_file=next(member for member in bundle.getmembers() if member.name.endswith('/VERSION'))
        old_version=bundle.extractfile(version_file).read().decode().strip()
    if old_version not in ['0.1.0-beta.2','0.1.0-beta.3','0.1.0-beta.4']:raise RuntimeError('unsupported admission upgrade fixture')
    result['old_version']=old_version
    result['old_archive']={k:q.EVIDENCE[k] for k in ['archive','archive_sha256','architecture','source_revision']}
    with tempfile.TemporaryDirectory(prefix='lake-bulk-old-') as temp:q.build_images(old_archive,Path(temp))
    q.ENV['QUALIFICATION_DESTINATION_IMAGE']=q.DESTINATION_IMAGE
    q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
    q.check('starts unchanged previous SQL',o.sql("SELECT extversion FROM pg_extension WHERE extname='lake_replication'"),old_version)
    q.sql('source','application',"CREATE TABLE admission_upgrade(id bigint PRIMARY KEY); INSERT INTO admission_upgrade VALUES(1); CREATE PUBLICATION admission_upgrade_pub FOR TABLE admission_upgrade; GRANT SELECT ON admission_upgrade TO lake_source")
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    o.sql(f"CREATE SUBSCRIPTION admission_upgrade_sub CONNECTION '{connection}' PUBLICATION admission_upgrade_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    q.check('previous native copy completes',q.wait_for('destination','analytics',"SELECT state FROM lake_replication.subscriptions WHERE subscription_name='admission_upgrade_sub'",'streaming'),'streaming')
    o.sql('ALTER SUBSCRIPTION admission_upgrade_sub DISABLE')
    q.check('previous wrapper drains before replacement',q.wait_for('destination','analytics',s.WRAPPERS,'0'),'0')
    checkpoint=o.sql('SELECT published_lsn FROM lake_replication.checkpoints')
    q.compose('stop','destination','pgduck',timeout=120)
    q.ENV['QUALIFICATION_DESTINATION_IMAGE']=new_image
    q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
    rejected=q.sql_result('destination','analytics',f"BEGIN; UPDATE lake_replication.worker_launch_config SET desired_state='running'; ALTER EXTENSION lake_replication UPDATE TO '{q.VERSION}'",check_result=False)
    q.check('admission upgrade rejects an enabled subscription',rejected.returncode!=0 and 'stop subscriptions' in rejected.stderr and 'upgrade' in rejected.stderr,True)
    o.sql(f"BEGIN; ALTER EXTENSION lake_replication UPDATE TO '{q.VERSION}'; ROLLBACK")
    q.check('upgrade rollback preserves previous SQL and no staging helper',o.sql("SELECT extversion||':'||(to_regprocedure('lake_replication.admit_staging()') IS NULL)::text FROM pg_extension WHERE extname='lake_replication'"),old_version+(':'+str(old_version!='0.1.0-beta.4').lower()))
    expected_helpers='false' if old_version=='0.1.0-beta.2' else 'true'
    q.check('rollback restores the previous engine admission helpers',o.sql("SELECT (to_regprocedure('lake_replication.admit_bulk()') IS NOT NULL)::text || ':' || (to_regprocedure('lake_replication.admit_stream()') IS NOT NULL)::text || ':' || (to_regclass('lake_replication.stream_admission') IS NOT NULL)::text"),':'.join([expected_helpers]*3))
    o.sql(f"ALTER EXTENSION lake_replication UPDATE TO '{q.VERSION}'")
    contract()
    q.check('upgrade preserves published checkpoint',o.sql('SELECT published_lsn FROM lake_replication.checkpoints'),checkpoint)
    q.check('upgrade preserves original Iceberg data',o.sql('SELECT count(*)||\':\'||min(id) FROM admission_upgrade'),'1:1')
    q.check('upgrade leaves native subscription disabled',o.sql('SELECT desired_state FROM lake_replication.worker_launch_config'),'stopped')
    q.sql('source','application','INSERT INTO admission_upgrade VALUES(2)')
    o.sql('ALTER SUBSCRIPTION admission_upgrade_sub ENABLE')
    q.check('upgraded native subscription catches up',q.wait_for('destination','analytics','SELECT string_agg(id::text,\',\' ORDER BY id) FROM admission_upgrade','1,2'),'1,2')
    o.sql('DROP SUBSCRIPTION admission_upgrade_sub')
    q.check('upgraded subscription removes source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='admission_upgrade_sub'"),'0')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpuset',default='0-1',help='Shared CPU affinity for source, destination, engine and storage')
    parser.add_argument('--cases',nargs='+',choices=['pipeline','checkpoint','managed','locks','pool','schema','rebuild','cancel','upgrade','scale','fairness'],default=['pipeline','managed','locks','pool','schema','rebuild','cancel','upgrade','scale','fairness'])
    parser.add_argument('--old-archive',type=Path,default=ROOT/f'dist/release/lake_replication-0.1.0-beta.2-linux-{q.native_architecture()}.tar.gz')
    parser.add_argument('--fairness-copies',type=int,choices=[0,1,4],default=4,help='Zero/one-copy diagnostic control or the unchanged four-copy release case')
    args=parser.parse_args()
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',args.cpuset):
        parser.error('invalid CPU affinity')
    root=ROOT/'.qualification'/(q.RUN+'_production_admission');root.mkdir(parents=True);q.RESULTS=root
    q.PROJECT+='-production-admission';q.SOURCE_IMAGE+='-production-admission';q.DESTINATION_IMAGE+='-production-admission'
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE,QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    result=dict(status='running',scope=__doc__,cases=[],parameters={**vars(args),'old_archive':str(args.old_archive)})
    paths=[Path(__file__),Path(__file__).with_name('managed_cases.py'),Path(__file__).with_name('pool_cases.py'),ROOT/'experiments/admission/operational_probe.py',ROOT/'experiments/admission/reserved_probe.py',ROOT/'experiments/admission/admission_measure.py',ROOT/'experiments/admission/compare.py',ROOT/'experiments/admission/sample.py',ROOT/'experiments/admission/scale.py',ROOT/'tests/qualification/probe.py',ROOT/'Cargo.lock',ROOT/'worker/Cargo.toml',*sorted((ROOT/'worker/src').glob('*.rs')),*sorted((ROOT/'migrations').glob('*.sql')),*[ROOT/'extensions/lake_replication'/name for name in ['lake_replication.c','lake_replication.control','Makefile','build-extension-sql.sh']]]
    paths.append(ROOT/'experiments/admission/source_workload.py')
    if 'checkpoint' in args.cases:paths.append(Path(__file__).with_name('checkpoint_cases.py'))
    if 'pipeline' in args.cases:paths.extend(Path(__file__).with_name(name) for name in ['beta5_cases.py','checkpoint_split_operations.py','copy_chunk_bounds.py'])
    paths.extend(getattr(m, 'diagnostic_sources', []))
    result['inputs']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    for path in paths:
        snapshot=root/'sources'/path.relative_to(ROOT);snapshot.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,snapshot)
    try:
        archive=q.archive_path();q.verify_archive(archive)
        result['archive']={k:q.EVIDENCE[k] for k in ['archive','archive_sha256','architecture','source_revision']}
        with tempfile.TemporaryDirectory(prefix='lake-production-admission-') as temp:q.build_images(archive,Path(temp))
        # The old-package build reuses q.DESTINATION_IMAGE. Preserve the actual
        # candidate under a separate tag before testing package replacement.
        image='lake-replication-production-admission:'+q.RUN
        q.command(['docker','tag',q.DESTINATION_IMAGE,image])
        result['image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip()
        text=m.compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n','')
        for service in ('source','destination','pgduck','storage'):
            text=re.sub(rf'^  {service}:$',f'  {service}:\n    cpuset: "{args.cpuset}"',text,flags=re.MULTILINE)
        for case in args.cases:
            print('CASE',case,flush=True);directory=root/case
            observed=dict(case=case,status='running')
            try:
                q.ENV['QUALIFICATION_DESTINATION_IMAGE']=image
                if case=='scale':observed.update(s.scale_case(4,16,64,image,text,directory))
                elif case=='fairness':
                    observed.update(m.fairness(argparse.Namespace(copy_rows=2000000,stream_transactions=120,bulk_copies=args.fairness_copies,require_queue=args.fairness_copies>1,capture_stream_admission=True,seed_after_subscriptions=False,admission_view=VIEW), 'bulk_only',image,text,directory))
                    observed['timing_validation']=m.fairness_timing_validation(observed)
                    if not observed['timing_validation']['valid']:observed['status']='finding'
                else:
                    directory.mkdir();q.COMPOSE_FILE=directory/'compose.yml'
                    q.COMPOSE_FILE.write_text(text.replace('exec pgduck_server ','exec pgduck_server --debug ') if case=='cancel' else text)
                    if case=='upgrade':upgrade(observed,image,args.old_archive)
                    else:
                        q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
                        if case=='checkpoint':
                            spec=importlib.util.spec_from_file_location('checkpoint_cases',Path(__file__).with_name('checkpoint_cases.py'))
                            checkpoint=importlib.util.module_from_spec(spec);spec.loader.exec_module(checkpoint)
                            checkpoint.run(q,c,observed)
                        elif case=='pipeline':
                            spec=importlib.util.spec_from_file_location('beta5_cases',Path(__file__).with_name('beta5_cases.py'))
                            pipeline=importlib.util.module_from_spec(spec);spec.loader.exec_module(pipeline)
                            pipeline.run(q,c)
                        elif case=='managed':
                            spec=importlib.util.spec_from_file_location('managed_cases',Path(__file__).with_name('managed_cases.py'))
                            managed=importlib.util.module_from_spec(spec);spec.loader.exec_module(managed)
                            managed.run(q,c)
                        elif case=='locks':locks(observed)
                        elif case=='pool':stream_pool(observed)
                        elif case=='schema':o.schema_case(observed)
                        elif case=='rebuild':o.rebuild_case(observed)
                        else:o.cancel_case(observed,2000000)
                    observed['status']='passed'
            except Exception as error:observed.update(status='error',error=str(error),error_type=type(error).__name__)
            finally:
                for connection in o.CLIENTS:connection.close()
                o.CLIENTS.clear()
                # Shared scale/fairness cases already capture logs and remove
                # their fixture. Never replace that evidence after cleanup.
                if case not in ('scale','fairness'):
                    logs=q.compose('logs','--no-color',check_result=False);(directory/'compose.log').write_text(logs.stdout+logs.stderr)
                cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300);observed['cleanup_returncode']=cleanup.returncode
                if cleanup.returncode:observed['status']='error'
                result['cases'].append(observed);result['checks']=q.EVIDENCE['checks'];s.write_json(directory/'results.json',observed);s.write_json(root/'results.json',result)
                print('RESULT',case,observed['status'],observed.get('error',''),flush=True)
                if observed['status']=='error':raise RuntimeError(observed.get('error','case failed'))
        result['status']='passed' if all(x['status']=='passed' for x in result['cases']) else 'completed_with_findings'
    except Exception as error:result.update(status='error',error=str(error));print('ERROR:',error,flush=True)
    finally:s.write_json(root/'results.json',result);print('Evidence:',root,flush=True)
    return int(result['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

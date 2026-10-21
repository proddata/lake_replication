#!/usr/bin/env python3
"""Publication transaction and external deployment fence on private physical copies."""
import hashlib
import importlib.util
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', ROOT/'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
cs = importlib.util.spec_from_file_location('comparison',Path(__file__).with_name('compare.py'))
comparison = importlib.util.module_from_spec(cs)
cs.loader.exec_module(comparison)
comparison.q = q
PRIMARY = 'primary-' + q.RUN
ROWS = "SELECT string_agg(id::text||':'||note,',' ORDER BY id) FROM public.owned_items"
META = "SELECT metadata_location FROM lake_iceberg.tables WHERE catalog_name=current_database() AND table_namespace='public' AND table_name='owned_items'"
WRAPPERS = "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'"


def sql(statement, service='destination'):
    return q.sql(service,'analytics',statement)


def check(label, actual, expected):
    q.check(label,actual,expected)


def rejected(service,statement,label):
    result=q.sql_result(service,'analytics',statement,check_result=False)
    check(label,result.returncode != 0 and 'publication ownership rejected' in result.stderr,True)


def rows(expected):
    check('primary rows reach '+expected,q.wait_for('destination','analytics',ROWS,expected),expected)


def pause():
    sql('ALTER SUBSCRIPTION owned_sub DISABLE')
    check('primary subscription wrapper drains',q.wait_for('destination','analytics',WRAPPERS,'0'),'0')
    check('primary source slot releases',q.wait_for('source','application',"SELECT active::text FROM pg_replication_slots WHERE slot_name='owned_sub'",'false'),'false')


def copy_checks(label):
    check(label+' starts',q.wait_for('physical-copy','analytics','SELECT 1','1',timeout=90),'1')
    check(label+' disables independent Iceberg autovacuum',sql('SHOW pg_lake_iceberg.autovacuum','physical-copy'),'off')
    check(label+' retains cluster system identifier',sql('SELECT system_identifier FROM pg_control_system()','physical-copy'),q.EVIDENCE['system_identifier'])
    check(label+' retains database OID',sql("SELECT oid FROM pg_database WHERE datname=current_database()",'physical-copy'),q.EVIDENCE['database_oid'])
    check(label+' retains database name',sql('SELECT current_database()','physical-copy'),'analytics')
    check(label+' passes the old tuple alone',sql("SELECT (origin_system_identifier=(SELECT system_identifier FROM pg_control_system()) AND origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND destination_database=current_database())::text FROM lake_replication.worker_launch_config",'physical-copy'),'true')
    check(label+' retains original deployment binding',sql('SELECT origin_instance_id FROM lake_replication.worker_launch_config','physical-copy'),PRIMARY)
    check(label+' fails runtime deployment ownership',sql('SELECT lake_replication.prototype_binding_matches(subscription_id)::text FROM lake_replication.worker_launch_config','physical-copy'),'false')
    sql('ALTER SUBSCRIPTION owned_sub ENABLE','physical-copy')
    check(label+' attempted enable stays stopped',q.wait_for('physical-copy','analytics','SELECT desired_state FROM lake_replication.worker_launch_config','stopped'),'stopped')
    check(label+' creates no subscription wrapper',sql(WRAPPERS,'physical-copy'),'0')
    checkpoint=sql('SELECT published_lsn FROM lake_replication.checkpoints','physical-copy')
    metadata=sql(META,'physical-copy')
    rejected('physical-copy',"SELECT * FROM lake_replication.publish_insert_batch(1,'unauthorized','0/1','0/2')",label+' direct publication is rejected before batch preparation')
    rejected('physical-copy',"SELECT lake_replication.finish_initial_sync_group(1,1)",label+' initial-copy publication is rejected')
    check(label+' leaves checkpoint unchanged',sql('SELECT published_lsn FROM lake_replication.checkpoints','physical-copy'),checkpoint)
    check(label+' leaves Iceberg metadata pointer unchanged',sql(META,'physical-copy'),metadata)
    drop=q.sql_result('physical-copy','analytics','DROP SUBSCRIPTION owned_sub',check_result=False)
    check(label+' cannot clean original source slot',drop.returncode != 0 and 'cannot authorize source slot cleanup' in drop.stderr,True)
    check(label+' preserves original source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='owned_sub'"),'1')
    setting=q.sql_result('physical-copy','analytics',f"SET lake_replication.prototype_instance_id='{PRIMARY}'",check_result=False)
    check(label+' cannot change deployment identity in a session',setting.returncode != 0 and 'cannot be changed without restarting' in setting.stderr,True)


def exercise():
    q.sql('source','application',"CREATE TABLE public.owned_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO public.owned_items VALUES(1,'one'); CREATE PUBLICATION owned_pub FOR TABLE public.owned_items; GRANT SELECT ON public.owned_items TO lake_source")
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    sql(f"CREATE SUBSCRIPTION owned_sub CONNECTION '{connection}' PUBLICATION owned_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    rows('1:one')
    check('fresh binding records external deployment identity',sql('SELECT origin_instance_id FROM lake_replication.worker_launch_config'),PRIMARY)
    before_checkpoint=sql('SELECT published_lsn FROM lake_replication.checkpoints')
    before_metadata=sql(META)
    pause()
    q.sql('source','application',"INSERT INTO public.owned_items VALUES(2,'two')")
    common=['docker','compose','-p',q.PROJECT,'-f',str(q.COMPOSE_FILE),'exec','-T','--user','postgres']
    environment={'LAKE_CREDENTIAL_FILE':'/credentials/destination-only.env','LAKE_SUBSCRIPTION_ID':'1',
        'LAKE_LOAD_SOURCE_CREDENTIALS':'1','LAKE_DISCOVER_PUBLICATION':'1','LAKE_CREATE_TABLES_USING_ICEBERG':'1','LAKE_DESTINATION_SCHEMA':'public','LAKE_SLOT':'owned_sub','LAKE_PUBLICATION':'owned_pub','LAKE_SOURCE_ADDRESS':'source:5432',
        'LAKE_DESTINATION_ADDRESS':'127.0.0.1:5432','LAKE_SOURCE_DATABASE':'application',
        'LAKE_DESTINATION_DATABASE':'analytics','LAKE_MAX_TRANSACTIONS':'1','LAKE_RECEIVER_IDLE_TIMEOUT_MS':'2000'}
    for name,value in environment.items(): common.extend(['-e',name+'='+value])
    receiver=q.command(common+['destination','/usr/local/bin/lake-replication-receiver'],timeout=30)
    (q.RESULTS/'receiver.jsonl').write_text(receiver.stdout+receiver.stderr)
    check('real receiver leaves source transaction pending for publication',sql("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'"),'1')
    # Start the real publisher while a revocation update is uncommitted. Its
    # transaction must wait and validate the new row after the update commits.
    locker=comparison.Psql('destination','analytics')
    publisher=None
    try:
        locker.query("BEGIN; UPDATE lake_replication.worker_launch_config SET origin_instance_id='revoked' WHERE subscription_id=1")
        publisher=subprocess.Popen(common+['destination','/usr/local/bin/lake-replication-publisher'],env=q.ENV,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        check('live publisher waits on the ownership row',q.wait_for('destination','analytics',"SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT%lake_replication.publish_insert_batch%'",'1'),'1')
        locker.query("UPDATE lake_replication.worker_launch_config SET desired_state='stopped'; COMMIT")
        output,errors=publisher.communicate(timeout=15)
        (q.RESULTS/'rejected-publisher.jsonl').write_text(output+errors)
        check('live publisher rejects committed revocation',publisher.returncode!=0 and 'publication ownership rejected' in output+errors,True)
    finally:
        locker.close()
        if publisher is not None and publisher.poll() is None:
            publisher.terminate();publisher.wait(timeout=10)
    pause()
    batch=sql('SELECT first_end_lsn::text,last_end_lsn::text FROM lake_replication.next_batch(1,100,1000,1048576)').split('|')
    check('rejected live transaction remains pending',len(batch),2)
    statement=f"SELECT * FROM lake_replication.publish_insert_batch(1,'revocation-test','{batch[0]}','{batch[1]}')"
    rejected('destination',statement,'revoked direct publication refuses actual staged transaction')
    rejected('destination',"SELECT lake_replication.finish_initial_sync_group(1,1)",'revoked initial-copy finish refuses target writes')
    check('revocation leaves rows unchanged',sql(ROWS),'1:one')
    check('revocation leaves publication checkpoint unchanged',sql('SELECT published_lsn FROM lake_replication.checkpoints'),before_checkpoint)
    check('revocation leaves Iceberg metadata unchanged',sql(META),before_metadata)
    check('guard error leaves transaction staged',sql("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'"),'1')

    # Controlled fault repair in the original fixture, not a rebind interface.
    sql(f"UPDATE lake_replication.worker_launch_config SET origin_instance_id='{PRIMARY}'; UPDATE lake_replication.subscriptions SET state='streaming',last_error=NULL WHERE last_error LIKE 'prototype: inherited destination binding%'")
    sql(statement)
    rows('1:one,2:two')
    check('valid publication consumes the pending transaction',sql("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'"),'0')
    # Inverse race: a successful validation holds a share lock until commit.
    owner=comparison.Psql('destination','analytics')
    try:
        owner.query('BEGIN; SELECT lake_replication.prototype_assert_writer(1)')
        contender=q.sql_result('destination','analytics',"SET lock_timeout='400ms'; UPDATE lake_replication.worker_launch_config SET origin_instance_id='other'",check_result=False)
        check('validated transaction prevents mid-transaction ownership rewrite',contender.returncode != 0 and 'lock timeout' in contender.stderr,True)
        owner.query('COMMIT')
    finally:
        owner.close()
    check('timed-out rewrite leaves owner unchanged',sql('SELECT origin_instance_id FROM lake_replication.worker_launch_config'),PRIMARY)
    sql('ALTER SUBSCRIPTION owned_sub ENABLE')
    q.sql('source','application',"INSERT INTO public.owned_items VALUES(3,'three')")
    rows('1:one,2:two,3:three')
    pause()
    q.EVIDENCE['system_identifier']=sql('SELECT system_identifier FROM pg_control_system()')
    q.EVIDENCE['database_oid']=sql("SELECT oid FROM pg_database WHERE datname=current_database()")
    # Actual physical PostgreSQL backup; never point two servers at one data dir.
    q.compose('exec','-T','--user','root','destination','install','-d','-m','0700','-o','postgres','-g','postgres','/physical-copy')
    q.compose('exec','-T','--user','postgres','destination','pg_basebackup','-h','/var/run/postgresql','-U','postgres','-D','/physical-copy','-X','stream','-c','fast','--no-password',timeout=180)
    q.compose('up','-d','--no-deps','physical-copy',timeout=180)
    copy_checks('physical copy with different external identity')
    q.ENV['QUAL_COPY_INSTANCE_ID']=''
    q.compose('up','-d','--no-deps','--force-recreate','physical-copy',timeout=180)
    copy_checks('physical copy with missing external identity')
    check('copied servers did not alter primary rows',sql(ROWS),'1:one,2:two,3:three')
    q.compose('restart','destination',timeout=180)
    check('primary restarts with the same external profile',q.wait_for('destination','analytics','SELECT lake_replication.prototype_binding_matches(1)::text','true',timeout=90),'true')
    sql('ALTER SUBSCRIPTION owned_sub ENABLE')
    q.sql('source','application',"INSERT INTO public.owned_items VALUES(4,'four')")
    rows('1:one,2:two,3:three,4:four')


def main():
    q.RESULTS=ROOT/'.qualification'/(q.RUN+'_ownership')
    q.RESULTS.mkdir(parents=True,exist_ok=False)
    q.EVIDENCE.update(experiment='transactional publication fence and physical copy external profile',production_changed=False,
       limits=['PostgreSQL 18/Linux arm64 private same-host fixtures with Docker init.',
               'External profile must be supplied independently of PGDATA; duplicated deployment configuration can duplicate authorization.',
               'Not a distributed lease, hostile-superuser boundary, split-brain or arbitrary physical-copy exclusivity proof.',
               'Old profiles require external fencing; no live takeover/rebind or application traffic cutover is qualified.'])
    parent_file=ROOT/'docs/evidence/2026-10-19-base-identity-origin-results.json'
    parent=json.loads(parent_file.read_text())
    q.EVIDENCE['parent_evidence_sha256']=hashlib.sha256(parent_file.read_bytes()).hexdigest()
    q.EVIDENCE['harness_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    names=('publication.py','publication.sql','Dockerfile.publication','compare.py','adapt.py','runtime.c.inc','binding.py','binding.sql')
    q.EVIDENCE['source_fingerprints']={n:hashlib.sha256(Path(__file__).with_name(n).read_bytes()).hexdigest() for n in names}
    sources=q.RESULTS/'sources';sources.mkdir()
    shutil.copy2(__file__,sources/'ownership.py')
    for n in names:shutil.copy2(Path(__file__).with_name(n),sources/n)
    started=False
    try:
        image=parent['image_id']
        check('use exact local origin-qualified parent',q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip(),image)
        for group in ('runtime_source_fingerprints','binding_fingerprints'):
            for n,digest in parent[group].items():
                if n in ('faults.py','probe.py','sample.py','Dockerfile.recovery','destination-init.sql','register.sql','recovery.py','recovery.rs.inc','Dockerfile','adapt.py','runtime.c.inc','binding.py','binding.sql','Dockerfile.binding') and n not in ('faults.py','probe.py','sample.py'):
                    if hashlib.sha256(Path(__file__).with_name(n).read_bytes()).hexdigest()!=digest:raise RuntimeError('parent source changed: '+n)
        tag=f'lake-replication-ownership-parent:{q.RUN}'
        q.command(['docker','tag',image,tag])
        new=f'lake-replication-ownership:{q.RUN}'
        build=q.command(['docker','build','--build-arg',f'QUAL_IMAGE={tag}','--tag',new,'--file',str(Path(__file__).with_name('Dockerfile.publication')),str(ROOT)],timeout=600)
        (q.RESULTS/'build.log').write_text(build.stdout+build.stderr)
        q.EVIDENCE['image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',new]).stdout.strip()
        archive=q.archive_path();q.verify_archive(archive)
        q.command(['git','diff','--exit-code',q.EVIDENCE['source_revision'],'--','worker','migrations','extensions/lake_replication'])
        with tempfile.TemporaryDirectory(prefix='lake-ownership-') as temporary:q.build_images(archive,Path(temporary))
        q.ENV.update(QUALIFICATION_DESTINATION_IMAGE=new,QUAL_COPY_INSTANCE_ID='secondary-'+q.RUN)
        text=q.COMPOSE_FILE.read_text().replace('shared_preload_libraries=pg_extension_base,lake_replication','shared_preload_libraries=pg_extension_base').replace('      - -c\n      - lake_replication.database_name=analytics\n','').replace('  destination:\n','  destination:\n    init: true\n')
        text=text.replace('      - max_worker_processes=24\n',f'      - max_worker_processes=24\n      - -c\n      - lake_replication.prototype_instance_id={PRIMARY}\n',1)
        text=text.replace('      - destination-data:/var/lib/postgresql/18/data\n','      - destination-data:/var/lib/postgresql/18/data\n      - copy-data:/physical-copy\n',1)
        command=['postgres','-c','max_worker_processes=24','-c','shared_preload_libraries=pg_extension_base','-c','lake_replication.prototype_instance_id=${QUAL_COPY_INSTANCE_ID:-}','-c','pg_lake_engine.host=host=/var/run/pgduck port=5332','-c','pg_lake_iceberg.autovacuum=off']
        copy='  physical-copy:\n    profiles: [physical-copy]\n    init: true\n    image: ${QUALIFICATION_DESTINATION_IMAGE}\n    environment:\n      PGDATA: /var/lib/postgresql/18/data\n    command: '+json.dumps(command)+'\n    volumes:\n      - copy-data:/var/lib/postgresql/18/data\n      - pgduck-socket:/var/run/pgduck\n      - credentials:/credentials:ro\n\n'
        text=text.replace('  storage:\n',copy+'  storage:\n')+'  copy-data:\n'
        q.COMPOSE_FILE=q.RESULTS/'compose.yml';q.COMPOSE_FILE.write_text(text)
        started=True
        q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
        exercise()
        q.EVIDENCE.update(passed=True,status='passed')
    except Exception as error:
        q.EVIDENCE.update(passed=False,status='error',error=str(error));print(f'ERROR: {error}',flush=True)
    finally:
        try:
            if started:
                logs=q.compose('logs','--no-color',check_result=False);(q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        finally:
            cleanup=q.compose('--profile','physical-copy','down','-v','--remove-orphans',check_result=False,timeout=180)
            if cleanup.returncode:q.EVIDENCE.update(passed=False,status='cleanup_error',cleanup_error=cleanup.stderr)
            (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2,sort_keys=True)+'\n');print(f'Evidence: {q.RESULTS}',flush=True)
    return 0 if q.EVIDENCE.get('passed') else 1


if __name__=='__main__':raise SystemExit(main())

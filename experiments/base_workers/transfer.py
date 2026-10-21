#!/usr/bin/env python3
"""Rehearse a manually fenced cold writer transfer; not a distributed lease."""
import hashlib
import importlib.util
import json
import shutil
import tempfile
from pathlib import Path
from urllib.parse import quote

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('qualification',ROOT/'tests/qualification/probe.py')
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)
OWNER='exclusive-'+q.RUN
ROWS="SELECT string_agg(id::text||':'||note,',' ORDER BY id) FROM public.transfer_items"
META="SELECT metadata_location FROM lake_iceberg.tables WHERE catalog_name=current_database() AND table_namespace='public' AND table_name='transfer_items'"
DIGEST="SELECT md5(lake_iceberg.metadata(metadata_location)::text) FROM lake_iceberg.tables WHERE catalog_name=current_database() AND table_namespace='public' AND table_name='transfer_items'"
LOCATION="SELECT option_value FROM pg_foreign_table f CROSS JOIN LATERAL pg_options_to_table(f.ftoptions) o WHERE f.ftrelid='public.transfer_items'::regclass AND option_name='location'"
WRAPPERS="SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'"


def sql(statement,service='destination'):
    return q.sql(service,'analytics',statement)


def check(name,actual,expected):q.check(name,actual,expected)


def inspect(service):
    cid=q.compose('ps','-a','-q',service).stdout.strip()
    if not cid or '\n' in cid:raise RuntimeError('expected one known container for '+service)
    return json.loads(q.command(['docker','inspect',cid]).stdout)[0]


def stopped_inventory(expected):
    rows=[]
    for service,cid in expected.items():
        d=inspect(service)
        if d['Id']!=cid or d['Config']['Labels'].get('com.docker.compose.project')!=q.PROJECT:
            raise RuntimeError('writer inventory identity changed')
        state=d['State']; policy=d['HostConfig']['RestartPolicy']['Name']
        rows.append(dict(service=service,id=cid,running=state['Running'],restarting=state['Restarting'],pid=state['Pid'],restart_policy=policy))
        if state['Running'] or state['Restarting'] or state['Pid']!=0 or policy not in ('','no'):
            raise RuntimeError('old writer not fenced: '+service)
    q.EVIDENCE['fenced_inventory']=rows


def refusal(name,operation):
    try:operation()
    except RuntimeError as error:
        q.EVIDENCE['refusals'].append(dict(case=name,reason=str(error)))
        check(name+' is refused',True,True)
    else:check(name+' is refused',False,True)


def source_safe(staged):
    actual=q.sql('source','application',"SELECT (NOT active AND plugin='pgoutput' AND slot_type='logical' AND database='application' AND NOT temporary AND NOT conflicting AND invalidation_reason IS NULL AND coalesce(wal_status,'lost')<>'lost' AND confirmed_flush_lsn<='"+staged+"'::pg_lsn)::text FROM pg_replication_slots WHERE slot_name='transfer_sub'")
    if actual!='true':raise RuntimeError('source slot is active, missing, unsafe or ahead of candidate staged checkpoint')


def candidate_state(manifest):
    if json.loads(sql('SELECT row_to_json(c) FROM lake_replication.worker_launch_config c','candidate'))!=manifest['launch_config']:
        raise RuntimeError('candidate launch configuration differs from reviewed manifest')
    if sql("SELECT src.system_identifier::text||':'||src.database_oid::text FROM lake_replication.sources src JOIN lake_replication.subscriptions sub USING(source_id)",'candidate')!=manifest['source_identity']:
        raise RuntimeError('candidate source identity differs from reviewed source')
    if sql('SELECT published_lsn FROM lake_replication.checkpoints','candidate')!=manifest['published_lsn']:
        raise RuntimeError('candidate checkpoint differs from reviewed manifest')
    if sql(META,'candidate')!=manifest['metadata']:
        raise RuntimeError('candidate catalog differs from frozen metadata snapshot')
    if sql(DIGEST,'candidate')!=manifest['metadata_digest']:
        raise RuntimeError('candidate Iceberg metadata content differs from reviewed snapshot')
    if sql(LOCATION,'candidate')!=manifest['location']:
        raise RuntimeError('candidate object location differs from reviewed binding')
    if sql('SELECT staged_lsn FROM lake_replication.checkpoints','candidate')!=manifest['staged_lsn']:
        raise RuntimeError('candidate staging checkpoint differs from reviewed manifest')
    if sql('SELECT desired_state FROM lake_replication.worker_launch_config','candidate')!='stopped':
        raise RuntimeError('candidate is not durably stopped')


def exercise():
    q.sql('source','application',"CREATE TABLE public.transfer_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO public.transfer_items VALUES(1,'one'),(2,'two'); CREATE PUBLICATION transfer_pub FOR TABLE public.transfer_items; GRANT SELECT ON public.transfer_items TO lake_source")
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    sql(f"CREATE SUBSCRIPTION transfer_sub CONNECTION '{connection}' PUBLICATION transfer_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    check('original completes copy',q.wait_for('destination','analytics',ROWS,'1:one,2:two'),'1:one,2:two')
    sql('ALTER SUBSCRIPTION transfer_sub DISABLE')
    check('original wrapper drains',q.wait_for('destination','analytics',WRAPPERS,'0'),'0')
    check('source slot releases',q.wait_for('source','application',"SELECT active::text FROM pg_replication_slots WHERE slot_name='transfer_sub'",'false'),'false')
    check('original Iceberg maintenance is disabled for frozen backup',sql('SHOW pg_lake_iceberg.autovacuum'),'off')
    manifest=dict(metadata=sql(META),metadata_digest=sql(DIGEST),location=sql(LOCATION),
                  published_lsn=sql('SELECT published_lsn FROM lake_replication.checkpoints'),
                  staged_lsn=sql('SELECT staged_lsn FROM lake_replication.checkpoints'),
                  system_identifier=sql('SELECT system_identifier FROM pg_control_system()'),
                  database_oid=sql("SELECT oid FROM pg_database WHERE datname=current_database()"),
                  launch_config=json.loads(sql('SELECT row_to_json(c) FROM lake_replication.worker_launch_config c')),
                  source_identity=q.sql('source','application',"SELECT system_identifier::text||':'||(SELECT oid::text FROM pg_database WHERE datname=current_database()) FROM pg_control_system()"))
    q.EVIDENCE['manifest']=manifest
    q.compose('exec','-T','--user','root','destination','install','-d','-m','0700','-o','postgres','-g','postgres','/physical-copy')
    q.compose('exec','-T','--user','root','destination','install','-d','-m','0775','-o','postgres','-g','postgres','/copy-socket')
    q.compose('exec','-T','--user','postgres','destination','pg_basebackup','-h','/var/run/postgresql','-U','postgres','-D','/physical-copy','-X','stream','-c','fast','--no-password',timeout=180)
    q.compose('up','-d','--no-deps','candidate',timeout=180)
    check('quarantined candidate starts',q.wait_for('candidate','analytics','SELECT 1','1',timeout=90),'1')
    check('candidate starts without background worker launcher',sql('SHOW pg_extension_base.enable_base_worker_launcher','candidate'),'off')
    check('quarantined candidate cannot own publication',sql('SELECT lake_replication.prototype_binding_matches(1)::text','candidate'),'false')
    denied=q.sql_result('candidate','analytics',"SELECT * FROM lake_replication.publish_insert_batch(1,'forbidden','0/1','0/2')",check_result=False)
    check('quarantined direct publication is rejected',denied.returncode!=0 and 'publication ownership rejected' in denied.stderr,True)
    expected={s:inspect(s)['Id'] for s in ('destination','pgduck')}
    refusal('transfer while original PostgreSQL and engine run',lambda:stopped_inventory(expected))
    check('refused transfer does not grant profile',sql("SHOW lake_replication.prototype_instance_id",'candidate'),'quarantine')
    q.compose('stop','destination',timeout=120)
    refusal('transfer while original pgduck still runs',lambda:stopped_inventory(expected))
    check('candidate remains unowned after partial fencing',sql('SELECT lake_replication.prototype_binding_matches(1)::text','candidate'),'false')
    q.compose('stop','pgduck',timeout=120)
    stopped_inventory(expected)
    check('all tracked old writers are stopped',True,True)
    # Copy has its own engine and socket; the stopped original engine is never reused.
    q.compose('up','-d','--no-deps','--wait','--wait-timeout','60','copy-pgduck',timeout=180)
    engine=q.compose('exec','-T','copy-pgduck','psql','-h','/var/run/pgduck','-p','5332','-U','postgres','-d','postgres','-c','SELECT 1',timeout=60)
    check('candidate engine serves its separate socket',engine.returncode,0)
    check('physical cluster identity is unchanged',sql('SELECT system_identifier FROM pg_control_system()','candidate'),manifest['system_identifier'])
    check('physical database identity is unchanged',sql("SELECT oid FROM pg_database WHERE datname=current_database()",'candidate'),manifest['database_oid'])
    check('source identity matches reviewed source',q.sql('source','application',"SELECT system_identifier::text||':'||(SELECT oid::text FROM pg_database WHERE datname=current_database()) FROM pg_control_system()"),manifest['source_identity'])
    bad=dict(manifest,published_lsn='0/0')
    refusal('transfer with wrong checkpoint manifest',lambda:candidate_state(bad))
    bad_config=dict(manifest,launch_config=dict(manifest['launch_config'],slot_name='unreviewed_slot'))
    refusal('transfer with unreviewed slot configuration',lambda:candidate_state(bad_config))
    check('failed validation leaves authorization absent',sql('SELECT lake_replication.prototype_binding_matches(1)::text','candidate'),'false')
    candidate_state(manifest);source_safe(manifest['staged_lsn']);stopped_inventory(expected)
    check('frozen candidate state validates without granting ownership',True,True)
    q.sql('source','application',"BEGIN; UPDATE public.transfer_items SET note='one-new' WHERE id=1; DELETE FROM public.transfer_items WHERE id=2; INSERT INTO public.transfer_items VALUES(3,'three'); COMMIT")
    # All checks precede profile grant. This fixture's controller has complete
    # inventory; Docker state snapshots are not a production fencing service.
    source_safe(manifest['staged_lsn']);stopped_inventory(expected)
    q.ENV.update(QUAL_TRANSFER_PROFILE=OWNER,QUAL_TRANSFER_LAUNCHER='true')
    q.compose('up','-d','--no-deps','--force-recreate','candidate',timeout=180)
    check('exclusive profile now matches copied binding',q.wait_for('candidate','analytics','SELECT lake_replication.prototype_binding_matches(1)::text','true',timeout=90),'true')
    check('grant does not rewrite stored origin',sql('SELECT origin_instance_id FROM lake_replication.worker_launch_config','candidate'),OWNER)
    check('candidate stays stopped until explicit enable',sql('SELECT desired_state FROM lake_replication.worker_launch_config','candidate'),'stopped')
    sql('ALTER SUBSCRIPTION transfer_sub ENABLE','candidate')
    check('transferred writer catches up through retained slot',q.wait_for('candidate','analytics',ROWS,'1:one-new,3:three'),'1:one-new,3:three')
    check('transfer does not repeat initial copy',sql('SELECT count(*) FROM lake_replication.initial_copies','candidate'),'1')
    check('transfer keeps the same Iceberg object location',sql(LOCATION,'candidate'),manifest['location'])
    check('publication creates a new metadata snapshot',sql(META,'candidate')!=manifest['metadata'],True)
    check('transfer retains exactly the original source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='transfer_sub'"),'1')
    stopped_inventory(expected)
    check('old writers remain stopped after publication',True,True)
    q.compose('restart','candidate',timeout=120)
    check('transferred profile survives ordinary restart',q.wait_for('candidate','analytics','SELECT lake_replication.prototype_binding_matches(1)::text','true',timeout=90),'true')
    q.sql('source','application',"INSERT INTO public.transfer_items VALUES(4,'four')")
    check('transferred writer streams after restart',q.wait_for('candidate','analytics',ROWS,'1:one-new,3:three,4:four'),'1:one-new,3:three,4:four')
    sql('ALTER SUBSCRIPTION transfer_sub DISABLE','candidate')
    check('candidate wrapper drains before failback review',q.wait_for('candidate','analytics',WRAPPERS,'0'),'0')
    check('source slot inactive before failback review',q.wait_for('source','application',"SELECT active::text FROM pg_replication_slots WHERE slot_name='transfer_sub'",'false'),'false')
    check('source progress exceeds the stale original backup',q.wait_for('source','application',"SELECT (confirmed_flush_lsn>'"+manifest['staged_lsn']+"'::pg_lsn)::text FROM pg_replication_slots WHERE slot_name='transfer_sub'",'true'),'true')
    refusal('blind failback to original stale data directory',lambda:source_safe(manifest['staged_lsn']))
    stopped_inventory(expected)
    check('refused failback never restarts old writers',True,True)


def main():
    q.RESULTS=ROOT/'.qualification'/(q.RUN+'_transfer');q.RESULTS.mkdir(parents=True,exist_ok=False)
    q.EVIDENCE.update(experiment='controlled exclusive writer cold transfer',production_changed=False,refusals=[],
      limits=['Linux arm64, private Docker inventory with restart policies disabled; not a production fencing controller or distributed lease.',
              'Controlled PostgreSQL-catalog Iceberg snapshot, drained replication and disabled Iceberg maintenance before backup.',
              'Source identity and retained WAL must remain valid; arbitrary old backups and blind failback are refused.',
              'No consumer routing, live failover, external REST-catalog takeover, hostile admin or unknown writer exclusion is qualified.'])
    parent_file=ROOT/'docs/evidence/2026-10-19-base-publication-ownership-results.json';parent=json.loads(parent_file.read_text())
    q.EVIDENCE['parent_evidence_sha256']=hashlib.sha256(parent_file.read_bytes()).hexdigest()
    q.EVIDENCE['runtime_source_fingerprints']=parent['source_fingerprints']
    q.EVIDENCE['harness_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest();shutil.copy2(__file__,q.RESULTS/'transfer.py')
    started=False
    try:
        for n,d in parent['source_fingerprints'].items():
            if hashlib.sha256(Path(__file__).with_name(n).read_bytes()).hexdigest()!=d:raise RuntimeError('runtime source changed: '+n)
        image=parent['image_id'];check('use exact publication-qualified local image',q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip(),image);q.EVIDENCE['image_id']=image
        archive=q.archive_path();q.verify_archive(archive)
        q.command(['git','diff','--exit-code',q.EVIDENCE['source_revision'],'--','worker','migrations','extensions/lake_replication'])
        with tempfile.TemporaryDirectory(prefix='lake-transfer-') as temporary:q.build_images(archive,Path(temporary))
        q.ENV.update(QUALIFICATION_DESTINATION_IMAGE=image,QUAL_TRANSFER_PROFILE='quarantine',QUAL_TRANSFER_LAUNCHER='false')
        text=q.COMPOSE_FILE.read_text().replace('shared_preload_libraries=pg_extension_base,lake_replication','shared_preload_libraries=pg_extension_base').replace('      - -c\n      - lake_replication.database_name=analytics\n','').replace('  destination:\n','  destination:\n    init: true\n')
        text=text.replace('      - max_worker_processes=24\n',f'      - max_worker_processes=24\n      - -c\n      - lake_replication.prototype_instance_id={OWNER}\n      - -c\n      - pg_lake_iceberg.autovacuum=off\n',1)
        text=text.replace('      - destination-data:/var/lib/postgresql/18/data\n','      - destination-data:/var/lib/postgresql/18/data\n      - copy-data:/physical-copy\n      - copy-socket:/copy-socket\n',1)
        command=['postgres','-c','max_worker_processes=24','-c','shared_preload_libraries=pg_extension_base','-c','lake_replication.prototype_instance_id=${QUAL_TRANSFER_PROFILE}','-c','pg_extension_base.enable_base_worker_launcher=${QUAL_TRANSFER_LAUNCHER}','-c','pg_lake_engine.host=host=/var/run/pgduck port=5332','-c','pg_lake_iceberg.autovacuum=off','-c','lake_replication.log_directory=/worker-logs']
        extra='  candidate:\n    profiles: [transfer]\n    init: true\n    image: ${QUALIFICATION_DESTINATION_IMAGE}\n    environment:\n      PGDATA: /var/lib/postgresql/18/data\n    command: '+json.dumps(command)+'\n    volumes:\n      - copy-data:/var/lib/postgresql/18/data\n      - copy-socket:/var/run/pgduck\n      - credentials:/credentials:ro\n      - copy-logs:/worker-logs\n\n  copy-pgduck:\n    profiles: [transfer]\n    image: ${QUALIFICATION_DESTINATION_IMAGE}\n    user: postgres\n    entrypoint: [/qualification/pgduck.sh]\n    volumes:\n      - copy-data:/var/lib/postgresql/18/data\n      - copy-socket:/var/run/pgduck\n\n'
        extra=extra.replace('    entrypoint: [/qualification/pgduck.sh]\n', '    entrypoint: [/qualification/pgduck.sh]\n    healthcheck:\n      test: [CMD, psql, -h, /var/run/pgduck, -p, \'5332\', -U, postgres, -d, postgres, -c, SELECT 1]\n      interval: 1s\n      timeout: 3s\n      retries: 60\n')
        text=text.replace('  storage:\n',extra+'  storage:\n')+'  copy-data:\n  copy-socket:\n  copy-logs:\n'
        q.COMPOSE_FILE=q.RESULTS/'compose.yml';q.COMPOSE_FILE.write_text(text)
        started=True;q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
        exercise();q.EVIDENCE.update(passed=True,status='passed')
    except Exception as error:q.EVIDENCE.update(passed=False,status='error',error=str(error));print(f'ERROR: {error}',flush=True)
    finally:
        try:
            if started:
                logs=q.compose('logs','--no-color',check_result=False);(q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        finally:
            cleanup=q.compose('--profile','transfer','down','-v','--remove-orphans',check_result=False,timeout=180)
            if cleanup.returncode:q.EVIDENCE.update(passed=False,status='cleanup_error',cleanup_error=cleanup.stderr)
            (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2,sort_keys=True)+'\n');print(f'Evidence: {q.RESULTS}',flush=True)
    return 0 if q.EVIDENCE.get('passed') else 1


if __name__=='__main__':raise SystemExit(main())

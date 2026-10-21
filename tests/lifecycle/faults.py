#!/usr/bin/env python3
"""Force Rust child crashes against the exported lifecycle-migration archive."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import time
from urllib.parse import quote

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('qualification',ROOT/'tests/qualification/probe.py')
q=importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)

def sql(statement):
    return q.sql('destination','analytics',statement)

def main():
    q.RESULTS=q.RESULTS.with_name(q.RUN+'_base_faults')
    q.PROJECT+='-base-faults'
    q.SOURCE_IMAGE+='-base-faults'
    q.DESTINATION_IMAGE+='-base-faults'
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE,QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    q.RESULTS.mkdir(parents=True)
    q.EVIDENCE['scope']='Packaged production base lifecycle; forced Rust supervisor/receiver SIGKILL, not PostgreSQL wrapper SIGKILL'
    q.EVIDENCE['inputs']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),ROOT/'tests/qualification/probe.py',ROOT/'extensions/lake_replication/lake_replication.c',ROOT/'worker/src/supervisor.rs',ROOT/'migrations/0023_base_worker_lifecycle.sql']}
    try:
        archive=q.archive_path()
        q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-base-faults-') as temp:
            q.build_images(archive,Path(temp))
        q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
        q.sql('source','application',"CREATE TABLE fault_items(id bigint PRIMARY KEY,note text); INSERT INTO fault_items VALUES(1,'initial'); CREATE PUBLICATION fault_pub FOR TABLE fault_items; GRANT SELECT ON fault_items TO lake_source")
        connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
        sql(f"CREATE SUBSCRIPTION fault_sub CONNECTION '{connection}' PUBLICATION fault_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        q.check('fault fixture completes initial copy',q.wait_for('destination','analytics','SELECT count(*) FROM fault_items','1'),'1')
        original_start=sql('SELECT pg_postmaster_start_time()')
        count="SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'"
        for row_id,kind in [(2,'supervisor'),(3,'receiver')]:
            q.check(kind+' fault starts with one wrapper',q.wait_for('destination','analytics',count,'1'),'1')
            receiver=None
            deadline=time.monotonic()+10
            while time.monotonic()<deadline:
                found=q.compose('exec','-T','destination','pgrep','-f','^/usr/local/bin/lake-replication-receiver$',check_result=False).stdout.strip()
                if found.isdigit():
                    receiver=found
                    break
                time.sleep(.1)
            q.check(kind+' fault locates its receiver',receiver is not None,True)
            supervisor=sql('SELECT supervisor_pid FROM lake_replication.service_status')
            victim=supervisor if kind=='supervisor' else receiver
            q.compose('exec','-T','destination','kill','-KILL',victim)
            q.sql('source','application',f"INSERT INTO fault_items VALUES({row_id},'after-{kind}-kill')")
            q.check(kind+' SIGKILL resumes publication',q.wait_for('destination','analytics','SELECT count(*)||\':\'||count(DISTINCT id) FROM fault_items',f'{row_id}:{row_id}',timeout=45),f'{row_id}:{row_id}')
            q.check(kind+' SIGKILL preserves postmaster',sql('SELECT pg_postmaster_start_time()'),original_start)
            q.check(kind+' SIGKILL leaves exactly one wrapper',q.wait_for('destination','analytics',count,'1'),'1')
            gone=q.compose('exec','-T','destination','sh','-c',f'kill -0 {victim} 2>/dev/null',check_result=False).returncode!=0
            q.check(kind+' victim is reaped',gone,True)
            if kind=='supervisor':
                gone=q.compose('exec','-T','destination','sh','-c',f'kill -0 {receiver} 2>/dev/null',check_result=False).returncode!=0
                q.check('supervisor crash reaps its old receiver descendant',gone,True)
        sql('DROP SUBSCRIPTION fault_sub')
        q.check('fault fixture cleans its source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='fault_sub'"),'0')
        q.EVIDENCE.update(status='passed',passed=True)
    except Exception as error:
        q.EVIDENCE.update(status='error',passed=False,error=str(error))
        print('ERROR:',error,flush=True)
    finally:
        logs=q.compose('logs','--no-color',check_result=False)
        (q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        worker=q.compose('exec','-T','destination','sh','-ec',"find /worker-logs -type f -name '*.log' -exec cat {} +",check_result=False)
        (q.RESULTS/'workers.jsonl').write_text(worker.stdout)
        cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
        q.EVIDENCE['cleanup_returncode']=cleanup.returncode
        (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2)+'\n')
        print('Evidence:',q.RESULTS,flush=True)
    return 0 if q.EVIDENCE.get('passed') and cleanup.returncode==0 else 1

if __name__=='__main__':
    raise SystemExit(main())

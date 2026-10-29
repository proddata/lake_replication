#!/usr/bin/env python3
"""Private PG18/pg_lake regression for empty filtered streamed commits.

Installs current SQL over the qualified archive in a disposable image.
--current-workers also rebuilds Rust workers. This is not release qualification.
"""
import argparse
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', ROOT/'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)


def sql(statement, database='analytics'):
    return q.sql('destination', database, statement)


def check(name, actual, expected):
    q.check(name, actual, expected)


def contract(database):
    """Exercise both fresh install and baseline-to-current upgrade."""
    def query(statement):
        return sql(statement, database)
    sub = query("SELECT lake_replication.create_subscription(1,1,1,'contract','contract',ARRAY['pub'])")
    def commit(xid=2, end='0/140'):
        return f"SELECT lake_replication.commit_stream({sub},{xid},'0/130','{end}','2026-01-01Z')"
    query(f"""INSERT INTO lake_replication.relations
        (subscription_id,remote_relation_id,schema_version,source_namespace,source_relation,replica_identity,columns)
        VALUES({sub},1,1,'public','items','d','[{{"name":"id","type_oid":20}}]');
        SELECT lake_replication.stage_transaction({sub},1,'0/100','0/110','0/120','2026-01-01Z',
        '[{{"operation":"insert","remote_relation_id":1,"relation_schema_version":1,"new_tuple":{{"id":"1"}}}}]');
        SELECT lake_replication.begin_stream({sub},2,'0/125');""")
    check(database+' empty stream commits zero changes', query(commit()), '0')
    check(database+' staging preserves unpublished predecessor', query(f"SELECT staged_lsn||':'||published_lsn FROM lake_replication.checkpoints WHERE subscription_id={sub}"), '0/140:0/0')
    check(database+' zero-change transaction has no change rows', query(f"SELECT jsonb_array_length(payload)||':'||(SELECT count(*) FROM lake_replication.changes c WHERE c.transaction_id=t.transaction_id) FROM lake_replication.transactions t WHERE subscription_id={sub} AND xid=2"), '0:0')
    query(f"SELECT lake_replication.begin_stream({sub},2,'0/125')")
    check(database+' replay commits idempotently', query(commit()), '0')
    check(database+' replay keeps exactly two transactions', query(f'SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={sub}'), '2')
    query(f"SELECT lake_replication.begin_stream({sub},2,'0/125')")
    failed=q.sql_result('destination',database,commit(end='0/141'),check_result=False)
    check(database+' conflicting replay fails closed',failed.returncode != 0 and 'conflicting replay' in failed.stderr,True)
    check(database+' conflict retains stream for retry',query(f'SELECT count(*) FROM lake_replication.streamed_transactions WHERE subscription_id={sub}'), '1')
    query(f'SELECT lake_replication.abort_stream({sub},2)')
    check(database+' transaction limit selects predecessor first',query(f'SELECT first_end_lsn||\':\'||last_end_lsn||\':\'||transaction_count||\':\'||row_count FROM lake_replication.next_batch({sub},1,1,1)'), '0/120:0/120:1:1')
    check(database+' mixed batch counts empty commits against transaction limit',query(f"SELECT transaction_count||':'||row_count FROM lake_replication.next_batch({sub},10,1,10000)"),'2:1')
    query(f"SELECT lake_replication.begin_stream({sub},1,'0/100')")
    failed=q.sql_result('destination',database,f"SELECT lake_replication.commit_stream({sub},1,'0/110','0/120','2026-01-01Z')",check_result=False)
    check(database+' empty replay cannot replace a data transaction',failed.returncode != 0 and 'conflicting replay' in failed.stderr,True)
    query(f"SELECT lake_replication.abort_stream({sub},1); BEGIN; SELECT lake_replication.begin_stream({sub},3,'0/145'); SELECT lake_replication.commit_stream({sub},3,'0/150','0/160','2026-01-01Z'); ROLLBACK")
    check(database+' rolled-back staging does not advance checkpoint',query(f'SELECT staged_lsn FROM lake_replication.checkpoints WHERE subscription_id={sub}'),'0/140')
    failed=q.sql_result('destination',database,f"SELECT lake_replication.mark_acknowledged({sub},'0/160')",check_result=False)
    check(database+' feedback cannot acknowledge rolled-back empty commit',failed.returncode != 0 and 'exceeds staged' in failed.stderr,True)
    # SQL contract uses prepare/finish to avoid fabricating a foreign destination.
    query(f"SELECT lake_replication.finish_batch(lake_replication.prepare_batch({sub},'first','0/120','0/120'))")
    check(database+' empty trailing transaction remains selectable',query(f"SELECT first_end_lsn||':'||last_end_lsn||':'||transaction_count||':'||row_count FROM lake_replication.next_batch({sub},1,1,1)"), '0/140:0/140:1:0')
    failed=q.sql_result('destination',database,f"BEGIN; SELECT * FROM lake_replication.publish_insert_batch({sub},'empty','0/140','0/140'); ROLLBACK;",check_result=False)
    check(database+' no-op publication can roll back',failed.returncode,0)
    check(database+' rollback retains old publication checkpoint',query(f'SELECT published_lsn FROM lake_replication.checkpoints WHERE subscription_id={sub}'),'0/120')
    check(database+' empty-only batch publishes no target rows',query(f"SELECT inserted_rows FROM lake_replication.publish_insert_batch({sub},'empty','0/140','0/140')"),'0')
    check(database+' publication replay is idempotent',query(f"SELECT inserted_rows FROM lake_replication.publish_insert_batch({sub},'empty','0/140','0/140')"),'0')
    query(f"SELECT lake_replication.begin_stream({sub},3,'0/145'); SELECT lake_replication.abort_stream({sub},3)")
    check(database+' aborted empty stream does not advance checkpoint',query(f'SELECT staged_lsn||\':\'||published_lsn FROM lake_replication.checkpoints WHERE subscription_id={sub}'),'0/140:0/140')
    failed=q.sql_result('destination',database,f"SELECT lake_replication.stage_transaction({sub},4,'0/100','0/105','0/115','2026-01-01Z','[]')",check_result=False)
    check(database+' older empty commit cannot regress staging',failed.returncode != 0 and 'does not advance' in failed.stderr,True)
    query(f"SELECT lake_replication.stage_transaction({sub},4,'0/145','0/150','0/160','2026-01-01Z','[]')")
    check(database+' ordinary empty commit also journals safely',query(f"SELECT row_count FROM lake_replication.next_batch({sub},10,10,1000)"),'0')


def wire():
    q.sql('source','application',"CREATE TABLE included(id bigint PRIMARY KEY,note text); CREATE TABLE excluded(id bigint,payload text); CREATE PUBLICATION filtered_pub FOR TABLE included; GRANT SELECT ON included TO lake_source")
    q.sql('source','application',"SELECT pg_create_logical_replication_slot('filtered_slot','pgoutput')")
    identity=q.sql('source','application',"SELECT (pg_control_system()).system_identifier||':'||(SELECT oid FROM pg_database WHERE datname=current_database())||':'||(pg_control_checkpoint()).timeline_id").split(':')
    sub=sql(f"SELECT lake_replication.create_subscription({','.join(identity)},'filtered','filtered_slot',ARRAY['filtered_pub'])")
    environment=["LAKE_CREDENTIAL_FILE=/credentials/worker.env", "LAKE_SOURCE_ADDRESS=source:5432", "LAKE_DESTINATION_ADDRESS=127.0.0.1:5432", "LAKE_SOURCE_DATABASE=application", "LAKE_DESTINATION_DATABASE=analytics", "LAKE_SLOT=filtered_slot", "LAKE_PUBLICATION=filtered_pub", f"LAKE_SUBSCRIPTION_ID={sub}", "LAKE_DISCOVER_PUBLICATION=1", "LAKE_DESTINATION_SCHEMA=public", "LAKE_CREATE_TABLES_USING_ICEBERG=1", "LAKE_MAX_TRANSACTIONS=1", "LAKE_DEBUG_STREAMING_IMMEDIATE=1", "LAKE_RECEIVER_IDLE_TIMEOUT_MS=5000"]
    def worker(name):
        result=q.compose('exec','-T','-u','postgres','destination','env',*environment,'/usr/local/bin/lake-replication-'+name,timeout=60,check_result=False)
        with (q.RESULTS/(name+'.jsonl')).open('a') as log:
            log.write(result.stdout+result.stderr)
        check(name+' succeeds',result.returncode,0)
        return result.stdout+result.stderr
    q.sql('source','application',"INSERT INTO included VALUES(1,'before-filtered')")
    worker('receiver')
    sql(f"CREATE TABLE included(id bigint,note text) USING iceberg; SELECT lake_replication.assign_destination({sub},remote_relation_id,schema_version,'included'::regclass) FROM lake_replication.relations WHERE subscription_id={sub}")
    # The excluded table is outside the publication. Low decoding memory forces
    # pgoutput to send Stream Start/Commit with no relevant row changes.
    q.sql('source','application',"INSERT INTO excluded SELECT n,repeat('x',2000) FROM generate_series(1,20000)n")
    output=worker('receiver')
    check('real pgoutput delivered empty streamed commit','staged streamed xid=' in output and 'changes=0' in output,True)
    check('wire journal retains earlier unpublished data and no-op',sql(f"SELECT count(*)||':'||count(*) FILTER(WHERE payload='[]') FROM lake_replication.transactions WHERE subscription_id={sub}"),'2:1')
    check('wire feedback acknowledges durable staging only',sql(f"SELECT (staged_lsn=acknowledged_lsn AND published_lsn='0/0' AND staged_lsn>'0/0')::text FROM lake_replication.checkpoints WHERE subscription_id={sub}"),'true')
    q.compose('restart','destination',timeout=120)
    check('destination recovers',q.wait_for('destination','analytics','SELECT 1','1',timeout=90),'1')
    check('restart retains zero-change staged transaction',sql(f"SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={sub} AND payload='[]' AND state='staged'"),'1')
    worker('publisher')
    check('mixed batch publishes included row once',sql('SELECT count(*) FROM included'),'1')
    check('restart publication consumes both commits',sql(f"SELECT count(*)||':'||count(*) FILTER(WHERE state='published') FROM lake_replication.transactions WHERE subscription_id={sub}"),'2:2')
    q.sql('source','application',"INSERT INTO included VALUES(2,'after-restart')")
    worker('receiver')
    worker('publisher')
    check('receiver resumes after empty commit and restart',sql("SELECT string_agg(note,',' ORDER BY id) FROM included"),'before-filtered,after-restart')
    # Empty-only batch at the tail must advance publication as well.
    q.sql('source','application',"INSERT INTO excluded SELECT n,repeat('y',2000) FROM generate_series(20001,40000)n")
    output=worker('receiver')
    check('second empty stream is observed','staged streamed xid=' in output and 'changes=0' in output,True)
    worker('publisher')
    check('empty-only batch leaves included data intact',sql('SELECT count(*) FROM included'),'2')
    check('all checkpoints converge after trailing empty commit',sql(f"SELECT (received_lsn=staged_lsn AND staged_lsn=published_lsn AND published_lsn=acknowledged_lsn)::text FROM lake_replication.checkpoints WHERE subscription_id={sub}"),'true')
    check('no durable stream fragments remain',sql(f'SELECT count(*) FROM lake_replication.streamed_transactions WHERE subscription_id={sub}'),'0')
    before=sql(f'SELECT staged_lsn FROM lake_replication.checkpoints WHERE subscription_id={sub}')
    common=['docker','compose','-p',q.PROJECT,'-f',str(q.COMPOSE_FILE),'exec','-T']
    source=subprocess.Popen(common+['source','psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','application','-c',"BEGIN; INSERT INTO excluded SELECT n,repeat('z',2000) FROM generate_series(40001,60000)n; SELECT pg_sleep(8); COMMIT"],env=q.ENV,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    receiver=subprocess.Popen(common+['-u','postgres','destination','env',*environment,'/usr/local/bin/lake-replication-receiver'],env=q.ENV,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        check('uncommitted empty stream is durably retained',q.wait_for('destination','analytics',f'SELECT count(*) FROM lake_replication.streamed_transactions WHERE subscription_id={sub}','1',timeout=6),'1')
        check('uncommitted empty stream does not advance staging',sql(f'SELECT staged_lsn FROM lake_replication.checkpoints WHERE subscription_id={sub}'),before)
        q.compose('exec','-T','destination','pkill','-KILL','-f','^/usr/local/bin/lake-replication-receiver$',timeout=15)
        receiver.communicate(timeout=10)
        check('receiver loss leaves durable stream',sql(f'SELECT count(*) FROM lake_replication.streamed_transactions WHERE subscription_id={sub}'),'1')
        _,errors=source.communicate(timeout=20)
        check('source commits while receiver is down',source.returncode,0)
        worker('receiver')
        worker('publisher')
        check('restarted receiver replays empty stream exactly once',sql(f"SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={sub} AND payload='[]'"),'3')
        check('interrupted stream leaves no fragments',sql(f'SELECT count(*) FROM lake_replication.streamed_transactions WHERE subscription_id={sub}'),'0')
        check('restart preserves real target rows',sql('SELECT count(*) FROM included'),'2')
        staged=sql(f'SELECT staged_lsn FROM lake_replication.checkpoints WHERE subscription_id={sub}')
        check('source never acknowledges beyond durable journal',q.sql('source','application',f"SELECT (confirmed_flush_lsn <= '{staged}'::pg_lsn)::text FROM pg_replication_slots WHERE slot_name='filtered_slot'"),'true')
        q.sql('source','application',"BEGIN; INSERT INTO included VALUES(3,'mixed-first'); INSERT INTO excluded SELECT n,repeat('m',2000) FROM generate_series(60001,80000)n; INSERT INTO included VALUES(4,'mixed-last'); COMMIT")
        output=worker('receiver')
        check('nonempty streamed transaction still stages all relevant changes','staged streamed xid=' in output and 'changes=2' in output,True)
        worker('publisher')
        check('mixed streamed transaction publishes relevant changes only',sql("SELECT string_agg(id::text,',' ORDER BY id) FROM included"),'1,2,3,4')
    finally:
        if receiver.poll() is None:
            try:
                q.compose('exec','-T','destination','pkill','-KILL','-f',
                          '^/usr/local/bin/lake-replication-receiver$',check_result=False,timeout=15)
            except Exception as error:
                q.EVIDENCE.setdefault('cleanup_errors',[]).append(dict(
                    operation='kill interrupted receiver',error_type=type(error).__name__,error=str(error)))
            stop_exec(receiver)
        stop_exec(source)


def stop_exec(process):
    """Bound local docker-exec cleanup; fixture teardown handles container children."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            q.EVIDENCE.setdefault('cleanup_errors',[]).append(dict(
                operation='reap docker exec',error='local docker exec did not exit after kill'))


def finalize_evidence(native_bulk):
    """Preserve the primary result even if Docker/log collection is unresponsive."""
    primary_status=q.EVIDENCE.get('status','error')
    q.EVIDENCE['test_status']=primary_status
    q.EVIDENCE['status']='finalizing'
    def persist():
        (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2)+'\n')
    def collect(operation, callback):
        try:
            return callback()
        except Exception as error:
            q.EVIDENCE.setdefault('cleanup_errors',[]).append(dict(
                operation=operation,error_type=type(error).__name__,error=str(error)))
            q.EVIDENCE['status']='error'
            return None
        finally:
            persist()
    persist()
    def host_resources():
        # These host reads remain available when docker exec itself is stuck.
        disk=os.statvfs(q.RESULTS)
        snapshot=dict(load_average=os.getloadavg(),
                      available_disk_bytes=disk.f_bavail*disk.f_frsize,
                      free_inodes=disk.f_favail)
        for name in ('meminfo','pressure/cpu','pressure/memory','pressure/io'):
            path=Path('/proc')/name
            if path.exists():
                snapshot[name]=path.read_text()
        q.EVIDENCE['host_resources']=snapshot
    collect('capture host resources',host_resources)
    if native_bulk:
        def worker_logs():
            logs=q.compose('exec','-T','destination','sh','-ec',
                "find /worker-logs -type f -name '*.log' -exec cat {} +",check_result=False,timeout=20)
            (q.RESULTS/'native-workers.jsonl').write_text(logs.stdout+logs.stderr)
            q.EVIDENCE['native_worker_logs_returncode']=logs.returncode
            q.EVIDENCE['native_worker_logs_sha256']=hashlib.sha256((logs.stdout+logs.stderr).encode()).hexdigest()
            if logs.returncode:
                raise RuntimeError('worker log collection failed')
        collect('collect worker logs',worker_logs)
    def compose_logs():
        logs=q.compose('logs','--no-color',check_result=False,timeout=20)
        q.EVIDENCE['logs']=logs.stdout+logs.stderr
        if logs.returncode:
            raise RuntimeError('compose log collection failed')
    collect('collect compose logs',compose_logs)
    cleanup=collect('remove fixture',lambda:q.compose('down','-v','--remove-orphans',
                                                    check_result=False,timeout=60))
    q.EVIDENCE['cleanup_returncode']=cleanup.returncode if cleanup is not None else None
    q.EVIDENCE['status']='passed' if (primary_status=='passed' and cleanup is not None
        and cleanup.returncode==0 and not q.EVIDENCE.get('cleanup_errors')) else 'error'
    persist()


def native():
    q.sql('source','application',"CREATE TABLE native_items(id bigint PRIMARY KEY,note text); INSERT INTO native_items VALUES(1,'initial'); CREATE PUBLICATION native_pub FOR TABLE native_items; GRANT SELECT ON native_items TO lake_source")
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    sql(f"CREATE SUBSCRIPTION native_filtered CONNECTION '{connection}' PUBLICATION native_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    check('native subscription completes initial copy',q.wait_for('destination','analytics',"SELECT state FROM lake_replication.subscriptions WHERE subscription_name='native_filtered'",'streaming'),'streaming')
    check('native idle receiver connects',q.wait_for('source','application',"SELECT active::text FROM pg_replication_slots WHERE slot_name='native_filtered'",'true'),'true')
    pid=q.sql('source','application',"SELECT active_pid FROM pg_replication_slots WHERE slot_name='native_filtered'")
    time.sleep(2)
    check('idle native receiver retains its connection beyond 200ms',q.sql('source','application',"SELECT active_pid FROM pg_replication_slots WHERE slot_name='native_filtered'"),pid)
    q.sql('source','application',"INSERT INTO excluded SELECT n,repeat('b',128) FROM generate_series(80001,2080000)n",check_result=True)
    q.sql('source','application',"INSERT INTO native_items VALUES(2,'after-unrelated-bulk')")
    check('native supervisor progresses beyond two-million-row excluded commit',q.wait_for('destination','analytics',"SELECT string_agg(note,',' ORDER BY id) FROM native_items",'initial,after-unrelated-bulk',timeout=90),'initial,after-unrelated-bulk')
    sub=sql("SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='native_filtered'")
    check('native runtime publishes the filtered commit',sql(f"SELECT (count(*)>0 AND bool_and(state='published'))::text FROM lake_replication.transactions WHERE subscription_id={sub} AND payload='[]'"),'true')
    began=time.monotonic()
    sql('ALTER SUBSCRIPTION native_filtered DISABLE')
    check('disable releases persistent receiver slot',q.wait_for('source','application',"SELECT active::text FROM pg_replication_slots WHERE slot_name='native_filtered'",'false',timeout=5),'false')
    q.EVIDENCE['native_disable_seconds']=time.monotonic()-began
    check('disable completes within five seconds',q.EVIDENCE['native_disable_seconds']<5,True)
    q.sql('source','application',"INSERT INTO native_items VALUES(3,'while-disabled')")
    received=q.compose('exec','-T','-u','postgres','destination','env',
        'LAKE_CREDENTIAL_FILE=/credentials/worker.env','LAKE_SOURCE_ADDRESS=source:5432',
        'LAKE_DESTINATION_ADDRESS=127.0.0.1:5432','LAKE_SOURCE_DATABASE=application',
        'LAKE_DESTINATION_DATABASE=analytics','LAKE_SLOT=native_filtered','LAKE_PUBLICATION=native_pub',
        f'LAKE_SUBSCRIPTION_ID={sub}','LAKE_MAX_TRANSACTIONS=1','LAKE_RECEIVER_IDLE_TIMEOUT_MS=5000',
        '/usr/local/bin/lake-replication-receiver',timeout=30,check_result=False)
    check('standalone receiver stages while supervisor is disabled',received.returncode,0)
    check('real transaction is durable but unpublished',sql(f"SELECT (staged_lsn>published_lsn)::text FROM lake_replication.checkpoints WHERE subscription_id={sub}"),'true')
    sql('ALTER SUBSCRIPTION native_filtered ENABLE')
    check('enable resumes once',q.wait_for('destination','analytics',"SELECT count(*) FROM native_items",'3'),'3')
    q.compose('stop','source',timeout=30)
    check('source outage is reported',q.wait_for('destination','analytics',f"SELECT (total_failures>0)::text FROM lake_replication.service_status WHERE subscription_id={sub}",'true',timeout=30),'true')
    q.compose('start','source',timeout=30)
    check('source recovers',q.wait_for('source','application','SELECT 1','1',timeout=60),'1')
    q.sql('source','application',"INSERT INTO native_items VALUES(4,'after-source-restart')")
    check('persistent receiver recovers after source outage',q.wait_for('destination','analytics',"SELECT string_agg(id::text,',' ORDER BY id) FROM native_items",'1,2,3,4',timeout=60),'1,2,3,4')
    # Stop during an open streamed transaction, not only while idle.
    common=['docker','compose','-p',q.PROJECT,'-f',str(q.COMPOSE_FILE),'exec','-T']
    source=subprocess.Popen(common+['source','psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','application','-c',"BEGIN; INSERT INTO native_items VALUES(5,'interrupted-stream'); INSERT INTO excluded SELECT n,repeat('c',2000) FROM generate_series(2080001,2100000)n; SELECT pg_sleep(8); COMMIT"],env=q.ENV,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        check('native receiver retains an uncommitted stream',q.wait_for('destination','analytics',f'SELECT count(*) FROM lake_replication.streamed_transactions WHERE subscription_id={sub}','1',timeout=6),'1')
        began=time.monotonic()
        sql('ALTER SUBSCRIPTION native_filtered DISABLE')
        check('disable interrupts streamed receive promptly',q.wait_for('source','application',"SELECT active::text FROM pg_replication_slots WHERE slot_name='native_filtered'",'false',timeout=5),'false')
        q.EVIDENCE['native_stream_disable_seconds']=time.monotonic()-began
        check('stream disable completes within five seconds',q.EVIDENCE['native_stream_disable_seconds']<5,True)
        _,errors=source.communicate(timeout=20)
        check('source commits after supervised stream interruption',source.returncode,0)
        sql('ALTER SUBSCRIPTION native_filtered ENABLE')
        check('enabled receiver replays interrupted stream exactly once',q.wait_for('destination','analytics',"SELECT string_agg(id::text,',' ORDER BY id) FROM native_items",'1,2,3,4,5'),'1,2,3,4,5')
        check('replayed native stream clears fragments',sql(f'SELECT count(*) FROM lake_replication.streamed_transactions WHERE subscription_id={sub}'),'0')
    finally:
        if source.poll() is None:
            source.terminate()
            source.communicate(timeout=10)
    began=time.monotonic()
    sql('DROP SUBSCRIPTION native_filtered')
    q.EVIDENCE['native_drop_seconds']=time.monotonic()-began
    check('drop completes within five seconds',q.EVIDENCE['native_drop_seconds']<5,True)
    check('native drop releases its source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='native_filtered'"),'0')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--native-bulk',action='store_true',help='Also test native bulk progress, idle connection lifetime, stop/resume and source recovery')
    parser.add_argument('--archive-only',action='store_true',help='Test exported SQL and native binaries unchanged, without a source overlay')
    parser.add_argument('--current-workers',action='store_true',help='Rebuild Rust workers from the current source into the test image')
    args=parser.parse_args()
    if args.archive_only and args.current_workers:parser.error('archive-only cannot rebuild workers')
    q.EVIDENCE['native_bulk_diagnostic']=args.native_bulk
    q.EVIDENCE['current_workers']=args.current_workers
    q.EVIDENCE['archive_only']=args.archive_only
    q.RESULTS=q.RESULTS.with_name(q.RUN+'_empty_filtered')
    q.RESULTS.mkdir(parents=True)
    q.PROJECT += '-empty-filtered'
    q.EVIDENCE['inputs']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in [Path(__file__),*(sorted((ROOT/'worker/src').glob('*.rs')) if args.current_workers else []),ROOT/'Cargo.lock',ROOT/'migrations/0022_empty_filtered_transactions.sql',*sorted((ROOT/'extensions/lake_replication').glob('lake_replication--*.sql'))]}
    q.EVIDENCE['scope']=('Exported archive SQL and native binaries unchanged; no overlay' if args.archive_only else 'Current SQL overlay; '+('current Rust workers' if args.current_workers else 'archived native binaries unchanged')+'; not release qualification')
    try:
        q.verify_archive(q.archive_path())
        with tempfile.TemporaryDirectory(prefix='lake-empty-filtered-') as directory:
            context=Path(directory)
            q.build_images(q.archive_path(),context)
            if not args.archive_only:
                for file in (ROOT/'extensions/lake_replication').glob('lake_replication--*.sql'):
                    (context/file.name).write_bytes(file.read_bytes())
                overlay=f'FROM {q.DESTINATION_IMAGE}\nUSER root\nCOPY lake_replication--*.sql /tmp/sql/\nRUN cp /tmp/sql/*.sql "$(pg_config --sharedir)/extension/"\n'
                if args.current_workers:
                    builder='lake-replication-worker-test:'+q.RUN
                    q.command(['docker','build','--target','worker-builder','-f',str(ROOT/'deploy/install/Dockerfile'),'-t',builder,str(ROOT)],timeout=900)
                    overlay+='COPY --from='+builder+' /src/target/release/lake-replication-receiver /src/target/release/lake-replication-supervisor /src/target/release/lake-replication-publisher /src/target/release/lake-replication-initial-sync /usr/local/bin/\n'
                (context/'Dockerfile.overlay').write_text(overlay)
                image='lake-replication-empty-filtered:'+q.RUN
                q.command(['docker','build','-f',str(context/'Dockerfile.overlay'),'-t',image,str(context)])
                q.EVIDENCE['image']=q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip()
                q.ENV['QUALIFICATION_DESTINATION_IMAGE']=image
            compose=q.COMPOSE_FILE.read_text().replace('      - wal_level=logical','      - logical_decoding_work_mem=64kB\n      - -c\n      - wal_level=logical')
            q.COMPOSE_FILE=q.RESULTS/'compose.yml'
            q.COMPOSE_FILE.write_text(compose)
            q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
            contract('analytics')
            sql('CREATE DATABASE upgraded')
            sql("CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION lake_replication VERSION '0.1.0' CASCADE; ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.4'",'upgraded')
            contract('upgraded')
            wire()
            if args.native_bulk:
                native()
            q.EVIDENCE['status']='passed'
    except Exception as error:
        q.EVIDENCE.update(status='error',error=str(error))
        print(str(error),file=sys.stderr)
    finally:
        finalize_evidence(args.native_bulk)
        print('Evidence:',q.RESULTS,flush=True)
    return 0 if q.EVIDENCE['status']=='passed' else 1


if __name__=='__main__':
    sys.exit(main())

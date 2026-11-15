#!/usr/bin/env python3
"""Small reproducible cost experiment; no performance acceptance threshold."""
import json
import os
import statistics
import subprocess
import sys
import threading
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from live import LiveProbe
from session import Session
from object_trace import ObjectTrace

class BenchmarkProbe(LiveProbe):
    def failure_snapshot(self,mode):
        result={}
        source="""SELECT jsonb_build_object('clock',clock_timestamp(),'rows_a',(SELECT count(*) FROM a),
            'rows_b',(SELECT count(*) FROM b),'slots',(SELECT jsonb_agg(jsonb_build_object(
                'slot',slot_name,'active',active,'confirmed',confirmed_flush_lsn,'restart',restart_lsn,'wal_status',wal_status))
                FROM pg_replication_slots))"""
        target='pull_mirror' if mode=='logical_pull' else 'push_mirror'
        destination=f"""SELECT jsonb_build_object('clock',clock_timestamp(),
            'journal_transactions',(SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={self.subscription}),
            'checkpoint',(SELECT to_jsonb(c) FROM lake_replication.checkpoints c WHERE subscription_id={self.subscription}),
            'mirror_rows_a',(SELECT count(*) FROM {target}.a),'mirror_rows_b',(SELECT count(*) FROM {target}.b))"""
        for service,statement in [('source',source),('destination',destination)]:
            args=self.sql_args()
            if service=='destination':
                args[args.index('source')]='destination';args[args.index('capture')]='analytics'
            try:
                response=self.command(args,input="SET statement_timeout='3s';"+statement,timeout=10,required=False)
                result[service]=json.loads(response.stdout) if response.returncode==0 else dict(error=response.stderr[-2000:])
            except Exception as error:result[service]=dict(error=str(error)[-2000:])
        return result

    def memory_cpu(self):
        result={}
        code="""from pathlib import Path
import json
s=dict(x.split() for x in Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())
memory={name:int(Path('/sys/fs/cgroup/'+name).read_text()) for name in ['memory.current','memory.peak']}
rss=pss=0
for process in Path('/proc').glob('[0-9]*'):
 try:
  if not process.joinpath('comm').read_text().startswith(('postgres','pgduck','lake-replic')):continue
  fields={line.split(':',1)[0]:int(line.split()[1]) for line in process.joinpath('smaps_rollup').read_text().splitlines() if line.startswith(('Rss:','Pss:'))}
  rss+=fields.get('Rss',0);pss+=fields.get('Pss',0)
 except (FileNotFoundError,ProcessLookupError,PermissionError):pass
if rss<=0:raise RuntimeError('database process memory is unavailable')
print(json.dumps({'cpu_usec':int(s['usage_usec']),'memory_bytes':memory['memory.current'],'container_lifetime_memory_peak_bytes':memory['memory.peak'],'process_rss_kib_sum':rss,'process_pss_kib_sum':pss}))"""
        for service in ['source','pgduck','destination','destination-pgduck']:
            result[service]=json.loads(self.command(self.compose+['exec','-T','--user','postgres',service,'python3','-c',code]).stdout)
        return result

    def resources(self):
        result=self.memory_cpu()
        result['wal_lsn']=self.value('SELECT pg_current_wal_insert_lsn()::text')
        result['database_bytes']=int(self.value('SELECT pg_database_size(current_database())'))
        result['objects']=int(self.command(self.compose+['exec','-T','storage','sh','-c',"find /data/lake-push -name xl.meta | wc -l"]).stdout)
        return result

    def measure(self,mode,consume):
        application=Session(self.sql_args())
        samples=[];sampling_errors=[];stop=threading.Event()
        def sample():
            while not stop.wait(2):
                try:
                    item=dict(elapsed_seconds=time.monotonic()-started,wall_elapsed_seconds=time.time()-wall_started,
                        services=self.memory_cpu(),observed_application_commits=self.application_committed)
                    if mode=='iceberg_push':
                        item['captured_sequence']=int(self.value('SELECT captured_sequence FROM lake_capture.streams'))
                        item['destination']=json.loads(self.dest(f"SELECT jsonb_build_object('imported_sequence',imported_sequence,'published_sequence',published_sequence) FROM lake_replication.push_status WHERE subscription_id={self.subscription}"))
                    samples.append(item)
                except Exception as error:sampling_errors.append(str(error));return
        sampler=None;executor=None;future=None;trace=None;started=None;wall_started=None
        self.drain_cancel=threading.Event()
        try:
            if self.trace_objects:
                trace=ObjectTrace(self,mode);trace.start()
            before=self.resources();latencies=[];started=time.monotonic();wall_started=time.time();self.application_committed=0
            if self.concurrent:
                sampler=threading.Thread(target=sample);sampler.start()
                executor=ThreadPoolExecutor(max_workers=1);future=executor.submit(consume)
            for transaction in range(self.transactions):
                low=transaction*self.rows+1;high=low+self.rows-1
                statement=f"BEGIN;INSERT INTO a SELECT i,repeat('x',256) FROM generate_series({low},{high}) i;INSERT INTO b SELECT i,repeat('y',256) FROM generate_series({low},{high}) i;COMMIT"
                write_started=time.monotonic();application.query(statement);latencies.append(time.monotonic()-write_started)
                self.application_committed=transaction+1
                if self.interval:
                    time.sleep(max(0,started+(transaction+1)*self.interval-time.monotonic()))
            writes_finished=time.monotonic()
            if future is None:consume()
            else:future.result(timeout=180)
            stop.set()
            if sampler:sampler.join(timeout=15)
            if sampling_errors:raise RuntimeError(sampling_errors[0])
            after=self.resources()
            record=dict(before=before,after=after,source_write_seconds=writes_finished-started,
                end_to_end_seconds=time.monotonic()-started,
                wall_elapsed_seconds=time.time()-wall_started,
                application_commit_median_seconds=statistics.median(latencies),
                application_commit_p95_seconds=sorted(latencies)[int(.95*(len(latencies)-1))],
                source_wal_bytes=int(self.value(f"SELECT pg_wal_lsn_diff('{after['wal_lsn']}','{before['wal_lsn']}')")),
                source_database_bytes_delta=after['database_bytes']-before['database_bytes'],
                physical_object_count_delta=after['objects']-before['objects'])
            record['resource_samples']=samples
            record['application_commit_p99_seconds']=sorted(latencies)[int(.99*(len(latencies)-1))]
            if mode=='iceberg_push':record['source_publication_transactions']=int(self.value('SELECT count(DISTINCT xmin::text) FROM lake_capture.receipts'))
            record['source_and_engine_cpu_seconds']=sum(after[s]['cpu_usec']-before[s]['cpu_usec'] for s in ['source','pgduck'])/1e6
            record['destination_and_engine_cpu_seconds']=sum(after[s]['cpu_usec']-before[s]['cpu_usec'] for s in ['destination','destination-pgduck'])/1e6
            record['all_database_cpu_seconds']=record['source_and_engine_cpu_seconds']+record['destination_and_engine_cpu_seconds']
            if trace:
                record['object_operations']=trace.finish()
                categories=record['object_operations']['categories']
                self.check(mode+' trace observes destination object uploads',categories.get('destination_publish',{}).get('api:s3.PutObject',0)>0,True)
                self.check(mode+' trace separates source capture uploads',categories.get('source_capture',{}).get('api:s3.PutObject',0)>0,mode=='iceberg_push')
            self.evidence['measurements'][mode]=record
        except Exception as error:
            self.drain_cancel.set();stop.set()
            self.evidence['measurements'][mode]=dict(status='error',error=str(error)[-2000:],
                observed_application_commits=getattr(self,'application_committed',0),resource_samples=list(samples),
                elapsed_seconds=time.monotonic()-started if started is not None else None,
                wall_elapsed_seconds=time.time()-wall_started if wall_started is not None else None,
                diagnostics=self.failure_snapshot(mode))
            raise
        finally:
            stop.set()
            if sampler:sampler.join(timeout=15)
            if executor:executor.shutdown(wait=True,cancel_futures=True)
            if trace:trace.close()
            application.close()

    def exercise(self):
        self.transactions=int(os.environ.get('PUSH_BENCH_TRANSACTIONS','12'));self.rows=int(os.environ.get('PUSH_BENCH_ROWS','20'))
        self.concurrent=os.environ.get('PUSH_BENCH_CONCURRENT')=='1';self.interval=float(os.environ.get('PUSH_BENCH_INTERVAL','0'))
        self.capture_group=int(os.environ.get('PUSH_BENCH_CAPTURE_GROUP','1'));self.capture_interval_ms=int(os.environ.get('PUSH_BENCH_CAPTURE_INTERVAL_MS','0'))
        self.trace_objects=os.environ.get('PUSH_BENCH_OBJECT_TRACE')=='1'
        order=os.environ.get('PUSH_BENCH_ORDER','pull_first')
        if order not in ['pull_first','push_first']:raise RuntimeError('PUSH_BENCH_ORDER must be pull_first or push_first')
        modes=['logical_pull','iceberg_push'] if order=='pull_first' else ['iceberg_push','logical_pull']
        if not 1<=self.transactions<=1000 or not 1<=self.rows<=1000 or not 0<=self.interval<=5 or not 1<=self.capture_group<=32 or not 0<=self.capture_interval_ms<=1000:raise RuntimeError('benchmark workload exceeds bounded fixture limits')
        self.evidence.update(scope='logical-pull versus source-push source-impact experiment',
            workload=dict(transactions=self.transactions,rows_per_table_per_transaction=self.rows,text_bytes=256,concurrent_drain=self.concurrent,transaction_interval_seconds=self.interval,capture_group=self.capture_group,capture_interval_ms=self.capture_interval_ms),
            methodology='One local ARM64 run, pull first then push; persistent psql writes; workers drain after writes; CPU from source and source-pgduck cgroup usage including observer overhead; memory snapshots include cache; MinIO xl.meta count is live physical object count, not request count. No production latency/overhead claim.',
            unqualified=['long-duration source benchmark','tail latency under sustained load','object request count','multi-run statistics','production sizing'],measurements={})
        if self.concurrent:
            self.evidence['methodology']='One local ARM64 run, pull first then push; paced persistent psql writes with concurrent capture/import/publication; two-second resource/progress sampling includes observer overhead. Application timing covers the BEGIN/INSERT/COMMIT round trip. Cross-database frontier observations are sequential, not atomic. RSS sums count shared pages repeatedly; PSS apportions shared pages. Cgroup peak is container lifetime, not interval peak; memory includes cache. Database size includes heap/catalog/index bytes, not only capture metadata. Object count is not request count. Timing includes final drain and observation. Publication transaction count uses fresh receipt xmin values in this short-lived fixture, not a durable public metric. No production acceptance threshold.'
        if self.trace_objects:
            self.evidence['methodology']+=' MinIO S3 traces aggregate observed requests by private source/destination prefix and file kind, excluding witness object/list requests and retaining no request headers, object paths or bodies. Bucket-wide requests, including controller discovery, remain separate. Trace byte counts describe observed protocol traffic, not exact object sizes. This is observed traffic, not a production billing or trace-completeness guarantee.'
            self.evidence['unqualified'].remove('object request count')
            self.evidence['unqualified'].append('production request billing and trace completeness')
        self.evidence['workload']['mode_order']=modes
        self.evidence['methodology']+=' Destination PostgreSQL and its engine are sampled too; all_database_cpu_seconds sums the four database/engine containers, excluding object storage and host/controller CPU. Sampling overhead is included.'
        if order=='push_first':
            self.evidence['methodology']=self.evidence['methodology'].replace('pull first then push','push first then pull')
            self.evidence['methodology']+=' Source maintenance and controlled capture cleanup finish before pull begins. The source application tables are truncated between modes. Database/catalog/cache history remains order-dependent; this is not a cold-cache experiment.'
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.identity=json.loads(self.value("SELECT jsonb_build_object('system_identifier',(SELECT system_identifier FROM pg_control_system())::text,'database_oid',(SELECT oid FROM pg_database WHERE datname=current_database())::text,'timeline',(SELECT timeline_id FROM pg_control_checkpoint()))"))
        for index,mode in enumerate(modes):
            if mode=='logical_pull':self.run_pull()
            else:self.run_push()
            if index==0:
                if mode=='iceberg_push':
                    self.sql(f"SELECT lake_capture.drop_stream('{self.stream}');SELECT lake_capture.start_manager()")
                    self.source_wait('SELECT count(*) FROM lake_capture.streams','0')
                    self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
                    self.check('reverse-order cleanup removes the source capture slot before pull',self.value(
                        "SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'0')
                self.sql('TRUNCATE a,b')
        for table in 'ab':
            self.check('pull and push produce identical rows '+table,self.dest(f'''SELECT NOT EXISTS(
                (TABLE pull_mirror.{table} EXCEPT ALL TABLE push_mirror.{table}) UNION ALL
                (TABLE push_mirror.{table} EXCEPT ALL TABLE pull_mirror.{table}))'''),'t')

    def run_pull(self):
        self.sql("CREATE PUBLICATION bench_pub FOR TABLE a,b;SELECT pg_create_logical_replication_slot('bench_pull','pgoutput')")
        self.subscription=int(self.dest(f"SELECT lake_replication.create_subscription({self.identity['system_identifier']},{self.identity['database_oid']},{self.identity['timeline']},'bench_pull','bench_pull',ARRAY['bench_pub'])"))
        self.dest('CREATE SCHEMA pull_mirror')
        columns=json.dumps([dict(name='id',type_oid=20,type_modifier=-1,flags=1),dict(name='note',type_oid=25,type_modifier=-1,flags=0)])
        for table in 'ab':
            oid=self.value(f"SELECT '{table}'::regclass::oid")
            version=self.dest(f"SELECT lake_replication.register_relation({self.subscription},{oid},'public','{table}','d','{columns}')")
            self.dest(f"CREATE TABLE pull_mirror.{table}(id bigint,note text) USING iceberg;SELECT lake_replication.assign_destination({self.subscription},{oid},{version},'pull_mirror.{table}'::regclass)")
        def pull():
            args=self.worker_args('lake-replication-receiver',LAKE_SLOT='bench_pull',LAKE_PUBLICATION='bench_pub',LAKE_MAX_TRANSACTIONS=str(self.transactions))
            if not self.concurrent:
                self.command(args);self.command(self.worker_args('lake-replication-publisher'))
                return
            transcript=tempfile.TemporaryFile(mode='w+')
            child=subprocess.Popen(args,text=True,stdout=transcript,stderr=subprocess.STDOUT)
            try:
                deadline=time.monotonic()+self.transactions*self.interval+90
                while child.poll() is None:
                    if self.drain_cancel.is_set():raise RuntimeError('benchmark pull drain cancelled')
                    self.command(self.worker_args('lake-replication-publisher'))
                    if time.monotonic()>deadline:raise RuntimeError('concurrent pull drain timed out')
                    time.sleep(.2)
                child.wait(timeout=15);transcript.seek(0);output=transcript.read()
                (self.output/'concurrent-pull-worker.log').write_text(output)
                if child.returncode:raise RuntimeError('pull receiver failed; see concurrent-pull-worker.log: '+output[-2000:])
                self.command(self.worker_args('lake-replication-publisher'))
            finally:
                if child.poll() is None:child.kill();child.wait(timeout=15)
                transcript.seek(0);(self.output/'concurrent-pull-worker.log').write_text(transcript.read())
                transcript.close()
        self.measure('logical_pull',pull)
        self.check('logical baseline produces the complete workload',self.dest('SELECT count(*) FROM pull_mirror.a'),str(self.transactions*self.rows))
        self.sql("SELECT pg_drop_replication_slot('bench_pull');DROP PUBLICATION bench_pub")

    def run_push(self):
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql(f'UPDATE lake_capture.streams SET transactions_per_read={self.capture_group},publication_interval_ms={self.capture_interval_ms}')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination('push_mirror')
        def push():
            if not self.concurrent:
                self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(self.transactions))
                self.importer(LAKE_PUSH_MAX_BATCHES=str(self.transactions))
                self.command(self.worker_args('lake-replication-publisher'))
            else:
                deadline=time.monotonic()+max(90,self.transactions*self.interval+90)
                while time.monotonic()<deadline:
                    if self.drain_cancel.is_set():raise RuntimeError('benchmark push drain cancelled')
                    self.importer(LAKE_PUSH_MAX_BATCHES='64')
                    self.command(self.worker_args('lake-replication-publisher'))
                    if self.dest(f'SELECT imported_sequence FROM lake_replication.push_bindings WHERE subscription_id={self.subscription}')==str(self.transactions):return
                    time.sleep(.1)
                raise RuntimeError('concurrent push drain timed out')
        self.measure('iceberg_push',push)
        self.evidence['source_files_before_maintenance']=json.loads(self.value('''
            SELECT coalesce(jsonb_object_agg(relation,profile),'{}'::jsonb) FROM (
                SELECT table_name::regclass::text AS relation,
                    jsonb_build_object('data_files',count(*),'physical_bytes',sum(file_size)) AS profile
                FROM lake_table.files WHERE content=0 AND table_name IN (
                    SELECT oid FROM pg_class WHERE relnamespace='lake_capture_log'::regnamespace)
                GROUP BY table_name) files'''))
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        trace=ObjectTrace(self,'source_maintenance') if self.trace_objects else None
        try:
            if trace:trace.start()
            before=self.resources();started=time.monotonic()
            self.command(self.capture_args(LAKE_CAPTURE_MAINTENANCE_ONLY='1'))
            after=self.resources()
            self.evidence['measurements']['source_maintenance']=dict(seconds=time.monotonic()-started,before=before,after=after,
                source_and_engine_cpu_seconds=sum(after[s]['cpu_usec']-before[s]['cpu_usec'] for s in ['source','pgduck'])/1e6)
            maintenance=self.evidence['measurements']['source_maintenance']
            maintenance['destination_and_engine_cpu_seconds']=sum(after[s]['cpu_usec']-before[s]['cpu_usec'] for s in ['destination','destination-pgduck'])/1e6
            maintenance['all_database_cpu_seconds']=maintenance['source_and_engine_cpu_seconds']+maintenance['destination_and_engine_cpu_seconds']
            if trace:self.evidence['measurements']['source_maintenance']['object_operations']=trace.finish()
        finally:
            if trace:trace.close()
        self.check('measured maintenance keeps the source log complete',self.value('SELECT count(*) FROM lake_capture_log.manifests'),str(self.transactions))
        profile=json.loads(self.value("SELECT jsonb_build_object('sample',min(path),'files',count(*),'physical_bytes',sum(file_size),'target_file_size_mb',(SELECT setting::int FROM pg_settings WHERE name='pg_lake_table.target_file_size_mb')) FROM lake_table.files WHERE content=0 AND table_name IN (SELECT oid FROM pg_class WHERE relnamespace='lake_capture_log'::regnamespace)"))
        sample=profile.pop('sample')
        query="SELECT DISTINCT compression FROM parquet_metadata('"+sample.replace("'","''")+"')"
        codec=self.command(self.compose+['exec','-T','--user','postgres','pgduck','psql','-XAt','-h','/var/run/pgduck','-p','5332','-U','postgres','-d','postgres','-c',query]).stdout.splitlines()
        profile['observed_sample_codecs']=sorted(set(codec));self.evidence['source_file_profile']=profile
        self.check('private logs use the pinned Snappy Parquet writer',profile['observed_sample_codecs'],['SNAPPY'])
        self.check('qualified approximate file target is recorded',profile['target_file_size_mb'],512)

if __name__=='__main__':sys.exit(BenchmarkProbe().run())

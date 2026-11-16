#!/usr/bin/env python3
"""Same-runtime comparison of coalesced direct and journal pipelines."""
from datetime import datetime,timezone
import hashlib
import json
import os
import statistics
import subprocess
import sys
import uuid
from benchmark import LocalBenchmark,ROOT
from wal_direct import enable_prelock


MODES=['journal_per_commit','direct_per_commit','journal_coalesced','direct_coalesced']


class DirectComparison(LocalBenchmark):
    def __init__(self,mode,workload,transactions,rows=4,interval=.25):
        coalesced=mode.endswith('coalesced')
        super().__init__(True,transactions,rows,interval,1000 if coalesced else 0,coalesced,None if mode.startswith('direct_') else 32)
        self.mode=mode;self.workload=workload;self.direct=mode.startswith('direct_');self.worker=None;self.worker_log=None;self.retention=[]
        self.evidence.update(scope=__doc__,mode=mode,workload_pattern=workload)
        self.evidence['methodology']='Fresh private volumes and identical cached PostgreSQL/core/pgduck/storage images in every mode. Both paths use the same text-valued events and current apply_change_run functions. One warmup transaction (one seed row per table) is published before measurement; it is excluded from publication counts and object traces. Direct worker persists across all measured transactions and uses bounded 1-second/32-transaction coalescing and uses a loopback superuser. Journal uses the existing managed supervisor, unprivileged worker, source schema validation and protocol streaming; a test-only next_batch wrapper caps publications at 1 for the matched control or 32 for the coalesced reference. Original journal admission, publication, generation fencing and recovery functions remain unchanged. This compares the implementations, not just the isolated cost of journal durability: worker lifecycle, privileges, metadata checks and protocol differ. Coalesced reference uses a 1-second interval and INSERT grouping. Two-second resource/progress observers are identical; CPU includes observer processes in database/engine containers, excludes host/controller/MinIO. Publication delay is a sampled upper bound. No competing local qualification, no cache clearing, no production capacity or significance claim.'

    def provenance(self):
        super().provenance()
        for path in (ROOT/'experiments/wal_direct').rglob('*'):
            if path.is_file():self.evidence['files_sha256'][str(path.relative_to(ROOT))]=hashlib.sha256(path.read_bytes()).hexdigest()

    def setup(self,mirror=True,seed=False):
        super().setup(mirror=False,seed=False)
        # Both schemas/control sets are installed in both modes; SQL bootstrap
        # happens before measurement and neither schema has a background service.
        self.sql((ROOT/'experiments/wal_direct/install.sql').read_text())
        enable_prelock(self)
        if not self.direct:
            self.sql((ROOT/'experiments/wal_direct/benchmark_batch_cap.sql').read_text())
            self.sql(f"ALTER DATABASE capture SET lake_direct.benchmark_batch_cap='{32 if self.mode=='journal_coalesced' else 1}'")

    def direct_settings(self):
        return {}

    def start_subscription(self):
        if not self.direct:return super().start_subscription()
        self.sql('''CREATE TABLE iceberg_mirror.a(id bigint NOT NULL,note text) USING iceberg;
            CREATE TABLE iceberg_mirror.b(id bigint NOT NULL,note text) USING iceberg''')
        self.sql("SELECT pg_create_logical_replication_slot('wal_direct_slot','pgoutput')")
        self.sql("""INSERT INTO lake_direct.stream(id,slot_name,publication,system_identifier,database_oid,timeline,published_lsn)
            SELECT 1,'wal_direct_slot','local_iceberg_pub',(pg_control_system()).system_identifier::text,
            (SELECT oid FROM pg_database WHERE datname=current_database()),(pg_control_checkpoint()).timeline_id,confirmed_flush_lsn
            FROM pg_replication_slots WHERE slot_name='wal_direct_slot';
            INSERT INTO lake_direct.mapping SELECT 'a'::regclass,'iceberg_mirror.a'::regclass,lake_direct.descriptor('a'),lake_direct.shape('a'),lake_direct.shape('iceberg_mirror.a');
            INSERT INTO lake_direct.mapping SELECT 'b'::regclass,'iceberg_mirror.b'::regclass,lake_direct.descriptor('b'),lake_direct.shape('b'),lake_direct.shape('iceberg_mirror.b');""")
        args=self.compose+['exec','-T']
        for key,value in dict(LAKE_DIRECT_EXPERIMENT='1',LAKE_DIRECT_ADDRESS='127.0.0.1:5432',LAKE_DIRECT_DATABASE='capture',
            LAKE_DIRECT_USER='postgres',LAKE_DIRECT_MAX_COMMITS=str(self.transactions+1),LAKE_DIRECT_PUBLISH_INTERVAL_MS=str(self.publish_interval_ms),LAKE_DIRECT_BATCH_MAX_TRANSACTIONS='32',**self.direct_settings()).items():args+=['-e',key+'='+value]
        self.worker_log=(self.output/'direct-worker.log').open('w')
        self.worker=subprocess.Popen(args+['postgres','lake-replication-direct'],stdout=self.worker_log,stderr=self.worker_log,text=True)

    def wait_initial_copy(self):
        if not self.direct:super().wait_initial_copy()
        self.sql("BEGIN;INSERT INTO a VALUES(0,'warmup');INSERT INTO b VALUES(0,'warmup');COMMIT")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','1')
        self.wait('SELECT count(*) FROM iceberg_mirror.b','1')
        if self.direct:self.wait('SELECT commits FROM lake_direct.stream','1')
        else:self.wait("SELECT count(*) FROM lake_replication.transactions WHERE state='published'",'1')
        self.check('warmup has exact source/mirror rows',self.value("SELECT (SELECT count(*) FROM a)=(SELECT count(*) FROM iceberg_mirror.a) AND (SELECT count(*) FROM b)=(SELECT count(*) FROM iceberg_mirror.b)"),'t')

    def frontier(self):
        if self.direct:
            if self.worker.poll() not in (None,0):raise RuntimeError('direct worker failed: '+(self.output/'direct-worker.log').read_text()[-2000:])
            frontier=int(self.value('SELECT commits-1 FROM lake_direct.stream'))
        else:frontier=super().frontier()
        retained=int(self.value("SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_insert_lsn(),restart_lsn)),0)::bigint FROM pg_replication_slots WHERE database=current_database()"))
        self.retention.append(retained)
        return frontier

    def transaction_sql(self,transaction):
        statement=super().transaction_sql(transaction)
        if self.workload=='mixed' and transaction:
            previous=(transaction-1)*self.rows+1
            mutations=''.join(f"UPDATE {table} SET note=repeat('u',256) WHERE id={previous};DELETE FROM {table} WHERE id={previous+1};" for table in 'ab')
            statement=statement.removesuffix('COMMIT')+mutations+'COMMIT'
        return statement

    def verify_source(self):
        count=1+self.transactions*self.rows-(self.transactions-1 if self.workload=='mixed' else 0)
        self.check('expected final source row counts',self.value("SELECT (SELECT count(*) FROM a)::text||':'||(SELECT count(*) FROM b)"),f'{count}:{count}')
        if self.workload=='mixed':self.check('all expected updates survive',self.value("SELECT (SELECT count(*) FROM a WHERE note=repeat('u',256))::text||':'||(SELECT count(*) FROM b WHERE note=repeat('u',256))"),f'{self.transactions-1}:{self.transactions-1}')

    def equal(self):
        for table in 'ab':
            source=self.value(f'SELECT row_to_json(t)::text FROM (TABLE public.{table} ORDER BY id) t')
            target=self.value(f'SELECT row_to_json(t)::text FROM (TABLE iceberg_mirror.{table} ORDER BY id) t')
            self.check('all final rows match exactly '+table,source==target,True)
            self.evidence.setdefault('final_rows_sha256',{})[table]=hashlib.sha256(source.encode()).hexdigest()

    def exercise(self):
        try:
            super().exercise()
            m=self.evidence['measurement']
            if self.direct:
                self.worker.wait(timeout=15)
                self.check('direct worker finishes every source transaction',self.worker.returncode,0)
                self.check('direct mode writes no bulk journal rows',self.value('SELECT (SELECT count(*) FROM lake_replication.transactions)+(SELECT count(*) FROM lake_replication.changes)'),'0')
                publications=int(self.value('SELECT publications-1 FROM lake_direct.stream'))
                import re
                from collections import Counter
                log=(self.output/'direct-worker.log').read_text()
                counts=[int(n) for n in re.findall(r'direct_published[^\n]*transactions=(\d+)',log)]
                counts += [1]*len(re.findall(r'direct_large_published ',log))
                if not counts and self.mode=='direct_per_commit':counts=[1]*int(self.value('SELECT commits FROM lake_direct.stream'))
                self.check('published source transaction count matches workload',sum(counts),self.transactions+1)
                self.check('published direct groups respect transaction cap',max(counts)<=32,True)
                distribution=dict(Counter(map(str,counts)))
            else:
                xids=','.join(map(str,self.application_xids))
                publications=int(self.value(f"SELECT count(DISTINCT batch_id) FROM lake_replication.transactions WHERE state='published' AND xid=ANY(ARRAY[{xids}]::bigint[])"))
                distribution=json.loads(self.value("SELECT coalesce(json_object_agg(n,c),'{}'::json) FROM (SELECT n,count(*) c FROM (SELECT count(*) n FROM lake_replication.transactions WHERE state='published' AND payload<>'[]'::jsonb GROUP BY batch_id) counts GROUP BY n) bins"))
                self.check('published journal application transaction count matches workload',sum(int(n)*int(c) for n,c in distribution.items()),self.transactions+1)
                m['bookkeeping_transactions_published_total']=int(self.value("SELECT count(*) FROM lake_replication.transactions WHERE state='published' AND payload='[]'::jsonb"))
                m['bookkeeping_only_publications_total']=int(self.value("SELECT count(*) FROM (SELECT batch_id FROM lake_replication.transactions WHERE state='published' AND batch_id IS NOT NULL GROUP BY batch_id HAVING bool_and(payload='[]'::jsonb)) t"))
                if self.mode=='journal_per_commit':
                    self.check('journal control publishes exactly one transaction per batch',self.value("SELECT max(n) FROM (SELECT count(*) n FROM lake_replication.transactions WHERE payload<>'[]'::jsonb GROUP BY batch_id) t"),'1')
                    self.check('journal publication count matches direct batching',publications,self.transactions)
            observations=[dict(services=m['before']),*m['samples'],dict(services=m['after'])]
            m['total_peak_sampled_pss_kib']=max(sum(v['process_pss_kib_sum'] for v in sample['services'].values()) for sample in observations)
            m['retained_wal_samples_bytes']=self.retention
            m['journal_relation_bytes_after']=int(self.value("SELECT pg_total_relation_size('lake_replication.transactions')+pg_total_relation_size('lake_replication.changes')"))
            m['direct_control_relation_bytes_after']=int(self.value("SELECT pg_total_relation_size('lake_direct.stream')+pg_total_relation_size('lake_direct.mapping')"))
            m.update(publications=publications,transactions_per_publication_distribution_including_warmup=distribution,
                peak_observed_retained_wal_bytes=max(self.retention),final_retained_wal_bytes=self.retention[-1],
                publication_latency_upper_bound_p95_seconds=m['observed_publication_latency_upper_bound_seconds']['p95'],
                observed_object_requests=m['object_operations']['categories']['local_mirror']['requests'])
        finally:
            if self.worker and self.worker.poll() is None:
                self.worker.terminate()
                try:self.worker.wait(timeout=5)
                except subprocess.TimeoutExpired:self.worker.kill();self.worker.wait()
            if self.worker_log:self.worker_log.close()


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repetitions',type=int,choices=range(1,4),default=2)
    parser.add_argument('--transactions',type=int,default=120)
    parser.add_argument('--workload',choices=['inserts','mixed'])
    parser.add_argument('--smoke',action='store_true')
    parser.add_argument('--modes',nargs='+',choices=MODES,default=['journal_coalesced','direct_coalesced'])
    args=parser.parse_args()
    if not 2<=args.transactions<=500:parser.error('transactions must be 2..500')
    output=ROOT/'.qualification'/('wal-direct-coalesced-comparison-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]);output.mkdir(parents=True)
    result=dict(status='running',scope=__doc__,settings=vars(args),runs=[],summary={},sleep_prevention=os.environ.get('WAL_DIRECT_SLEEP_INHIBITED','not asserted'))
    def save():
        path=output/'results.tmp';path.write_text(json.dumps(result,indent=2)+'\n');path.replace(output/'results.json')
    save()
    try:
        for workload in [args.workload] if args.workload else ['inserts','mixed']:
            for repeat in range(args.repetitions):
                for mode in args.modes if repeat%2==0 else list(reversed(args.modes)):
                    print(f'Measuring {workload}, repetition {repeat+1}, {mode}',flush=True)
                    probe=DirectComparison(mode,workload,8 if args.smoke else args.transactions);code=probe.run()
                    result['runs'].append(dict(workload=workload,repetition=repeat+1,mode=mode,evidence=str(probe.output.relative_to(ROOT)/'results.json'),fixture=probe.evidence));save()
                    if code:raise RuntimeError('comparison fixture failed: '+probe.project)
        metrics=['total_database_and_engine_cpu_seconds','wal_bytes','observed_object_requests','application_commit_p99_seconds','post_write_drain_seconds','publication_latency_upper_bound_p95_seconds','peak_observed_retained_wal_bytes','total_peak_sampled_pss_kib','database_bytes_delta','journal_relation_bytes_after','direct_control_relation_bytes_after','publications']
        for workload in sorted({r['workload'] for r in result['runs']}):
            runs=[r for r in result['runs'] if r['workload']==workload]
            if len({json.dumps(r['fixture']['final_rows_sha256'],sort_keys=True) for r in runs})!=1:raise RuntimeError('final row hashes differ across modes: '+workload)
            result['summary'][workload]={}
            for mode in args.modes:
                ms=[r['fixture']['measurement'] for r in runs if r['mode']==mode]
                result['summary'][workload][mode]={key:dict(median=statistics.median(m[key] for m in ms),minimum=min(m[key] for m in ms),maximum=max(m[key] for m in ms)) for key in metrics}
        result['status']='passed';save();return 0
    except Exception as error:
        result.update(status='error',error=str(error));save();print('ERROR: '+str(error),file=sys.stderr);return 1
    finally:print('Comparison evidence: '+str(output/'results.json'),flush=True)


if __name__=='__main__':sys.exit(main())

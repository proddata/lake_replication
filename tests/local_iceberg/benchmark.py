#!/usr/bin/env python3
"""Fresh-fixture local mirroring versus application-only resource comparison."""
from datetime import datetime,timezone
import json
from bisect import bisect_right
import os
from pathlib import Path
import statistics
import sys
import threading
import time
import uuid
from probe import LocalIcebergProbe,ROOT
from session import Session
from object_trace import ObjectTrace

RESOURCE_CODE="""from pathlib import Path
import json
cpu=dict(x.split() for x in Path('/sys/fs/cgroup/cpu.stat').read_text().splitlines())
rss=pss=0
for process in Path('/proc').glob('[0-9]*'):
 try:
  if not process.joinpath('comm').read_text().startswith(('postgres','pgduck','lake-replic')):continue
  fields={line.split(':',1)[0]:int(line.split()[1]) for line in process.joinpath('smaps_rollup').read_text().splitlines() if line.startswith(('Rss:','Pss:'))}
  rss+=fields.get('Rss',0);pss+=fields.get('Pss',0)
 except (FileNotFoundError,ProcessLookupError,PermissionError):pass
if not rss:raise RuntimeError('process memory is unavailable')
print(json.dumps(dict(cpu_usec=int(cpu['usage_usec']),memory_bytes=int(Path('/sys/fs/cgroup/memory.current').read_text()),process_rss_kib_sum=rss,process_pss_kib_sum=pss)))"""


def percentile(values,fraction):return sorted(values)[int(fraction*(len(values)-1))] if values else None


class LocalBenchmark(LocalIcebergProbe):
    def __init__(self,mirror,transactions,rows,interval,publish_interval_ms=0,group_inserts=False,journal_transactions=None):
        super().__init__();self.mirror=mirror;self.initial_rows=0;self.transactions=transactions;self.rows=rows;self.interval=interval;self.publish_interval_ms=publish_interval_ms;self.application_xids=[];self.group_inserts=group_inserts;self.journal_transactions=journal_transactions
        self.evidence.update(scope='single-instance local mirror resource measurement' if mirror else 'same-image application-only resource baseline',
            workload=dict(transactions=transactions,rows_per_table_per_transaction=rows,transaction_interval_seconds=interval,text_bytes=256,publish_interval_ms=publish_interval_ms,group_independent_inserts=group_inserts,journal_max_transactions=journal_transactions),
            unqualified=['production sizing','cold cache','long-duration stability','exact publication latency','host/controller/storage CPU'],
            methodology='Fresh private volumes per mode. Both modes install the same core extension, roles, publication, grants and one pgduck engine; only mirroring starts a subscription. One persistent application psql session. Two-second CPU/memory and publication observers run in both modes. Mirror progress uses published source XIDs in the local journal; there are no recurring Iceberg data scans. The bounded fresh-database experiment rejects source XID wrap. CPU is cgroup usage including container observer overhead; host/controller/storage CPU is excluded. RSS double counts shared pages; PSS apportions them. Memory samples include caches. Publication latency is an observed upper bound with sampling and query delay, not exact commit-to-Iceberg latency. All S3 activity under the fixture iceberg prefix is final mirror output; the local trace labels that prefix local_mirror. No intermediate capture logs exist. No global cache clearing or production acceptance threshold.')

    def resources(self):
        return {service:json.loads(self.command(self.compose+['exec','-T','--user','postgres',service,'python3','-c',RESOURCE_CODE]).stdout) for service in ['postgres','pgduck']}

    def database(self):
        return json.loads(self.value("SELECT json_build_object('wal_lsn',pg_current_wal_insert_lsn(),'database_bytes',pg_database_size(current_database()))"))

    def frontier(self):
        if self.mirror:
            published=int(self.value("SELECT coalesce(max(xid),0) FROM lake_replication.transactions WHERE state='published'"))
            return bisect_right(tuple(self.application_xids),published)
        return int(self.value('SELECT least(coalesce((SELECT max(id) FROM a),0),coalesce((SELECT max(id) FROM b),0))'))//self.rows

    def application_sql_args(self):
        return self.sql_args()

    def transaction_sql(self,transaction):
        low=transaction*self.rows+1;high=low+self.rows-1
        return f"BEGIN;SELECT pg_current_xact_id()::text;INSERT INTO a SELECT i,repeat('x',256) FROM generate_series({low},{high}) i;INSERT INTO b SELECT i,repeat('y',256) FROM generate_series({low},{high}) i;COMMIT"

    def verify_source(self):
        self.check('both source heaps contain every application row',self.value('SELECT (SELECT count(*) FROM a)::text||\':\'||(SELECT count(*) FROM b)'),f'{self.transactions*self.rows}:{self.transactions*self.rows}')

    def exercise(self):
        self.setup(mirror=False,seed=False)
        if self.publish_interval_ms:self.sql(f'ALTER DATABASE capture SET lake_replication.publish_interval_ms={self.publish_interval_ms}')
        if self.group_inserts:self.sql('ALTER DATABASE capture SET lake_replication.group_independent_inserts=on')
        if self.mirror:
            self.start_subscription()
            self.wait('SELECT count(*) FROM iceberg_mirror.a',str(self.initial_rows))
            self.wait('SELECT count(*) FROM iceberg_mirror.b',str(self.initial_rows))
        if self.mirror:
            self.wait_initial_copy()
            if self.journal_transactions:self.sql(f'UPDATE lake_replication.journal_limits SET max_transactions={self.journal_transactions}')
        trace=ObjectTrace(self,'local' if self.mirror else 'baseline');application=None;sampler=None;stop=threading.Event()
        samples=[];errors=[];commits=[];latencies=[];started=None
        def sample():
            while not stop.wait(2):
                try:
                    services=self.resources();frontier=self.frontier()
                    samples.append(dict(elapsed_seconds=time.monotonic()-started,services=services,published_application_commits=frontier,observed_application_commits=len(self.application_xids)))
                except Exception as error:errors.append(str(error));return
        self.evidence['measurement']=dict(status='running',samples=samples)
        try:
            trace.start();application=Session(self.application_sql_args());before_db=self.database();before=self.resources();started=time.monotonic()
            sampler=threading.Thread(target=sample);sampler.start()
            for transaction in range(self.transactions):
                if errors:raise RuntimeError('resource observer failed: '+errors[0])
                begin=time.monotonic()
                response=application.query(self.transaction_sql(transaction))
                xid=int(response[0])
                if xid>4294967295 or (self.application_xids and xid<=self.application_xids[-1]):raise RuntimeError('benchmark source XID wrap is outside the bounded workload')
                self.application_xids.append(xid)
                latencies.append(time.monotonic()-begin);commits.append(time.monotonic()-started)
                if self.interval:time.sleep(max(0,started+(transaction+1)*self.interval-time.monotonic()))
            writes_finished=time.monotonic()-started
            deadline=time.monotonic()+90
            while self.frontier()!=self.transactions:
                if errors:raise RuntimeError('resource observer failed: '+errors[0])
                if time.monotonic()>deadline:raise RuntimeError('mirror failed to drain within its bounded timeout')
                time.sleep(.2)
            published_at=time.monotonic()-started
            stop.set();sampler.join(timeout=20)
            if sampler.is_alive():raise RuntimeError('resource sampler did not stop')
            if errors:raise RuntimeError(errors[0])
            after=self.resources();after_db=self.database();operations=trace.finish()
            if 'source_capture' in operations['categories']:operations['categories']['local_mirror']=operations['categories'].pop('source_capture')
            observations=sorted([(s['elapsed_seconds'],s['published_application_commits']) for s in samples]+[(published_at,self.transactions)])
            lag=[max(0,next(observed for observed,count in observations if count>=i+1)-committed) for i,committed in enumerate(commits)]
            cpu={service:(after[service]['cpu_usec']-before[service]['cpu_usec'])/1e6 for service in before}
            record=dict(status='passed',before=before,after=after,samples=samples,application_commits=self.transactions,
                application_commit_median_seconds=statistics.median(latencies),application_commit_p95_seconds=percentile(latencies,.95),
                application_commit_p99_seconds=percentile(latencies,.99),write_phase_seconds=writes_finished,end_to_end_seconds=published_at,
                cpu_seconds=cpu,total_database_and_engine_cpu_seconds=sum(cpu.values()),
                database_bytes_delta=after_db['database_bytes']-before_db['database_bytes'],
                wal_bytes=int(self.value(f"SELECT pg_wal_lsn_diff('{after_db['wal_lsn']}','{before_db['wal_lsn']}')")),
                object_operations=operations,
                application_transactions_per_second=self.transactions/writes_finished,
                publication_transactions_per_second=self.transactions/published_at,
                post_write_drain_seconds=max(0,published_at-writes_finished),
                maximum_observed_backlog_transactions=max([max(0,s['observed_application_commits']-s['published_application_commits']) for s in samples]+[0]))
            if self.mirror:record['observed_publication_latency_upper_bound_seconds']=dict(median=statistics.median(lag),p95=percentile(lag,.95),maximum=max(lag),sample_interval_seconds=2)
            record['sampled_peak_pss_kib']={service:max([before[service]['process_pss_kib_sum'],after[service]['process_pss_kib_sum']]+[s['services'][service]['process_pss_kib_sum'] for s in samples]) for service in before}
            self.evidence['measurement']=record
            self.verify_source()
            if self.mirror:
                self.equal()
                self.check('trace observes final Iceberg writes',operations['categories'].get('local_mirror',{}).get('api:s3.PutObject',0)>0,True)
            else:self.check('application-only baseline writes no Iceberg objects',operations['categories'].get('local_mirror',{}).get('api:s3.PutObject',0),0)
        except Exception as error:
            self.evidence['measurement'].update(status='error',error=str(error),application_commits=len(commits))
            diagnostic=self.sql("SELECT json_build_object('heap_a_rows',(SELECT count(*) FROM a),'heap_b_rows',(SELECT count(*) FROM b),'journal_states',(SELECT jsonb_object_agg(state,n) FROM (SELECT state,count(*) AS n FROM lake_replication.transactions GROUP BY state) t),'slots',(SELECT jsonb_agg(jsonb_build_object('wal_status',wal_status,'retained_wal_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn))) FROM pg_replication_slots WHERE database=current_database()))",required=False)
            self.evidence['measurement']['failure_diagnostic']=diagnostic.stdout.strip() if diagnostic.returncode==0 else diagnostic.stderr[-1000:]
            raise
        finally:
            stop.set()
            if sampler:sampler.join(timeout=20)
            if application:application.close()
            trace.close()


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--transactions',type=int,default=120);parser.add_argument('--rows',type=int,default=10)
    parser.add_argument('--group-inserts',action='store_true');parser.add_argument('--journal-transactions',type=int);parser.add_argument('--publish-interval-ms',type=int,default=0);parser.add_argument('--interval',type=float,default=.25);parser.add_argument('--order',choices=['baseline_first','mirror_first'],default='baseline_first')
    args=parser.parse_args()
    if not(1<=args.transactions<=10000 and 1<=args.rows<=1000 and args.transactions*args.rows<=1000000 and 0<=args.interval<=5 and 0<=args.publish_interval_ms<=60000):parser.error('workload exceeds fixture bounds (10,000 transactions, 1,000 rows/transaction, 1,000,000 rows/table)')
    if args.journal_transactions is not None and not 1<=args.journal_transactions<=128:parser.error('journal transaction bound must be 1..128')
    output=ROOT/'.qualification'/('local-iceberg-benchmark-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]);output.mkdir(parents=True)
    result=dict(scope='fresh-fixture single-instance local mirroring versus application-only baseline',settings=vars(args),runs=[],status='running')
    try:
        for mirror in ([False,True] if args.order=='baseline_first' else [True,False]):
            probe=LocalBenchmark(mirror,args.transactions,args.rows,args.interval,args.publish_interval_ms,args.group_inserts,args.journal_transactions);code=probe.run()
            result['runs'].append(dict(mode='local_mirror' if mirror else 'application_only',evidence=str((probe.output/'results.json').relative_to(ROOT)),passed=code==0,measurement=probe.evidence.get('measurement',{})))
            if code:raise RuntimeError('benchmark fixture failed: '+probe.project)
        result['status']='passed';return 0
    except Exception as error:
        result.update(status='error',error=str(error));print('ERROR: '+str(error),file=sys.stderr);return 1
    finally:
        (output/'results.json').write_text(json.dumps(result,indent=2)+'\n');print('Benchmark evidence: '+str(output/'results.json'),flush=True)


if __name__=='__main__':sys.exit(main())

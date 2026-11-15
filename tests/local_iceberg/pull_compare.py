#!/usr/bin/env python3
"""Counterbalanced local mirroring versus separate-instance logical pull."""
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import statistics
import sys
import uuid
from benchmark import LocalBenchmark,RESOURCE_CODE,ROOT


class ComparisonProbe(LocalBenchmark):
    def __init__(self,remote,workload,transactions,rows,interval):
        super().__init__(True,transactions,rows,interval,1000,True,32)
        self.remote=remote;self.workload=workload;self.db_samples=[]
        if remote:self.compose+=['-f',str(ROOT/'deploy/local/remote-pull.compose.yml')]
        self.evidence.update(scope='remote logical pull' if remote else 'same-instance loopback logical pull')
        self.evidence['workload']['mutation_pattern']=workload
        self.evidence['methodology']+=' Comparison uses identical runtime image, 1-second coalescing, INSERT grouping enabled, journal count 32 and default byte limit. Remote source is plain PostgreSQL from the same image, with no replication extension or engine loaded. Destination retains unused empty source-fixture heaps; these are not selected by its receiver. One engine in either topology. All containers run on this host; no WAN simulation. Remote has an additional resource-sampling call; observer CPU is included. Final comparisons are outside the measured interval.'

    def application_sql_args(self):
        args=self.sql_args()
        if self.remote:args[args.index('exec')+2]='remote-source'
        return args

    def source_value(self,query):
        return self.command(self.application_sql_args(),input=query).stdout.strip()

    def setup(self,mirror=True,seed=False):
        super().setup(mirror=False,seed=False)
        if self.remote:
            self.source_value('''CREATE ROLE local_source LOGIN REPLICATION;
                CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
                CREATE PUBLICATION local_iceberg_pub FOR TABLE a,b;GRANT SELECT ON a,b TO local_source;''')
            self.check('remote source loads no replication or Iceberg extensions',self.source_value("SELECT count(*) FROM pg_extension WHERE extname IN ('lake_replication','pg_lake')"),'0')
        self.check('application and publisher use identical PostgreSQL versions',self.source_value('SELECT version()'),self.value('SELECT version()'))

    def start_subscription(self):
        host='remote-source' if self.remote else '127.0.0.1'
        self.sql(f"""CREATE SUBSCRIPTION local_iceberg CONNECTION
            'host={host} port=5432 dbname=capture user=local_source sslmode=disable'
            PUBLICATION local_iceberg_pub WITH(lake_target='iceberg',lake_target_schema='iceberg_mirror',
            lake_credential_file='/tmp/local-iceberg.env')""")

    def resources(self):
        values=super().resources()
        if self.remote:
            values['remote-source']=json.loads(self.command(self.compose+['exec','-T','--user','postgres','remote-source','python3','-c',RESOURCE_CODE]).stdout)
        return values

    def database(self):
        query="SELECT json_build_object('wal_lsn',pg_current_wal_insert_lsn(),'database_bytes',pg_database_size(current_database()))"
        source=json.loads(self.source_value(query));destination=json.loads(self.value(query))
        self.db_samples.append(dict(application=source,publisher=destination))
        return source

    def transaction_sql(self,transaction):
        statement=super().transaction_sql(transaction)
        if self.workload=='mixed' and transaction:
            previous=(transaction-1)*self.rows+1
            mutations=''.join(f"UPDATE {table} SET note=repeat('u',256) WHERE id={previous};DELETE FROM {table} WHERE id={previous+1};" for table in 'ab')
            statement=statement.removesuffix('COMMIT')+mutations+'COMMIT'
        return statement

    def verify_source(self):
        rows=self.transactions*self.rows-(self.transactions-1 if self.workload=='mixed' else 0)
        self.check('both source heaps have the expected final row counts',self.source_value("SELECT (SELECT count(*) FROM a)::text||':'||(SELECT count(*) FROM b)"),f'{rows}:{rows}')
        if self.workload=='mixed':
            self.check('both source heaps retain every expected update',self.source_value("SELECT (SELECT count(*) FROM a WHERE note=repeat('u',256))::text||':'||(SELECT count(*) FROM b WHERE note=repeat('u',256))"),f'{self.transactions-1}:{self.transactions-1}')

    def equal(self):
        for table in 'ab':
            source=self.source_value(f'SELECT row_to_json(t)::text FROM (SELECT id,note FROM public.{table} ORDER BY id) t')
            target=self.value(f'SELECT row_to_json(t)::text FROM (SELECT id,note FROM iceberg_mirror.{table} ORDER BY id) t')
            self.check('mirror exactly matches application heap '+table,source==target,True)
            self.evidence.setdefault('final_rows_sha256',{})[table]=hashlib.sha256(source.encode()).hexdigest()

    def exercise(self):
        super().exercise()
        m=self.evidence['measurement'];cpu=m['cpu_seconds'];app='remote-source' if self.remote else 'postgres'
        m['application_postgres_cpu_seconds']=cpu[app]
        m['application_host_database_and_engine_cpu_seconds']=cpu[app]+(cpu['pgduck'] if not self.remote else 0)
        def lsn(value):
            high,low=value.split('/');return (int(high,16)<<32)+int(low,16)
        m['wal_bytes_by_database']={name:lsn(self.db_samples[-1][name]['wal_lsn'])-lsn(self.db_samples[0][name]['wal_lsn']) for name in ['application','publisher']}
        # Local roles refer to one physical database; do not double-count it.
        m['total_wal_bytes']=sum(m['wal_bytes_by_database'].values()) if self.remote else m['wal_bytes_by_database']['application']
        m['application_postgres_peak_pss_kib']=m['sampled_peak_pss_kib'][app]
        m['application_host_peak_sampled_pss_kib']=max(sum(s['services'][service]['process_pss_kib_sum'] for service in ([app] if self.remote else ['postgres','pgduck'])) for s in [dict(services=m['before']),*m['samples'],dict(services=m['after'])])
        m['total_peak_sampled_pss_kib']=max(sum(v['process_pss_kib_sum'] for v in s['services'].values()) for s in [dict(services=m['before']),*m['samples'],dict(services=m['after'])])
        m['publication_latency_upper_bound_p95_seconds']=m['observed_publication_latency_upper_bound_seconds']['p95']
        m['observed_object_requests']=m['object_operations']['categories']['local_mirror']['requests']
        self.check('matching journal transaction and payload limits',self.value("SELECT max_transactions::text||':'||max_payload_bytes FROM lake_replication.journal_limits"),'32:67108864')


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repetitions',type=int,default=2,choices=range(1,5))
    parser.add_argument('--workload',choices=['inserts','mixed'])
    parser.add_argument('--smoke',action='store_true')
    args=parser.parse_args()
    output=ROOT/'.qualification'/('local-pull-comparison-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]);output.mkdir(parents=True)
    result=dict(scope=__doc__,settings=vars(args),status='running',runs=[],summary={},
        methodology='Sequential fresh private volumes, two modes per repetition, order reverses each repetition. Identical PG/runtime image, publisher, engine, storage and batching configuration. INSERT case: 1,000 transactions at 100 offered tx/s, four rows/table. Mixed case: 120 transactions at four offered tx/s, four INSERTs/table plus one UPDATE and one DELETE/table targeting the preceding transaction. Mixed source order prevents pure INSERT regrouping. No application-only baseline here: compares replication topologies directly. CPU includes container observers, excludes controller/MinIO/host. Memory is sampled process PSS, not a reservation or hard bound. Publication delay is an upper bound including two-second sampling/query delay. Fresh volumes do not clear global caches; all containers share local host resources/network. Descriptive repeat ranges, no statistical significance or production capacity claim.')
    def save():
        temporary=output/'results.tmp';temporary.write_text(json.dumps(result,indent=2)+'\n');temporary.replace(output/'results.json')
    save()
    try:
        for workload in [args.workload] if args.workload else ['inserts','mixed']:
            for repeat in range(args.repetitions):
                for remote in ([False,True] if repeat%2==0 else [True,False]):
                    mode='remote_pull' if remote else 'same_instance'
                    print(f'Measuring {workload}, repetition {repeat+1}, {mode}',flush=True)
                    transactions,interval=(12,.1) if args.smoke else ((1000,.01) if workload=='inserts' else (120,.25))
                    probe=ComparisonProbe(remote,workload,transactions,4,interval);code=probe.run()
                    result['runs'].append(dict(workload=workload,repetition=repeat+1,mode=mode,passed=code==0,evidence=str(probe.output.relative_to(ROOT)/'results.json'),fixture=probe.evidence));save()
                    if code:raise RuntimeError(f'comparison fixture failed: {probe.project}')
        metrics=['application_postgres_cpu_seconds','application_host_database_and_engine_cpu_seconds','total_database_and_engine_cpu_seconds','application_commit_p99_seconds','post_write_drain_seconds','maximum_observed_backlog_transactions','application_host_peak_sampled_pss_kib','total_peak_sampled_pss_kib','total_wal_bytes','publication_latency_upper_bound_p95_seconds','observed_object_requests']
        for workload in sorted({r['workload'] for r in result['runs']}):
            digests={json.dumps(r['fixture']['final_rows_sha256'],sort_keys=True) for r in result['runs'] if r['workload']==workload}
            if len(digests)!=1:raise RuntimeError('final rows differ across topology/repetitions: '+workload)
            result['summary'][workload]={}
            for mode in ['same_instance','remote_pull']:
                measurements=[r['fixture']['measurement'] for r in result['runs'] if r['workload']==workload and r['mode']==mode]
                result['summary'][workload][mode]={key:dict(median=statistics.median(m[key] for m in measurements),minimum=min(m[key] for m in measurements),maximum=max(m[key] for m in measurements)) for key in metrics}
        result['status']='passed';save();return 0
    except Exception as error:
        result.update(status='error',error=str(error));save();print('ERROR: '+str(error),file=sys.stderr);return 1
    finally:print('Comparison evidence: '+str(output/'results.json'),flush=True)


if __name__=='__main__':sys.exit(main())

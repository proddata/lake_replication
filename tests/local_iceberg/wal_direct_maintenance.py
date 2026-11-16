#!/usr/bin/env python3
"""Bounded direct mixed load with the existing native autovacuum on a shorter interval."""
import argparse
import json
import re
import sys
import time
from wal_direct_sizing import SizingComparison

class DirectMaintenance(SizingComparison):
    def __init__(self,transactions=600,naptime=20,interval=.1):
        super().__init__('direct_coalesced',transactions,4,interval,256,'sustained','expanded')
        self.naptime=naptime;self.file_samples=[]
        self.evidence.update(scope=__doc__,native_autovacuum_naptime_seconds=naptime,offered_transactions_per_second=1/interval,file_samples=self.file_samples,
            methodology=self.evidence['methodology']+' This fixture configures native pg_lake autovacuum through private ALTER SYSTEM settings and reloads its existing worker. The release and deployment defaults are unchanged. File counts and active backend/wait states are sampled with progress and can miss brief peaks. Wait samples describe observed states, not CPU attribution. Maintenance CPU, WAL and S3 operations are inside measurement; source/mirror verification is afterward.')

    def setup(self,mirror=True,seed=False):
        super().setup(mirror,seed)
        self.evidence['autovacuum_default_seconds']=int(self.value("SELECT setting FROM pg_settings WHERE name='pg_lake_iceberg.autovacuum_naptime'"))
        self.sql(f"ALTER SYSTEM SET pg_lake_iceberg.autovacuum_naptime='{self.naptime}s'")
        self.sql("ALTER SYSTEM SET pg_lake_iceberg.log_autovacuum_min_duration=0")
        self.sql('SELECT pg_reload_conf()')
        self.wait("SELECT setting FROM pg_settings WHERE name='pg_lake_iceberg.autovacuum_naptime'",str(self.naptime))
        self.check('native autovacuum is enabled',self.value('SHOW pg_lake_iceberg.autovacuum'),'on')

    def frontier(self):
        value=super().frontier()
        diagnostic=json.loads(self.value("""SELECT json_build_object(
            'files',(SELECT coalesce(json_object_agg(table_name::text,n),'{}'::json) FROM
                (SELECT table_name,count(*) n FROM lake_table.files WHERE table_name IN
                ('iceberg_mirror.a'::regclass,'iceberg_mirror.b'::regclass) GROUP BY table_name) x),
            'activity',(SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_type',backend_type,
                'application_name',application_name,'state',state,'wait_event_type',wait_event_type,
                'wait_event',wait_event,'query_age_seconds',extract(epoch FROM clock_timestamp()-query_start),
                'blocking_pids',pg_blocking_pids(pid),'query',left(query,240))),'[]'::json)
                FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active'))"""))
        self.file_samples.append(dict(monotonic_seconds=time.monotonic(),published_source_transactions=value,observed_source_transactions=len(self.application_xids),**diagnostic))
        return value

    def exercise(self):
        try:
            super().exercise()
        finally:
            # Retain maintenance/retry evidence even when the drain deadline fails.
            try:self.collect_maintenance()
            except Exception as error:self.evidence['maintenance_diagnostic_error']=str(error)
        if 'maintenance_diagnostic_error' in self.evidence:
            raise RuntimeError(self.evidence['maintenance_diagnostic_error'])
        self.check('native autovacuum completes during the workload',bool(self.evidence['vacuum_events']),True)
        self.check('file counts drop while source publication continues',bool(self.evidence['observed_compaction_drops']),True)
        self.check('publication retries remain within the default bound',self.evidence['maximum_retries_for_one_publication']<=3,True)
        self.check('maintenance preserves all source publication receipts',self.value('SELECT commits FROM lake_direct.stream'),str(self.transactions+1))

    def collect_maintenance(self):
        worker_log=(self.output/'direct-worker.log').read_text()
        self.evidence['deadlock_retries']=worker_log.count('direct_retry sqlstate=40P01')
        self.evidence['maximum_retries_for_one_publication']=max([int(n) for n in re.findall(r'direct_retry sqlstate=40P01 retry=(\d+)',worker_log)]+[0])
        logs=self.command(self.compose+['logs','--no-color','postgres']).stdout
        vacuum=re.findall(r'Vacuuming iceberg table ([^ ]+) on database [^ ]+ took ([0-9.]+) s',logs)
        self.evidence['vacuum_events']=[dict(table=table,elapsed_seconds=float(elapsed)) for table,elapsed in vacuum]
        decreases=[]
        for previous,current in zip(self.file_samples,self.file_samples[1:]):
            if sum(current['files'].values())<sum(previous['files'].values()):decreases.append(dict(before=previous,after=current))
        self.evidence['observed_compaction_drops']=decreases

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--transactions',type=int,default=600)
    parser.add_argument('--naptime',type=int,default=20);parser.add_argument('--interval',type=float,default=.1);args=parser.parse_args()
    if not 300<=args.transactions<=10000 or not 5<=args.naptime<=60 or not .1<=args.interval<=.5:parser.error('bounded fixture requires 300..10000 transactions, 5..60 second naptime and .1.. .5 second interval')
    sys.exit(DirectMaintenance(args.transactions,args.naptime,args.interval).run())

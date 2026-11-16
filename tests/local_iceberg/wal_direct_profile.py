#!/usr/bin/env python3
"""Diagnostic fixed-batch publication timings, metadata growth and compaction intervention."""
from datetime import datetime, timezone
import argparse
import copy
import hashlib
import json
import re
import statistics
import sys
import time
from wal_direct_sizing import SizingComparison
from probe import ROOT,LocalIcebergProbe
from object_trace import ObjectTrace
from session import Session
from wal_direct import DirectProbe

LOADED=hashlib.sha256((ROOT/'tests/local_iceberg/wal_direct_profile.py').read_bytes()).hexdigest()

class DirectProfile(SizingComparison):
    def __init__(self,windows=32,batch=32,statements=False,native_catalog=False):
        super().__init__('direct_coalesced',(windows+8)*batch,4,0,256,'sustained','expanded')
        self.native_catalog=native_catalog
        self.statements_enabled=statements
        if statements:self.compose+=['-f',str(ROOT/'deploy/local/profile-statements.compose.yml')]
        self.windows=windows;self.batch=batch;self.next_transaction=0;self.trace=None
        self.evidence.update(scope=__doc__,diagnostic_only=True,statement_profile=statements,native_catalog_profile=native_catalog,growth_windows=windows,batch_source_transactions=batch,
            methodology='One evolving private fixture, fixed prequeued batches, fresh direct worker per batch. 512-MiB/one-CPU PostgreSQL and 2-GiB/one-CPU engine with 1536-MB query limit, swap disabled, INSERT grouping and 16-MiB row groups. Target autovacuum disabled to isolate file growth; native VACUUM is the explicit intervention. Split BEGIN/apply/COMMIT adds round trips; SQL wrappers write unlogged diagnostic rows. Apply-operation elapsed times are nested within worker apply time and must not be summed with it. Resource observers run outside worker wall timing but their CPU is included in cgroup deltas. Source queueing, observer/catalog reads and compaction are outside publication timings. One trace spans the fixture with excluded per-window completion witnesses. This diagnoses growth and intervention; it is not an offered-load capacity or isolated CPU-saving comparison.')
        self.evidence['windows']=[]

    def provenance(self):
        super().provenance()
        if self.evidence['files_sha256']['tests/local_iceberg/wal_direct_profile.py']!=LOADED:raise RuntimeError('loaded profile source changed')
        import os
        code="from pathlib import Path;import hashlib,json;p=Path('/usr/share/pg_lake/lake-direct-metadata-prototype.json');print(json.dumps(dict(libraries={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in [Path('/usr/lib/postgresql/18/lib/pg_lake_table.so'),Path('/usr/lib/postgresql/18/lib/pg_lake_iceberg.so')]},experimental_marker=json.loads(p.read_text()) if p.exists() else None,diagnostic_marker=json.loads(Path('/usr/share/pg_lake/lake-direct-catalog-profile.json').read_text()) if Path('/usr/share/pg_lake/lake-direct-catalog-profile.json').exists() else None)))"
        self.evidence['native_runtime']=json.loads(self.command(['docker','run','--rm','--entrypoint','python3',os.environ['PUSH_PG_LAKE_IMAGE'],'-c',code]).stdout)

    def bootstrap(self):
        self.setup(mirror=False,seed=False)
        self.sql("ALTER DATABASE capture SET track_functions='all'")
        if self.statements_enabled:self.sql('CREATE EXTENSION pg_stat_statements')
        self.sql('''CREATE TABLE iceberg_mirror.a(id bigint NOT NULL,note text) USING iceberg;
            CREATE TABLE iceberg_mirror.b(id bigint NOT NULL,note text) USING iceberg;
            ALTER FOREIGN TABLE iceberg_mirror.a OPTIONS(ADD autovacuum_enabled 'false');
            ALTER FOREIGN TABLE iceberg_mirror.b OPTIONS(ADD autovacuum_enabled 'false')''')
        self.sql("SELECT pg_create_logical_replication_slot('wal_direct_slot','pgoutput')")
        self.sql("""INSERT INTO lake_direct.stream(id,slot_name,publication,system_identifier,database_oid,timeline,published_lsn)
            SELECT 1,'wal_direct_slot','local_iceberg_pub',(pg_control_system()).system_identifier::text,
            (SELECT oid FROM pg_database WHERE datname=current_database()),(pg_control_checkpoint()).timeline_id,confirmed_flush_lsn
            FROM pg_replication_slots WHERE slot_name='wal_direct_slot';
            INSERT INTO lake_direct.mapping SELECT 'a'::regclass,'iceberg_mirror.a'::regclass,lake_direct.descriptor('a'),lake_direct.shape('a'),lake_direct.shape('iceberg_mirror.a');
            INSERT INTO lake_direct.mapping SELECT 'b'::regclass,'iceberg_mirror.b'::regclass,lake_direct.descriptor('b'),lake_direct.shape('b'),lake_direct.shape('iceberg_mirror.b');""")
        self.sql((ROOT/'experiments/wal_direct/profile.sql').read_text())
        if self.native_catalog:
            self.sql("""CREATE FUNCTION lake_direct.catalog_profile(integer) RETURNS text
              AS '$libdir/pg_lake_table','lake_direct_catalog_profile' LANGUAGE C STRICT;
              REVOKE ALL ON FUNCTION lake_direct.catalog_profile(integer) FROM PUBLIC;
              CREATE UNLOGGED TABLE lake_direct.native_profiles(boundary pg_lsn PRIMARY KEY,metrics jsonb);
              CREATE OR REPLACE FUNCTION lake_direct.publish(p_previous pg_lsn,p_commit pg_lsn,p_end pg_lsn,p_changes jsonb,p_transactions bigint DEFAULT 1)
              RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,lake_direct AS $$
              DECLARE result boolean;
              BEGIN
                PERFORM set_config('lake_direct.profile_boundary',p_end::text,true);
                PERFORM lake_direct.catalog_profile(1);
                result:=lake_direct.profile_original_publish(p_previous,p_commit,p_end,p_changes,p_transactions);
                INSERT INTO lake_direct.native_profiles VALUES(p_end,lake_direct.catalog_profile(0)::jsonb);
                RETURN result;
              END $$;""")

    def files(self):
        return json.loads(self.value("""SELECT json_build_object(
         'current_files',(SELECT coalesce(json_agg(x),'[]'::json) FROM (
           SELECT table_name::text,content,count(*) AS files,sum(row_count) AS physical_rows,
            sum(deleted_row_count) AS deleted_rows,sum(file_size) AS bytes
           FROM lake_table.files WHERE table_name IN ('iceberg_mirror.a'::regclass,'iceberg_mirror.b'::regclass)
           GROUP BY table_name,content ORDER BY table_name,content) x),
         'catalog_bytes',(SELECT json_object_agg(c.oid::regclass::text,pg_total_relation_size(c.oid))
          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname IN ('lake_table','lake_iceberg') AND c.relkind='r'),
         'file_catalog_stats',(SELECT row_to_json(x) FROM (SELECT n_live_tup,n_dead_tup,seq_scan,idx_scan
          FROM pg_stat_user_tables WHERE relid='lake_table.files'::regclass) x))"""))

    def functions(self):
        self.sql('SELECT pg_stat_clear_snapshot()')
        return json.loads(self.value("SELECT coalesce(json_object_agg(funcid::text,json_build_object('name',schemaname||'.'||funcname,'calls',calls,'total_ms',total_time,'self_ms',self_time)),'{}'::json) FROM pg_stat_user_functions"))

    def statements(self):
        if not self.statements_enabled:return {}
        return json.loads(self.value("""SELECT coalesce(json_object_agg(userid::text||':'||queryid::text,json_build_object(
          'query',left(query,2000),'calls',calls,'exec_ms',total_exec_time,'plan_ms',total_plan_time,
          'rows',rows,'shared_hits',shared_blks_hit,'shared_reads',shared_blks_read)),'{}'::json)
          FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
          AND NOT toplevel AND (query LIKE '%lake_table%' OR query LIKE '%lake_iceberg%')
          AND query NOT LIKE '%pg_stat_statements%'"""))

    def objects(self):
        self.trace.ended.clear();self.trace.witness('end')
        if not self.trace.ended.wait(10):raise RuntimeError('profile object trace completion witness missing')
        if self.trace.error:raise RuntimeError(self.trace.error)
        return copy.deepcopy(self.trace.counts)

    def save_windows(self):
        (self.output/'windows.json').write_text(json.dumps(self.evidence['windows'],indent=2)+'\n')

    def run_window(self,session,phase,number):
        for _ in range(self.batch):
            response=session.query(self.transaction_sql(self.next_transaction));self.application_xids.append(int(response[0]));self.next_transaction+=1
        before_files=self.files();before_functions=self.functions();before_statements=self.statements();before_objects=self.objects();before=self.resources()
        started=time.monotonic()
        result=self.command(DirectProbe.worker(self,LAKE_DIRECT_MAX_COMMITS=str(self.batch),LAKE_DIRECT_PUBLISH_INTERVAL_MS='60000',LAKE_DIRECT_BATCH_MAX_TRANSACTIONS=str(self.batch),LAKE_DIRECT_TRACE_PHASES='1'),timeout=180)
        elapsed=time.monotonic()-started;after=self.resources();after_objects=self.objects();after_functions=self.functions();after_statements=self.statements()
        (self.output/f'{phase}-{number:03d}.log').write_text(result.stdout+result.stderr)
        matches=re.findall(r'direct_phase end_lsn=(\S+) phase=(\w+) elapsed_us=(\d+)',result.stderr)
        if len(matches)!=3 or {p for _,p,_ in matches}!={'begin','apply','commit'}:raise RuntimeError('expected one traced publication per window')
        boundary=matches[0][0];phases={p:int(us)/1000 for _,p,us in matches}
        operations=json.loads(self.value(f"SELECT json_object_agg(operation,x) FROM (SELECT operation,count(*) calls,sum(change_rows) change_rows,sum(elapsed_ms) total_ms,max(elapsed_ms) maximum_ms FROM lake_direct.profile_calls WHERE boundary='{boundary}'::pg_lsn GROUP BY operation) x"))
        fn=[]
        for key,value in after_functions.items():
            previous=before_functions.get(key,dict(calls=0,total_ms=0,self_ms=0));calls=value['calls']-previous['calls']
            if calls:fn.append(dict(name=value['name'],calls=calls,total_ms=value['total_ms']-previous['total_ms'],self_ms=value['self_ms']-previous['self_ms']))
        catalog_statements=[]
        for key,value in after_statements.items():
            previous=before_statements.get(key,{field:0 for field in ['calls','exec_ms','plan_ms','rows','shared_hits','shared_reads']})
            delta={field:value[field]-previous[field] for field in ['calls','exec_ms','plan_ms','rows','shared_hits','shared_reads']}
            if delta['calls']:catalog_statements.append(dict(queryid=key,query=value['query'],**delta))
        objects={category:{key:value-before_objects.get(category,{}).get(key,0) for key,value in values.items()} for category,values in after_objects.items()}
        record=dict(phase=phase,number=number,source_transactions=self.batch,total_source_transactions=self.next_transaction,
            worker_wall_seconds=elapsed,worker_phases_ms=phases,operation_elapsed=operations,
            cpu_seconds={s:(after[s]['cpu_usec']-before[s]['cpu_usec'])/1e6 for s in before},resources_before=before,resources_after=after,
            files_before=before_files,files_after=self.files(),objects=objects,catalog_statements=sorted(catalog_statements,key=lambda q:q['exec_ms'],reverse=True),functions=sorted(fn,key=lambda f:f['self_ms'],reverse=True)[:20])
        if self.native_catalog:record['native_catalog']=json.loads(self.value(f"SELECT metrics FROM lake_direct.native_profiles WHERE boundary='{boundary}'::pg_lsn"))
        self.evidence['windows'].append(record);self.save_windows()
        if int(self.value('SELECT commits FROM lake_direct.stream'))!=self.next_transaction:raise RuntimeError('publication source count drifted')
        if int(self.value('SELECT publications FROM lake_direct.stream'))!=len(self.evidence['windows']):raise RuntimeError('more than one publication per fixed window')
        for s in before:
            if after[s]['memory_events']['oom_kill']!=before[s]['memory_events']['oom_kill']:raise RuntimeError('OOM during profile')
        print(f"Profile {phase} {number}: apply={phases['apply']:.1f}ms commit={phases['commit']:.1f}ms files={sum(f['files'] for f in record['files_after']['current_files'])}",flush=True)

    def explain_update(self):
        # Match apply_change_run's narrow typed-key UPDATE. Roll back its
        # target write; plans are diagnosis outside the measured windows.
        key=self.next_transaction*4
        old=json.dumps({'id':key});new=json.dumps({'id':key,'note':'u'*256})
        statement=f"""BEGIN;
        EXPLAIN(ANALYZE,BUFFERS,TIMING FALSE,FORMAT JSON)
        WITH source AS (
          SELECT old_tuple,new_tuple,ordinality FROM unnest(ARRAY['{old}']::jsonb[],ARRAY['{new}']::jsonb[])
          WITH ORDINALITY AS input(old_tuple,new_tuple,ordinality)
        ), applied AS (
          UPDATE iceberg_mirror.a AS destination
          SET id=(jsonb_populate_record(NULL::iceberg_mirror.a,source.new_tuple)).id,
              note=(jsonb_populate_record(NULL::iceberg_mirror.a,source.new_tuple)).note
          FROM source WHERE destination.id=(jsonb_populate_record(NULL::iceberg_mirror.a,source.old_tuple)).id
            AND destination.id=ANY(ARRAY[{key}]::bigint[])
          RETURNING source.ordinality
        ) SELECT count(*),count(DISTINCT ordinality) FROM applied;
        ROLLBACK"""
        result=self.command(self.sql_args(),input=statement,timeout=120)
        return json.loads(result.stdout)

    def exercise(self):
        self.bootstrap();session=Session(self.sql_args());self.trace=ObjectTrace(self,'direct-profile')
        try:
            self.trace.start()
            for number in range(self.windows):self.run_window(session,'growth',number)
            for number in range(4):self.run_window(session,'before_compaction',number)
            LocalIcebergProbe.equal(self)
            self.evidence['explain_before_compaction']=self.explain_update()
            before_state=self.value('SELECT published_lsn FROM lake_direct.stream');before_files=self.files();before_resources=self.resources();started=time.monotonic()
            for table in 'ab':self.command(self.sql_args(),input=f'VACUUM iceberg_mirror.{table}',timeout=180)
            self.evidence['compaction']=dict(elapsed_seconds=time.monotonic()-started,before=before_files,after=self.files(),resources_before=before_resources,resources_after=self.resources())
            self.evidence['explain_after_compaction']=self.explain_update()
            self.check('compaction preserves direct checkpoint',self.value('SELECT published_lsn FROM lake_direct.stream'),before_state)
            LocalIcebergProbe.equal(self)
            self.check('compaction reduces current mirror file count',sum(f['files'] for f in self.evidence['compaction']['after']['current_files'])<sum(f['files'] for f in before_files['current_files']),True)
            for number in range(4):self.run_window(session,'after_compaction',number)
            LocalIcebergProbe.equal(self);DirectProbe.journal_empty(self)
            self.check('every fixed batch has one apply and one commit phase',len(self.evidence['windows']),self.windows+8)
            self.check('profile covers INSERT/UPDATE/DELETE',all(op in self.evidence['windows'][-1]['operation_elapsed'] for op in ['insert','update','delete']),True)
            self.check('no-swap PostgreSQL cap is observed',self.evidence['windows'][-1]['resources_after']['postgres']['swap_max'],'0')
            if self.statements_enabled:
                self.evidence['statement_profile_info']=json.loads(self.value('SELECT row_to_json(i) FROM pg_stat_statements_info i'))
                self.check('statement profile retains entries without eviction',int(self.evidence['statement_profile_info']['dealloc']),0)
                self.check('nested catalog SQL is captured',bool(self.evidence['windows'][-1]['catalog_statements']),True)
            self.evidence['object_trace']=self.trace.finish();self.trace=None
            def stats(windows):
                return {name:statistics.median(w['worker_phases_ms'][name] for w in windows) for name in ['begin','apply','commit']}
            self.evidence['phase_medians_ms']={phase:stats([w for w in self.evidence['windows'] if w['phase']==phase]) for phase in ['growth','before_compaction','after_compaction']}
        finally:
            session.close()
            if self.trace:self.trace.close()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--windows',type=int,default=32)
    parser.add_argument('--statements',action='store_true')
    parser.add_argument('--native-catalog',action='store_true')
    args=parser.parse_args()
    if not 2<=args.windows<=48:parser.error('windows must be 2..48')
    if args.native_catalog and not args.statements:parser.error('--native-catalog requires --statements')
    sys.exit(DirectProfile(args.windows,statements=args.statements,native_catalog=args.native_catalog).run())

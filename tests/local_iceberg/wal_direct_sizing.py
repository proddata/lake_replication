#!/usr/bin/env python3
"""Bounded direct/journal load and transaction-size measurements under CPU/RAM caps."""
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from benchmark import RESOURCE_CODE, ROOT
from wal_direct_compare import DirectComparison

LOADED_FILES={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in [ROOT/'tests/local_iceberg'/name for name in ['wal_direct_sizing.py','wal_direct_compare.py','benchmark.py']]}

# Separate observers run as the owner of each process.
# Earlier postgres-user observers cannot read that worker's smaps_rollup.
RESOURCE_EXTRA = r'''
from pathlib import Path
import json
result=json.loads(BASE)
cgroup=Path('/sys/fs/cgroup')
result['memory_peak_bytes']=int((cgroup/'memory.peak').read_text())
result['memory_max']=(cgroup/'memory.max').read_text().strip()
result['swap_max']=(cgroup/'memory.swap.max').read_text().strip()
result['swap_current_bytes']=int((cgroup/'memory.swap.current').read_text())
result['cpu_max']=(cgroup/'cpu.max').read_text().strip()
result['memory_events']={k:int(v) for k,v in (line.split() for line in (cgroup/'memory.events').read_text().splitlines())}
result['memory_stat']={k:int(v) for k,v in (line.split() for line in (cgroup/'memory.stat').read_text().splitlines()) if k in ('anon','file','shmem','kernel')}
workers=[]
for p in Path('/proc').glob('[0-9]*'):
 try:
  if b'lake-replication-direct' not in p.joinpath('cmdline').read_bytes().split(b'\0')[0]:continue
  fields={l.split(':')[0]:int(l.split()[1]) for l in p.joinpath('status').read_text().splitlines() if l.startswith(('VmRSS:','VmHWM:'))}
  memory={l.split(':',1)[0]:int(l.split()[1]) for l in p.joinpath('smaps_rollup').read_text().splitlines() if l.startswith(('Rss:','Pss:'))}
  result['process_rss_kib_sum']+=memory.get('Rss',0);result['process_pss_kib_sum']+=memory.get('Pss',0)
  workers.append(dict(pid=int(p.name),**fields,**memory))
 except (FileNotFoundError,ProcessLookupError,PermissionError):pass
result['direct_workers']=workers
print(json.dumps(result))
'''

class SizingComparison(DirectComparison):
    def __init__(self,mode,transactions,rows,interval,text_bytes=256,phase='sustained',engine_budget='baseline'):
        super().__init__(mode,'inserts' if phase=='memory' else 'mixed',transactions,rows,interval)
        self.compose+=['-f',str(ROOT/'deploy/local/limits.compose.yml')]
        self.engine_budget=engine_budget
        if engine_budget=='expanded':self.compose+=['-f',str(ROOT/'deploy/local/sizing.compose.yml')]
        self.text_bytes=text_bytes;self.phase=phase;self.group_inserts=True;self.last_progress=0
        self.evidence['workload'].update(group_independent_inserts=True,text_bytes=text_bytes)
        self.evidence.update(scope=__doc__,phase=phase,text_bytes=text_bytes,engine_budget=engine_budget,
            unqualified=['production sizing','24-hour stability','hard process RSS admission','cold cache','exact publication latency','maximum capacity'])
        self.evidence['methodology']+=f' This cohort uses one CPU/512 MiB for PostgreSQL and one CPU/{2 if engine_budget=="expanded" else 1} GiB for pgduck ({1536 if engine_budget=="expanded" else 768}-MB engine query limit). Postgres-user observers read database/engine process memory; a separate root observer adds the root-owned direct worker without requiring ptrace capabilities. All modes run both observers. Worker exit diagnostics record its lifetime RSS peak and CPU, including warmup. memory.peak includes fixture startup and page cache; it is not process RSS. Sustained phase has paced mixed transactions; memory phase has one large INSERT transaction and no offered pacing. Memory payloads use per-row MD5 strings with varied blocks. Samples are every two seconds and can miss short peaks; cgroup lifetime peak and worker VmHWM supplement samples. Expanded profile disables swap; both RAM and swap limits are recorded. Logical decoding work memory is 64 KiB in both modes, so larger transactions stream without the immediate debug setting. No concurrent qualification runs.'

    def provenance(self):
        super().provenance()
        for name,digest in LOADED_FILES.items():
            if self.evidence['files_sha256'][name]!=digest:raise RuntimeError('loaded benchmark source changed: '+name)

    def frontier(self):
        value=super().frontier()
        if time.monotonic()-self.last_progress>=20:
            progress=dict(mode=self.mode,published=value,observed_application_commits=len(self.application_xids),retained_wal_bytes=self.retention[-1])
            (self.output/'progress.json').write_text(json.dumps(progress)+'\n')
            print('Progress: '+json.dumps(progress),flush=True);self.last_progress=time.monotonic()
        return value

    def setup(self,mirror=True,seed=False):
        super().setup(mirror,seed)
        self.sql("ALTER DATABASE capture SET pg_lake_table.target_row_group_size_mb=16;ALTER DATABASE capture SET logical_decoding_work_mem='64kB'")
        self.evidence['writer_configuration']=dict(target_row_group_size_mb=16,query_threads=1,query_memory_limit=('1536MB' if self.engine_budget=='expanded' else '768MB'),group_independent_inserts=True,logical_decoding_work_mem='64kB')

    def direct_settings(self):
        if self.mode=='direct_streamed_per_commit':return dict(LAKE_DIRECT_STREAMING='1',LAKE_DIRECT_CHUNK_BYTES='1048576',LAKE_DIRECT_SPILL_BYTES='268435456')
        return {}

    def resources(self):
        result={}
        for service in ['postgres','pgduck']:
            base=self.command(self.compose+['exec','-T','--user','postgres',service,'python3','-c',RESOURCE_CODE]).stdout
            code='BASE='+repr(base)+'\n'+RESOURCE_EXTRA
            result[service]=json.loads(self.command(self.compose+['exec','-T','--user','root',service,'python3','-c',code]).stdout)
        return result

    def transaction_sql(self,transaction):
        if self.phase!='memory':return super().transaction_sql(transaction)
        low=transaction*self.rows+1;high=low+self.rows-1;blocks=self.text_bytes//32
        values=f"SELECT i,(SELECT string_agg(md5(i::text||':'||j::text),'' ORDER BY j) FROM generate_series(1,{blocks}) j) FROM generate_series({low},{high}) i"
        return 'BEGIN;SELECT pg_current_xact_id()::text;'+''.join(f'INSERT INTO {table} {values};' for table in 'ab')+'COMMIT'

    def exercise(self):
        try:super().exercise()
        except Exception:
            try:
                ids=self.command(self.compose+['ps','--all','-q']).stdout.split()
                self.evidence['failure_container_states']=json.loads(self.command(['docker','inspect',*ids]).stdout) if ids else []
            except Exception as error:self.evidence['failure_container_states_error']=str(error)
            try:self.evidence['failure_resources']=self.resources()
            except Exception as error:self.evidence['failure_resources_error']=str(error)
            raise
        m=self.evidence['measurement']
        snapshots=[m['before'],*(sample['services'] for sample in m['samples']),m['after']]
        m['cgroup_lifetime_peak_bytes']={service:max(s[service]['memory_peak_bytes'] for s in snapshots) for service in ['postgres','pgduck']}
        m['worker_peak_observed_hwm_kib']=max([w.get('VmHWM',0) for s in snapshots for w in s['postgres']['direct_workers']]+[0])
        for service in ['postgres','pgduck']:
            self.check(service+' uses the requested memory cap',m['after'][service]['memory_max'],str((512 if service=='postgres' else (2048 if self.engine_budget=='expanded' else 1024))*1024*1024))
            if self.engine_budget=='expanded':self.check(service+' has swap disabled',m['after'][service]['swap_max'],'0')
            quota,period=map(int,m['after'][service]['cpu_max'].split())
            self.check(service+' has one CPU worth of quota',quota,period)
            self.check(service+' has no OOM kills during measurement',m['after'][service]['memory_events']['oom_kill']-m['before'][service]['memory_events']['oom_kill'],0)
        if self.direct:
            log=(self.output/'direct-worker.log').read_text()
            import re
            usage=re.search(r'direct_resources lifetime_peak_rss_kib=(\d+) lifetime_cpu_seconds=([0-9.]+)',log)
            self.check('direct worker supplies lifetime resource diagnostics',usage is not None,True)
            self.check('memory observer sees direct worker before measurement',len(m['before']['postgres']['direct_workers']),1)
            m.update(worker_lifetime_peak_rss_kib=int(usage[1]),worker_lifetime_cpu_seconds=float(usage[2]))
            m['spill_peak_observed_bytes']=max([int(n) for n in re.findall(r'direct_spilled[^\n]*bytes=(\d+)',log)]+[0])
        if self.phase=='memory':self.check('large transaction source row counts',self.value("SELECT (SELECT count(*) FROM a)::text||':'||(SELECT count(*) FROM b)"),f'{self.rows+1}:{self.rows+1}')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase',choices=['sustained','memory','smoke'],default='sustained')
    parser.add_argument('--transactions',type=int,default=3000)
    parser.add_argument('--engine-budget',choices=['baseline','expanded'],default='baseline')
    parser.add_argument('--continue-on-failure',action='store_true')
    parser.add_argument('--modes',nargs='+',choices=['journal_coalesced','direct_coalesced','journal_per_commit','direct_streamed_per_commit'])
    args=parser.parse_args()
    if not 2<=args.transactions<=10000:parser.error('transactions must be 2..10000')
    output=ROOT/'.qualification'/('wal-direct-sizing-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]);output.mkdir()
    result=dict(status='running',scope=__doc__,settings=vars(args),runs=[])
    def save():
        temp=output/'results.tmp';temp.write_text(json.dumps(result,indent=2)+'\n');temp.replace(output/'results.json')
    save()
    try:
        if args.phase=='memory':cases=[(mode,1,rows,0,1024,'memory') for rows in [1000,5000,20000] for mode in (['journal_per_commit','direct_streamed_per_commit'] if rows!=5000 else ['direct_streamed_per_commit','journal_per_commit'])]
        elif args.phase=='smoke':cases=[(mode,8,4,.1,256,'sustained') for mode in ['journal_coalesced','direct_coalesced','journal_per_commit','direct_streamed_per_commit']]
        else:cases=[(mode,args.transactions,4,.1,256,'sustained') for mode in ['journal_coalesced','direct_coalesced']]
        if args.modes:cases=[case for case in cases if case[0] in args.modes]
        if not cases:raise RuntimeError('no requested modes belong to this phase')
        for mode,transactions,rows,interval,text_bytes,phase in cases:
            print(f'Measuring {mode}: {transactions} transactions, {rows} rows/table/transaction, phase={phase}',flush=True)
            probe=SizingComparison(mode,transactions,rows,interval,text_bytes,phase,args.engine_budget);code=probe.run()
            result['runs'].append(dict(mode=mode,rows=rows,phase=phase,evidence=str(probe.output/'results.json'),fixture=probe.evidence));save()
            if code and not args.continue_on_failure:raise RuntimeError('sizing fixture failed: '+probe.project)
        for rows in {r['rows'] for r in result['runs']}:
            group=[r for r in result['runs'] if r['rows']==rows and r['fixture'].get('status')=='passed']
            if len(group)>1 and len({json.dumps(r['fixture']['final_rows_sha256'],sort_keys=True) for r in group})!=1:raise RuntimeError('final row hashes differ across modes')
        failures=[r['evidence'] for r in result['runs'] if r['fixture'].get('status')!='passed']
        result.update(status='partial' if failures else 'passed',failed_fixtures=failures);save();return 1 if failures else 0
    except Exception as error:
        result.update(status='error',error=str(error));save();print('ERROR: '+str(error),file=sys.stderr);return 1
    finally:print('Sizing evidence: '+str(output/'results.json'),flush=True)

if __name__=='__main__':sys.exit(main())

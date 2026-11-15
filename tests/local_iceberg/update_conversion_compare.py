#!/usr/bin/env python3
"""Counterbalanced full-pipeline comparison of wide UPDATE row conversion."""
from datetime import datetime,timezone
import hashlib
import json
import statistics
import sys
import uuid
from pull_compare import ComparisonProbe
from probe import ROOT


class UpdateConversionBenchmark(ComparisonProbe):
    def __init__(self,candidate,columns=48,rows=32,transactions=120):
        super().__init__(False,'updates',transactions,rows,.25)
        self.candidate=candidate;self.columns=columns;self.initial_rows=1000
        self.fields=['note']+[f'c{i}' for i in range(columns-2)]
        self.evidence.update(scope=__doc__,sql_overlay='migrations/0034_update_row_conversion.sql' if candidate else None,
            columns_per_table=columns,initial_rows_per_table=1000)

    def setup(self,mirror=True,seed=False):
        super().setup(mirror=False,seed=False)
        if self.candidate:self.sql((ROOT/'migrations/0034_update_row_conversion.sql').read_text())
        for table in 'ab':
            if self.columns>2:self.sql(f'ALTER TABLE {table} '+','.join(f'ADD COLUMN {c} text' for c in self.fields[1:]))
            self.sql(f'INSERT INTO {table} SELECT i,'+','.join("repeat('o',64)" for c in self.fields)+' FROM generate_series(1,1000) i')

    def transaction_sql(self,transaction):
        low=(transaction%(1000//self.rows))*self.rows+1;high=low+self.rows-1
        assignments=','.join(f"{c}=repeat('n{transaction}',16)" for c in self.fields)
        return 'BEGIN;SELECT pg_current_xact_id()::text;'+''.join(f'UPDATE {table} SET {assignments} WHERE id BETWEEN {low} AND {high};' for table in 'ab')+'COMMIT'

    def verify_source(self):
        self.check('source UPDATE workload retains every row',self.source_value("SELECT (SELECT count(*) FROM a)::text||':'||(SELECT count(*) FROM b)"),'1000:1000')
        changed=min(self.transactions,1000//self.rows)*self.rows
        for table in 'ab':
            self.check('source changes expected keys '+table,self.source_value(f"SELECT count(*) FROM {table} WHERE note<>repeat('o',64)"),str(changed))
            expression=' AND '.join(f'{c} IS NOT DISTINCT FROM note' for c in self.fields[1:]) or 'true'
            self.check('source updates every assigned field '+table,self.source_value(f'SELECT bool_and({expression}) FROM {table}'),'t')

    def equal(self):
        for table in 'ab':
            source=self.source_value(f'SELECT row_to_json(t)::text FROM (SELECT * FROM public.{table} ORDER BY id) t')
            target=self.value(f'SELECT row_to_json(t)::text FROM (SELECT * FROM iceberg_mirror.{table} ORDER BY id) t')
            self.check('all UPDATE columns match exactly '+table,source==target,True)
            self.evidence.setdefault('final_rows_sha256',{})[table]=hashlib.sha256(source.encode()).hexdigest()


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--columns',type=int,choices=[2,32,48])
    parser.add_argument('--repetitions',type=int,default=2,choices=range(1,4))
    parser.add_argument('--smoke',action='store_true')
    args=parser.parse_args()
    output=ROOT/'.qualification'/('update-conversion-comparison-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]);output.mkdir(parents=True)
    result=dict(status='running',settings=vars(args),scope=__doc__,runs=[],summary={},
        methodology='Same cached v7 binaries, fresh private volumes, candidate installs only migration 0034 before start. Both have DELETE filtering and corrected 1-second coalescing, INSERT grouping on, journal capacity 32/64 MiB. 1000 initial rows/table; 120 transactions at 4 offered tx/s, each updates a rolling 32-row range in each table and all non-key fields (64-byte initial text, changing text afterward). Narrow and wide cases differ only in column count. Two runs/variant with reversed order. Initial copy and final full-row validation excluded from resource interval. No diagnostic wrappers. CPU includes container observers, excludes host/controller/MinIO. Shared local caches/hardware, no production sizing, capacity maximum or statistical significance.')
    def save():
        path=output/'results.tmp';path.write_text(json.dumps(result,indent=2)+'\n');path.replace(output/'results.json')
    save()
    try:
        for columns in [args.columns] if args.columns else [2,48]:
            for repeat in range(args.repetitions):
                for candidate in ([False,True] if repeat%2==0 else [True,False]):
                    print(f'Measuring columns={columns}, repetition={repeat+1}, candidate={candidate}',flush=True)
                    probe=UpdateConversionBenchmark(candidate,columns,32,12 if args.smoke else 120);code=probe.run()
                    result['runs'].append(dict(columns=columns,repetition=repeat+1,candidate=candidate,evidence=str(probe.output.relative_to(ROOT)/'results.json'),fixture=probe.evidence));save()
                    if code:raise RuntimeError('fixture failed: '+probe.project)
        metrics=['total_database_and_engine_cpu_seconds','application_commit_p99_seconds','post_write_drain_seconds','publication_latency_upper_bound_p95_seconds','observed_object_requests','total_wal_bytes']
        for columns in sorted({r['columns'] for r in result['runs']}):
            runs=[r for r in result['runs'] if r['columns']==columns]
            if len({json.dumps(r['fixture']['final_rows_sha256'],sort_keys=True) for r in runs})!=1:raise RuntimeError('final rows differ across variants')
            result['summary'][str(columns)]={}
            for candidate in [False,True]:
                ms=[r['fixture']['measurement'] for r in runs if r['candidate']==candidate]
                result['summary'][str(columns)]['candidate' if candidate else 'control']={key:dict(median=statistics.median(m[key] for m in ms),minimum=min(m[key] for m in ms),maximum=max(m[key] for m in ms)) for key in metrics}
        result['status']='passed';save();return 0
    except Exception as error:
        result.update(status='error',error=str(error));save();print('ERROR: '+str(error),file=sys.stderr);return 1
    finally:print('Comparison evidence: '+str(output/'results.json'),flush=True)


if __name__=='__main__':sys.exit(main())

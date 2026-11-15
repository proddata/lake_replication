#!/usr/bin/env python3
"""Counterbalanced same-runtime DELETE key-filter comparison."""
from datetime import datetime,timezone
import json
from pathlib import Path
import statistics
import sys
import uuid
from pull_compare import ComparisonProbe
from probe import ROOT


class DeleteFilterBenchmark(ComparisonProbe):
    def __init__(self,candidate,seed_rows=0,transactions=120):
        super().__init__(False,'mixed',transactions,4,.25)
        self.candidate=candidate;self.seed_rows=seed_rows;self.initial_rows=seed_rows
        self.evidence.update(scope='same-runtime redundant DELETE key predicate comparison',
            sql_overlay='migrations/0033_delete_key_filter.sql' if candidate else None,seed_rows_per_table=seed_rows)

    def setup(self,mirror=True,seed=False):
        super().setup(mirror=False,seed=False)
        if self.candidate:self.sql((ROOT/'migrations/0033_delete_key_filter.sql').read_text())
        if self.seed_rows:
            self.sql(f"INSERT INTO a SELECT i,repeat('x',256) FROM generate_series(1,{self.seed_rows}) i;INSERT INTO b SELECT i,repeat('y',256) FROM generate_series(1,{self.seed_rows}) i")

    def transaction_sql(self,transaction):
        low=self.seed_rows+transaction*self.rows+1;high=low+self.rows-1
        body=f"BEGIN;SELECT pg_current_xact_id()::text;INSERT INTO a SELECT i,repeat('x',256) FROM generate_series({low},{high}) i;INSERT INTO b SELECT i,repeat('y',256) FROM generate_series({low},{high}) i;"
        if transaction:
            previous=low-self.rows
            body+=''.join(f"UPDATE {table} SET note=repeat('u',256) WHERE id={previous};DELETE FROM {table} WHERE id={previous+1};" for table in 'ab')
        return body+'COMMIT'

    def verify_source(self):
        expected=self.seed_rows+self.transactions*self.rows-self.transactions+1
        self.check('source row counts include seed and exact mixed mutations',self.source_value("SELECT (SELECT count(*) FROM a)::text||':'||(SELECT count(*) FROM b)"),f'{expected}:{expected}')
        self.check('source retains every expected update',self.source_value("SELECT (SELECT count(*) FROM a WHERE note=repeat('u',256))::text||':'||(SELECT count(*) FROM b WHERE note=repeat('u',256))"),f'{self.transactions-1}:{self.transactions-1}')


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repetitions',type=int,default=2,choices=range(1,4))
    parser.add_argument('--seed-rows',type=int,default=0,choices=[0,20000])
    parser.add_argument('--smoke',action='store_true')
    args=parser.parse_args()
    output=ROOT/'.qualification'/('delete-filter-comparison-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]);output.mkdir(parents=True)
    result=dict(status='running',settings=vars(args),runs=[],scope=__doc__,
        methodology='Same cached binary image in all fresh fixtures; candidate installs only migration 0033 before snapshot/start. 120 transactions at 4 offered tx/s, paired INSERT/UPDATE/DELETE, four inserted rows/table/transaction. Initial seed copy excluded from measurement. Reversed mode order on second repetition. Observers, batching and journal limits identical. No diagnostic wrappers or extra worker timing phases. CPU excludes host/controller/storage. Short local descriptive comparison, not production sizing or confidence intervals.')
    def save():
        p=output/'results.tmp';p.write_text(json.dumps(result,indent=2)+'\n');p.replace(output/'results.json')
    save()
    try:
        for repeat in range(args.repetitions):
            for candidate in ([False,True] if repeat%2==0 else [True,False]):
                print(f'Measuring repetition {repeat+1}, candidate={candidate}, seed={args.seed_rows}',flush=True)
                probe=DeleteFilterBenchmark(candidate,args.seed_rows,12 if args.smoke else 120);code=probe.run()
                result['runs'].append(dict(repetition=repeat+1,candidate=candidate,fixture=probe.evidence,evidence=str(probe.output.relative_to(ROOT)/'results.json')));save()
                if code:raise RuntimeError('fixture failed: '+probe.project)
        if len({json.dumps(r['fixture']['final_rows_sha256'],sort_keys=True) for r in result['runs']})!=1:raise RuntimeError('final row digests differ across runs')
        metrics=['total_database_and_engine_cpu_seconds','application_commit_p99_seconds','post_write_drain_seconds','publication_latency_upper_bound_p95_seconds','observed_object_requests','total_wal_bytes']
        result['summary']={}
        for candidate in [False,True]:
            ms=[r['fixture']['measurement'] for r in result['runs'] if r['candidate']==candidate]
            result['summary']['candidate' if candidate else 'control']={key:dict(median=statistics.median(m[key] for m in ms),minimum=min(m[key] for m in ms),maximum=max(m[key] for m in ms)) for key in metrics}
        result['status']='passed';save();return 0
    except Exception as error:
        result.update(status='error',error=str(error));save();print('ERROR: '+str(error),file=sys.stderr);return 1
    finally:print('Comparison evidence: '+str(output/'results.json'),flush=True)


if __name__=='__main__':sys.exit(main())

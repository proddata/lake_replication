#!/usr/bin/env python3
"""Counterbalanced publication-window comparison with durable batch statistics."""
from collections import Counter
from datetime import datetime,timezone
import json
import os
import statistics
import sys
import uuid
from pull_compare import ComparisonProbe
from probe import ROOT


class WindowBenchmark(ComparisonProbe):
    def exercise(self):
        super().exercise()
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        batches=json.loads(self.value("""SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY batch_id),'[]'::jsonb) FROM (
            SELECT b.batch_id,b.first_end_lsn,b.last_end_lsn,b.published_at,count(*) AS transactions
            FROM lake_replication.batches b JOIN lake_replication.transactions t USING(batch_id)
            WHERE b.state='published' GROUP BY b.batch_id) t"""))
        sizes=[b['transactions'] for b in batches]
        self.check('batch receipts cover every source transaction',sum(sizes),self.transactions)
        code="""from pathlib import Path
p=Path('/var/lib/postgresql/18/data/lake_replication_logs')
for f in sorted(p.glob('*')):
 if f.is_file():print(f.read_text())
"""
        logs=self.command(self.compose+['exec','-T','--user','postgres','postgres','python3','-c',code]).stdout
        (self.output/'publication-events.jsonl').write_text(logs)
        flushes=[];published=[]
        for line in logs.splitlines():
            try:event=json.loads(line)
            except json.JSONDecodeError:continue
            if event.get('event')=='publication_flush':flushes.append(event)
            if event.get('event')=='batch_published':published.append(event)
        reasons=dict(Counter(event['reason'] for event in flushes))
        m=self.evidence['measurement'];m.update(publication_batches=len(batches),
            mean_transactions_per_batch=statistics.mean(sizes),median_transactions_per_batch=statistics.median(sizes),
            minimum_transactions_per_batch=min(sizes),maximum_transactions_per_batch=max(sizes),
            flush_reasons=reasons,flush_telemetry_available=bool(flushes))
        self.evidence['publication_batches']=batches
        self.evidence['publication_flushes']=flushes
        if flushes:
            self.check('coalescing telemetry permits one batch per live-stream flush',all(e['max_batches']=='1' for e in flushes if e['reason']!='receiver_exit'),True)
            self.check('publisher logs cover every committed batch',len(published),len(batches))
            self.check('publisher events carry a flush reason',all(e.get('flush_reason') in ['interval','journal_pressure','receiver_exit','immediate'] for e in published),True)


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--control-image',required=True)
    parser.add_argument('--candidate-image',required=True)
    parser.add_argument('--workload',choices=['mixed','inserts'])
    parser.add_argument('--repetitions',type=int,default=2,choices=range(1,4))
    parser.add_argument('--smoke',action='store_true')
    args=parser.parse_args()
    output=ROOT/'.qualification'/('coalescing-window-comparison-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]);output.mkdir(parents=True)
    result=dict(status='running',scope=__doc__,settings=vars(args),runs=[],summary={},
        methodology='Sequential fresh same-instance fixtures, reversed order in second repetition. Identical extension SQL (including DELETE filtering), publisher settings, storage and observers; worker versions differ. Mixed: 120 transactions at 4 offered tx/s, four INSERT rows/table and subsequent UPDATE/DELETE of preceding keys. INSERT: 1000 transactions at 100 offered tx/s, four rows/table. 1-second coalescing, grouping enabled, journal count 32 / 64 MiB. Timing window excludes final batch/log collection and exact-row verification. Durably published batch receipts count source transactions. Control predates flush-reason telemetry. CPU includes container observers and excludes host/controller/MinIO. Publication latency includes two-second sample/query delay. Short local comparison, no cold-cache isolation, statistical significance, production sizing or capacity maximum.')
    def save():
        path=output/'results.tmp';path.write_text(json.dumps(result,indent=2)+'\n');path.replace(output/'results.json')
    save();previous=os.environ.get('PUSH_PG_LAKE_IMAGE')
    try:
        for workload in [args.workload] if args.workload else ['mixed','inserts']:
            for repeat in range(args.repetitions):
                for candidate in ([False,True] if repeat%2==0 else [True,False]):
                    os.environ['PUSH_PG_LAKE_IMAGE']=args.candidate_image if candidate else args.control_image
                    print(f'Measuring {workload}, repetition={repeat+1}, candidate={candidate}',flush=True)
                    transactions,interval=(12,.1) if args.smoke else ((120,.25) if workload=='mixed' else (1000,.01))
                    probe=WindowBenchmark(False,workload,transactions,4,interval);code=probe.run()
                    result['runs'].append(dict(workload=workload,repetition=repeat+1,candidate=candidate,evidence=str(probe.output.relative_to(ROOT)/'results.json'),fixture=probe.evidence));save()
                    if code:raise RuntimeError('fixture failed: '+probe.project)
        metrics=['total_database_and_engine_cpu_seconds','application_commit_p99_seconds','post_write_drain_seconds','publication_latency_upper_bound_p95_seconds','observed_object_requests','total_wal_bytes','publication_batches','mean_transactions_per_batch','maximum_observed_backlog_transactions']
        for workload in sorted({r['workload'] for r in result['runs']}):
            runs=[r for r in result['runs'] if r['workload']==workload]
            if len({json.dumps(r['fixture']['final_rows_sha256'],sort_keys=True) for r in runs})!=1:raise RuntimeError('final rows differ between variants')
            result['summary'][workload]={}
            for candidate in [False,True]:
                ms=[r['fixture']['measurement'] for r in runs if r['candidate']==candidate]
                result['summary'][workload]['candidate' if candidate else 'control']={key:dict(median=statistics.median(m[key] for m in ms),minimum=min(m[key] for m in ms),maximum=max(m[key] for m in ms)) for key in metrics}
        result['status']='passed';save();return 0
    except Exception as error:
        result.update(status='error',error=str(error));save();print('ERROR: '+str(error),file=sys.stderr);return 1
    finally:
        if previous is None:os.environ.pop('PUSH_PG_LAKE_IMAGE',None)
        else:os.environ['PUSH_PG_LAKE_IMAGE']=previous
        print('Comparison evidence: '+str(output/'results.json'),flush=True)


if __name__=='__main__':sys.exit(main())

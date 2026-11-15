#!/usr/bin/env python3
"""Bounded repeated, counterbalanced experiments with fresh disposable fixtures."""
import argparse
from datetime import datetime, timezone
import json
import os
import statistics
import sys
import uuid
from benchmark import BenchmarkProbe
from probe import ROOT


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repetitions',type=int,default=3,help='fresh fixtures per mode order (1..5)')
    parser.add_argument('--transactions',type=int,default=120)
    parser.add_argument('--rows',type=int,default=10)
    parser.add_argument('--interval',type=float,default=.25)
    parser.add_argument('--group',type=int,default=32)
    parser.add_argument('--capture-interval-ms',type=int,default=1000)
    parser.add_argument('--trace',action='store_true')
    args=parser.parse_args()
    if not (1<=args.repetitions<=5 and 1<=args.transactions<=1000 and 1<=args.rows<=1000
            and 0<=args.interval<=5 and 1<=args.group<=32 and 0<=args.capture_interval_ms<=1000):
        parser.error('workload exceeds bounded fixture limits')
    stamp=datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]
    output=ROOT/'.qualification'/('lake-push-matrix-'+stamp)
    output.mkdir(parents=True)
    evidence=dict(scope='repeated counterbalanced local pull/push database resource experiment',status='running',
        settings=vars(args),runs=[],
        methodology='Each paired pull/push experiment uses a fresh disposable fixture. Both mode orders run in every repetition, and the first order alternates between repetitions. Within-fixture caches/catalog history and host caches remain order-dependent; this is not cold-cache isolation. Median/min/max summaries are descriptive small-sample statistics, not confidence intervals or production acceptance thresholds. Each referenced fixture retains exact images, inputs, resource/frontier samples, traces and limitations. Storage/host/controller CPU is excluded.')
    def save():
        temporary=output/'results.tmp'
        temporary.write_text(json.dumps(evidence,indent=2)+'\n')
        temporary.replace(output/'results.json')
    os.environ.update(PUSH_BENCH_CONCURRENT='1',PUSH_BENCH_TRANSACTIONS=str(args.transactions),
        PUSH_BENCH_ROWS=str(args.rows),PUSH_BENCH_INTERVAL=str(args.interval),
        PUSH_BENCH_CAPTURE_GROUP=str(args.group),PUSH_BENCH_CAPTURE_INTERVAL_MS=str(args.capture_interval_ms),
        PUSH_BENCH_OBJECT_TRACE='1' if args.trace else '0')
    save()
    try:
        for repetition in range(args.repetitions):
            orders=['pull_first','push_first'] if repetition%2==0 else ['push_first','pull_first']
            for order in orders:
                os.environ['PUSH_BENCH_ORDER']=order
                probe=BenchmarkProbe()
                result=probe.run()
                evidence['runs'].append(dict(repetition=repetition+1,order=order,
                    evidence=str((probe.output/'results.json').relative_to(ROOT)),passed=result==0,
                    measurements=probe.evidence.get('measurements',{})))
                save()
                if result:raise RuntimeError('fixture failed: '+probe.project)
        metrics=['source_and_engine_cpu_seconds','destination_and_engine_cpu_seconds','all_database_cpu_seconds',
            'source_wal_bytes','source_database_bytes_delta','end_to_end_seconds','wall_elapsed_seconds',
            'application_commit_median_seconds','application_commit_p95_seconds','application_commit_p99_seconds']
        summary={}
        for order in ['pull_first','push_first']:
            summary[order]={}
            for mode in ['logical_pull','iceberg_push']:
                rows=[run['measurements'][mode] for run in evidence['runs'] if run['order']==order]
                values={key:[row[key] for row in rows] for key in metrics}
                if args.trace:
                    for category in ['source_capture','destination_publish','bucket_or_other']:
                        values[category+'_observed_requests']=[row['object_operations']['categories'].get(category,{}).get('requests',0) for row in rows]
                summary[order][mode]={key:dict(n=len(v),median=statistics.median(v),minimum=min(v),maximum=max(v)) for key,v in values.items()}
            rows=[run['measurements']['source_maintenance'] for run in evidence['runs'] if run['order']==order]
            values={key:[row[key] for row in rows] for key in ['seconds','source_and_engine_cpu_seconds','destination_and_engine_cpu_seconds','all_database_cpu_seconds']}
            if args.trace:
                values['source_capture_observed_requests']=[row['object_operations']['categories'].get('source_capture',{}).get('requests',0) for row in rows]
            summary[order]['source_maintenance']={key:dict(n=len(v),median=statistics.median(v),minimum=min(v),maximum=max(v)) for key,v in values.items()}
        evidence.update(status='passed',summary=summary)
        save()
        print('Matrix evidence: '+str(output/'results.json'),flush=True)
        return 0
    except Exception as error:
        evidence.update(status='error',error=str(error));save()
        print('ERROR: '+str(error)+'; evidence: '+str(output/'results.json'),flush=True)
        return 1


if __name__=='__main__':sys.exit(main())

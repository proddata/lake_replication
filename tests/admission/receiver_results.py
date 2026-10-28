"""Audit diagnostic snapshots, independently of performance pass/fail."""
import argparse
import hashlib
import json
import math
from pathlib import Path


def lsn(text):
    high,low=text.split('/')
    return (int(high,16)<<32)|int(low,16)


def audit(case):
    trace=case.get('trace',[])
    checks={'stable_measurement_clock':case.get('timing_validation',{}).get('valid',False),
            'snapshots_observed':bool(trace) and any(t.get('receiver_progress') for t in trace),
            'final_snapshot_capture_succeeded':isinstance(case.get('diagnostic',{}).get('receiver_progress',{}).get('final'),list),
            'one_record_per_subscription':True,'snapshot_clocks_consistent':True,
            'durable_frontier_never_regresses':True,'valid_record_fields':True}
    previous={};generations=set();maximum=0
    for point in trace:
        records=point.get('receiver_progress',[]);maximum=max(maximum,len(records));keys=set()
        for row in records:
            try:
                key=(row['database'],row['subscription_id'])
                if key in keys:checks['one_record_per_subscription']=False
                keys.add(key);generations.add((key,row['pid'],row['started_epoch']))
                started,snapshot,elapsed=(row[k] for k in ['started_epoch','snapshot_epoch','elapsed_seconds'])
                if not all(math.isfinite(v) for v in [started,snapshot,elapsed]) or elapsed<0 or abs(snapshot-started-elapsed)>1:
                    checks['snapshot_clocks_consistent']=False
                frontier=lsn(row['durably_staged_lsn'])
                if frontier<previous.get(key,0):checks['durable_frontier_never_regresses']=False
                previous[key]=frontier
                message=row['last_message'];age=row['last_message_age_at_snapshot_seconds']
                valid=(message is None)==(age is None)
                if message is not None:
                    valid=valid and math.isfinite(age) and age>=0 and message['kind'] in ['data','keepalive']
                    lsn(message['wal_end'])
                if row.get('observed_commit') is not None:lsn(row['observed_commit']['end_lsn'])
                checks['valid_record_fields'] &= valid
            except (KeyError,TypeError,ValueError,OverflowError):checks['valid_record_fields']=False
    return {'valid':all(checks.values()),'checks':checks,'maximum_sampled_records':maximum,
            'observed_generations':len(generations),
            'scope':'Snapshot integrity only; freshness or WAL endpoints do not establish receiver health'}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('results',type=Path)
    args=parser.parse_args();result=audit(json.loads(args.results.read_text()))
    result.update(raw_results_sha256=hashlib.sha256(args.results.read_bytes()).hexdigest(),
                  analysis_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (args.results.parent/'receiver-audit.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2));return int(not result['valid'])


if __name__=='__main__':raise SystemExit(main())

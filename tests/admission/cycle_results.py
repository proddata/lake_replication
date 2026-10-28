"""Separate journal insertion, durable-stage logging and publisher-cycle timings."""
import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path


def distribution(values):
    values=sorted(values)
    if not values:return None
    return {'count':len(values),'p50':values[(len(values)-1)//2],
            'p95':values[math.ceil(len(values)*.95)-1],
            'maximum':values[-1],'sum':sum(values)}


def analyze(case,transactions,logs):
    checks={'stable_measurement_clock':case.get('timing_validation',{}).get('valid',False),
            'complete_transaction_capture':len(transactions)==3 and len({d['database'] for d in transactions})==3,'journal_markers_inside_interval':True,
            'staged_events_matched':True,'cycle_monotonic_order':True,'cycle_events_captured':True}
    streams=[]
    for database in transactions:
        rows=database['transactions'];name=database['database'];oid=database['database_oid']
        events=[]
        for filename,lines in logs.items():
            if not filename.startswith(f'lake_replication_db{oid}_'):continue
            for line in lines.splitlines():
                try:event=json.loads(line)
                except json.JSONDecodeError:continue
                if isinstance(event,dict):events.append(event)
        staged={int(e['xid']):e for e in events if e.get('event')=='transaction_staged' and 'xid' in e}
        total=[];ingest=[];publication=[];stage_feedback=[]
        checks['complete_transaction_capture'] &= len(rows)==case['stream_transactions'] and len({r['xid'] for r in rows})==len(rows)
        for row in rows:
            source,journal,published=(row[k] for k in ['source_commit_epoch','journal_insert_epoch','publication_commit_epoch'])
            checks['journal_markers_inside_interval'] &= source<=journal<=published
            total.append(1000*(published-source));ingest.append(1000*(journal-source));publication.append(1000*(published-journal))
            event=staged.get(row['xid'])
            if event:stage_feedback.append(event['timestamp_unix_ms']-1000*journal)
            else:checks['staged_events_matched']=False
        cycles=defaultdict(list)
        for event in events:
            if event.get('component')=='diagnostic_cycle':cycles[(event['pid'],event['cycle'])].append(event)
        preflight=[];spawn=[];publisher=[];gap=[];complete=0;finished_by_pid={}
        for key,records in sorted(cycles.items(),key=lambda item:min(e['timestamp_unix_ms'] for e in item[1])):
            records.sort(key=lambda e:int(e['elapsed_us']));marks={e['event']:e for e in records}
            if 'cycle_started' in marks:
                prior=finished_by_pid.get(key[0])
                if prior is not None:gap.append(marks['cycle_started']['timestamp_unix_ms']-prior)
            if 'cycle_finished' in marks:
                complete+=1;finished_by_pid[key[0]]=marks['cycle_finished']['timestamp_unix_ms']
            for left,right,target in [('cycle_started','preflight_finished',preflight),('preflight_finished','receiver_spawned',spawn)]:
                if left in marks and right in marks:
                    delta=(int(marks[right]['elapsed_us'])-int(marks[left]['elapsed_us']))/1000
                    checks['cycle_monotonic_order'] &= delta>=0;target.append(delta)
            start=None
            for event in records:
                if event['event']=='publisher_started':start=int(event['elapsed_us'])
                elif event['event']=='publisher_finished' and start is not None:
                    delta=(int(event['elapsed_us'])-start)/1000;publisher.append(delta)
                    checks['cycle_monotonic_order'] &= delta>=0;start=None
        checks['cycle_events_captured'] &= bool(preflight) and bool(publisher) and complete>0
        batches={(e.get('batch_id'),e.get('published_lsn')):e for e in events if e.get('component')=='publisher' and e.get('event')=='batch_published'}
        failures=defaultdict(int)
        for event in events:
            if event.get('level') in ['WARN','ERROR']:
                failures[event.get('component','')+':'+event.get('event','')]+=1
        streams.append({'database':name,'transactions':len(rows),'warning_error_event_counts':dict(failures),'complete_cycles':complete,'observed_cycles':len(cycles),
            'source_to_publication_ms':distribution(total),'source_to_journal_insert_ms':distribution(ingest),
            'journal_insert_to_publication_commit_ms':distribution(publication),
            'journal_insert_to_stage_feedback_log_ms':distribution(stage_feedback),
            'preflight_ms':distribution(preflight),'receiver_spawn_ms':distribution(spawn),
            'publisher_invocation_ms':distribution(publisher),'between_cycle_gap_ms':distribution(gap),
            'published_batch_transactions':distribution([int(e['transactions']) for e in batches.values()]),
            'published_batch_bytes':distribution([int(e['bytes']) for e in batches.values()])})
    return {'valid':all(checks.values()),'checks':checks,'streams':streams,
            'scope':'Journal insertion is an internal timestamp, not staging commit. Stage log follows staging/feedback. Publisher invocation includes process/connect/SQL/exit.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('results',type=Path)
    args=parser.parse_args();root=args.results.parent.parent
    paths=[args.results,root/'cycle-transactions.json',root/'cycle-worker-logs.json']
    result=analyze(*[json.loads(p.read_text()) for p in paths])
    result['input_sha256']={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    result['analysis_source_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (root/'cycle-analysis.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2));return int(not result['valid'])


if __name__=='__main__':raise SystemExit(main())

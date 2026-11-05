"""Attach nested pg_lake timers to matched publication COMMIT messages."""
from collections import defaultdict
import math
import re


def stats(values):
    values=sorted(values)
    return dict(count=len(values),mean_ms=sum(values)/len(values),
        p95_ms=values[math.ceil(.95*len(values))-1],max_ms=values[-1])


DETAILS=['catalog_files','file_diff','metadata_file_scan','new_file_stats','read_metadata',
         'process_operations','finalize_snapshot','write_metadata']


def analyze(log,matched,require_details=False):
    errors=[];pending=defaultdict(list);sequence=defaultdict(int);messages={}
    for line in log.splitlines():
        prefix=re.search(r'lake-profile\|([^|]+)\|[^|]*\|(\d+)\|\s*(.*)',line)
        if not prefix:continue
        db,pid,body=prefix.groups();key=(db,int(pid))
        timer=re.search(r'lake-commit\|([a-z_]+)\|([0-9.]+)\|(\d+)',body)
        if timer:
            phase,elapsed,count=timer.groups()
            pending[key].append(dict(phase=phase,ms=float(elapsed),count=int(count)))
        duration=re.search(r'LOG:\s+duration: ([0-9.]+) ms',body)
        if duration:
            sequence[key]+=1
            messages[(*key,sequence[key])]=(float(duration.group(1)),pending.pop(key,[]))
    rows=[]
    if not matched.get('valid'):errors.append('wire/server correlation is invalid')
    for publication in matched['matched_publications']:
        if publication.get('phase')!='commit':errors.append('split commit phase required');continue
        key=(publication['database'],publication['backend_pid'],publication['sequence'])
        found=messages.get(key)
        if found is None:errors.append('commit message absent from server log');continue
        duration,timers=found
        if abs(duration-publication['server_ms'])>.001:errors.append('commit duration identity differs');continue
        grouped=defaultdict(list)
        for timer in timers:grouped[timer['phase']].append(timer)
        if len(grouped['metadata_total'])!=1 or grouped['metadata_total'][0]['count']<1:
            errors.append('expected one nonempty metadata callback');continue
        if any(len(grouped[phase])!=1 for phase in ['uploads','upload_commands']):
            errors.append('upload parent missing or duplicated');continue
        if not grouped['metadata_prepare'] or not grouped['iceberg_metadata']:
            errors.append('metadata child timers missing');continue
        count=grouped['upload_commands'][0]['count']
        if count<1 or len(grouped['upload_connect'])!=count:
            errors.append('upload connection count differs from command count');continue
        sums={phase:sum(timer['ms'] for timer in values) for phase,values in grouped.items()}
        if require_details and any(not grouped[phase] for phase in DETAILS):errors.append('detailed metadata timers missing')
        if all(grouped[phase] for phase in DETAILS):
            if sum(sums[p] for p in ['catalog_files','file_diff','new_file_stats'])>sums['metadata_prepare']+2:
                errors.append('preparation details exceed parent')
            if sums['metadata_file_scan']>sums['file_diff']+2:errors.append('file scan exceeds diff parent')
            if sum(sums[p] for p in ['read_metadata','process_operations','finalize_snapshot','write_metadata'])>sums['iceberg_metadata']+2:
                errors.append('Iceberg details exceed parent')
        if sums['metadata_total']>duration+2:errors.append('metadata timer exceeds commit')
        if sums['metadata_prepare']+sums['iceberg_metadata']+sums['uploads']>sums['metadata_total']+2:
            errors.append('nonoverlapping child timers exceed metadata parent')
        if sums['upload_connect']>sums['upload_commands']+2 or sums['upload_commands']>sums['uploads']+2:
            errors.append('upload child timers exceed enclosing parent')
        rows.append(dict(database=key[0],backend_pid=key[1],sequence=key[2],
            batch_id=publication['batch_id'],published_lsn=publication['published_lsn'],
            commit_ms=duration,metadata_total_ms=sums['metadata_total'],
            metadata_prepare_ms=sums['metadata_prepare'],iceberg_metadata_ms=sums['iceberg_metadata'],
            uploads_ms=sums['uploads'],upload_commands_ms=sums['upload_commands'],
            upload_connect_sum_ms=sums['upload_connect'],upload_count=count,
            commit_remainder_ms=duration-sums['metadata_total'],
            metadata_remainder_ms=sums['metadata_total']-sums['metadata_prepare']-sums['iceberg_metadata']-sums['uploads'],
            detail_ms={phase:sums[phase] for phase in DETAILS if grouped[phase]},
            detail_counts={phase:sum(t['count'] for t in grouped[phase]) for phase in DETAILS if grouped[phase]}))
    if not rows or len(rows)!=len(matched['matched_publications']):errors.append('not every publication has internal commit timing')
    databases={}
    for db in sorted({row['database'] for row in rows}):
        values=[row for row in rows if row['database']==db]
        databases[db]={field:stats([row[field] for row in values]) for field in
            ['commit_ms','metadata_total_ms','metadata_prepare_ms','iceberg_metadata_ms','uploads_ms',
             'upload_commands_ms','upload_connect_sum_ms','commit_remainder_ms','metadata_remainder_ms']}
        databases[db]['details']={phase:stats([row['detail_ms'][phase] for row in values if phase in row['detail_ms']])
            for phase in DETAILS if any(phase in row['detail_ms'] for row in values)}
    return dict(valid=not errors,errors=errors,matched_count=len(rows),publications=rows,databases=databases,
        scope=__doc__,limitations='Timer-only binary overlay perturbs execution. Nested parent/child times must not be added. Upload connections are acquired serially while prior commands may run concurrently. Commit remainder includes other callbacks, logging and durability, not pure WAL time. No CPU or causal speedup claim.')

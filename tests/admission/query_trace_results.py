"""Match successful publisher wire messages to server durations; never infer commit time."""
from collections import defaultdict
import json
import math
import re


def stats(values):
    ordered=sorted(values)
    return dict(count=len(ordered),mean_ms=sum(ordered)/len(ordered),
        p95_ms=ordered[math.ceil(.95*len(ordered))-1],max_ms=ordered[-1])


def correlate(server_log,worker_log):
    server=defaultdict(list)
    for db,app,pid,duration in re.findall(r'lake-profile\|([^|]+)\|([^|]*)\|(\d+)\|\s*LOG:\s+duration: ([0-9.]+) ms',server_log):
        server[db,int(pid)].append(float(duration))
    sessions=defaultdict(list);connections={};pending={};batches={};errors=[];spawns={}
    for line in worker_log.splitlines():
        try:event=json.loads(line)
        except json.JSONDecodeError:continue
        if not isinstance(event,dict):continue
        if event.get('event')=='publisher_spawn':
            child=int(event['child_pid'])
            if child in spawns:errors.append('duplicate publisher spawn')
            spawns[child]=event
        elif event.get('event')=='query_timing' and event.get('role')=='publisher':
            key=(int(event['pid']),int(event['connection']))
            if event['operation']=='connect':
                if key in connections:errors.append('duplicate connection event')
                connections[key]=event
                continue
            sessions[key].append(event)
            if event['operation']=='publish_batch' and event.get('phase','message') in ('message','commit') and event['status']=='ok':
                if int(event['pid']) in pending:errors.append('publication event missing before next publication')
                pending[int(event['pid'])]=event
        elif event.get('event')=='batch_published' and event.get('component')=='publisher':
            timing=pending.pop(int(event['pid']),None)
            if timing is None:errors.append('batch has no preceding publisher timing')
            else:batches[int(event['pid']),int(timing['connection']),int(timing['sequence'])]=event
    if pending:errors.append('successful publication timing has no batch event')
    matched=[];used=set()
    for key,queries in sessions.items():
        connection=connections.get(key)
        if connection is None:errors.append('session has no connection timing');continue
        spawn=spawns.get(key[0])
        if spawn is None:errors.append('publisher has no spawn timing');continue
        if any(spawn[field]!=connection[field] for field in ['database','subscription_id','generation']):
            errors.append('publisher spawn and connection identity differ');continue
        startup_ms=int(connection['timestamp_unix_ms'])-int(spawn['started_unix_ms'])-int(connection['elapsed_us'])/1000
        if startup_ms < -2:errors.append('connection startup clock window invalid')
        backend=(connection['database'],int(connection['backend_pid']))
        if backend in used:errors.append('backend reused by multiple traced connections');continue
        used.add(backend)
        queries.sort(key=lambda q:int(q['sequence']))
        if [int(q['sequence']) for q in queries]!=list(range(1,len(queries)+1)):
            errors.append('query sequence missing or duplicated');continue
        durations=server.get(backend,[])
        if len(durations)!=len(queries):errors.append(f'client/server count mismatch: {backend}');continue
        if any(q['status']!='ok' or (q['database'],int(q['backend_pid']))!=backend for q in queries):
            errors.append('failed query or inconsistent backend identity');continue
        phases=[]
        for query,duration in zip(queries,durations):
            client_ms=int(query['elapsed_us'])/1000
            if client_ms+2<duration:errors.append('server duration exceeds enclosing client duration')
            if query['operation']!='publish_batch':continue
            row=dict(database=backend[0],backend_pid=backend[1],worker_pid=key[0],
                connection=key[1],sequence=int(query['sequence']),subscription_id=query['subscription_id'],
                generation=query['generation'],phase=query.get('phase','message'),
                client_ms=client_ms,server_ms=duration,client_minus_server_ms=client_ms-duration,
                connection_ms=int(connection['elapsed_us'])/1000,
                spawn_ms=int(spawn['spawn_elapsed_us'])/1000,startup_before_connect_ms=startup_ms)
            phases.append(row)
            if row['phase'] not in ('message','commit'):continue
            expected=['message'] if row['phase']=='message' else ['begin']+(['fence'] if query['generation']!='0' else [])+['statement','commit']
            if [p['phase'] for p in phases]!=expected:errors.append('publication phase sequence incomplete or duplicated')
            batch=batches.get((*key,int(query['sequence'])))
            if batch is None:errors.append('publication timing has no matching batch');phases=[];continue
            matched.append(dict(row,batch_id=batch['batch_id'],published_lsn=batch['published_lsn'],phases=phases))
            phases=[]
        if phases:errors.append('incomplete publication phase group')
    if not matched:errors.append('no matched publications')
    if len(matched)!=len(batches):errors.append('not every publication matched')
    databases={}
    for database in sorted({row['database'] for row in matched}):
        rows=[row for row in matched if row['database']==database]
        databases[database]={field:stats([row[field] for row in rows]) for field in
            ['client_ms','server_ms','client_minus_server_ms']}
        unique={row['worker_pid']:row for row in rows}
        databases[database]['connections']={field:stats([row[field] for row in unique.values()]) for field in
            ['connection_ms','spawn_ms','startup_before_connect_ms']}
        databases[database]['summary_phase']=rows[0]['phase']
        databases[database]['phases']={phase:{field:stats([p[field] for row in rows for p in row['phases'] if p['phase']==phase])
            for field in ['client_ms','server_ms','client_minus_server_ms']} for phase in sorted({p['phase'] for row in rows for p in row['phases']})}
    return dict(scope=__doc__,valid=not errors,errors=errors,matched_publications=matched,
        databases=databases,coverage=dict(publisher_spawns=len(spawns),publisher_connections=len(connections),
            spawn_pids_without_connection=sorted(set(spawns)-{key[0] for key in connections})),
        limitations='Successful publisher sessions only; orphan spawns may be unfinished or failed and are retained in coverage. Matches by database/backend and complete per-connection message sequence. Summary phase is message or commit; split phases add round trips and cannot be compared as performance with single-message transactions. Server time is not CPU time; residual includes transport and scheduling. Connection times must not be summed per batch; startup estimate uses millisecond wall-clock timestamps.')

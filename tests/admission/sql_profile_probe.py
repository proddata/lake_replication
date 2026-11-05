#!/usr/bin/env python3
"""Local archive workload with pg_stat_statements instrumentation.

Profiling changes overhead. Timings do not qualify archived defaults and must
not be compared with uninstrumented observations. pg_stat_statements is added
for diagnosis; --trace-workers adds a launcher wrapper to enable opt-in worker
timing, preserving archived binary bytes. No production SQL is replaced.
"""
import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
import sys


def load(name, filename):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(filename))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def duration_summary(log):
    durations={}
    for database,subscription,generation,pid,milliseconds in re.findall(
        r'lake-profile\|([^|]+)\|lake_replication_worker/(\d+)/(\d+)\|(\d+)\|\s*LOG:\s+duration: ([0-9.]+) ms',log):
        durations.setdefault(database,[]).append(float(milliseconds))
    summaries={}
    for database,values in durations.items():
        values.sort()
        summaries[database]=dict(count=len(values),total_ms=sum(values),
            p95_ms=values[math.ceil(.95*len(values))-1],max_ms=max(values))
    return dict(scope='Simple-query message durations including transaction finalization; worker sessions only, includes setup and validation, not CPU time. No individual statement attribution.',databases=summaries)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpuset',default='0')
    parser.add_argument('--analyze-log',type=Path,help='Analyze a retained timing log without running a workload')
    parser.add_argument('--trace-workers',action='store_true',help='Diagnostic supervisor wrapper enables opt-in timing in a newly built tracing archive')
    parser.add_argument('--trace-phases',action='store_true',help='Split diagnostic transactions into phase requests; adds round trips, never qualification')
    parser.add_argument('--profile-pg-lake-commit',action='store_true',help='Rebuild pinned pg_lake libraries with commit timers; diagnostic binary overlay')
    args=parser.parse_args()
    if args.trace_phases and not args.trace_workers:parser.error('--trace-phases requires --trace-workers')
    if args.profile_pg_lake_commit and not args.trace_phases:parser.error('--profile-pg-lake-commit requires --trace-phases')
    if args.analyze_log:
        summary=duration_summary(args.analyze_log.read_text())
        print(json.dumps(summary,indent=2))
        return int(len(summary['databases'])!=7)
    p=load('production_probe','probe.py')
    original_sql,original_compose,original_text=p.q.sql,p.q.compose,p.m.compose_text
    profile=load('sql_profile','checkpoint_sql_profile.py').Profile(p.q,original_sql,transaction_timing=True)
    installed=set()
    wrapper_metadata={}
    commit_overlay=load('commit_profile_overlay','commit_profile_overlay.py').Overlay(p.q,p.ROOT) if args.profile_pg_lake_commit else None
    if args.trace_workers:
        original_build=p.q.build_images
        def build_images(archive,context):
            original_build(archive,context)
            if commit_overlay:commit_overlay.build(context)
            executable='/usr/local/bin/lake-replication-supervisor'
            before=p.q.command(['docker','run','--rm','--entrypoint','sha256sum',p.q.DESTINATION_IMAGE,executable]).stdout.split()[0]
            wrapper='#!/bin/sh\nexport LAKE_TRACE_TRANSACTIONS=1\n'+('export LAKE_TRACE_TRANSACTION_PHASES=1\n' if args.trace_phases else '')+'exec '+executable+'-trace-real "$@"\n'
            recipe=f'FROM {p.q.DESTINATION_IMAGE}\nUSER root\nRUN mv {executable} {executable}-trace-real\nCOPY --chmod=0755 trace-wrapper {executable}\n'
            (context/'trace-wrapper').write_text(wrapper);(context/'Dockerfile.trace-wrapper').write_text(recipe)
            wrapper_metadata.update(base_image_id=p.q.command(['docker','image','inspect','--format','{{.Id}}',p.q.DESTINATION_IMAGE]).stdout.strip(),wrapper_sha256=hashlib.sha256(wrapper.encode()).hexdigest(),supervisor_sha256=before)
            p.q.command(['docker','build','-t',p.q.DESTINATION_IMAGE,'-f',str(context/'Dockerfile.trace-wrapper'),str(context)],timeout=300)
            after=p.q.command(['docker','run','--rm','--entrypoint','sha256sum',p.q.DESTINATION_IMAGE,executable+'-trace-real']).stdout.split()[0]
            if before!=after:raise ValueError('wrapper changed archived supervisor bytes')
            (p.q.RESULTS/'trace-wrapper').write_text(wrapper)
            (p.q.RESULTS/'Dockerfile.trace-wrapper').write_text(recipe)
        p.q.build_images=build_images

    def install(database):
        if database not in installed:
            profile.install(database);installed.add(database)

    def sql(service,database,statement,**kwargs):
        result=original_sql(service,database,statement,**kwargs)
        if service=='destination' and 'CREATE EXTENSION lake_replication' in statement:install(database)
        return result

    def compose(*values,**kwargs):
        if values and values[0]=='logs' and 'analytics' in installed:
            if args.trace_workers:(p.q.RESULTS/'worker-trace.jsonl').write_text(p.q.worker_logs())
            profile.capture()
        result=original_compose(*values,**kwargs)
        if values and values[0]=='up':install('analytics')
        if values and values[0]=='down':installed.clear()
        return result

    p.q.sql,p.q.compose=sql,compose
    def compose_text():
        text=profile.compose_text(original_text())
        anchor='      - lake_replication.log_directory=/worker-logs\n'
        if text.count(anchor)!=1:raise ValueError('destination logging hook changed')
        settings=['log_duration=on','log_statement=none','log_min_duration_statement=-1',
            'log_min_duration_sample=-1','log_transaction_sample_rate=0',
            'log_line_prefix=lake-profile|%d|%a|%p| ']
        if commit_overlay:settings.append('lake_commit_diagnostic.enabled=on')
        return text.replace(anchor,anchor+''.join('      - -c\n      - '+setting+'\n' for setting in settings))
    p.m.compose_text=compose_text
    p.m.diagnostic_sources=[Path(__file__),Path(__file__).with_name('checkpoint_sql_profile.py'),Path(__file__).with_name('query_trace_results.py')]
    if commit_overlay:p.m.diagnostic_sources.extend(Path(__file__).with_name(name) for name in ['commit_profile_overlay.py','commit_profile_results.py'])
    p.__doc__=__doc__
    sys.argv=[sys.argv[0],'--cases','fairness','--cpuset',args.cpuset]
    code=p.main()
    path=p.q.RESULTS/'results.json';result=json.loads(path.read_text())
    result['sql_profiling']=True
    result['trace_workers']=args.trace_workers
    result['trace_phases']=args.trace_phases
    result['pg_lake_commit_overlay']=commit_overlay.metadata if commit_overlay else None
    result['diagnostic_wrapper']=wrapper_metadata
    log=(p.q.RESULTS/'fairness/compose.log').read_text()
    result['server_query_durations']=duration_summary(log)
    if args.trace_workers:
        matched=load('query_trace_results','query_trace_results.py').correlate(log,(p.q.RESULTS/'worker-trace.jsonl').read_text())
        (p.q.RESULTS/'matched-query-timings.json').write_text(json.dumps(matched,indent=2)+'\n')
        result['matched_query_timings']={k:v for k,v in matched.items() if k!='matched_publications'}
        if not matched['valid'] or len(matched['databases'])!=3:
            result.update(status='error',error='publisher timing correlation failed');code=1
        if commit_overlay:
            internals=load('commit_profile_results','commit_profile_results.py').analyze(log,matched,require_details=True)
            (p.q.RESULTS/'commit-internals-analysis.json').write_text(json.dumps(internals,indent=2)+'\n')
            result['commit_internals']={k:v for k,v in internals.items() if k!='publications'}
            if not internals['valid']:result.update(status='error',error='internal commit attribution failed');code=1
    if len(result['server_query_durations']['databases'])!=7:
        result.update(status='error',error='worker duration logs missing measured databases');code=1
    if profile.error or not profile.captured:
        result.update(status='error',error=profile.error or 'SQL profile not captured');code=1
    path.write_text(json.dumps(result,indent=2)+'\n')
    return code


if __name__=='__main__':raise SystemExit(main())

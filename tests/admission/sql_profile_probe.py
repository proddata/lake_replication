#!/usr/bin/env python3
"""Local archive workload with pg_stat_statements instrumentation.

Profiling changes overhead. Timings do not qualify archived defaults and must
not be compared with uninstrumented observations. No production SQL replacement
or executable overlay is installed; pg_stat_statements is added for diagnosis.
"""
import argparse
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
    args=parser.parse_args()
    if args.analyze_log:
        summary=duration_summary(args.analyze_log.read_text())
        print(json.dumps(summary,indent=2))
        return int(len(summary['databases'])!=7)
    p=load('production_probe','probe.py')
    original_sql,original_compose,original_text=p.q.sql,p.q.compose,p.m.compose_text
    profile=load('sql_profile','checkpoint_sql_profile.py').Profile(p.q,original_sql,transaction_timing=True)
    installed=set()

    def install(database):
        if database not in installed:
            profile.install(database);installed.add(database)

    def sql(service,database,statement,**kwargs):
        result=original_sql(service,database,statement,**kwargs)
        if service=='destination' and 'CREATE EXTENSION lake_replication' in statement:install(database)
        return result

    def compose(*values,**kwargs):
        if values and values[0]=='logs' and 'analytics' in installed:profile.capture()
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
        return text.replace(anchor,anchor+''.join('      - -c\n      - '+setting+'\n' for setting in settings))
    p.m.compose_text=compose_text
    p.m.diagnostic_sources=[Path(__file__),Path(__file__).with_name('checkpoint_sql_profile.py')]
    p.__doc__=__doc__
    sys.argv=[sys.argv[0],'--cases','fairness','--cpuset',args.cpuset]
    code=p.main()
    path=p.q.RESULTS/'results.json';result=json.loads(path.read_text())
    result['sql_profiling']=True
    log=(p.q.RESULTS/'fairness/compose.log').read_text()
    result['server_query_durations']=duration_summary(log)
    if len(result['server_query_durations']['databases'])!=7:
        result.update(status='error',error='worker duration logs missing measured databases');code=1
    if profile.error or not profile.captured:
        result.update(status='error',error=profile.error or 'SQL profile not captured');code=1
    path.write_text(json.dumps(result,indent=2)+'\n')
    return code


if __name__=='__main__':raise SystemExit(main())

#!/usr/bin/env python3
"""Checkpoint partition diagnostic: original control or SQL overlay, never release qualification."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cases',nargs='+',choices=['managed','fairness'],default=['managed','fairness'])
    parser.add_argument('--cpuset',default='0-1')
    parser.add_argument('--variant',choices=['original','split'],default='split')
    parser.add_argument('--skip-operations',action='store_true',help='Performance-only fixture; focused safety checks were qualified separately')
    parser.add_argument('--profile-sql',action='store_true',help='Local pg_stat_statements instrumentation; not comparable to unprofiled timings')
    parser.add_argument('--profile-staging',action='store_true',help='Isolated rollback-only append plans before managed safety checks')
    parser.add_argument('--chunk-store',action='store_true',help='Store each unchanged 1000-row transaction as one durable JSON chunk (diagnostic only)')
    parser.add_argument('--chunk-pause-ms',type=int,choices=[0,40],default=0,help='Archived post-commit pacing configuration; requires chunk storage')
    parser.add_argument('--copy-chunk-rows',type=int,choices=[1000,2000,4000],help='Archived initial-sync configuration overlay; byte budget stays 8 MiB')
    args=parser.parse_args()
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',args.cpuset):
        parser.error('invalid CPU affinity')
    if args.skip_operations and args.cases!=['fairness']:
        parser.error('skip-operations requires the fairness-only case')
    if args.variant=='original' and not args.skip_operations:
        parser.error('the original control requires skip-operations and fairness only')
    if args.profile_sql and (not args.skip_operations or args.cases!=['fairness']):
        parser.error('SQL profiling requires a performance-only fixture')
    if args.profile_staging and (args.skip_operations or args.cases!=['managed']):
        parser.error('staging profiling requires the managed safety fixture')
    if args.chunk_store and (args.variant!='split' or args.profile_staging or args.copy_chunk_rows not in (None,1000)):
        parser.error('chunk storage requires split checkpoints, the unchanged 1000-row cap, and no isolated per-row profile')
    if args.chunk_pause_ms and not args.chunk_store:
        parser.error('post-commit pacing requires chunk storage')
    os.environ.setdefault('VERSION','0.1.0-beta.4')
    spec=importlib.util.spec_from_file_location('production_probe',Path(__file__).with_name('probe.py'))
    p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
    if p.q.VERSION!='0.1.0-beta.4':parser.error('historical overlays require a beta.4 archive; use the unmodified archive probe for beta.5')
    original_sql=p.q.sql;original_compose=p.q.compose;original_text=p.m.compose_text
    installed=set();installs=[]
    overlay=Path(__file__).with_name('checkpoint_split.sql').read_text()
    chunk_sql=Path(__file__).with_name('staging_chunks.sql').read_text() if args.chunk_store else None
    if args.chunk_store:p.q.STAGING_ROWS='initial_copy_chunks'
    spec=importlib.util.spec_from_file_location('split_operations',Path(__file__).with_name('checkpoint_split_operations.py'))
    operations=importlib.util.module_from_spec(spec);spec.loader.exec_module(operations)
    profile=None
    if args.profile_sql:
        spec=importlib.util.spec_from_file_location('checkpoint_sql_profile',Path(__file__).with_name('checkpoint_sql_profile.py'))
        profiler=importlib.util.module_from_spec(spec);spec.loader.exec_module(profiler)
        profile=profiler.Profile(p.q,original_sql)
    chunk=None
    if args.copy_chunk_rows or args.chunk_pause_ms:
        spec=importlib.util.spec_from_file_location('copy_chunk',Path(__file__).with_name('copy_chunk.py'))
        chunk_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(chunk_module)
        chunk=chunk_module.Overlay(p.q,args.copy_chunk_rows or 1000,args.chunk_pause_ms)
        original_build=p.q.build_images
        def build_images(archive,context):
            original_build(archive,context)
            chunk.build(context)
        p.q.build_images=build_images

    def install(database):
        if database not in installed:
            if args.variant=='split':
                original_sql('destination',database,overlay)
                installs.append(database)
            if chunk_sql:original_sql('destination',database,chunk_sql)
            if profile:profile.install(database)
            installed.add(database)
            metadata_path.write_text(json.dumps(metadata,indent=2)+'\n')

    def sql(service,database,statement,**kwargs):
        result=original_sql(service,database,statement,**kwargs)
        if service=='destination' and 'CREATE EXTENSION lake_replication' in statement:
            install(database)
        return result

    def compose(*values,**kwargs):
        if values and values[0]=='logs' and profile and 'analytics' in installed:
            profile.capture()
        result=original_compose(*values,**kwargs)
        if values and values[0]=='down':installed.clear()
        if values and values[0]=='up' and 'analytics' not in installed:
            install('analytics')
            if not args.skip_operations:operations.run(p.q,p.c)
            if args.profile_staging:
                spec=importlib.util.spec_from_file_location('staging_cost',Path(__file__).with_name('staging_cost.py'))
                cost=importlib.util.module_from_spec(spec);spec.loader.exec_module(cost)
                cost.run(p.q,p.c)
            if args.chunk_store and not args.skip_operations:
                spec=importlib.util.spec_from_file_location('staging_chunk_operations',Path(__file__).with_name('staging_chunk_operations.py'))
                checks=importlib.util.module_from_spec(spec);spec.loader.exec_module(checks)
                checks.run(p.q,p.c)
            if (chunk or args.chunk_store) and not args.skip_operations:
                spec=importlib.util.spec_from_file_location('copy_chunk_bounds',Path(__file__).with_name('copy_chunk_bounds.py'))
                bounds=importlib.util.module_from_spec(spec);spec.loader.exec_module(bounds)
                bounds.run(p.q,p.c,chunk.rows if chunk else 1000)
        return result

    def compose_text():
        text=original_text()
        for service in ('source','destination','pgduck','storage'):
            text=re.sub(rf'^  {service}:$',f'  {service}:\n    cpuset: "{args.cpuset}"',text,flags=re.MULTILINE)
        return profile.compose_text(text) if profile else text

    paths=[Path(__file__),Path(__file__).with_name('checkpoint_split.sql'),Path(__file__).with_name('checkpoint_split_operations.py')]
    if profile:paths.append(Path(__file__).with_name('checkpoint_sql_profile.py'))
    if args.profile_staging:paths.append(Path(__file__).with_name('staging_cost.py'))
    paths.extend(Path(__file__).with_name(name) for name in ['staging_chunks.sql','staging_chunk_operations.py','copy_chunk_bounds.py'])
    if chunk:paths.extend(Path(__file__).with_name(name) for name in ['copy_chunk.py','copy_chunk_bounds.py'])
    inputs={str(path.relative_to(p.ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    p.m.diagnostic_sources=paths
    p.q.sql=sql;p.q.compose=compose;p.m.compose_text=compose_text
    scope=f'{__doc__} Variant={args.variant}; SQL overlay={args.variant=="split"}; SQL profiling={args.profile_sql}; copy chunk rows={args.copy_chunk_rows}.'
    p.__doc__=scope
    metadata=dict(scope=scope,variant=args.variant,sql_overlay=args.variant=='split',
        focused_operations=not args.skip_operations,production_migrations_changed=False,
        sql_profiling=args.profile_sql,
        staging_profiling=args.profile_staging,
        chunk_storage=args.chunk_store,
        pause_after_chunk_ms=args.chunk_pause_ms,
        initial_copy_override=chunk.metadata if chunk else None,
        service_cpu_affinity=args.cpuset,installations=installs,inputs=inputs)
    directory=p.ROOT/'.qualification'/(p.q.RUN+'_production_admission')
    metadata_path=directory/'checkpoint-split-prototype.json'
    sys.argv=[sys.argv[0],'--cases',*args.cases]
    code=p.main()
    metadata_path.write_text(json.dumps(metadata,indent=2)+'\n')
    result_path=p.q.RESULTS/'results.json'
    result=json.loads(result_path.read_text())
    result['scope']=scope;result['prototype']=metadata
    if profile and (profile.error or not profile.captured):
        result.update(status='error',error=profile.error or 'SQL profile was not captured');code=1
    for case in result['cases']:
        case['sql_overlay']=metadata['sql_overlay']
        case['initial_copy_override']=metadata['initial_copy_override']
        case['chunk_storage']=args.chunk_store
        case_path=p.q.RESULTS/case['case']/'results.json'
        case_path.write_text(json.dumps(case,indent=2)+'\n')
    result_path.write_text(json.dumps(result,indent=2)+'\n')
    return code


if __name__=='__main__':raise SystemExit(main())

#!/usr/bin/env python3
"""Local CPU-constrained staging diagnostics, not release qualification.

The default uses the archive unchanged. Optional modes add a per-copy pause
wrapper or an initial-sync Rust overlay with a shared byte coordinator.
Production sources and migrations are never edited by this driver.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys
import tempfile


def main():
    root=Path(__file__).resolve().parents[2]
    driver_source=Path(__file__).read_bytes()
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--copies',type=int,choices=[1,4],default=4)
    parser.add_argument('--cpuset',default='0-1',help='Shared source/destination/engine/storage CPU affinity')
    parser.add_argument('--source-cpuset',help='Optional separate source CPU affinity for topology controls')
    parser.add_argument('--storage-cpuset',help='Optional separate storage CPU affinity for topology controls')
    parser.add_argument('--budget-mib',type=int,help='Isolated shared byte-budget prototype; zero measures coordinator overhead only')
    parser.add_argument('--burst-kib',type=int,default=256,help='Shared diagnostic burst and maximum chunk size')
    parser.add_argument('--budget-operations',action='store_true',help='Run deterministic and cross-database budget checks before measurement')
    parser.add_argument('--pause-ms',type=int,default=0,help='Experimental pause after each 1000-row chunk; zero uses the unchanged image')
    args=parser.parse_args()
    for value in (args.cpuset,args.source_cpuset,args.storage_cpuset):
        if value is not None and not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',value):
            parser.error('CPU affinity must be a CPU index/range list')
    affinity={service:args.cpuset for service in ('source','destination','pgduck','storage')}
    affinity['source']=args.source_cpuset or args.cpuset
    affinity['storage']=args.storage_cpuset or args.cpuset
    if not 0 <= args.pause_ms <= 100:
        parser.error('pause-ms must be between 0 and 100')
    if args.budget_mib is not None and not 0 <= args.budget_mib <= 512:
        parser.error('budget-mib must be between 0 and 512')
    if not 1 <= args.burst_kib <= 8192:
        parser.error('burst-kib must be between 1 and 8192')
    if args.budget_mib is not None and args.pause_ms:
        parser.error('choose shared budget or per-copy pause, not both')
    if args.budget_operations and args.budget_mib is None:
        parser.error('budget-operations requires budget-mib')
    pause=args.pause_ms
    spec=importlib.util.spec_from_file_location('production_probe',root/'tests/admission/probe.py')
    p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
    original_text=p.m.compose_text
    original_fairness=p.m.fairness
    original_build=p.q.build_images
    original_compose=p.q.compose
    budget=None
    if args.budget_mib is not None:
        spec=importlib.util.spec_from_file_location('diagnostic_budget',Path(__file__).with_name('staging_budget.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        budget=module.Prototype(p.q,root,args.budget_mib,args.burst_kib)

    def compose(*values,**kwargs):
        if values and values[0]=='down' and budget and budget.installed and not budget.captured:
            budget.capture()
        result=original_compose(*values,**kwargs)
        if values and values[0]=='up' and budget and not budget.installed:
            budget.install()
            if args.budget_operations:
                budget.operations(p.c)
        return result

    def fairness(options,*values,**kwargs):
        options.abort_on_clock_step=True
        return original_fairness(options,*values,**kwargs)

    def compose_text():
        text=original_text()
        for service,cpuset in affinity.items():
            text=re.sub(rf'^  {service}:$',f'  {service}:\n    cpuset: "{cpuset}"',text,flags=re.MULTILINE)
        return text

    def build_images(archive,context):
        original_build(archive,context)
        if budget:
            budget.build()
            return
        if pause == 0:
            return
        with tempfile.TemporaryDirectory(prefix='lake-staging-pause-') as temp:
            directory=Path(temp)
            (directory/'initial-sync').write_text('#!/bin/sh\nexec env LAKE_INITIAL_COPY_CHUNK_PAUSE_MS='+str(pause)+' /usr/local/bin/lake-replication-initial-sync-unpaced "$@"\n')
            (directory/'Dockerfile').write_text('FROM '+p.q.DESTINATION_IMAGE+'\nUSER root\nRUN mv /usr/local/bin/lake-replication-initial-sync /usr/local/bin/lake-replication-initial-sync-unpaced\nCOPY --chmod=0755 initial-sync /usr/local/bin/lake-replication-initial-sync\n')
            p.q.command(['docker','build','-t',p.q.DESTINATION_IMAGE,str(directory)],timeout=300)

    p.m.compose_text=compose_text
    p.m.fairness=fairness
    p.q.build_images=build_images
    p.q.compose=compose
    sys.argv=[sys.argv[0],'--cases','fairness','--fairness-copies',str(args.copies)]
    code=p.main()
    metadata={'scope':__doc__,'image_overlay':pause>0 or budget is not None,'pause_ms_per_1000_row_chunk':pause,'service_cpu_affinity':affinity,
              'driver_sha256':hashlib.sha256(driver_source).hexdigest()}
    audit=None
    analysis_source=None
    if budget:
        metadata['shared_byte_budget']=budget.metadata()
        spec=importlib.util.spec_from_file_location('budget_results',Path(__file__).with_name('budget_results.py'))
        analysis=importlib.util.module_from_spec(spec);spec.loader.exec_module(analysis)
        analysis_source=hashlib.sha256(Path(analysis.__file__).read_bytes()).hexdigest()
        case_path=p.q.RESULTS/'fairness/results.json'
        if case_path.exists():
            observed=json.loads(case_path.read_text());observed['diagnostic']=metadata
            audit=analysis.audit(observed)
        else:
            audit={'valid':False,'error':'No fairness results available for budget audit'}
        metadata['shared_byte_budget']['audit']=audit
        print('BUDGET AUDIT', 'passed' if audit['valid'] else 'failed',flush=True)
        if not audit['valid']:
            code=1
    for path in (p.q.RESULTS/'results.json',p.q.RESULTS/'fairness/results.json'):
        if not path.exists():
            continue
        data=json.loads(path.read_text());data['diagnostic']=metadata
        if audit is not None and not audit['valid']:
            data['status']='error'
        data['scope']=__doc__;path.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
    if audit is not None and (p.q.RESULTS/'fairness/results.json').exists():
        audited={**audit,'analysis_source_sha256':analysis_source,
            'raw_results_sha256':hashlib.sha256((p.q.RESULTS/'fairness/results.json').read_bytes()).hexdigest()}
        (p.q.RESULTS/'fairness/budget-analysis.json').write_text(json.dumps(audited,indent=2)+'\n')
    (p.q.RESULTS/'diagnostic-driver.py').write_bytes(driver_source)
    return code


if __name__ == '__main__':
    raise SystemExit(main())

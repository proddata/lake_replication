#!/usr/bin/env python3
"""Local CPU-constrained staging diagnostics, not release qualification.

The default uses the archive unchanged. Optional modes add a per-copy pause,
a shared staging byte coordinator, receiver/supervisor observations, or a
transaction-scoped staging permit.
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
    parser.add_argument('--copies',type=int,choices=[0,1,4],default=4)
    parser.add_argument('--cpuset',default='0-1',help='Shared source/destination/engine/storage CPU affinity')
    parser.add_argument('--source-cpuset',help='Optional separate source CPU affinity for topology controls')
    parser.add_argument('--storage-cpuset',help='Optional separate storage CPU affinity for topology controls')
    parser.add_argument('--budget-mib',type=int,help='Isolated shared byte-budget prototype; zero measures coordinator overhead only')
    parser.add_argument('--burst-kib',type=int,default=256,help='Shared diagnostic burst and maximum chunk size')
    parser.add_argument('--budget-operations',action='store_true',help='Run deterministic and cross-database budget checks before measurement')
    parser.add_argument('--staging-gate-mode',choices=['serial','accounting'],default='serial',help='Accounting control omits locking but keeps identical append/counter SQL')
    parser.add_argument('--staging-gate',action='store_true',help='Serialize initial-copy append transactions using a separate diagnostic C lock')
    parser.add_argument('--staging-gate-operations',action='store_true',help='Check staging permit cleanup and native initial-copy cancellation')
    parser.add_argument('--receiver-batch',type=int,choices=[1,64],default=1,help='Isolated receiver lifetime; 64 requires cycle timing and guards eight unpublished transactions')
    parser.add_argument('--cycle-timing',action='store_true',help='Instrument supervisor phases and retain transaction/log timing; requires receiver-progress')
    parser.add_argument('--receiver-operations',action='store_true',help='Run receiver progress lifecycle checks before measurement')
    parser.add_argument('--receiver-progress',action='store_true',help='Build isolated receiver progress telemetry; requires stream pressure observation')
    parser.add_argument('--capture-stream-pressure',action='store_true',help='Compare destination queues against the fixture commit prefix; diagnostic only')
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
    if args.copies == 0 and (args.budget_mib is not None or args.pause_ms):
        parser.error('streaming-only control cannot include copy pacing')
    if args.receiver_progress and (not args.capture_stream_pressure or args.budget_mib is not None or args.pause_ms):
        parser.error('receiver-progress requires capture-stream-pressure and cannot combine with pacing overlays')
    if args.receiver_operations and not args.receiver_progress:
        parser.error('receiver-operations requires receiver-progress')
    if args.cycle_timing and not args.receiver_progress:
        parser.error('cycle-timing requires receiver-progress')
    if args.receiver_batch>1 and not args.cycle_timing:
        parser.error('receiver-batch 64 requires cycle-timing')
    if args.staging_gate and (not args.receiver_progress or args.copies==0):
        parser.error('staging-gate requires receiver-progress and at least one large copy')
    if args.staging_gate_operations and not args.staging_gate:
        parser.error('staging-gate-operations requires staging-gate')
    if args.staging_gate_operations and args.staging_gate_mode!='serial':
        parser.error('staging-gate-operations requires serial mode')
    if args.staging_gate_mode!='serial' and not args.staging_gate:
        parser.error('staging-gate-mode requires staging-gate')
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

    gate=None
    if args.staging_gate:
        spec=importlib.util.spec_from_file_location('staging_gate',Path(__file__).with_name('staging_gate.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        gate=module.Prototype(p.q,root,args.staging_gate_mode)
    original_sql=p.q.sql
    def sql(service,database,statement,**kwargs):
        result=original_sql(service,database,statement,**kwargs)
        if gate and service=='destination' and 'CREATE EXTENSION lake_replication' in statement:
            gate.install(database,original_sql)
        return result

    receiver=None
    if args.receiver_progress:
        spec=importlib.util.spec_from_file_location('receiver_progress',Path(__file__).with_name('receiver_progress.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        receiver=module.Prototype(p.q,root,args.cycle_timing,args.receiver_batch,gate)

    def compose(*values,**kwargs):
        if values and values[0]=='down' and gate and gate.installed and not gate.captured:
            gate.capture()
        if values and values[0]=='down' and receiver and receiver.installed and not receiver.captured:
            receiver.capture()
        if values and values[0]=='down' and budget and budget.installed and not budget.captured:
            budget.capture()
        result=original_compose(*values,**kwargs)
        if values and values[0]=='up' and gate and 'analytics' not in gate.installed:
            gate.install('analytics',original_sql)
            if args.staging_gate_operations:
                gate.operations(p.c)
        if values and values[0]=='up' and receiver and not receiver.installed:
            receiver.install()
            if args.receiver_operations:
                spec=importlib.util.spec_from_file_location('receiver_operations',Path(__file__).with_name('receiver_operations.py'))
                operations=importlib.util.module_from_spec(spec);spec.loader.exec_module(operations)
                operations.run(receiver,p.c)
        if values and values[0]=='up' and budget and not budget.installed:
            budget.install()
            if args.budget_operations:
                budget.operations(p.c)
        return result

    def fairness(options,*values,**kwargs):
        options.abort_on_clock_step=True
        options.capture_receiver_progress=bool(receiver)
        options.capture_staging_gate=bool(gate)
        if args.capture_stream_pressure:
            spec=importlib.util.spec_from_file_location('pressure',Path(__file__).with_name('pressure.py'))
            pressure=importlib.util.module_from_spec(spec);spec.loader.exec_module(pressure)
            options.pressure_observer=pressure
        return original_fairness(options,*values,**kwargs)

    def compose_text():
        text=original_text()
        for service,cpuset in affinity.items():
            text=re.sub(rf'^  {service}:$',f'  {service}:\n    cpuset: "{cpuset}"',text,flags=re.MULTILINE)
        return text

    def build_images(archive,context):
        original_build(archive,context)
        if receiver:
            receiver.build()
            return
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

    p.m.diagnostic_sources=[Path(__file__).with_name('pressure.py')] if args.capture_stream_pressure else []
    if receiver:
        p.m.diagnostic_sources.extend(root/'tests/admission'/name for name in ['receiver_operations.py','receiver_results.py'])
        p.m.diagnostic_sources.extend(root/'tests/admission'/name for name in receiver.inputs)
    if args.cycle_timing:
        p.m.diagnostic_sources.append(root/'tests/admission/cycle_results.py')
    if gate:
        p.m.diagnostic_sources.extend(root/'tests/admission'/name for name in gate.inputs)
    p.m.compose_text=compose_text
    p.m.fairness=fairness
    p.q.build_images=build_images
    p.q.compose=compose
    p.q.sql=sql
    sys.argv=[sys.argv[0],'--cases','fairness','--fairness-copies',str(args.copies)]
    code=p.main()
    metadata={'scope':__doc__,'image_overlay':pause>0 or budget is not None or receiver is not None,'pause_ms_per_1000_row_chunk':pause,'service_cpu_affinity':affinity,
              'capture_stream_pressure':args.capture_stream_pressure,
              'driver_sha256':hashlib.sha256(driver_source).hexdigest()}
    if gate:
        metadata['staging_gate']=gate.metadata()
    if receiver:
        metadata['receiver_progress']=receiver.metadata()
    receiver_audit=None
    receiver_analysis_source=None
    if receiver:
        spec=importlib.util.spec_from_file_location('receiver_results',Path(__file__).with_name('receiver_results.py'))
        analysis=importlib.util.module_from_spec(spec);spec.loader.exec_module(analysis)
        receiver_analysis_source=hashlib.sha256(Path(analysis.__file__).read_bytes()).hexdigest()
        case_path=p.q.RESULTS/'fairness/results.json'
        observed=json.loads(case_path.read_text()) if case_path.exists() else {}
        observed['diagnostic']=metadata
        receiver_audit=analysis.audit(observed)
        if args.cycle_timing:
            try:
                spec=importlib.util.spec_from_file_location('cycle_results',Path(__file__).with_name('cycle_results.py'))
                cycles=importlib.util.module_from_spec(spec);spec.loader.exec_module(cycles)
                cycle_audit=cycles.analyze(observed,
                    json.loads((p.q.RESULTS/'cycle-transactions.json').read_text()),
                    json.loads((p.q.RESULTS/'cycle-worker-logs.json').read_text()))
            except Exception as error:cycle_audit={'valid':False,'error':str(error)}
            cycle_audit['analysis_source_sha256']=hashlib.sha256(Path(__file__).with_name('cycle_results.py').read_bytes()).hexdigest()
            metadata['receiver_progress']['cycle_analysis']=cycle_audit
            receiver_audit['checks']['cycle_timing_capture_valid']=cycle_audit['valid']
            receiver_audit['valid'] &= cycle_audit['valid']
        if gate:
            try:
                gate_audit=gate.audit(observed)
            except Exception as error:gate_audit={'valid':False,'error':str(error)}
            metadata['staging_gate']['audit']=gate_audit
            receiver_audit['checks']['staging_gate_valid']=gate_audit['valid']
            receiver_audit['valid'] &= gate_audit['valid']
        metadata['receiver_progress']['audit']=receiver_audit
        print('RECEIVER AUDIT', 'passed' if receiver_audit['valid'] else 'failed',flush=True)
        if not receiver_audit['valid']:code=1
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
        if (audit is not None and not audit['valid']) or (receiver_audit is not None and not receiver_audit['valid']):
            data['status']='error'
        data['scope']=__doc__;path.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
    if audit is not None and (p.q.RESULTS/'fairness/results.json').exists():
        audited={**audit,'analysis_source_sha256':analysis_source,
            'raw_results_sha256':hashlib.sha256((p.q.RESULTS/'fairness/results.json').read_bytes()).hexdigest()}
        (p.q.RESULTS/'fairness/budget-analysis.json').write_text(json.dumps(audited,indent=2)+'\n')
    if receiver_audit is not None and (p.q.RESULTS/'fairness/results.json').exists():
        result={**receiver_audit,'analysis_source_sha256':receiver_analysis_source,
            'raw_results_sha256':hashlib.sha256((p.q.RESULTS/'fairness/results.json').read_bytes()).hexdigest()}
        (p.q.RESULTS/'fairness/receiver-audit.json').write_text(json.dumps(result,indent=2)+'\n')
    (p.q.RESULTS/'diagnostic-driver.py').write_bytes(driver_source)
    return code


if __name__ == '__main__':
    raise SystemExit(main())

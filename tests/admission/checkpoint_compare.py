#!/usr/bin/env python3
"""Fixed control/treatment/control comparison on one host; not release qualification.

Every workload gate and failed observation is retained. Findings do not stop the
sequence; fixture/provenance errors do. No best-of or automatic retries.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]
SEQUENCE=('original','split','original')


def summarize(result):
    """Keep validity separate from performance so a slow control isn't hidden."""
    if result['status'] not in ('passed','completed_with_findings'):
        raise ValueError('fixture did not complete: '+result.get('error',result['status']))
    if len(result['cases'])!=1 or result['cases'][0]['case']!='fairness':
        raise ValueError('expected exactly one fairness case')
    case=result['cases'][0]
    if case['status'] not in ('passed','finding'):
        raise ValueError('fairness fixture did not complete')
    if not result['checks'] or not all(c['passed'] for c in result['checks']):
        raise ValueError('archive or exact-data assertion failed')
    if len(case['streams'])!=3:
        raise ValueError('expected three measured streams')
    if case.get('cleanup_returncode')!=0:
        raise ValueError('fixture cleanup failed')
    failures=[key for key,value in case['checks'].items() if not value]
    if not case['timing_validation']['valid']:failures.append('observer_clock_validation')
    if case['engine_oom']:failures.append('engine_oom')
    if case['unexpected_pg_crash']:failures.append('unexpected_pg_crash')
    for stream in case['streams']:
        failures.extend(stream['database']+':'+key for key,value in stream['checks'].items() if not value)
    valid=(case['timing_validation']['valid'] and not case['engine_oom']
        and not case['unexpected_pg_crash'] and case['checks']['publication_overlap_observed']
        and all(s['checks']['source_schedule_within_one_interval'] for s in case['streams']))
    return dict(status=result['status'],measurement_valid=valid,
        source_producer=case.get('source_producer','legacy_serial_session'),
        observer_clock_sampling=case.get('observer_clock_sampling','legacy_batch_start'),
        workload_passed=result['status']=='passed' and case['status']=='passed' and not failures and valid,
        failed_gates=failures,copy_elapsed_seconds=case['copy_elapsed_seconds'],
        timing_validation=case['timing_validation'],engine_oom=case['engine_oom'],
        unexpected_pg_crash=case['unexpected_pg_crash'],
        streams=[{k:s[k] for k in ['database','p95_publication_ms','latency_phases_p95_ms',
            'source_max_schedule_lateness_seconds','checks']} for s in case['streams']])


def compare(runs,sequence=SEQUENCE):
    if len(runs)!=3 or tuple(r['variant'] for r in runs)!=tuple(sequence):
        raise ValueError('expected fixed control/treatment/control order')
    regimes={r.get('source_producer','legacy_serial_session') for r in runs}
    if len(regimes)!=1:raise ValueError('source producer regime changed within comparison')
    clocks={r.get('observer_clock_sampling','legacy_batch_start') for r in runs}
    if len(clocks)!=1:raise ValueError('observer clock sampling changed within comparison')
    before,split,after=runs
    result=dict(measurement_valid=all(r['measurement_valid'] for r in runs),
        source_producer=next(iter(regimes)),
        observer_clock_sampling=next(iter(clocks)),
        candidate_workload_passed=split['workload_passed'],
        all_workloads_passed=all(r['workload_passed'] for r in runs),
        control_copy_drift_ratio=after['copy_elapsed_seconds']/before['copy_elapsed_seconds'],
        candidate_copy_ratio_to_control_mean=split['copy_elapsed_seconds']/
            ((before['copy_elapsed_seconds']+after['copy_elapsed_seconds'])/2),
        inference_limit='One sequential comparison; host drift is bracketed, not eliminated. No statistical speedup or release qualification is claimed.')
    if tuple(sequence)==SEQUENCE:
        result['original_copy_drift_ratio']=result['control_copy_drift_ratio']
        result['split_copy_ratio_to_original_mean']=result['candidate_copy_ratio_to_control_mean']
    return result


def runtime_identity(observed,variant,cpuset,chunk_rows=None):
    metadata=observed['prototype']
    if (metadata['variant']!=variant or metadata['sql_overlay']!=(variant=='split')
        or metadata['focused_operations'] or metadata.get('sql_profiling')
        or metadata['service_cpu_affinity']!=cpuset):
        raise ValueError('child variant/setup declaration does not match')
    identity=dict(archive=observed['archive'],inputs=observed['inputs'])
    override=metadata.get('initial_copy_override')
    if chunk_rows is None:
        if override:raise ValueError('unexpected copy configuration overlay')
        identity['image_id']=observed['image_id']
    else:
        if (not override or override['chunk_rows']!=chunk_rows
            or override['chunk_row_text_bytes']!=8388608):
            raise ValueError('copy row/byte budget does not match')
        identity.update(base_destination_image_id=override['base_destination_image_id'],
            archived_worker_sha256=override['worker_sha256'])
    return identity


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpuset',default='0-1')
    parser.add_argument('--copy-chunk-rows',type=int,choices=[2000,4000],help='Compare 1000/candidate/1000 rows with split checkpoints throughout; byte budget unchanged')
    args=parser.parse_args()
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',args.cpuset):
        parser.error('invalid CPU affinity')
    chunk_caps=(1000,args.copy_chunk_rows,1000) if args.copy_chunk_rows else (None,None,None)
    sequence=tuple(f'rows_{n}' for n in chunk_caps) if args.copy_chunk_rows else SEQUENCE
    variants=('split',)*3 if args.copy_chunk_rows else SEQUENCE
    suffix='copy_chunk_comparison' if args.copy_chunk_rows else 'checkpoint_comparison'
    directory=ROOT/'.qualification'/(datetime.now(timezone.utc).strftime('qualification_%Y%m%d_%H%M%S_')+suffix)
    directory.mkdir(parents=True)
    result=dict(scope=__doc__,status='running',sequence=list(sequence),shared_cpu_affinity=args.cpuset,
        experiment='copy_chunk_rows' if args.copy_chunk_rows else 'checkpoint_partition',
        copy_chunk_caps=list(chunk_caps),
        runs=[],runner_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (directory/'checkpoint_compare.py').write_bytes(Path(__file__).read_bytes())
    def save():(directory/'results.json').write_text(json.dumps(result,indent=2)+'\n')
    save()
    common=None
    try:
        for index,(label,variant,chunk_rows) in enumerate(zip(sequence,variants,chunk_caps)):
            print(f'COMPARISON {index+1}/3 {label}',flush=True)
            result['current_stage']=dict(index=index+1,variant=label)
            save()
            command=[sys.executable,str(Path(__file__).with_name('checkpoint_split_probe.py')),
                '--cases','fairness','--skip-operations','--variant',variant,'--cpuset',args.cpuset]
            if chunk_rows:command.extend(['--copy-chunk-rows',str(chunk_rows)])
            log=directory/f'{index+1}-{label}.log'
            with log.open('w') as output:
                child=subprocess.Popen(command,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
                for line in child.stdout:
                    output.write(line);output.flush();print(line,end='',flush=True)
                child.wait()
            matches=re.findall(r'^Evidence: (.+)$',log.read_text(),re.MULTILINE)
            if len(matches)!=1:raise ValueError('missing unique child evidence path')
            evidence=Path(matches[0]).resolve()
            if not evidence.is_relative_to(ROOT/'.qualification'):raise ValueError('unexpected evidence path')
            path=evidence/'results.json';observed=json.loads(path.read_text())
            metadata=observed['prototype']
            identity=runtime_identity(observed,variant,args.cpuset,chunk_rows)
            if common is None:common=identity;result['common_inputs']=identity
            elif common!=identity:raise ValueError('archive, base runtime or harness changed between observations')
            summary=summarize(observed)
            expected_code=0 if observed['status']=='passed' else 1
            if child.returncode!=expected_code:raise ValueError('child status and process exit disagree')
            result['runs'].append(dict(variant=label,checkpoint_variant=variant,
                initial_copy_override=metadata.get('initial_copy_override'),image_id=observed['image_id'],
                evidence=str(evidence.relative_to(ROOT)),
                results_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),**summary))
            save()
        result['comparison']=compare(result['runs'],sequence)
        result.pop('current_stage',None)
        result['status']='passed' if result['comparison']['all_workloads_passed'] else 'completed_with_findings'
    except Exception as error:
        result.update(status='error',error=str(error))
        print('COMPARISON ERROR:',error,flush=True)
    finally:
        save();print('Comparison evidence:',directory,flush=True)
    return int(result['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

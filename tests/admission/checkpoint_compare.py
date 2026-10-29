#!/usr/bin/env python3
"""Fixed original/split/original comparison on one host; not release qualification.

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
    for stream in case['streams']:
        failures.extend(stream['database']+':'+key for key,value in stream['checks'].items() if not value)
    valid=(case['timing_validation']['valid'] and not case['engine_oom']
        and not case['unexpected_pg_crash'] and case['checks']['publication_overlap_observed']
        and all(s['checks']['source_schedule_within_one_interval'] for s in case['streams']))
    return dict(status=result['status'],measurement_valid=valid,
        workload_passed=result['status']=='passed' and case['status']=='passed' and not failures and valid,
        failed_gates=failures,copy_elapsed_seconds=case['copy_elapsed_seconds'],
        timing_validation=case['timing_validation'],engine_oom=case['engine_oom'],
        unexpected_pg_crash=case['unexpected_pg_crash'],
        streams=[{k:s[k] for k in ['database','p95_publication_ms','latency_phases_p95_ms',
            'source_max_schedule_lateness_seconds','checks']} for s in case['streams']])


def compare(runs):
    if len(runs)!=3 or tuple(r['variant'] for r in runs)!=SEQUENCE:
        raise ValueError('expected fixed original/split/original order')
    before,split,after=runs
    return dict(measurement_valid=all(r['measurement_valid'] for r in runs),
        candidate_workload_passed=split['workload_passed'],
        all_workloads_passed=all(r['workload_passed'] for r in runs),
        original_copy_drift_ratio=after['copy_elapsed_seconds']/before['copy_elapsed_seconds'],
        split_copy_ratio_to_original_mean=split['copy_elapsed_seconds']/
            ((before['copy_elapsed_seconds']+after['copy_elapsed_seconds'])/2),
        inference_limit='One sequential comparison; host drift is bracketed, not eliminated. No statistical speedup or release qualification is claimed.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpuset',default='0-1')
    args=parser.parse_args()
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',args.cpuset):
        parser.error('invalid CPU affinity')
    directory=ROOT/'.qualification'/datetime.now(timezone.utc).strftime('qualification_%Y%m%d_%H%M%S_checkpoint_comparison')
    directory.mkdir(parents=True)
    result=dict(scope=__doc__,status='running',sequence=list(SEQUENCE),shared_cpu_affinity=args.cpuset,
        runs=[],runner_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (directory/'checkpoint_compare.py').write_bytes(Path(__file__).read_bytes())
    def save():(directory/'results.json').write_text(json.dumps(result,indent=2)+'\n')
    save()
    common=None
    try:
        for index,variant in enumerate(SEQUENCE):
            print(f'COMPARISON {index+1}/3 {variant}',flush=True)
            result['current_stage']=dict(index=index+1,variant=variant)
            save()
            command=[sys.executable,str(Path(__file__).with_name('checkpoint_split_probe.py')),
                '--cases','fairness','--skip-operations','--variant',variant,'--cpuset',args.cpuset]
            log=directory/f'{index+1}-{variant}.log'
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
            if (metadata['variant']!=variant or metadata['sql_overlay']!=(variant=='split')
                or metadata['focused_operations'] or metadata.get('sql_profiling')
                or metadata['service_cpu_affinity']!=args.cpuset):
                raise ValueError('child variant/setup declaration does not match')
            identity=dict(archive=observed['archive'],image_id=observed['image_id'],inputs=observed['inputs'])
            if common is None:common=identity;result['common_inputs']=identity
            elif common!=identity:raise ValueError('archive, container image or harness changed between observations')
            summary=summarize(observed)
            expected_code=0 if observed['status']=='passed' else 1
            if child.returncode!=expected_code:raise ValueError('child status and process exit disagree')
            result['runs'].append(dict(variant=variant,evidence=str(evidence.relative_to(ROOT)),
                results_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),**summary))
            save()
        result['comparison']=compare(result['runs'])
        result.pop('current_stage',None)
        result['status']='passed' if result['comparison']['all_workloads_passed'] else 'completed_with_findings'
    except Exception as error:
        result.update(status='error',error=str(error))
        print('COMPARISON ERROR:',error,flush=True)
    finally:
        save();print('Comparison evidence:',directory,flush=True)
    return int(result['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

#!/usr/bin/env python3
"""Fixed GUC comparison on one host, unchanged archive and workload.

Configuration diagnostic only. Keep every validity/performance finding; never
retry or choose a best run. No SQL schema/function or executable overlay.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

from checkpoint_compare import compare, summarize

ROOT=Path(__file__).resolve().parents[2]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpuset',default='0-1')
    parser.add_argument('--mode',choices=['pacing','toast'],required=True)
    args=parser.parse_args()
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',args.cpuset):parser.error('invalid CPU affinity')
    sequence=(40,50,40) if args.mode=='pacing' else ('pglz','lz4','pglz')
    labels=tuple(str(value) for value in sequence)
    root=ROOT/'.qualification'/datetime.now(timezone.utc).strftime('copy_configuration_%Y%m%d_%H%M%S')
    root.mkdir(parents=True)
    report=dict(scope=__doc__,mode=args.mode,sequence=list(sequence),cpuset=args.cpuset,runs=[],status='running',configuration_only=True,sql_overlay=False,binary_overlay=False,runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),summary_sha256=hashlib.sha256(Path(__file__).with_name('checkpoint_compare.py').read_bytes()).hexdigest())
    shutil.copy2(__file__,root/'copy_configuration_compare.py')
    shutil.copy2(Path(__file__).with_name('checkpoint_compare.py'),root/'checkpoint_compare.py')
    common=None
    try:
        for index,value in enumerate(sequence):
            pause=value if args.mode=='pacing' else 40
            compression=None if args.mode=='pacing' else value
            before=set((ROOT/'.qualification').glob('*production_admission/results.json'))
            with (root/f'{index+1}-{value}.log').open('w') as log:
                command=[sys.executable,str(Path(__file__).with_name('probe.py')),'--cases','fairness','--cpuset',args.cpuset,'--initial-copy-pause-ms',str(pause)]
                if compression is not None:command.extend(['--toast-compression',compression])
                code=subprocess.call(command,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
            created=set((ROOT/'.qualification').glob('*production_admission/results.json'))-before
            if len(created)!=1:raise RuntimeError('ambiguous comparison evidence')
            path=created.pop();observed=json.loads(path.read_text())
            if code!=(0 if observed['status']=='passed' else 1):raise RuntimeError('child process and result status disagree')
            identity=dict(archive=observed.get('archive'),image_id=observed.get('image_id'),inputs=observed.get('inputs'))
            if common is None:common=identity;report['common_identity']=identity
            elif identity!=common:raise RuntimeError('archive/image/harness changed within comparison')
            if not observed.get('configuration_diagnostic') or (observed['parameters']['initial_copy_pause_ms']!=pause or observed['parameters']['toast_compression']!=compression):raise RuntimeError('incorrect diagnostic configuration')
            run=dict(variant=str(value),returncode=code,evidence=str(path.relative_to(ROOT)),results_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            run.update(summarize(observed))
            case=observed['cases'][0]
            if set(case['copy_pacing_configuration'].values())!={pause} or set(case['copy_pacing_worker_values'])!={pause}:raise RuntimeError('declared pacing not observed')
            run['observed_worker_pauses']=case['copy_pacing_worker_values']
            if compression is not None:
                if set(case['toast_configuration'].values())!={compression} or set(case['copy_compression_observed'].values())!={compression}:raise RuntimeError('declared compression not observed')
                run['observed_compression']=case['copy_compression_observed']
            report['runs'].append(run)
            (root/'results.json').write_text(json.dumps(report,indent=2)+'\n')
            print(index+1,value,run['status'],run['copy_elapsed_seconds'],flush=True)
        report['comparison']=compare(report['runs'],sequence=labels)
        report['status']='passed' if report['comparison']['all_workloads_passed'] else 'completed_with_findings'
    except Exception as error:
        report.update(status='error',error=str(error))
        print('ERROR:',error,flush=True)
    finally:
        (root/'results.json').write_text(json.dumps(report,indent=2)+'\n')
        print('Evidence:',root,flush=True)
    # Every control finding remains a failed workflow gate. The report exposes
    # candidate absolute gates separately without promoting archived defaults.
    return int(report['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

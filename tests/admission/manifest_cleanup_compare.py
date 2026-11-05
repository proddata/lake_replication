#!/usr/bin/env python3
"""Fixed control/candidate/control for a guarded pg_lake cleanup prototype."""
from datetime import datetime,timezone
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
from checkpoint_compare import compare,summarize

ROOT=Path(__file__).resolve().parents[2]


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--cpuset',default='0')
    parser.add_argument('--correctness-only',action='store_true',help='Validate automated preflight without repeating the performance cohort')
    args=parser.parse_args()
    root=ROOT/'.qualification'/datetime.now(timezone.utc).strftime('manifest_cleanup_comparison_%Y%m%d_%H%M%S');root.mkdir(parents=True)
    report=dict(scope=__doc__,status='running',runs=[],binary_overlay=True,release_qualification=False,correctness_only=args.correctness_only)
    for file in [Path(__file__),Path(__file__).with_name('checkpoint_compare.py')]:shutil.copy2(file,root/file.name)
    common=None;sequence=('control','candidate','control')
    try:
        # Native runs need the same safety checks as the local prototype before
        # measuring the candidate. These checks are outside workload clocks.
        with (root/'avro-count-check.json').open('w') as output:
            subprocess.run([sys.executable,str(Path(__file__).with_name('manifest_count_probe.py'))],cwd=ROOT,stdout=output,check=True)
        checks=[];snapshots=[]
        for variant in ('control','candidate'):
            before=set((ROOT/'.qualification').glob('*manifest_cleanup/results.json'))
            with (root/f'correctness-{variant}.log').open('w') as log:
                code=subprocess.call([sys.executable,str(Path(__file__).with_name('manifest_cleanup_probe.py')),'--variant',variant,'--case','correctness'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
            created=set((ROOT/'.qualification').glob('*manifest_cleanup/results.json'))-before
            if len(created)!=1:raise ValueError('ambiguous correctness evidence')
            path=created.pop();raw=json.loads(path.read_text())
            if code or raw['status']!='passed' or not all(c['passed'] for c in raw['checks']):raise ValueError('correctness fixture failed')
            states=path.with_name('snapshots.json');snapshots.append(json.loads(states.read_text()))
            checks.append(dict(variant=variant,evidence=str(path.relative_to(ROOT)),results_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                snapshots_sha256=hashlib.sha256(states.read_bytes()).hexdigest(),skip_witness_count=raw['skip_witness_count'],libraries=raw['manifest_cleanup']['overlay']['profiled_libraries']))
        if snapshots[0]!=snapshots[1] or checks[0]['libraries']!=checks[1]['libraries']:raise ValueError('correctness states or libraries differ')
        report['correctness']=dict(runs=checks,canonical_snapshots_equal=True)
        if args.correctness_only:report['status']='passed';return 0
        for index,variant in enumerate(sequence):
            before=set((ROOT/'.qualification').glob('*production_admission/results.json'))
            with (root/f'{index+1}-{variant}.log').open('w') as log:
                code=subprocess.call([sys.executable,str(Path(__file__).with_name('manifest_cleanup_probe.py')),'--variant',variant,'--cpuset',args.cpuset],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
            created=set((ROOT/'.qualification').glob('*production_admission/results.json'))-before
            if len(created)!=1:raise ValueError('ambiguous workload evidence')
            path=created.pop();raw=json.loads(path.read_text());metadata=raw['manifest_cleanup']
            if code!=(0 if raw['status']=='passed' else 1):raise ValueError('exit code differs from result status')
            if metadata['variant']!=variant or metadata['timing_instrumentation']:raise ValueError('incorrect variant or timing instrumentation')
            identity=dict(archive=raw['archive'],image_id=raw['image_id'],inputs=raw['inputs'],overlay=metadata['overlay'])
            if common is None:common=identity;report['common_identity']=identity
            elif common!=identity:raise ValueError('archive, image, harness or overlay changed within cohort')
            for point in ['before','after']:
                inventory=raw['cases'][0][point]
                if set(inventory['manifest_cleanup_setting'].values())!={metadata['setting']} or set(inventory['commit_timing_setting'].values())!={'off'}:
                    raise ValueError('declared settings not observed')
            if 'lake-commit|' in (path.parent/'fairness/compose.log').read_text():raise ValueError('timing logs enabled in performance cohort')
            row=dict(variant=variant,evidence=str(path.relative_to(ROOT)),results_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),**summarize(raw));report['runs'].append(row)
            (root/'results.json').write_text(json.dumps(report,indent=2)+'\n');print(index+1,variant,row['status'],row['copy_elapsed_seconds'],flush=True)
        report['comparison']=compare(report['runs'],sequence=sequence)
        report['status']='passed' if report['comparison']['all_workloads_passed'] else 'completed_with_findings'
    except Exception as error:report.update(status='error',error=str(error));print('ERROR',error,flush=True)
    finally:
        (root/'results.json').write_text(json.dumps(report,indent=2)+'\n');print('Evidence:',root,flush=True)
    return int(report['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

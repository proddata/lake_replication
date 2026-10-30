#!/usr/bin/env python3
"""Diagnostic four-slot streaming pool on the production beta.3 bulk candidate."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
from pathlib import Path
import shutil
import tempfile

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('production_probe',ROOT/'tests/admission/probe.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
q,c,m,s=p.q,p.c,p.m,p.s
VIEW='lake_replication.prototype_stream_admission'


def locks():
    clients=[c.Psql('destination','analytics') for _ in range(5)]
    pool=ThreadPoolExecutor(max_workers=1)
    try:
        for connection in clients[:4]:connection.query('BEGIN; SELECT lake_replication.experiment_admit_stream(); SELECT lake_replication.experiment_admit_stream()')
        q.check('pool admits four nested/reentrant transactions',q.sql('destination','analytics',f"SELECT count(*) FROM {VIEW} WHERE admission_state='admitted'"),'4')
        clients[4].query('BEGIN');pid=clients[4].query('SELECT pg_backend_pid()')[0]
        pending=pool.submit(clients[4].query,'SELECT lake_replication.experiment_admit_stream()')
        q.check('fifth stream queues',q.wait_for('destination','analytics',f"SELECT count(*) FROM {VIEW} WHERE admission_state='waiting_for_stream'",'1'),'1')
        q.sql('destination','analytics',f'SELECT pg_cancel_backend({pid})')
        try:pending.result(timeout=10)
        except RuntimeError as error:q.check('queued stream cancellation propagates','canceling statement' in str(error),True)
        else:raise AssertionError('stream cancellation did not abort')
        for connection in clients[:4]:connection.query('ROLLBACK')
        q.check('stream pool drains after rollback',q.sql('destination','analytics',f'SELECT count(*) FROM {VIEW}'),'0')
    finally:
        for connection in clients:connection.close()
        pool.shutdown(wait=True,cancel_futures=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repetitions',type=int,default=3)
    parser.add_argument('--cases',nargs='+',choices=['locks','scale','fairness'],default=['locks','scale','fairness'])
    args=parser.parse_args()
    if 'PG_FUNCTION_INFO_V1(lake_replication_admit_stream)' in (ROOT/'extensions/lake_replication/lake_replication.c').read_text():
        parser.error('historical overlay requires its retained pre-adoption source snapshot; use tests/admission/probe.py for production packages')
    if q.VERSION != '0.1.0-beta.3':parser.error('stream pool diagnostic requires the beta.3 bulk-only candidate')
    if not 1 <= args.repetitions <= 6:parser.error('use one to six diagnostic repetitions')
    root=ROOT/'.qualification'/(q.RUN+'_stream_pool');root.mkdir(parents=True);q.RESULTS=root
    q.PROJECT+='-stream-pool';q.SOURCE_IMAGE+='-stream-pool';q.DESTINATION_IMAGE+='-stream-pool'
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE,QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    result=dict(status='running',scope=__doc__,cases=[],parameters=vars(args),production_changed=False)
    paths=[Path(__file__),Path(__file__).with_name('stream_pool.c.inc'),Path(__file__).with_name('stream_pool.sql'),Path(__file__).with_name('Dockerfile.stream-pool'),ROOT/'experiments/admission/scale.py',ROOT/'experiments/admission/admission_measure.py',ROOT/'experiments/admission/compare.py',ROOT/'tests/admission/probe.py']
    paths.append(ROOT/'experiments/admission/source_workload.py')
    result['inputs']={str(x.relative_to(ROOT)):hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}
    for x in paths:
        target=root/'sources'/x.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(x,target)
    try:
        archive=q.archive_path();q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-stream-pool-') as temp:q.build_images(archive,Path(temp))
        image='lake-replication-stream-pool:'+q.RUN
        build=q.command(['docker','build','--build-arg',f'QUAL_IMAGE={q.DESTINATION_IMAGE}','-t',image,'-f',str(Path(__file__).with_name('Dockerfile.stream-pool')),str(ROOT)],timeout=600)
        (root/'build.log').write_text(build.stdout+build.stderr);result['image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip()
        text=m.compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n','')
        if 'locks' in args.cases:
            directory=root/'locks';directory.mkdir();q.COMPOSE_FILE=directory/'compose.yml';q.COMPOSE_FILE.write_text(text);q.ENV['QUALIFICATION_DESTINATION_IMAGE']=image
            try:
                q.compose('up','-d','--wait','--wait-timeout','300',timeout=600);locks()
            finally:
                logs=q.compose('logs','--no-color',check_result=False);(directory/'compose.log').write_text(logs.stdout+logs.stderr);q.compose('down','-v','--remove-orphans',timeout=300)
        for repetition in range(1,(args.repetitions if 'scale' in args.cases else 0)+1):
            print('CASE scale',repetition,flush=True)
            case=s.scale_case(4,16,64,image,text,root/f'scale-{repetition}')
            result['cases'].append({**case,'case':'scale','repetition':repetition});s.write_json(root/'results.json',result)
            print('RESULT scale',repetition,case['status'],flush=True)
            if case['status']!='passed':break
        if 'fairness' in args.cases:
            print('CASE fairness',flush=True)
            params=argparse.Namespace(copy_rows=2000000,stream_transactions=120,bulk_copies=4,
                seed_after_subscriptions=False,admission_view='lake_replication.bulk_admission')
            case=m.fairness(params,'bulk_only',image,text,root/'fairness')
            case['timing_validation']=m.fairness_timing_validation(case)
            if not case['timing_validation']['valid']:case['status']='finding'
            result['cases'].append({**case,'case':'fairness','variant':'stream_pool'})
            print('RESULT fairness',case['status'],flush=True)
        expected=(args.repetitions if 'scale' in args.cases else 0)+int('fairness' in args.cases)
        result['status']='passed' if len(result['cases'])==expected and all(x['status']=='passed' for x in result['cases']) else 'completed_with_findings'

    except Exception as error:result.update(status='error',error=str(error));print('ERROR:',error,flush=True)
    finally:
        result['archive_provenance']=q.EVIDENCE;s.write_json(root/'results.json',result);print('Evidence:',root,flush=True)
    return int(result['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

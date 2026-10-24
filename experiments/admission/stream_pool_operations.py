#!/usr/bin/env python3
"""Cross-database cleanup, busy-pool progress and mixed-lane abort diagnostics."""
import argparse
import hashlib
import importlib.util
from pathlib import Path
import shutil
import tempfile

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('stream_pool_diagnostic',Path(__file__).with_name('stream_pool_probe.py'))
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
q,c,m,s=p.q,p.c,p.m,p.s
case_spec=importlib.util.spec_from_file_location('pool_behavior',ROOT/'tests/admission/pool_cases.py')
cases=importlib.util.module_from_spec(case_spec);case_spec.loader.exec_module(cases)
cases.q,cases.c=q,c
cases.VIEW=p.VIEW
cases.STREAM='SELECT lake_replication.experiment_admit_stream()'
CLIENTS=cases.CLIENTS
aborts,progress,mixed=cases.aborts,cases.progress,cases.mixed


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cases',nargs='+',choices=['aborts','progress','mixed'],default=['aborts','progress','mixed'])
    args=parser.parse_args()
    if 'PG_FUNCTION_INFO_V1(lake_replication_admit_stream)' in (ROOT/'extensions/lake_replication/lake_replication.c').read_text():
        parser.error('historical overlay requires its retained pre-adoption source snapshot; use tests/admission/probe.py for production packages')
    if q.VERSION!='0.1.0-beta.3':parser.error('requires the beta.3 bulk-only archive')
    root=ROOT/'.qualification'/(q.RUN+'_stream_pool_operations');root.mkdir(parents=True);q.RESULTS=root
    q.PROJECT+='-stream-pool-operations';q.SOURCE_IMAGE+='-stream-pool-operations';q.DESTINATION_IMAGE+='-stream-pool-operations'
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE,QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    result=dict(status='running',scope=__doc__,cases=[],production_streaming_changed=False)
    paths=[x for x in Path(__file__).parent.glob('*') if x.is_file()]
    paths += [ROOT/'tests/qualification/probe.py',ROOT/'tests/admission/probe.py',ROOT/'tests/admission/pool_cases.py',ROOT/'extensions/lake_replication/lake_replication.c',ROOT/'worker/Cargo.toml',ROOT/'Cargo.lock',*sorted((ROOT/'migrations').glob('*.sql'))]
    result['inputs']={str(x.relative_to(ROOT)):hashlib.sha256(x.read_bytes()).hexdigest() for x in paths}
    for path in paths:
        target=root/'sources'/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,target)
    try:
        archive=q.archive_path();q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-stream-operations-') as temp:q.build_images(archive,Path(temp))
        image='lake-replication-stream-pool-operations:'+q.RUN
        build=q.command(['docker','build','--build-arg',f'QUAL_IMAGE={q.DESTINATION_IMAGE}','-t',image,'-f',str(Path(__file__).with_name('Dockerfile.stream-pool')),str(ROOT)],timeout=600)
        (root/'build.log').write_text(build.stdout+build.stderr)
        result['image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip()
        text=m.compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n','')
        # One fresh fixture per case makes aborted/dead clients unable to affect
        # later assertions; each case creates its own second database.
        for name in args.cases:
            print('CASE',name,flush=True);directory=root/name;directory.mkdir();q.COMPOSE_FILE=directory/'compose.yml';q.COMPOSE_FILE.write_text(text)
            q.ENV['QUALIFICATION_DESTINATION_IMAGE']=image;observed=dict(case=name,status='running')
            try:
                q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
                if name!='aborts':
                    q.sql('destination','analytics','CREATE DATABASE pool_other')
                    q.sql('destination','pool_other','CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
                {'aborts':aborts,'progress':progress,'mixed':mixed}[name](observed);observed['status']='passed'
            except Exception as error:observed.update(status='error',error=str(error),error_type=type(error).__name__)
            finally:
                for connection in CLIENTS:connection.close()
                CLIENTS.clear()
                logs=q.compose('logs','--no-color',check_result=False);(directory/'compose.log').write_text(logs.stdout+logs.stderr)
                cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300);observed['cleanup_returncode']=cleanup.returncode
                if cleanup.returncode:observed['status']='error'
                result['cases'].append(observed);result['checks']=q.EVIDENCE['checks'];s.write_json(root/'results.json',result)
                print('RESULT',name,observed['status'],observed.get('error',''),flush=True)
                if observed['status']=='error':raise RuntimeError(observed.get('error','case failed'))
        result['status']='passed'
    except Exception as error:result.update(status='error',error=str(error));print('ERROR:',error,flush=True)
    finally:
        result['archive_provenance']=q.EVIDENCE;s.write_json(root/'results.json',result);print('Evidence:',root,flush=True)
    return int(result['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

#!/usr/bin/env python3
"""Build pinned S3 fixtures and verify a round trip before pg_lake compilation."""
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('qualification',Path(__file__).with_name('probe.py'))
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)


def main():
    q.RESULTS=ROOT/'.qualification'/(q.RUN+'_storage_preflight');q.RESULTS.mkdir(parents=True)
    q.PROJECT+='-storage-preflight'
    q.EVIDENCE['scope']=__doc__
    q.EVIDENCE['inputs']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),Path(__file__).with_name('storage.py'),Path(__file__).with_name('Dockerfile.storage'),ROOT/'deploy/qualification/compose.yml']}
    try:
        q.build_storage_images()
        q.compose('up','-d','storage',timeout=120)
        q.compose('run','--rm','--no-deps','-T','storage-init',timeout=120)
        q.check('source-built storage initializes its bucket',True,True)
        script='''set -eu
mc alias set qualification http://storage:9000 lake-qualification qualification-object-secret
printf 'qualification-source-fixture\n' > /tmp/expected
mc cp /tmp/expected qualification/lake-qualification/fixture-preflight
mc cat qualification/lake-qualification/fixture-preflight > /tmp/actual
cmp /tmp/expected /tmp/actual
mc rm qualification/lake-qualification/fixture-preflight
if mc stat qualification/lake-qualification/fixture-preflight >/dev/null 2>&1; then exit 1; fi
printf 'fixture-roundtrip-verified\n'
'''
        result=q.compose('run','--rm','--no-deps','-T','--entrypoint','/bin/sh','storage-init','-ec',script,timeout=120)
        q.check('source-built fixtures put/get/delete exact bytes','fixture-roundtrip-verified' in result.stdout,True)
        q.EVIDENCE['status']='passed'
    except Exception as error:q.EVIDENCE.update(status='error',error=str(error));print('ERROR:',error,flush=True)
    finally:
        logs=q.compose('logs','--no-color',check_result=False);(q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=120);q.EVIDENCE['cleanup_returncode']=cleanup.returncode
        (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2)+'\n');print('Evidence:',q.RESULTS,flush=True)
    return int(q.EVIDENCE.get('status')!='passed' or cleanup.returncode!=0)


if __name__=='__main__':raise SystemExit(main())

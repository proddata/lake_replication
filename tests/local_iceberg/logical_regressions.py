#!/usr/bin/env python3
"""Existing remote logical archive cases against the changed default core/workers."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('logical_cases',ROOT/'tests/qualification/probe.py')
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)


def main():
    q.RESULTS.mkdir(parents=True,exist_ok=False);exit_code=1
    q.EVIDENCE.update(scope='unchanged logical-mode qualification cases on current local-mirroring core/workers',
        unqualified=['all other release targets','production qualification'])
    try:
        for setting,target in [('LOCAL_LOGICAL_SOURCE_IMAGE','QUALIFICATION_SOURCE_IMAGE'),('PUSH_PG_LAKE_IMAGE','QUALIFICATION_DESTINATION_IMAGE')]:
            reference=os.environ.get(setting)
            if not reference:raise RuntimeError('Select cached '+setting)
            metadata=json.loads(q.command(['docker','image','inspect',reference]).stdout)[0]
            q.ENV[target]=metadata['Id'];q.EVIDENCE.setdefault('images',{})[target]=dict(reference=reference,id=metadata['Id'])
        q.EVIDENCE['files_sha256']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            [Path(__file__),ROOT/'tests/qualification/probe.py',ROOT/'worker/src/supervisor.rs',ROOT/'extensions/lake_replication/lake_replication.c']}
        q.build_storage_images();q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
        q.verify_upgrade_contract();q.exercise_replication();q.exercise_interrupted_initial_copy();q.verify_uninstaller()
        q.EVIDENCE['status']='passed';exit_code=0
    except Exception as error:
        q.EVIDENCE.update(status='error',error=str(error));print('ERROR: '+str(error),file=sys.stderr,flush=True)
    finally:
        logs=q.compose('logs','--no-color',check_result=False);(q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
        q.EVIDENCE['cleanup_returncode']=cleanup.returncode
        if cleanup.returncode:q.EVIDENCE['status']='error';exit_code=1
        (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2,sort_keys=True)+'\n');print('Evidence: '+str(q.RESULTS),flush=True)
    return exit_code


if __name__=='__main__':sys.exit(main())

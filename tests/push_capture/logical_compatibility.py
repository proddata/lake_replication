#!/usr/bin/env python3
"""Run existing archive logical qualification cases with push installed/removed.

Requires explicit cached image references. Each phase receives fresh private
volumes; this supplements, rather than replaces, the other logical test gates.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('logical_qualification',ROOT/'tests/qualification/probe.py')
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)


def main():
    q.RESULTS.mkdir(parents=True,exist_ok=False)
    q.EVIDENCE.update(scope='existing logical archive qualification with optional push installed and removed',
        unqualified=['all other logical test targets','automatic failover','production sizing'],phases={})
    started=False
    try:
        for variable,target in [('PUSH_LOGICAL_SOURCE_IMAGE','QUALIFICATION_SOURCE_IMAGE'),
                                ('PUSH_DESTINATION_IMAGE','QUALIFICATION_DESTINATION_IMAGE')]:
            reference=os.environ.get(variable)
            if not reference:raise RuntimeError('Select a cached '+variable)
            metadata=json.loads(q.command(['docker','image','inspect',reference]).stdout)[0]
            q.ENV[target]=metadata['Id']
            q.EVIDENCE.setdefault('images',{})[target]=dict(reference=reference,id=metadata['Id'])
        q.EVIDENCE['case_sha256']=hashlib.sha256((ROOT/'tests/qualification/probe.py').read_bytes()).hexdigest()
        q.build_storage_images()
        for phase in ['installed','removed']:
            begin=len(q.EVIDENCE['checks'])
            q.compose('up','-d','--wait','--wait-timeout','300',timeout=600);started=True
            q.sql('destination','analytics','CREATE EXTENSION lake_replication_push')
            if phase=='removed':q.sql('destination','analytics','DROP EXTENSION lake_replication_push')
            q.check(phase+': optional extension state',q.sql('destination','analytics',
                "SELECT count(*) FROM pg_extension WHERE extname='lake_replication_push'"),'1' if phase=='installed' else '0')
            q.verify_upgrade_contract()
            q.exercise_replication()
            q.exercise_interrupted_initial_copy()
            if phase=='installed':q.sql('destination','analytics','DROP EXTENSION lake_replication_push')
            q.verify_uninstaller()
            q.EVIDENCE['phases'][phase]=dict(status='passed',checks=len(q.EVIDENCE['checks'])-begin)
            logs=q.compose('logs','--no-color',check_result=False)
            (q.RESULTS/(phase+'-compose.log')).write_text(logs.stdout+logs.stderr)
            q.compose('down','-v','--remove-orphans',timeout=300);started=False
        q.EVIDENCE['status']='passed'
        return 0
    except Exception as error:
        q.EVIDENCE.update(status='error',error=str(error))
        print('ERROR: '+str(error),file=sys.stderr,flush=True)
        return 1
    finally:
        if started:
            logs=q.compose('logs','--no-color',check_result=False)
            (q.RESULTS/'failure-compose.log').write_text(logs.stdout+logs.stderr)
        q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
        (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2,sort_keys=True)+'\n')
        print('Evidence: '+str(q.RESULTS),flush=True)


if __name__=='__main__':sys.exit(main())

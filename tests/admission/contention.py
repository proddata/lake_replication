#!/usr/bin/env python3
"""Exploratory staging backpressure, unchanged archived Rust binaries/SQL.

Only an isolated initial-sync shell wrapper sets the existing chunk-pause knob.
This image overlay is a diagnostic, not a production package qualification.
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
    parser.add_argument('--copies',type=int,choices=[1,4],default=4)
    parser.add_argument('--cpuset',default='0-1',help='Shared source/destination/engine/storage CPU affinity')
    parser.add_argument('--source-cpuset',help='Optional separate source CPU affinity for topology controls')
    parser.add_argument('--storage-cpuset',help='Optional separate storage CPU affinity for topology controls')
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
    pause=args.pause_ms
    spec=importlib.util.spec_from_file_location('production_probe',root/'tests/admission/probe.py')
    p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
    original_text=p.m.compose_text
    original_build=p.q.build_images

    def compose_text():
        text=original_text()
        for service,cpuset in affinity.items():
            text=re.sub(rf'^  {service}:$',f'  {service}:\n    cpuset: "{cpuset}"',text,flags=re.MULTILINE)
        return text

    def build_images(archive,context):
        original_build(archive,context)
        if pause == 0:
            return
        with tempfile.TemporaryDirectory(prefix='lake-staging-pause-') as temp:
            directory=Path(temp)
            (directory/'initial-sync').write_text('#!/bin/sh\nexec env LAKE_INITIAL_COPY_CHUNK_PAUSE_MS='+str(pause)+' /usr/local/bin/lake-replication-initial-sync-unpaced "$@"\n')
            (directory/'Dockerfile').write_text('FROM '+p.q.DESTINATION_IMAGE+'\nUSER root\nRUN mv /usr/local/bin/lake-replication-initial-sync /usr/local/bin/lake-replication-initial-sync-unpaced\nCOPY --chmod=0755 initial-sync /usr/local/bin/lake-replication-initial-sync\n')
            p.q.command(['docker','build','-t',p.q.DESTINATION_IMAGE,str(directory)],timeout=300)

    p.m.compose_text=compose_text
    p.q.build_images=build_images
    sys.argv=[sys.argv[0],'--cases','fairness','--fairness-copies',str(args.copies)]
    code=p.main()
    metadata={'scope':__doc__,'image_overlay':pause>0,'pause_ms_per_1000_row_chunk':pause,'service_cpu_affinity':affinity,
              'driver_sha256':hashlib.sha256(driver_source).hexdigest()}
    for path in (p.q.RESULTS/'results.json',p.q.RESULTS/'fairness/results.json'):
        if not path.exists():
            continue
        data=json.loads(path.read_text());data['diagnostic']=metadata
        data['scope']=__doc__;path.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n')
    (p.q.RESULTS/'diagnostic-driver.py').write_bytes(driver_source)
    return code


if __name__ == '__main__':
    raise SystemExit(main())

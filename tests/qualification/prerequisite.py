#!/usr/bin/env python3
"""Prepare and verify a source-keyed cache of the upstream-only pg_lake image."""
import argparse
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys


def digest(path):
    result=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):
            result.update(block)
    return result.hexdigest()


def identity(root,revision,architecture):
    if not re.fullmatch('[0-9a-f]{40}',revision) or architecture not in ('arm64','amd64'):
        raise ValueError('invalid prerequisite revision or architecture')
    paths=['deploy/poc/Dockerfile','deploy/prepare-upstream.py','tests/qualification/prerequisite.py']
    return dict(format=1,pg_lake_revision=revision,architecture=architecture,
                inputs={name:digest(root/name) for name in paths})


def cache_key(expected):
    fingerprint=hashlib.sha256(json.dumps(expected,sort_keys=True).encode()).hexdigest()
    return 'pg-lake-prerequisite-v1-'+expected['architecture']+'-'+fingerprint


def ensure(command,root,revision,base_image,*,live=False):
    import os
    exists=command(['docker','image','inspect',base_image],check_result=False).returncode==0
    if not exists or os.environ.get('QUALIFICATION_REBUILD_BASE')=='1':
        command([sys.executable,str(root/'deploy/prepare-upstream.py'),revision])
        command(['docker','build','--target','pg-lake-base','--build-arg',
                 'PG_LAKE_REV='+revision,'--tag',base_image,'--file',
                 str(root/'deploy/poc/Dockerfile'),str(root)],
                timeout=7200,capture_output=not live)


def image(command,base_image,expected):
    result=json.loads(command(['docker','image','inspect',base_image]).stdout)[0]
    labels=result['Config'].get('Labels') or {}
    if (result['Os']!='linux' or result['Architecture']!=expected['architecture']
        or labels.get('org.opencontainers.image.revision')!=expected['pg_lake_revision']
        or labels.get('org.opencontainers.image.source')!='https://github.com/Snowflake-Labs/pg_lake'):
        raise ValueError('prerequisite image architecture/source labels do not match')
    return result['Id']


def save(command,base_image,expected,directory):
    image_id=image(command,base_image,expected)
    directory.mkdir(parents=True,exist_ok=True)
    archive=directory/'pg-lake-base.tar.gz'
    temporary=archive.with_suffix('.tmp')
    # Stream directly to compressed storage: an extra uncompressed image can
    # exhaust the runner disk. Keep Docker stderr visible in the build log.
    process=subprocess.Popen(['docker','image','save',base_image],stdout=subprocess.PIPE)
    try:
        with gzip.open(temporary,'wb',compresslevel=1) as output:
            shutil.copyfileobj(process.stdout,output,1024*1024)
        if process.wait(timeout=60)!=0:
            raise RuntimeError('docker image save failed')
        temporary.replace(archive)
    finally:
        process.stdout.close()
        if process.poll() is None:
            process.kill();process.wait()
        temporary.unlink(missing_ok=True)
    manifest=dict(identity=expected,image_id=image_id,archive_sha256=digest(archive))
    (directory/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return manifest


def validate(manifest,expected,archive):
    # Fail before Docker sees bytes from a stale or corrupted cache.
    if manifest['identity']!=expected:
        raise ValueError('prerequisite cache inputs do not match')
    if digest(archive)!=manifest['archive_sha256']:
        raise ValueError('prerequisite cache archive checksum does not match')


def load(command,base_image,expected,directory):
    manifest=json.loads((directory/'manifest.json').read_text())
    archive=directory/'pg-lake-base.tar.gz'
    validate(manifest,expected,archive)
    command(['docker','image','load','--input',str(archive)],timeout=600)
    if image(command,base_image,expected)!=manifest['image_id']:
        raise ValueError('loaded prerequisite image ID does not match')
    return manifest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['key','build','save','load'])
    parser.add_argument('directory',nargs='?',type=Path)
    args=parser.parse_args()
    if args.mode in ('save','load') and args.directory is None:
        parser.error('save/load requires a cache directory')
    spec=importlib.util.spec_from_file_location('qualification',Path(__file__).with_name('probe.py'))
    q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)
    expected=identity(q.ROOT,q.PG_LAKE_REVISION,q.native_architecture())
    if args.mode=='key':
        print(cache_key(expected));return
    if args.mode=='build':
        ensure(q.command,q.ROOT,q.PG_LAKE_REVISION,q.BASE_IMAGE,live=True)
        result=dict(identity=expected,image_id=image(q.command,q.BASE_IMAGE,expected))
    else:
        operation=save if args.mode=='save' else load
        result=operation(q.command,q.BASE_IMAGE,expected,args.directory)
    destination=q.ROOT/'.qualification'/('prerequisite-'+args.mode+'.json')
    destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    main()

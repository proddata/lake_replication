#!/usr/bin/env python3
"""Build opt-in native artifacts from explicitly selected cached prerequisites."""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

ROOT=Path(__file__).resolve().parents[2]


def run(*args):
    result=subprocess.run(args,cwd=ROOT,text=True,capture_output=True)
    if result.returncode:raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def inputs(mode):
    paths=[ROOT/'Cargo.toml',ROOT/'Cargo.lock',ROOT/'LICENSE',ROOT/'worker/Cargo.toml',ROOT/f'deploy/push/Dockerfile.{mode}',ROOT/'deploy/install/third-party-licenses.py',ROOT/'deploy/push/install-optional.sh',ROOT/'deploy/push/build-bundles.py']
    if mode=='capture':paths.append(ROOT/'deploy/push/source-grants.sql')
    paths+=list((ROOT/'worker').rglob('*.rs'))
    extension='lake_replication_capture' if mode=='capture' else 'lake_replication_push'
    for path in (ROOT/'extensions'/extension).iterdir():
        if path.name=='Makefile' or path.suffix in ('.c','.h','.control') or path.name in ('schema.sql','schema_codec.sql','schema_history.sql','schema_admission.sql','schema_publish.sql','api.sql','lake_replication_capture--0.1.0.sql'):
            paths.append(path)
    return {str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(set(paths))}


def build(mode,base,output):
    provenance=inputs(mode)
    digest=hashlib.sha256(json.dumps(provenance,sort_keys=True).encode()).hexdigest()
    base_info=json.loads(run('docker','image','inspect',base))[0]
    image=f'lake-push-bundle-{mode}:{digest[:16]}'
    command=['docker','build','--pull=false','--build-arg','BASE_IMAGE='+base,
        '--build-arg','PUSH_BUILD_INPUT_SHA256='+digest,'-f',f'deploy/push/Dockerfile.{mode}','-t',image,'.']
    result=subprocess.run(command,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    (output/(mode+'-build.log')).write_text(result.stdout)
    if result.returncode:raise RuntimeError(f'{mode} build failed; see {output/(mode+"-build.log")}')
    if inputs(mode)!=provenance:raise RuntimeError('build inputs changed during '+mode+' build')
    if json.loads(run('docker','image','inspect',base))[0]['Id']!=base_info['Id']:
        raise RuntimeError('prerequisite image changed during build')
    info=json.loads(run('docker','image','inspect',image))[0]
    architecture=info['Architecture']
    if architecture not in ('arm64','amd64'):raise RuntimeError('unsupported native architecture '+architecture)
    extension='lake_replication_capture' if mode=='capture' else 'lake_replication_push'
    shared=run('docker','run','--pull=never','--rm','--entrypoint','pg_config',image,'--sharedir')
    library=run('docker','run','--pull=never','--rm','--entrypoint','pg_config',image,'--pkglibdir')
    postgres=run('docker','run','--pull=never','--rm','--entrypoint','pg_config',image,'--version')
    if not postgres.startswith('PostgreSQL 18.'):raise RuntimeError('PostgreSQL 18 prerequisite required')
    name=f'{extension}-0.1.0-prototype-pg18-linux-{architecture}'
    with tempfile.TemporaryDirectory(prefix='push-bundle-') as temporary:
        package=Path(temporary)/name;package.mkdir()
        container=run('docker','create',image)
        try:
            files={f'{library}/{extension}.so':f'payload/lib/{extension}.so',
                f'{shared}/extension/{extension}.control':f'payload/extension/{extension}.control',
                f'{shared}/extension/{extension}--0.1.0.sql':f'payload/extension/{extension}--0.1.0.sql'}
            binaries=['lake-replication-capture'] if mode=='capture' else ['lake-replication-log-importer','lake-replication-supervisor','lake-replication-push-publisher']
            files.update({f'/usr/local/bin/{binary}':f'payload/bin/{binary}' for binary in binaries})
            run('docker','cp',container+':/usr/local/share/lake-push-third-party-licenses',str(package/'THIRD-PARTY-LICENSES'))
            run('docker','cp',container+':'+shared+'/pg_lake/lake-replication-patchset.json',str(package/'PG_LAKE_RUNTIME.json'))
            if mode!='capture':
                prerequisites=[library+'/lake_replication.so',shared+'/extension/lake_replication.control',
                    shared+'/extension/lake_replication--0.1.0-beta.5.sql']
                prerequisites += ['/usr/local/bin/lake-replication-'+worker for worker in ['receiver','publisher','initial-sync']]
                qualified=run('docker','run','--pull=never','--rm','--network=none','--entrypoint','sha256sum',image,*prerequisites)
                (package/'DEFAULT_DESTINATION_SHA256SUMS').write_text(qualified+'\n')
            for source,target in files.items():
                destination=package/target;destination.parent.mkdir(parents=True,exist_ok=True)
                run('docker','cp',container+':'+source,str(destination))
        finally:run('docker','rm',container)
        # Verify SQL/control payloads are exactly the qualified source inputs.
        sql=(ROOT/'extensions'/extension/(extension+'--0.1.0.sql')).read_bytes() if mode=='capture' else b''.join(
            (ROOT/'extensions'/extension/path).read_bytes() for path in ['schema.sql','schema_codec.sql','schema_history.sql','schema_admission.sql','schema_publish.sql','api.sql'])
        if (package/f'payload/extension/{extension}--0.1.0.sql').read_bytes()!=sql:
            raise RuntimeError('installed SQL does not match build inputs')
        for filename in ['LICENSE','Cargo.lock']:
            shutil.copyfile(ROOT/filename,package/filename)
        if mode=='capture':shutil.copyfile(ROOT/'deploy/push/source-grants.sql',package/'SOURCE_GRANTS.sql')
        shutil.copyfile(ROOT/'deploy/push/install-optional.sh',package/'install.sh')
        (package/'install.sh').chmod(0o755)
        (package/'EXTENSION').write_text(extension+'\n')
        (package/'TARGETARCH').write_text(architecture+'\n')
        (package/'POSTGRES_VERSION').write_text(postgres+'\n')
        (package/'README.md').write_text('Optional PostgreSQL 18 source-push prototype. See docs/design/push-replication.md and the companion README in the source repository. Install only with stopped managed subscriptions and a stopped server; retain the matching default worker bundle for rollback. Source and destination companions are independent. This artifact does not include PostgreSQL, pg_lake, pgduck, object credentials or a production support guarantee.\n')
        manifest=dict(format_version=1,prototype=True,component=extension,architecture=architecture,
            postgres=postgres,image_id=info['Id'],base_image_id=base_info['Id'],input_sha256=digest,inputs=provenance,
            source_commit=run('git','rev-parse','HEAD'),source_worktree=run('git','status','--porcelain'),
            files={str(path.relative_to(package)):hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(package.rglob('*')) if path.is_file()})
        (package/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
        sums=''.join(hashlib.sha256(path.read_bytes()).hexdigest()+'  '+str(path.relative_to(package))+'\n'
            for path in sorted(package.rglob('*')) if path.is_file())
        (package/'SHA256SUMS').write_text(sums)
        target=output/(name+'.tar.gz')
        with target.open('wb') as stream,gzip.GzipFile(filename='',mode='wb',fileobj=stream,mtime=0) as compressed,tarfile.open(fileobj=compressed,mode='w') as archive:
            for path in sorted(package.rglob('*')):
                entry=archive.gettarinfo(str(path),arcname=name+'/'+str(path.relative_to(package)))
                entry.uid=entry.gid=0;entry.uname=entry.gname='';entry.mtime=0
                with path.open('rb') if path.is_file() else io.BytesIO() as content:
                    archive.addfile(entry,content if path.is_file() else None)
        (output/(target.name+'.sha256')).write_text(hashlib.sha256(target.read_bytes()).hexdigest()+'  '+target.name+'\n')
        print(target,flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-base',default=os.getenv('PUSH_SOURCE_BASE_IMAGE'))
    parser.add_argument('--destination-base',default=os.getenv('PUSH_DESTINATION_BASE_IMAGE'))
    parser.add_argument('--output',type=Path,default=ROOT/'dist/push')
    args=parser.parse_args()
    if not args.source_base or not args.destination_base:parser.error('select cached --source-base and --destination-base prerequisite images')
    args.output.mkdir(parents=True,exist_ok=True)
    build('capture',args.source_base,args.output.resolve())
    build('importer',args.destination_base,args.output.resolve())

if __name__=='__main__':main()

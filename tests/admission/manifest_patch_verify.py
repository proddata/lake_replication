#!/usr/bin/env python3
"""Verify the unguarded upstream patch locally; explicit binary overlay, not release qualification."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--cpuset',default='0')
    parser.add_argument('--cases',nargs='+',choices=['pipeline','managed','fairness'],default=['pipeline','managed','fairness'])
    parser.add_argument('--correctness-only',action='store_true',help='Run independent row/file/manifest snapshots against the unguarded patch')
    args=parser.parse_args()
    spec=importlib.util.spec_from_file_location('production_probe',Path(__file__).with_name('probe.py'))
    p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
    patch=p.ROOT/'tests/admission/patches/pg_lake_skip_clean_manifests.patch'
    metadata=dict(scope=__doc__,patch_sha256=hashlib.sha256(patch.read_bytes()).hexdigest(),binary_overlay=True,
        upstream_revision='8180647d4535fdb1ed223f92f301ba6094a8ec3e',diagnostic_guc=False,timers=False)
    original_build=p.q.build_images
    def build_images(archive,context):
        original_build(archive,context)
        q=p.q;libraries=['pg_lake_iceberg','pg_lake_table']
        metadata['base_image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',q.DESTINATION_IMAGE]).stdout.strip()
        revision=q.command(['docker','run','--rm','--entrypoint','git',q.DESTINATION_IMAGE,'-C','/opt/pg_lake','rev-parse','HEAD']).stdout.strip()
        if revision!=metadata['upstream_revision']:raise ValueError('unexpected upstream revision')
        metadata['sources']={}
        for name in ['pg_lake_iceberg/include/pg_lake/iceberg/manifest_spec.h',
                     'pg_lake_iceberg/src/iceberg/read_manifest.c',
                     'pg_lake_iceberg/src/iceberg/operations/manifest_merge.c']:
            expected=hashlib.sha256((p.ROOT/'vendor/pg_lake'/name).read_bytes()).hexdigest()
            actual=q.command(['docker','run','--rm','--entrypoint','sha256sum',q.DESTINATION_IMAGE,'/opt/pg_lake/'+name]).stdout.split()[0]
            if actual!=expected:raise ValueError('cached patch source differs: '+name)
            metadata['sources'][name]=actual
        metadata['original_libraries']={name:q.command(['docker','run','--rm','--entrypoint','sha256sum',q.DESTINATION_IMAGE,f'/usr/lib/postgresql/18/lib/{name}.so']).stdout.split()[0] for name in libraries}
        (context/'candidate.patch').write_bytes(patch.read_bytes())
        recipe=[f'FROM {q.DESTINATION_IMAGE}','USER root','COPY candidate.patch /tmp/candidate.patch',
            'RUN git -C /opt/pg_lake apply --check /tmp/candidate.patch && git -C /opt/pg_lake apply /tmp/candidate.patch']
        for name in libraries:
            recipe.append(f'RUN find /opt/pg_lake/{name}/src -name "*.o" -delete && make -C /opt/pg_lake/{name} -j2 {name}.so with_llvm=no && cp /opt/pg_lake/{name}/{name}.so /usr/lib/postgresql/18/lib/{name}.so')
        text='\n'.join(recipe)+'\n';(context/'Dockerfile.manifest-patch').write_text(text)
        q.command(['docker','build','-t',q.DESTINATION_IMAGE,'-f',str(context/'Dockerfile.manifest-patch'),str(context)],timeout=600)
        metadata['patched_libraries']={name:q.command(['docker','run','--rm','--entrypoint','sha256sum',q.DESTINATION_IMAGE,f'/usr/lib/postgresql/18/lib/{name}.so']).stdout.split()[0] for name in libraries}
        (q.RESULTS/'Dockerfile.manifest-patch').write_text(text)
    p.q.build_images=build_images;p.__doc__=__doc__
    p.m.diagnostic_sources=[Path(__file__),patch]
    if args.correctness_only:
        helper=Path(__file__).with_name('manifest_cleanup_probe.py')
        spec=importlib.util.spec_from_file_location('manifest_cleanup_probe',helper)
        fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
        q=p.q;q.RESULTS=p.ROOT/'.qualification'/(q.RUN+'_manifest_patch');q.RESULTS.mkdir()
        q.PROJECT+='-manifest-patch';result=dict(status='running',scope=__doc__)
        try:
            archive=q.archive_path();q.verify_archive(archive)
            result['archive']={key:q.EVIDENCE[key] for key in ['archive','archive_sha256','architecture','source_revision']}
            with tempfile.TemporaryDirectory() as temp:build_images(archive,Path(temp))
            result['image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',q.DESTINATION_IMAGE]).stdout.strip()
            text=p.m.compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n','')
            q.COMPOSE_FILE=q.RESULTS/'compose.yml';q.COMPOSE_FILE.write_text(text)
            q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
            fixture.correctness(p,q.RESULTS);result['status']='passed'
        except Exception as error:result.update(status='error',error=str(error));print('ERROR',error,flush=True)
        finally:
            logs=q.compose('logs','--no-color',check_result=False);(q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
            cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
            result['cleanup_returncode']=cleanup.returncode
            if cleanup.returncode:result['status']='error'
            result['checks']=q.EVIDENCE['checks'];result['upstream_patch']=metadata
            result['fixture_sha256']=hashlib.sha256(helper.read_bytes()).hexdigest()
            (q.RESULTS/'results.json').write_text(json.dumps(result,indent=2)+'\n');print('Evidence:',q.RESULTS,flush=True)
        return int(result['status']!='passed')
    sys.argv=[sys.argv[0],'--cases',*args.cases,'--cpuset',args.cpuset]
    code=p.main();path=p.q.RESULTS/'results.json';result=json.loads(path.read_text())
    result['upstream_patch']=metadata;path.write_text(json.dumps(result,indent=2)+'\n');return code


if __name__=='__main__':raise SystemExit(main())

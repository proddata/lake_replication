"""Pinned source-built S3 fixtures, independent of unavailable public images."""
import hashlib
import json
from pathlib import Path

FIXTURES={
    'storage':dict(target='minio',variable='QUALIFICATION_STORAGE_IMAGE',revision='0d7408fc9969caf07de6a8c3a84f9fbb10a6739e'),
    'storage-init':dict(target='mc',variable='QUALIFICATION_STORAGE_INIT_IMAGE',revision='b00526b153a31b36767991a4f5ce2cced435ee8e'),
}


def ensure(command,env,evidence,root,results,architecture):
    dockerfile=root/'tests/qualification/Dockerfile.storage'
    recipe=hashlib.sha256(dockerfile.read_bytes()).hexdigest()
    images={}
    for service,spec in FIXTURES.items():
        default=f"lake-replication-fixture-{spec['target']}:{spec['revision'][:12]}-{recipe[:12]}-{architecture}"
        image=env.get(spec['variable']) or default
        overridden=image!=default
        query=['docker','image','inspect','--format','{"id":{{json .Id}},"architecture":{{json .Architecture}},"labels":{{json .Config.Labels}}}',image]
        found=command(query,check_result=False)
        if found.returncode and not overridden:
            print(f"Building {service} fixture from pinned source {spec['revision']}",flush=True)
            built=command(['docker','build','--platform',f'linux/{architecture}','--target',spec['target'],
                '--build-arg',f'FIXTURE_RECIPE_SHA={recipe}','-t',image,'-f',str(dockerfile),str(root)],check_result=False,timeout=1800)
            results.mkdir(parents=True,exist_ok=True)
            (results/f"storage-{spec['target']}-build.log").write_text(built.stdout+built.stderr)
            if built.returncode:raise RuntimeError(f"source fixture build failed for {service}: {built.stderr[-3000:]}")
            found=command(query,check_result=False)
        if found.returncode:raise RuntimeError(f'fixture image unavailable: {image}')
        metadata=json.loads(found.stdout)
        if metadata['architecture']!=architecture:raise RuntimeError(f'fixture architecture mismatch: {image}')
        labels=metadata.get('labels') or {}
        if not overridden and (labels.get('org.opencontainers.image.revision')!=spec['revision'] or labels.get('io.lake-replication.fixture.recipe')!=recipe):
            raise RuntimeError(f'cached fixture identity mismatch: {image}')
        env[spec['variable']]=image
        images[service]=dict(image=image,image_id=metadata['id'],architecture=architecture,
            source_revision=labels.get('org.opencontainers.image.revision'),recipe_sha256=recipe if not overridden else None,
            origin='explicit_override' if overridden else 'pinned_source_build')
    evidence['storage_fixtures']=images
    return images

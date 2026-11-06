"""Build the supported pg_lake runtime from a verified, cached upstream image."""
import hashlib
import json
from pathlib import Path


def prepare(command, root, base_image_id, architecture, revision):
    directory = root / 'deploy/pg_lake'
    inputs = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted(directory.rglob('*')) if path.is_file() and '__pycache__' not in path.parts}
    inputs['tests/qualification/runtime.py'] = hashlib.sha256((root / 'tests/qualification/runtime.py').read_bytes()).hexdigest()
    identity = dict(base_image_id=base_image_id, architecture=architecture, revision=revision, inputs=inputs)
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    image = 'pg-lake-runtime:' + architecture + '-' + fingerprint[:20]
    if command(['docker', 'image', 'inspect', image], check_result=False).returncode:
        # Docker FROM needs a local image reference, rather than a bare image ID.
        source_tag = 'pg-lake-runtime-source:' + base_image_id.split(':')[1]
        command(['docker', 'tag', base_image_id, source_tag])
        command(['docker', 'build', '--build-arg', 'BASE_IMAGE=' + source_tag,
                 '--tag', image, '--file', str(directory / 'Dockerfile'), str(directory)], timeout=600)
    inspection = json.loads(command(['docker', 'image', 'inspect', image]).stdout)[0]
    manifest = json.loads((directory / 'patchset.json').read_text())
    labels = inspection['Config'].get('Labels') or {}
    if (inspection['Os'] != 'linux' or inspection['Architecture'] != architecture or
            labels.get('org.opencontainers.image.revision') != revision or
            labels.get('io.proddata.pg-lake.patchset') != manifest['patchset']):
        raise ValueError('runtime architecture or patchset label differs')
    # Check installed bytes against the recorded coherent build, not only labels.
    verify = '''import hashlib,json,pathlib,subprocess
share=pathlib.Path(subprocess.check_output(['pg_config','--sharedir'],text=True).strip())
lib=pathlib.Path(subprocess.check_output(['pg_config','--pkglibdir'],text=True).strip())
marker=json.loads((share/'pg_lake/lake-replication-patchset.json').read_text())
assert marker['state']=='after'
assert set(marker['libraries'])=={'pg_lake_iceberg','pg_lake_table'}
for name,digest in marker['libraries'].items():
 assert hashlib.sha256((lib/(name+'.so')).read_bytes()).hexdigest()==digest
print(json.dumps(marker))
'''
    marker = json.loads(command(['docker', 'run', '--rm', '--entrypoint', 'python3', image, '-c', verify]).stdout)
    if (marker['patch_sha256'] != manifest['patch_sha256'] or
            marker['manifest_sha256'] != inputs['deploy/pg_lake/patchset.json'] or
            marker['upstream_revision'] != revision or revision not in manifest['upstream_revisions']):
        raise ValueError('runtime marker differs from the pinned patchset')
    return image, dict(identity=identity, image_id=inspection['Id'], installed=marker)

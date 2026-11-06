#!/usr/bin/env python3
"""Verify, apply and rebuild the pinned pg_lake prerequisite patchset offline."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def inspect(source):
    manifest = json.loads((HERE / 'patchset.json').read_text())
    patch = HERE / 'patches/skip-clean-manifests.patch'
    if digest(patch) != manifest['patch_sha256']:
        raise ValueError('patch checksum differs from manifest')
    revision = command(['git', '-C', str(source), 'rev-parse', 'HEAD'], capture_output=True).stdout.strip()
    if revision not in manifest['upstream_revisions']:
        raise ValueError('unsupported pg_lake revision: ' + revision)
    actual = {name: digest(source / name) for name in manifest['files']}
    states = [state for state in ('before', 'after') if all(
        actual[name] == hashes[state + '_sha256'] for name, hashes in manifest['files'].items())]
    if len(states) != 1:
        raise ValueError('pg_lake patch files are modified or only partly patched')
    # Upstream builds modify nested submodules; only parent-tree changes could
    # contaminate the modules rebuilt here. The exact patch files are hashed above.
    changed = command(['git', '-C', str(source), 'diff', '--name-only', '--ignore-submodules=all', 'HEAD'], capture_output=True).stdout.splitlines()
    if set(changed) - set(manifest['files']):
        raise ValueError('unrelated tracked pg_lake source changes')
    return manifest, dict(patchset=manifest['patchset'], upstream_revision=revision,
                         patch_sha256=manifest['patch_sha256'], state=states[0],
                         manifest_sha256=digest(HERE / 'patchset.json'))


def apply(source):
    _, report = inspect(source)
    if report['state'] == 'before':
        patch = str(HERE / 'patches/skip-clean-manifests.patch')
        command(['git', '-C', str(source), 'apply', '--check', patch])
        command(['git', '-C', str(source), 'apply', patch])
    _, report = inspect(source)
    if report['state'] != 'after':
        raise ValueError('patch did not produce the expected source hashes')
    return report


def install(source, jobs):
    pg_config = shutil.which(os.environ.get('PG_CONFIG', 'pg_config'))
    if not pg_config:
        raise ValueError('pg_config is required')
    version = command([pg_config, '--version'], capture_output=True).stdout.strip()
    if not version.startswith('PostgreSQL 18.'):
        raise ValueError('the runtime prerequisite requires PostgreSQL 18')
    report = apply(source)
    report['postgres_version'] = version
    modules = ('pg_lake_iceberg', 'pg_lake_table')
    # The header layout is shared. Build both completely before installing either.
    for module in modules:
        for obj in (source / module / 'src').rglob('*.o'):
            obj.unlink()
        command(['make', '-C', str(source / module), '-j' + str(jobs), module + '.so', 'with_llvm=no', 'PG_CONFIG=' + pg_config])
    library_dir = Path(command([pg_config, '--pkglibdir'], capture_output=True).stdout.strip())
    for module in modules:
        target = library_dir / (module + '.so')
        temporary = target.with_suffix('.so.patchset-new')
        try:
            shutil.copy2(source / module / (module + '.so'), temporary)
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
    report['libraries'] = {module: digest(library_dir / (module + '.so')) for module in modules}
    share = Path(command([pg_config, '--sharedir'], capture_output=True).stdout.strip())
    marker = share / 'pg_lake/lake-replication-patchset.json'
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps(report, indent=2) + '\n')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['check', 'apply', 'install'])
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--jobs', type=int, default=2)
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error('--jobs must be positive')
    source = args.source.resolve()
    report = (inspect(source)[1] if args.mode == 'check' else apply(source)
              if args.mode == 'apply' else install(source, args.jobs))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

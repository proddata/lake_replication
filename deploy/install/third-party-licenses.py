#!/usr/bin/env python3
"""Collect locked native Rust dependency notices for the exported bundle.

Includes build dependencies conservatively. No network access: Cargo must have
already downloaded the locked native graph while building the worker binaries.
"""
import hashlib
import json
from pathlib import Path
import re
import tarfile
import subprocess
import sys
import tomllib

ALLOWED = {'MIT', 'Apache-2.0', 'ISC', 'BSD-3-Clause', 'Zlib', 'Unlicense'}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def collect(output):
    host = next(line.split(': ', 1)[1] for line in subprocess.check_output(
        ['rustc', '-vV'], text=True).splitlines() if line.startswith('host: '))
    metadata = json.loads(subprocess.check_output([
        'cargo', 'metadata', '--locked', '--offline', '--format-version', '1', '--filter-platform', host], text=True))
    lockfile = Path(metadata['workspace_root']) / 'Cargo.lock'
    locked = {(p['name'], p['version']): p for p in tomllib.loads(lockfile.read_text())['package']}
    output.mkdir(parents=True, exist_ok=False)
    packages = []
    for package in sorted(metadata['packages'], key=lambda p: (p['name'], p['version'])):
        if package['id'] in metadata['workspace_members']:
            continue  # The bundle's root LICENSE covers this project.
        expression = package.get('license') or ''
        terms = set(re.findall(r'[A-Za-z0-9.+-]+', expression)) - {'AND', 'OR'}
        if not terms or terms - ALLOWED:
            raise RuntimeError(f"unreviewed license for {package['name']}: {expression}")
        root = Path(package['manifest_path']).parent.resolve()
        expected = locked[(package['name'], package['version'])]
        stem = package['name'] + '-' + package['version']
        archive = root.parents[2] / 'cache' / root.parent.name / (stem + '.crate')
        if sha(archive) != expected['checksum']:
            raise RuntimeError('published crate checksum differs from Cargo.lock')
        declared = package.get('license_file')
        if declared:
            declared = Path(declared)
            if declared.is_absolute():
                declared = declared.relative_to(root)
        files = []
        with tarfile.open(archive, 'r:gz') as bundle:
            for member in sorted(bundle.getmembers(), key=lambda m: m.name):
                relative = Path(member.name).relative_to(stem)
                if relative.is_absolute() or '..' in relative.parts:
                    raise RuntimeError('unsafe published notice path')
                if not member.isfile() or not (relative.name.upper().startswith(
                        ('LICENSE', 'COPYING', 'NOTICE', 'COPYRIGHT')) or relative == declared):
                    continue
                data = bundle.extractfile(member).read()
                destination = output / stem / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(data)
                files.append({'path': str(Path(stem) / relative), 'sha256': hashlib.sha256(data).hexdigest()})
        if not files:
            raise RuntimeError(f'no published notice files for {stem}')
        packages.append({'name': package['name'], 'version': package['version'],
            'license': expression, 'source': package['source'],
            'checksum': expected['checksum'], 'files': files})
    sysroot = Path(subprocess.check_output(['rustc', '--print', 'sysroot'], text=True).strip())
    notices = [p for p in (sysroot / 'share/doc/rust').glob('*')
               if p.is_file() and p.name.startswith(('COPYRIGHT', 'LICENSE'))]
    if not notices:
        raise RuntimeError('Rust standard-library/toolchain notices are missing')
    toolchain_files = []
    for notice in sorted(notices):
        destination = output / 'rust-toolchain' / notice.name
        destination.parent.mkdir(exist_ok=True)
        destination.write_bytes(notice.read_bytes())
        toolchain_files.append({'path': str(Path('rust-toolchain') / notice.name), 'sha256': sha(notice)})
    report = {'format': 1, 'target': host, 'scope': 'Locked native Rust graph, including build dependencies',
        'toolchain': {'rustc': subprocess.check_output(['rustc', '-vV'], text=True), 'files': toolchain_files},
        'cargo_lock_sha256': sha(lockfile), 'allowed_license_identifiers': sorted(ALLOWED), 'packages': packages}
    (output / 'inventory.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print(f'Collected verified notice files for {len(packages)} locked dependencies ({host})')


if __name__ == '__main__':
    collect(Path(sys.argv[1]))

#!/usr/bin/env python3
"""Verify bundle notices, generate SBOMs and scan exact exported archives."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('license_policy', ROOT / 'deploy/install/third-party-licenses.py')
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(args, log):
    result = subprocess.run(args, text=True, capture_output=True, timeout=900)
    with log.open('a') as output:
        output.write(result.stdout + result.stderr)
    return result


def audit(archive, directory, args):
    directory.mkdir()
    expected = Path(str(archive) + '.sha256').read_text().split()
    digest = sha(archive)
    if expected != [digest, archive.name]:
        raise RuntimeError('archive checksum mismatch')
    with tempfile.TemporaryDirectory(prefix='lake-security-') as temp:
        with tarfile.open(archive) as bundle:
            bundle.extractall(temp, filter='data')
        roots = list(Path(temp).iterdir())
        if len(roots) != 1 or not roots[0].is_dir():
            raise RuntimeError('expected one bundle directory')
        root = roots[0]
        for line in (root / 'SHA256SUMS').read_text().splitlines():
            checksum, name = line.split(maxsplit=1)
            path = (root / name.removeprefix('./')).resolve()
            if not path.is_relative_to(root.resolve()) or sha(path) != checksum:
                raise RuntimeError('inner bundle checksum mismatch')
        revision = (root / 'SOURCE_REVISION').read_text().strip()
        architecture = (root / 'TARGETARCH').read_text().strip()
        version = (root / 'VERSION').read_text().strip()
        if args.expected_revision and revision != args.expected_revision:
            raise RuntimeError('archive revision differs from candidate')
        inventory = json.loads((root / 'THIRD-PARTY-LICENSES/inventory.json').read_text())
        target = {'arm64': 'aarch64-unknown-linux-gnu', 'amd64': 'x86_64-unknown-linux-gnu'}[architecture]
        if inventory['target'] != target or inventory['cargo_lock_sha256'] != sha(root / 'Cargo.lock'):
            raise RuntimeError('license inventory target/lock mismatch')
        if sha(root / 'Cargo.lock') != sha(ROOT / 'Cargo.lock'):
            raise RuntimeError('archive dependencies differ from checked source')
        if not inventory['toolchain']['files']:
            raise RuntimeError('Rust runtime/toolchain notices are absent')
        for notice in inventory['toolchain']['files']:
            path = (root / 'THIRD-PARTY-LICENSES' / notice['path']).resolve()
            if not path.is_relative_to((root / 'THIRD-PARTY-LICENSES').resolve()) or sha(path) != notice['sha256']:
                raise RuntimeError('Rust runtime/toolchain notice checksum mismatch')
        locked = {(p['name'], p['version']): p for p in tomllib.loads((root / 'Cargo.lock').read_text())['package']}
        if set(inventory['allowed_license_identifiers']) != policy.ALLOWED:
            raise RuntimeError('archive license policy differs from checked source')
        for package in inventory['packages']:
            terms = set(re.findall(r'[A-Za-z0-9.+-]+', package['license'])) - {'AND', 'OR'}
            if not terms or terms - policy.ALLOWED:
                raise RuntimeError('unreviewed dependency license')
            if locked[(package['name'], package['version'])]['checksum'] != package['checksum']:
                raise RuntimeError('inventory crate checksum differs from lock')
            if not package['files']:
                raise RuntimeError('missing dependency notices')
            for notice in package['files']:
                path = (root / 'THIRD-PARTY-LICENSES' / notice['path']).resolve()
                if not path.is_relative_to((root / 'THIRD-PARTY-LICENSES').resolve()) or sha(path) != notice['sha256']:
                    raise RuntimeError('notice checksum mismatch')
        sbom = directory / 'sbom.syft.json'
        spdx = directory / 'sbom.spdx.json'
        if command([args.syft, 'scan', 'dir:' + str(root), '--source-name', 'lake_replication',
                    '--source-version', version, '-o', 'syft-json=' + str(sbom),
                    '-o', 'spdx-json=' + str(spdx)], directory / 'scanner.log').returncode:
            raise RuntimeError('SBOM generation failed')
        artifacts = json.loads(sbom.read_text())['artifacts']
        packages = {(p['name'], p['version']) for p in artifacts}
        required = {(p['name'], p['version']) for p in inventory['packages']}
        if required - packages:
            raise RuntimeError('SBOM missed native dependencies: ' + str(sorted(required - packages)))
        scan = directory / 'vulnerabilities.json'
        result = command([args.grype, 'sbom:' + str(sbom), '--fail-on', 'high',
                          '--output', 'json', '--file', str(scan)], directory / 'scanner.log')
        if result.returncode not in (0, 2):
            raise RuntimeError('vulnerability scanner failed')
        findings = json.loads(scan.read_text())
        return dict(status='passed' if result.returncode == 0 else 'finding', archive=archive.name,
            archive_sha256=digest, revision=revision, architecture=architecture,
            cargo_lock_sha256=sha(root / 'Cargo.lock'), native_dependency_count=len(required),
            sbom_package_count=len(packages), sbom_coverage_verified=True,
            licenses=[{k:p[k] for k in ['name','version','license','checksum']} for p in inventory['packages']],
            sbom_sha256=sha(sbom), spdx_sha256=sha(spdx), vulnerabilities_sha256=sha(scan),
            vulnerability_count=len(findings['matches']),
            vulnerabilities=[{'id':m['vulnerability']['id'],'severity':m['vulnerability']['severity'],
                'package':m['artifact']['name'],'version':m['artifact']['version']} for m in findings['matches']],
            scanner=findings['descriptor'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archives', nargs='+', type=Path)
    parser.add_argument('--syft', default=os.environ.get('SYFT', 'syft'))
    parser.add_argument('--grype', default=os.environ.get('GRYPE', 'grype'))
    parser.add_argument('--expected-revision')
    parser.add_argument('--output', type=Path, default=ROOT / '.qualification' / datetime.now(timezone.utc).strftime('security_%Y%m%d_%H%M%S'))
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    report = {'scope': __doc__, 'archives': []}
    for archive in args.archives:
        try:
            result = audit(archive.resolve(), args.output / archive.name.removesuffix('.tar.gz'), args)
        except Exception as error:
            result = {'status': 'error', 'archive': archive.name, 'error': str(error)}
        report['archives'].append(result)
        print(result['archive'], result['status'], result.get('error', ''), flush=True)
    report['status'] = 'passed' if all(r['status'] == 'passed' for r in report['archives']) else 'failed'
    (args.output / 'results.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    print('Evidence:', args.output, flush=True)
    return int(report['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())

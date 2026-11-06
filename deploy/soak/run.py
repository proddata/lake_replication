#!/usr/bin/env python3
"""Preflight or execute a bounded pilot on a disposable native Linux Docker host."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[2]
GIB = 1024**3


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True, timeout=30).strip()


def archive_identity(path, expected_revision, architecture):
    fields = Path(str(path) + '.sha256').read_text().split()
    if len(fields) != 2 or fields[1].lstrip('*') != path.name:
        raise ValueError('invalid adjacent archive checksum manifest')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != fields[0]:
        raise ValueError('archive checksum mismatch')
    metadata = {}
    with tarfile.open(path, 'r:gz') as bundle:
        for field in ('SOURCE_REVISION', 'TARGETARCH'):
            members = [m for m in bundle.getmembers() if m.name.endswith('/' + field)]
            if len(members) != 1 or not members[0].isfile() or members[0].size > 256:
                raise ValueError('invalid archive identity field: ' + field)
            metadata[field] = bundle.extractfile(members[0]).read().decode().strip()
    if metadata != dict(SOURCE_REVISION=expected_revision, TARGETARCH=architecture):
        raise ValueError('archive revision or native architecture differs from selected candidate')
    return dict(name=path.name, sha256=digest, **metadata)


def capacity(free, budget_mib, reserve_mib):
    if free < (budget_mib + reserve_mib) * 1024**2:
        raise ValueError('insufficient free space for the object budget plus reserve')


def preflight(args):
    if platform.system() != 'Linux':
        raise ValueError('remote pilots require native Linux')
    architecture = {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine())
    if not architecture:
        raise ValueError('unsupported native architecture')
    # A remote pilot must never use a local recipe experiment or modified vendor.
    if command('git', 'status', '--porcelain', '--untracked-files=normal'):
        raise ValueError('use a clean committed checkout for the remote pilot')
    expected = command('git', 'rev-parse', 'HEAD:vendor/pg_lake')
    actual = command('git', '-C', str(ROOT / 'vendor/pg_lake'), 'rev-parse', 'HEAD')
    if expected != actual or command('git', '-C', str(ROOT / 'vendor/pg_lake'),
                                     'status', '--porcelain', '--untracked-files=no'):
        raise ValueError('pg_lake submodule differs from the committed pin')
    archive = archive_identity(args.archive, args.candidate, architecture)
    info = json.loads(command('docker', 'info', '--format', '{{json .}}'))
    docker_arch = {'x86_64': 'amd64', 'aarch64': 'arm64', 'amd64': 'amd64', 'arm64': 'arm64'}.get(info['Architecture'])
    if info['OSType'] != 'linux' or docker_arch != architecture:
        raise ValueError('Docker must run locally with the native host architecture')
    if info['NCPU'] < 4 or info['MemTotal'] < 14 * GIB:
        raise ValueError('pilot host needs at least four CPUs and approximately 16 GiB RAM')
    # Do not accept a remote Docker context: paths/free-space checks must refer
    # to the machine actually storing Docker's volumes and images.
    context = json.loads(command('docker', 'context', 'inspect'))[0]
    endpoint = os.environ.get('DOCKER_HOST') or context['Endpoints']['docker']['Host']
    if not endpoint.startswith('unix://'):
        raise ValueError('use a local Unix-socket Docker daemon')
    paths = {str(ROOT), info['DockerRootDir']}
    free = {path: shutil.disk_usage(path).free for path in paths}
    for value in free.values():
        capacity(value, args.max_storage_mib, args.min_free_mib)
    if Path('/var/run/reboot-required').exists():
        raise ValueError('reboot the fresh host before running the pilot')
    return dict(timestamp=datetime.now(timezone.utc).isoformat(),
                scope='Pilot only; this launcher cannot start a 24-hour qualification',
                runner_revision=command('git', 'rev-parse', 'HEAD'),
                pg_lake_revision=expected, architecture=architecture, archive=archive,
                kernel=platform.release(), cpu_count=info['NCPU'], memory_bytes=info['MemTotal'],
                docker_version=info['ServerVersion'],
                buildx_version=command('docker', 'buildx', 'version'),
                compose_version=command('docker', 'compose', 'version', '--short'),
                docker_root=info['DockerRootDir'], free_bytes=free,
                duration_seconds=args.duration_seconds, max_storage_mib=args.max_storage_mib,
                min_free_mib=args.min_free_mib,
                maintenance_profile='none', observe_maintenance=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['preflight', 'run'])
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--candidate', required=True, help='Full source revision of the qualified archive')
    parser.add_argument('--duration-seconds', type=int, choices=[600, 7200], default=600)
    parser.add_argument('--max-storage-mib', type=int, default=65536)
    parser.add_argument('--min-free-mib', type=int, default=16384)
    args = parser.parse_args()
    if not re.fullmatch('[0-9a-f]{40}', args.candidate):
        parser.error('candidate must be a full lowercase Git revision')
    if args.min_free_mib < 10240 or args.max_storage_mib < 1:
        parser.error('retain at least 10 GiB free and a positive object-storage budget')
    args.archive = args.archive.expanduser().resolve()
    evidence = preflight(args)
    print(json.dumps(evidence, indent=2), flush=True)
    if args.mode == 'preflight':
        return
    (ROOT / '.qualification').mkdir(exist_ok=True)
    lock = os.open(ROOT / '.qualification/remote-pilot.lock', os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.set_inheritable(lock, True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    directory = ROOT / '.qualification' / ('remote_' + stamp)
    directory.mkdir(parents=True, mode=0o700)
    (directory / 'host.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print('Remote host evidence:', directory, flush=True)
    # systemd supplies the lifetime/signal supervision. Exec keeps the actual
    # harness as the service's main process, so SIGINT reaches its finally block.
    os.chdir(ROOT)
    environment = dict(os.environ, QUALIFICATION_ARCHIVE=str(args.archive),
                       QUALIFICATION_PG_LAKE_REVISION=evidence['pg_lake_revision'], PYTHONUNBUFFERED='1')
    with (directory / 'console.log').open('w') as output:
        os.dup2(output.fileno(), 1)
        os.dup2(output.fileno(), 2)
    os.execve(sys.executable, [sys.executable, '-u', str(ROOT / 'tests/soak/probe.py'),
        '--duration-seconds', str(args.duration_seconds), '--max-storage-mib', str(args.max_storage_mib),
        '--min-free-mib', str(args.min_free_mib), '--maintenance-profile', 'none',
        '--observe-maintenance'], environment)


if __name__ == '__main__':
    main()

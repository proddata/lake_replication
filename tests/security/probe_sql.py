#!/usr/bin/env python3
"""Run SQL regressions in a disposable PG18 container using a cached image."""
import argparse
import os
from pathlib import Path
import secrets
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]


def run(args, **kwargs):
    kwargs.setdefault('timeout', 120)
    return subprocess.run(args, cwd=ROOT, check=True, text=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True, help='locally cached PostgreSQL 18 image')
    args = parser.parse_args()
    name = 'lake-sql-security-' + secrets.token_hex(6)
    try:
        run(['docker', 'run', '--pull=never', '--rm', '-d', '--name', name,
             '--user', 'postgres', '--entrypoint', '/bin/bash',
             '-p', '127.0.0.1::5432', args.image, '-ec',
             'initdb -D /tmp/lake-security-data --auth=trust --username=postgres '
             '--encoding=UTF8 --locale=C >/tmp/lake-security-init.log; '
             'printf "host all all all trust\\n" >>/tmp/lake-security-data/pg_hba.conf; '
             'exec postgres -D /tmp/lake-security-data -h 0.0.0.0 '
             '-c standard_conforming_strings=off -c client_encoding=LATIN1 '
             '-c wal_level=logical'],
            capture_output=True)
        deadline = time.monotonic() + 40
        while True:
            ready = subprocess.run(['docker', 'exec', name, 'pg_isready',
                                    '-h', '127.0.0.1', '-U', 'postgres'],
                                   capture_output=True, timeout=5)
            if ready.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('disposable PostgreSQL server did not become ready')
            time.sleep(0.2)
        settings = run(['docker', 'exec', name, 'psql', '-h', '127.0.0.1', '-U', 'postgres', '-Atc',
                        "SELECT current_setting('server_version_num')::int / 10000, "
                        "current_setting('standard_conforming_strings'), "
                        "current_setting('client_encoding')"], capture_output=True).stdout.strip()
        if settings != '18|off|LATIN1':
            raise RuntimeError('unexpected server defaults: ' + settings)
        port = run(['docker', 'port', name, '5432/tcp'], capture_output=True).stdout.strip()
        environment = os.environ.copy()
        environment.update(LAKE_SQL_TEST_ADDRESS=port, LAKE_SQL_TEST_USER='postgres')
        run(['cargo', 'test', '--offline', '--test', 'sql_security', '--', '--ignored'],
            env=environment)
    finally:
        subprocess.run(['docker', 'rm', '-f', name], capture_output=True, timeout=15)


if __name__ == '__main__':
    main()

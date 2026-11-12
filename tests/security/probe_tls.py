#!/usr/bin/env python3
"""Exercise native discovery, worker TLS policy, and cleanup in disposable PG18."""
import argparse
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]


def run(args, **kwargs):
    kwargs.setdefault('timeout', 120)
    return subprocess.run(args, cwd=ROOT, check=True, text=True, **kwargs)


def literal(value):
    return "E'" + value.replace('\\', '\\\\').replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', required=True, help='cached pg_lake destination image with build tools')
    args = parser.parse_args()
    name = 'lake-tls-security-' + secrets.token_hex(6)

    def sql(statement, succeeds=True):
        result = subprocess.run(['docker', 'exec', '-i', name, 'psql', '-XAt',
                                 '-v', 'ON_ERROR_STOP=1', '-h', '127.0.0.1',
                                 '-U', 'postgres', '-d', 'postgres'],
                                input=statement, text=True, capture_output=True, timeout=30)
        if succeeds and result.returncode:
            raise RuntimeError(result.stderr)
        if not succeeds:
            assert result.returncode, 'unexpectedly accepted insecure source policy'
            return result
        return result.stdout.strip()

    def create(sub, connection):
        return (f'BEGIN; CREATE SUBSCRIPTION {sub} CONNECTION {literal(connection)} '
                "PUBLICATION tls_pub WITH(lake_target='iceberg',create_slot=false); "
                "UPDATE lake_replication.worker_launch_config SET desired_state='stopped' "
                f"WHERE subscription_id=(SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='{sub}'); COMMIT;")

    base = 'host=source.test hostaddr=127.0.0.1 port=5432 dbname=postgres user=postgres'
    good = base + ' sslmode=verify-full sslrootcert=/tmp/tls-certs/ca.crt'
    bad_connections = [
        base + ' sslrootcert=/tmp/tls-certs/wrong.crt',
        good.replace('host=source.test', 'host=wrong.test'),
        good.replace('port=5432', 'port=5433'),
    ]
    try:
        run(['docker', 'run', '--pull=never', '--rm', '-d', '--name', name,
             '--entrypoint', '/bin/bash',
             '--mount', f'type=bind,src={ROOT},dst=/input,readonly',
             '-p', '127.0.0.1::5432', '-p', '127.0.0.1::5433', args.image,
             '/input/tests/security/tls-entrypoint.sh'], capture_output=True)
        deadline = time.monotonic() + 50
        while True:
            ready = subprocess.run(['docker', 'exec', name, 'pg_isready',
                                    '-h', '127.0.0.1', '-U', 'postgres'],
                                   capture_output=True, timeout=5)
            if ready.returncode == 0:
                break
            if time.monotonic() > deadline:
                raise RuntimeError('TLS fixture did not become ready')
            time.sleep(.2)
        sql('CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication; '
            'CREATE TABLE tls_items(id bigint PRIMARY KEY); CREATE PUBLICATION tls_pub FOR TABLE tls_items;')
        for mode in ('prefer', 'allow', 'require', 'verify-ca'):
            sql(create('bad_mode', base + ' sslmode=' + mode), succeeds=False)
        sql(create('missing_ca', base), succeeds=False)
        sql(create('relative_ca', base + ' sslrootcert=relative.crt'), succeeds=False)
        sql(create('redirected_loopback', 'host=localhost hostaddr=192.0.2.1 dbname=postgres'), succeeds=False)
        for connection in ('host=/tmp dbname=postgres', 'host=source.test service=hidden', 'host=a,b'):
            sql(create('implicit_policy', connection), succeeds=False)
        for connection in bad_connections:
            sql(create('bad_certificate', connection), succeeds=False)
        assert sql('SELECT count(*) FROM lake_replication.subscriptions') == '0'
        print('PASS discovery rejects weak modes, missing trust, bad CA, wrong hostname, and plaintext', flush=True)

        for index, connection in enumerate([
            'host=127.0.0.1 port=5432 dbname=postgres user=postgres',
            base + ' sslmode=disable',
        ]):
            sub = f'plaintext_lab_{index}'
            sql(create(sub, connection))
            assert sql("SELECT (lake_replication.source_tls_settings(subscription_id))[1] "
                       f"FROM lake_replication.subscriptions WHERE subscription_name='{sub}'") == 'disable'
            sql(f'DROP SUBSCRIPTION {sub}')

        sql("SELECT pg_create_logical_replication_slot('tls_good','pgoutput')")
        # URI input and default remote verify-full both exercise serialization.
        uri = ('postgresql://postgres@source.test:5432/postgres?hostaddr=127.0.0.1'
               '&sslrootcert=%2Ftmp%2Ftls-certs%2Fca.crt')
        sql(create('tls_good', uri))
        subscription = sql("SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='tls_good'")
        assert sql(f'SELECT array_to_string(lake_replication.source_tls_settings({subscription}),\'|\')') == 'verify-full|/tmp/tls-certs/ca.crt|source.test'
        sql("CREATE ROLE policy_reader; GRANT lake_replication_worker TO policy_reader; SET ROLE policy_reader; "
            f"SELECT lake_replication.source_tls_settings({subscription}); RESET ROLE;")
        sql("CREATE ROLE no_policy; SET ROLE no_policy; "
            f"SELECT lake_replication.source_tls_settings({subscription})", succeeds=False)
        with tempfile.TemporaryDirectory(prefix='lake-public-ca-') as temp:
            for filename in ('ca.crt', 'wrong.crt'):
                run(['docker', 'cp', f'{name}:/tmp/tls-certs/{filename}', str(Path(temp) / filename)], capture_output=True)
            tls_address = run(['docker', 'port', name, '5432/tcp'], capture_output=True).stdout.strip()
            plain_address = run(['docker', 'port', name, '5433/tcp'], capture_output=True).stdout.strip()
            environment = os.environ | {
                'LAKE_SQL_TEST_ADDRESS': tls_address, 'LAKE_TEST_PLAIN_ADDRESS': plain_address,
                'LAKE_LOAD_SOURCE_CREDENTIALS': '1', 'LAKE_SUBSCRIPTION_ID': subscription,
                'LAKE_TEST_CA_FILE': str(Path(temp) / 'ca.crt'),
                'LAKE_TEST_BAD_CA_FILE': str(Path(temp) / 'wrong.crt'),
                'LAKE_SOURCE_TLS_MODE': 'disable', 'LAKE_SOURCE_TLS_SERVER_NAME': 'wrong.test',
                'LAKE_SOURCE_TLS_ROOT_CERT': '/intentionally-wrong-ca',
            }
            run(['cargo', 'test', '--offline', '--test', 'source_tls', '--', '--ignored'], env=environment)
        sql('DROP SUBSCRIPTION tls_good')
        assert sql("SELECT count(*) FROM pg_replication_slots WHERE slot_name='tls_good'") == '0'
        print('PASS verified URI discovery, worker policy, access controls, and native slot cleanup', flush=True)

        for index, connection in enumerate(bad_connections):
            sub = f'tls_cleanup_{index}'
            sql(f"SELECT pg_create_logical_replication_slot('{sub}','pgoutput')")
            sql(create(sub, good))
            sql(f'UPDATE lake_replication.source_connections SET conninfo={literal(connection)} '
                f"WHERE subscription_id=(SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='{sub}')")
            sql(f'DROP SUBSCRIPTION {sub}')
            assert sql(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{sub}'") == '1'
            sql(f"SELECT pg_drop_replication_slot('{sub}')")
        print('PASS cleanup refuses bad CA, wrong hostname, and plaintext without dropping source slots', flush=True)
    except BaseException:
        subprocess.run(['docker', 'logs', '--tail', '45', name], timeout=15)
        raise
    finally:
        subprocess.run(['docker', 'rm', '-f', name], capture_output=True, timeout=15)


if __name__ == '__main__':
    main()

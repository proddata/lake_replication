#!/usr/bin/env python3
"""Qualify explicit launcher-managed logins from an exported native archive."""
import hashlib
import importlib.util
import json
import secrets
from pathlib import Path
import tempfile
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('identity_qualification', ROOT/'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)


def literal(value):
    return "E'"+str(value).replace('\\', '\\\\').replace("'", "''")+"'"


def destination(statement):
    return q.sql('destination', 'analytics', statement)


def source(statement):
    return q.sql('source', 'application', statement)


def subscription(name, file):
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD, safe='')}@source:5432/application?sslmode=disable"
    destination(f'CREATE SUBSCRIPTION {name} CONNECTION {literal(connection)} PUBLICATION identity_pub '
                f"WITH(lake_target='iceberg',lake_credential_file={literal(file)})")
    return int(destination('SELECT subscription_id FROM lake_replication.subscriptions '
                           f'WHERE subscription_name={literal(name)}'))


def exercise():
    source("CREATE TABLE identity_items(id bigint PRIMARY KEY,payload text); "
           "INSERT INTO identity_items VALUES(1,'snapshot'); "
           "CREATE PUBLICATION identity_pub FOR TABLE identity_items; "
           "GRANT SELECT ON identity_items TO lake_source")
    # This role is deliberately privileged: it verifies login selection, not
    # minimum grants. Privilege isolation remains a separate qualification task.
    destination("CREATE ROLE identity_explicit_worker LOGIN SUPERUSER PASSWORD "+literal(q.ADMIN_PASSWORD))
    q.compose('run', '--rm', '--no-deps', '--entrypoint', 'sh', 'credentials-init', '-ec', '''
set -eu
umask 077
sed '/^LAKE_DESTINATION_USER=/d' /credentials/destination-only.env > /credentials/identity-missing.env
cp /credentials/identity-missing.env /credentials/identity-blank.env
printf '%s\\n' 'LAKE_DESTINATION_USER=' >> /credentials/identity-blank.env
cp /credentials/identity-missing.env /credentials/identity-explicit.env
printf '%s\\n' 'LAKE_DESTINATION_USER=identity_explicit_worker' >> /credentials/identity-explicit.env
chown postgres:postgres /credentials/identity-*.env
chmod 0600 /credentials/identity-*.env
''')
    postmaster = destination('SELECT pg_postmaster_start_time()')
    for name, file in [('identity_missing', 'identity-missing.env'), ('identity_blank', 'identity-blank.env')]:
        identity = subscription(name, '/credentials/'+file)
        script = ("from pathlib import Path; print(''.join(p.read_text() for p in "
                  f"Path('/worker-logs').glob('*_{identity}_supervisor.log')))" )
        deadline = time.monotonic()+20
        logs = ''
        while time.monotonic() < deadline:
            logs = q.compose('exec', '-T', 'destination', 'python3', '-c', script).stdout
            if 'managed workers require a non-empty LAKE_DESTINATION_USER' in logs:
                break
            time.sleep(.2)
        q.check(name+' reports missing explicit identity',
                'managed workers require a non-empty LAKE_DESTINATION_USER' in logs, True)
        q.check(name+' does not copy destination rows', destination("SELECT to_regclass('identity_items') IS NULL"), 't')
        q.check(name+' does not acquire a source slot', source(
                f'SELECT count(*) FROM pg_replication_slots WHERE slot_name={literal(name)}'), '0')
        q.check(name+' logs do not expose a password', q.ADMIN_PASSWORD not in logs and q.SOURCE_PASSWORD not in logs, True)
        destination(f'DROP SUBSCRIPTION {name}')
    identity = subscription('identity_explicit', '/credentials/identity-explicit.env')
    q.check('explicit identity completes initial copy', q.wait_for('destination', 'analytics',
            "SELECT payload FROM identity_items WHERE id=1", 'snapshot', timeout=90), 'snapshot')
    q.check('destination sessions use configured login', destination(
            "SELECT count(*)>0 FROM pg_stat_activity WHERE usename='identity_explicit_worker' AND datname='analytics'"), 't')
    supervisor = int(destination('SELECT supervisor_pid FROM lake_replication.service_status '
                                 f'WHERE subscription_id={identity}'))
    check_env = (f"from pathlib import Path; e=Path('/proc/{supervisor}/environ').read_bytes().split(b'\\0'); "
                 "print(any(v.startswith(b'LAKE_LOAD_SOURCE_CREDENTIALS=1') for v in e) "
                 "and not any(v.startswith(b'LAKE_USER=') for v in e))")
    q.check('launcher omits implicit LAKE_USER', q.compose('exec', '-T', '--user', 'postgres', 'destination',
            'python3', '-c', check_env).stdout.strip(), 'True')
    source("UPDATE identity_items SET payload='streamed' WHERE id=1")
    q.check('explicit identity streams updates', q.wait_for('destination', 'analytics',
            'SELECT payload FROM identity_items WHERE id=1', 'streamed'), 'streamed')
    destination('ALTER SUBSCRIPTION identity_explicit DISABLE')
    q.check('explicit identity stops', q.wait_for('destination', 'analytics',
            f'SELECT health_state FROM lake_replication.subscription_status WHERE subscription_id={identity}', 'stopped'), 'stopped')
    destination('ALTER SUBSCRIPTION identity_explicit ENABLE')
    source("INSERT INTO identity_items VALUES(2,'after-enable')")
    q.check('explicit identity reloads after enable', q.wait_for('destination', 'analytics',
            'SELECT payload FROM identity_items WHERE id=2', 'after-enable'), 'after-enable')
    destination('DROP SUBSCRIPTION identity_explicit')
    q.check('explicit identity source slot removed', q.wait_for('source', 'application',
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name='identity_explicit'", '0'), '0')
    q.check('all identity control rows removed', destination('SELECT count(*) FROM lake_replication.subscriptions'), '0')
    q.check('identity failures and recovery preserve postmaster', destination('SELECT pg_postmaster_start_time()'), postmaster)


def main():
    # The shared harness uses second-resolution names. Isolate concurrent
    # probe invocations and standard qualification runs, including image tags.
    token=secrets.token_hex(4)
    q.PROJECT+='-identity-'+token
    q.SOURCE_IMAGE+='-identity-'+token
    q.DESTINATION_IMAGE+='-identity-'+token
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE,
                 QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    q.RESULTS = ROOT/'.qualification'/(q.RUN+'_worker_identity_'+token)
    q.RESULTS.mkdir(parents=True)
    q.EVIDENCE.update(status='running', scope=__doc__, release_qualified=False,
                     minimum_grants_qualified=False, subscription_isolation_qualified=False,
                     project=q.PROJECT)
    q.EVIDENCE['inputs'] = {}
    for p in [Path(__file__), Path(q.__file__)]:
        name=p.relative_to(ROOT);target=q.RESULTS/'sources'/name
        target.parent.mkdir(parents=True, exist_ok=True);target.write_bytes(p.read_bytes())
        q.EVIDENCE['inputs'][str(name)]=hashlib.sha256(p.read_bytes()).hexdigest()
    try:
        archive=q.archive_path();q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-worker-identity-') as temp:
            q.build_images(archive, Path(temp))
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        exercise()
        q.EVIDENCE['status']='passed'
    except Exception as error:
        q.EVIDENCE.update(status='error', error=str(error))
        print('ERROR:', error, flush=True)
    finally:
        try:
            logs=q.compose('logs', '--no-color', check_result=False)
            (q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        finally:
            cleanup=q.compose('down', '-v', '--remove-orphans', check_result=False, timeout=300)
            q.EVIDENCE['cleanup_returncode']=cleanup.returncode
            if cleanup.returncode:q.EVIDENCE['status']='error'
            (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE, indent=2, sort_keys=True)+'\n')
            print('Evidence:', q.RESULTS, flush=True)
    return int(q.EVIDENCE['status']!='passed')


if __name__=='__main__':
    raise SystemExit(main())

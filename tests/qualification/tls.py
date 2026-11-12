#!/usr/bin/env python3
"""Exercise certificate-verified TLS with unchanged archived native workers."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', Path(__file__).with_name('probe.py'))
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)


def sql(statement):
    return q.sql('destination', 'analytics', statement)


def configure():
    entry = q.RESULTS / 'source-tls-entrypoint.sh'
    entry.write_text((ROOT / 'deploy/tls/entrypoint.sh').read_text().replace(
        'DNS:localhost,IP:127.0.0.1', 'DNS:localhost,DNS:source,IP:127.0.0.1').replace(
        'exec /usr/local/bin/docker-entrypoint.sh',
        'cp /tls-lab/require-tls.sh /docker-entrypoint-initdb.d/30-require-tls.sh\nchmod 0644 /docker-entrypoint-initdb.d/30-require-tls.sh\nexec /usr/local/bin/docker-entrypoint.sh'))
    hba = q.RESULTS / 'require-tls.sh'
    hba.write_text('#!/bin/sh\nset -eu\nsed -i \'1ihostnossl all all all reject\' "$PGDATA/pg_hba.conf"\n')
    text = (ROOT / 'deploy/qualification/compose.yml').read_text()
    text = text.replace('  source:\n', '  source:\n    entrypoint: [bash, /tls-lab/entrypoint.sh]\n', 1)
    text = text.replace('      - password_encryption=scram-sha-256\n',
        '      - password_encryption=scram-sha-256\n      - -c\n      - ssl=on\n      - -c\n      - ssl_cert_file=/certs/server.crt\n      - -c\n      - ssl_key_file=/certs/server.key\n', 1)
    text = text.replace('      - source-data:/var/lib/postgresql/18/data\n',
        f'      - source-data:/var/lib/postgresql/18/data\n      - certificates:/certs\n      - {entry}:/tls-lab/entrypoint.sh:ro\n      - {hba}:/tls-lab/require-tls.sh:ro\n', 1)
    text = text.replace('      - credentials:/credentials:ro\n', '      - credentials:/credentials:ro\n      - certificates:/certs:ro\n')
    q.COMPOSE_FILE.write_text(text + '  certificates:\n')


def exercise():
    plaintext = q.compose('exec', '-T', 'source', 'sh', '-ec',
        'PGSSLMODE=disable PGPASSWORD="$POSTGRES_PASSWORD" psql -XAt -h 127.0.0.1 -U postgres -d application -c "SELECT 1"', check_result=False)
    q.check('TLS source refuses plaintext TCP', plaintext.returncode != 0 and 'no encryption' in plaintext.stderr, True)
    q.compose('run', '--rm', '--no-deps', '-T', '--user', 'root', '--entrypoint', '/bin/bash', 'credentials-init', '-ec', """set -eu
for setting in good bad_ca bad_name; do
  grep -v '^LAKE_SOURCE_TLS_' /credentials/destination-only.env > /credentials/tls_$setting.env
  printf '%s\n' LAKE_SOURCE_TLS_MODE=verify-full >> /credentials/tls_$setting.env
  root=/certs/ca.crt; server=source
  if test "$setting" = bad_ca; then root=/certs/untrusted-ca.crt; fi
  if test "$setting" = bad_name; then server=wrong.example; fi
  printf '%s\n' "LAKE_SOURCE_TLS_ROOT_CERT=$root" "LAKE_SOURCE_TLS_SERVER_NAME=$server" >> /credentials/tls_$setting.env
  chown postgres:postgres /credentials/tls_$setting.env
  chmod 600 /credentials/tls_$setting.env
done
""")
    q.sql('source', 'application', "CREATE TABLE tls_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO tls_items VALUES(1,'initial'); CREATE PUBLICATION tls_pub FOR TABLE tls_items; GRANT SELECT ON tls_items TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD, safe='')}@source:5432/application?sslmode=verify-full&sslrootcert=%2Fcerts%2Fca.crt"
    source_ip = q.compose('exec', '-T', 'source', 'hostname', '-i').stdout.strip().split()[0]
    for row, setting, error in [(2, 'bad_ca', 'UnknownIssuer'), (3, 'bad_name', 'wrong.example')]:
        sub = 'tls_' + setting
        bad_connection = connection.replace('ca.crt', 'untrusted-ca.crt') if setting == 'bad_ca' else connection + f'&host=wrong.example&hostaddr={source_ip}'
        refusal = q.sql_result('destination', 'analytics',
            f"CREATE SUBSCRIPTION {sub} CONNECTION '{bad_connection}' PUBLICATION tls_pub WITH(lake_target='iceberg')", check_result=False)
        q.check(setting + ' native discovery rejects invalid certificate', refusal.returncode != 0, True)
        sql(f'CREATE SCHEMA {sub}')
        # Corrupt the stored trust before commit to independently exercise the
        # worker check. Ordinary CREATE/ALTER already refuse this connection.
        sql(f"BEGIN; CREATE SUBSCRIPTION {sub} CONNECTION '{connection}' PUBLICATION tls_pub WITH(lake_target='iceberg',lake_target_schema='{sub}',lake_credential_file='/credentials/tls_{setting}.env'); "
            f"UPDATE lake_replication.source_connections SET conninfo='{bad_connection}' WHERE subscription_id=(SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='{sub}'); COMMIT")
        diagnostic = f"SELECT coalesce(bool_or(ss.last_error LIKE '%initial sync exited%'),false)::text FROM lake_replication.service_status ss JOIN lake_replication.subscriptions s USING(subscription_id) WHERE s.subscription_name='{sub}'"
        q.check(setting + ' failed child is visible in service status', q.wait_for('destination', 'analytics', diagnostic, 'true'), 'true')
        # Initial-sync certificate details live in its structured child log;
        # service_status deliberately reports the child exit rather than stderr.
        deadline = time.monotonic() + 10
        matched = False
        while time.monotonic() < deadline:
            records = q.worker_logs().splitlines()
            for line in records:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get('component') == 'initial_sync' and record.get('event') == 'process_failed' and error in record.get('message', ''):
                    matched = True
            if matched:
                break
            time.sleep(.2)
        q.check(setting + ' archive worker rejects invalid certificate', matched, True)
        q.check(setting + ' rejection cannot advance checkpoint', sql(f"SELECT (received_lsn='0/0' AND staged_lsn='0/0' AND published_lsn='0/0')::text FROM lake_replication.checkpoints JOIN lake_replication.subscriptions USING(subscription_id) WHERE subscription_name='{sub}'"), 'true')
        sql(f"ALTER SUBSCRIPTION {sub} CONNECTION '{connection}'")
        query = f'SELECT count(*)::text FROM {sub}.tls_items'
        q.check(setting + ' corrected trust permits native snapshot', q.wait_for('destination', 'analytics', query, str(row - 1)), str(row - 1))
        q.sql('source', 'application', f"INSERT INTO tls_items VALUES({row},'live-tls')")
        q.check(setting + ' archive worker streams over verified TLS and SCRAM', q.wait_for('destination', 'analytics', query, str(row)), str(row))
        q.check(setting + ' source observes encrypted worker sessions', q.sql('source', 'application', "SELECT (count(*)>0 AND bool_and(s.ssl))::text FROM pg_stat_activity a JOIN pg_stat_ssl s USING(pid) WHERE a.usename='lake_source'"), 'true')
        q.check(setting + ' copy and streaming retain exact values', sql(f"SELECT string_agg(id||':'||note,',' ORDER BY id) FROM {sub}.tls_items"), '1:initial' + ''.join(f',{n}:live-tls' for n in range(2, row + 1)))
        sql(f'DROP SUBSCRIPTION {sub}')
        q.check(setting + ' native cleanup removes its slot over TLS', q.sql('source', 'application', f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{sub}'"), '0')


def main():
    q.RESULTS = ROOT / '.qualification' / (q.RUN + '_archive_tls')
    q.PROJECT += '-archive-tls'
    q.RESULTS.mkdir(parents=True)
    q.COMPOSE_FILE = q.RESULTS / 'compose.yml'
    configure()
    q.EVIDENCE['scope'] = __doc__
    q.EVIDENCE['inputs'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__), Path(__file__).with_name('probe.py'), ROOT / 'deploy/tls/entrypoint.sh', ROOT / 'Cargo.lock', ROOT / 'worker/Cargo.toml']}
    exit_code = 1
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-tls-artifact-') as temp:
            q.build_images(archive, Path(temp))
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        exercise()
        q.EVIDENCE['status'] = 'passed'
        exit_code = 0
    except Exception as error:
        q.EVIDENCE.update(status='error', error=str(error))
        print('ERROR:', error, flush=True)
    finally:
        logs = q.compose('logs', '--no-color', check_result=False)
        (q.RESULTS / 'compose.log').write_text(logs.stdout + logs.stderr)
        workers = q.compose('exec', '-T', 'destination', 'sh', '-ec',
                            "find /worker-logs -maxdepth 1 -type f -name '*.log' -exec cat {} +", check_result=False)
        (q.RESULTS / 'workers.jsonl').write_text(workers.stdout + workers.stderr)
        diagnostics = q.sql_result('destination', 'analytics',
            "SELECT jsonb_agg(jsonb_build_object('name',s.subscription_name,'state',s.state,'error',s.last_error,'service_error',ss.last_error,'source_error',ss.last_source_error)) FROM lake_replication.subscriptions s JOIN lake_replication.service_status ss USING(subscription_id)", check_result=False)
        (q.RESULTS / 'service-diagnostics.json').write_text(diagnostics.stdout)
        cleanup = q.compose('down', '-v', '--remove-orphans', check_result=False, timeout=300)
        q.EVIDENCE['cleanup_returncode'] = cleanup.returncode
        if cleanup.returncode:
            q.EVIDENCE['status'] = 'error'
            exit_code = 1
        (q.RESULTS / 'results.json').write_text(json.dumps(q.EVIDENCE, indent=2, sort_keys=True) + '\n')
        print('Evidence:', q.RESULTS, flush=True)
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Exercise packaged active-subscription DROP and a forced busy-slot retry."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import tempfile
import time
from urllib.parse import quote

import probe as q


def destination(statement):
    return q.sql('destination', 'analytics', statement)


def source(statement):
    return q.sql('source', 'application', statement)


def create(name):
    connection = (f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD, safe='')}"
                  '@source:5432/application?sslmode=disable')
    destination(f"CREATE SUBSCRIPTION {name} CONNECTION '{connection}' "
                "PUBLICATION slot_cleanup_pub WITH (lake_target='iceberg', "
                "lake_credential_file='/credentials/destination-only.env')")
    q.check(name+' initial copy completes', q.wait_for('destination', 'analytics',
            'SELECT count(*) FROM slot_cleanup_items', '3', timeout=90), '3')
    q.check(name+' receiver holds its source slot', q.wait_for('source', 'application',
            f"SELECT active FROM pg_replication_slots WHERE slot_name='{name}'", 't'), 't')


def removed(name):
    q.check(name+' source slot removed', q.wait_for('source', 'application',
            f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{name}'", '0', timeout=15), '0')
    q.check(name+' control rows removed', destination(
            f"SELECT count(*) FROM lake_replication.subscriptions WHERE subscription_name='{name}'"), '0')
    destination('DROP TABLE slot_cleanup_items')


def exercise():
    source("CREATE TABLE slot_cleanup_items(id bigint PRIMARY KEY, payload text NOT NULL); "
           "INSERT INTO slot_cleanup_items VALUES (1,'one'),(2,'two'),(3,'three'); "
           "CREATE PUBLICATION slot_cleanup_pub FOR TABLE slot_cleanup_items; "
           "GRANT SELECT ON slot_cleanup_items TO lake_source")
    postmaster = destination('SELECT pg_postmaster_start_time()')
    for number in range(1, 4):
        name = f'slot_active_drop_{number}'
        create(name)
        source(f"UPDATE slot_cleanup_items SET payload='pass-{number}' WHERE id=1")
        q.check(name+' streams before active DROP', q.wait_for('destination', 'analytics',
                'SELECT payload FROM slot_cleanup_items WHERE id=1', f'pass-{number}'), f'pass-{number}')
        destination(f'DROP SUBSCRIPTION {name}')
        removed(name)
        q.check(name+' preserves postmaster', destination('SELECT pg_postmaster_start_time()'), postmaster)

    # Hold the stopped subscription's slot with an independent pgoutput client.
    # Release it only AFTER observing pg_drop_replication_slot's busy-slot error,
    # so this test cannot pass without surviving an actual subtransaction error.
    name = 'slot_retry_held'
    create(name)
    destination(f'ALTER SUBSCRIPTION {name} DISABLE')
    q.check('retry fixture subscription stops', q.wait_for('destination', 'analytics',
            f"SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='{name}'",
            'stopped'), 'stopped')
    q.check('retry fixture releases native slot', q.wait_for('source', 'application',
            f"SELECT active FROM pg_replication_slots WHERE slot_name='{name}'", 'f'), 'f')
    with ThreadPoolExecutor(max_workers=2) as pool:
        receiver = pool.submit(q.compose, 'exec', '-T', 'source', 'pg_recvlogical', '-U', 'postgres',
                '-d', 'application', '--slot='+name, '--start', '--no-loop', '--file=/dev/null',
                '--option=proto_version=1', '--option=publication_names=slot_cleanup_pub',
                check_result=False, timeout=40)
        try:
            q.check('independent receiver holds retry slot', q.wait_for('source', 'application',
                    f"SELECT active FROM pg_replication_slots WHERE slot_name='{name}'", 't', timeout=10), 't')
            drop = pool.submit(destination, f'DROP SUBSCRIPTION {name}')
            needle = f'replication slot "{name}" is active'
            deadline = time.monotonic()+4
            busy = False
            while time.monotonic() < deadline:
                log = q.compose('logs', '--no-color', 'source').stdout
                if needle in log:
                    busy = True
                    break
                time.sleep(.05)
            q.check('DROP encountered an actual busy-slot error', busy, True)
            # Give an incorrectly optimized success flag time to skip its retry.
            time.sleep(.4)
            q.check('DROP still waits while independent receiver holds slot', drop.done(), False)
        finally:
            source(f"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                   f"WHERE slot_name='{name}' AND active_pid IS NOT NULL")
            receiver.result(timeout=10)
        drop.result(timeout=15)
    removed(name)
    q.check('busy-slot retry preserves postmaster', destination('SELECT pg_postmaster_start_time()'), postmaster)


def main():
    q.RESULTS = q.ROOT/'.qualification'/(q.RUN+'_slot_cleanup')
    q.RESULTS.mkdir(parents=True)
    q.EVIDENCE.update(scope=__doc__, release_qualified=False, status='running')
    q.EVIDENCE['inputs'] = {}
    for path in [Path(__file__), Path(q.__file__)]:
        relative = path.relative_to(q.ROOT)
        q.EVIDENCE['inputs'][str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
        target = q.RESULTS/'sources'/relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-slot-cleanup-') as temp:
            q.build_images(archive, Path(temp))
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        exercise()
        q.EVIDENCE['status'] = 'passed'
    except Exception as error:
        q.EVIDENCE.update(status='error', error=str(error))
        print('ERROR:', error, flush=True)
    finally:
        try:
            logs = q.compose('logs', '--no-color', check_result=False)
            (q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        finally:
            cleanup = q.compose('down', '-v', '--remove-orphans', check_result=False, timeout=300)
            q.EVIDENCE['cleanup_returncode'] = cleanup.returncode
            if cleanup.returncode:
                q.EVIDENCE['status'] = 'error'
            (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE, indent=2, sort_keys=True)+'\n')
            print('Evidence:', q.RESULTS, flush=True)
    return int(q.EVIDENCE['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())

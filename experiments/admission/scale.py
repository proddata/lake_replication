#!/usr/bin/env python3
"""Shared multi-database restart and worker-budget cases for admission experiments."""
import importlib.util
import json
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('comparison', Path(__file__).with_name('compare.py'))
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
q = c.q
WRAPPERS = "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'"
OWNERS = "SELECT count(*) FROM extension_base.list_base_workers() w JOIN pg_extension e ON e.oid=w.extension_id WHERE e.extname='lake_replication' AND w.database_id=(SELECT oid FROM pg_database WHERE datname=current_database())"


def owners(names, expected):
    # Extension OIDs are database-local: resolve each registration in its own
    # database rather than joining every worker to analytics.pg_extension.
    for db in names:
        q.check(db + ' replication owner count', q.wait_for('destination', db, OWNERS, str(expected)), str(expected))


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def inventory():
    return {
        'backend_types': json.loads(q.sql('destination', 'analytics',
            "SELECT coalesce(json_agg(t),'[]') FROM (SELECT backend_type,count(*) FROM pg_stat_activity GROUP BY 1 ORDER BY 1)t")),
        'base_workers': json.loads(q.sql('destination', 'analytics',
            'SELECT coalesce(json_agg(w),\'[]\') FROM extension_base.list_base_workers() w')),
        'destination': c.resources('destination'),
        'pgduck': c.resources('pgduck'),
        'source_slots': json.loads(q.sql('source', 'application',
            "SELECT coalesce(json_agg(t),'[]') FROM (SELECT slot_name,active,wal_status FROM pg_replication_slots ORDER BY slot_name)t")),
        'max_worker_processes': q.sql('destination', 'analytics', 'SHOW max_worker_processes'),
    }


def scale_case(databases, subscriptions, budget, image, text, directory):
    directory.mkdir()
    q.COMPOSE_FILE = directory / 'compose.yml'
    q.COMPOSE_FILE.write_text(text.replace('max_worker_processes=24', f'max_worker_processes={budget}')
        .replace('shared_preload_libraries=pg_extension_base,lake_replication', 'shared_preload_libraries=pg_extension_base')
        .replace('      - -c\n      - lake_replication.database_name=analytics\n', ''))
    q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = image
    result = dict(databases=databases, subscriptions=subscriptions, worker_budget=budget)
    names = ['analytics'] + [f'scale_db_{i}' for i in range(1, databases)]
    try:
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        for db in names[1:]:
            q.sql('destination', 'analytics', f'CREATE DATABASE {db}')
            q.sql('destination', db, 'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
        owners(names, 1)
        result['empty_inventory'] = inventory()
        connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD, safe='')}@source:5432/application"
        started = time.monotonic()
        for i in range(subscriptions):
            q.sql('source', 'application', f'CREATE TABLE scale_items_{i}(id bigint PRIMARY KEY,note text); '
                f"INSERT INTO scale_items_{i} VALUES(1,'initial'); CREATE PUBLICATION scale_pub_{i} FOR TABLE scale_items_{i}; "
                f'GRANT SELECT ON scale_items_{i} TO lake_source')
            q.sql('destination', names[i % databases], f"CREATE SUBSCRIPTION scale_sub_{i} CONNECTION '{connection}' PUBLICATION scale_pub_{i} "
                "WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        for i in range(subscriptions):
            db = names[i % databases]
            q.check(f'{db}/scale_sub_{i} initial copy', q.wait_for('destination', db,
                f"SELECT state FROM lake_replication.subscriptions WHERE subscription_name='scale_sub_{i}'", 'streaming', timeout=120), 'streaming')
        result['setup_until_all_streaming_seconds'] = time.monotonic() - started
        q.check('one wrapper per enabled subscription', q.wait_for('destination', 'analytics', WRAPPERS, str(subscriptions)), str(subscriptions))
        result['active_inventory'] = inventory()
        # One source transaction changes every publication. Check actual Iceberg
        # rows after catch-up and after restart; process counts alone are insufficient.
        for marker in ('live', 'restarted'):
            if marker == 'restarted':
                q.compose('restart', 'destination', timeout=180)
                q.wait_for('destination', 'analytics', 'SELECT 1', '1', timeout=90)
            q.sql('source', 'application', 'BEGIN; ' + '; '.join(
                f"UPDATE scale_items_{i} SET note='{marker}' WHERE id=1" for i in range(subscriptions)) + '; COMMIT')
            began = time.monotonic()
            for i in range(subscriptions):
                q.check(f'{marker} actual Iceberg rows {i}', q.wait_for('destination', names[i % databases],
                    f'SELECT note FROM scale_items_{i} WHERE id=1', marker, timeout=120), marker)
            result[marker + '_all_visible_seconds'] = time.monotonic() - began
            owners(names, 1)
            q.check(marker + ' all wrappers present', q.wait_for('destination', 'analytics', WRAPPERS, str(subscriptions)), str(subscriptions))
        result['restart_inventory'] = inventory()
        for i in range(subscriptions):
            q.sql('destination', names[i % databases], f'ALTER SUBSCRIPTION scale_sub_{i} DISABLE')
        q.check('all disabled wrappers drain', q.wait_for('destination', 'analytics', WRAPPERS, '0'), '0')
        q.check('disabled source slots release', q.wait_for('source', 'application',
            'SELECT count(*) FROM pg_replication_slots WHERE active', '0'), '0')
        result['disabled_inventory'] = inventory()
        for i in range(subscriptions):
            q.sql('destination', names[i % databases], f'DROP SUBSCRIPTION scale_sub_{i}')
        q.check('native cleanup removes every source slot', q.sql('source', 'application', 'SELECT count(*) FROM pg_replication_slots'), '0')
        for db in names:
            q.sql('destination', db, 'DROP EXTENSION lake_replication CASCADE')
        owners(names, 0)
        result['dropped_inventory'] = inventory()
        result['status'] = 'passed'
    except Exception as error:
        result.update(status='finding', error=str(error))
        try:
            result['failure_inventory'] = inventory()
        except Exception as secondary:
            result['inventory_error'] = str(secondary)
        print('FINDING: ' + str(error), flush=True)
    except KeyboardInterrupt:
        result.update(status='interrupted')
        raise
    finally:
        try:
            logs = q.compose('logs', '--no-color', check_result=False)
            (directory / 'compose.log').write_text(logs.stdout + logs.stderr)
        finally:
            q.compose('down', '-v', '--remove-orphans', timeout=300)
            write_json(directory / 'results.json', result)
    return result

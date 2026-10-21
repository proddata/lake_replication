#!/usr/bin/env python3
"""Measure current prototype worker budgets and matched single-subscription performance.

Disposable private fixtures only. Subscriptions per case are TOTAL, distributed
evenly among databases; a separate source publication/table avoids target clashes.
"""
import argparse
import hashlib
import importlib.util
import json
import shutil
import tempfile
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--skip-scale', action='store_true')
    parser.add_argument('--skip-performance', action='store_true')
    parser.add_argument('--repetitions', type=int, default=5)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error('repetitions must be positive')
    q.RESULTS = ROOT / '.qualification' / (q.RUN + '_scale')
    q.RESULTS.mkdir(parents=True, exist_ok=False)
    result = dict(status='running', scale_cases=[], cases=[], parameters=vars(args),
        scope='Current publication/recovery prototype, Linux arm64; total subscriptions distributed across databases; no production adoption qualification')
    root = q.RESULTS
    original_compose = q.COMPOSE_FILE.read_text()
    parent_file = ROOT / 'docs/evidence/2026-10-19-base-publication-ownership-results.json'
    parent = json.loads(parent_file.read_text())
    sources = root / 'sources'
    sources.mkdir()
    for path in Path(__file__).parent.iterdir():
        if path.is_file():
            shutil.copy2(path, sources / path.name)
    result['source_fingerprints'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sources.iterdir()}
    result['parent_evidence_sha256'] = hashlib.sha256(parent_file.read_bytes()).hexdigest()
    try:
        for name, digest in parent['source_fingerprints'].items():
            if hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() != digest:
                raise RuntimeError('qualified parent source changed: ' + name)
        image = parent['image_id']
        q.check('exact current publication-qualified image', q.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', image]).stdout.strip(), image)
        archive = q.archive_path()
        q.verify_archive(archive)
        q.command(['git', 'diff', '--exit-code', q.EVIDENCE['source_revision'], '--', 'worker', 'migrations', 'extensions/lake_replication'])
        with tempfile.TemporaryDirectory(prefix='lake-scale-') as temporary:
            q.build_images(archive, Path(temporary))
        result['image_id'] = image
        result['other_running_containers'] = q.command(['docker', 'ps', '--format', '{{.ID}} {{.Names}} {{.Image}}']).stdout.splitlines()
        result['docker_info'] = json.loads(q.command(['docker', 'info', '--format', '{{json .}}']).stdout)
        text = original_compose.replace('  destination:\n', '  destination:\n    init: true\n')
        # Concurrent UPDATE/RETURNING queries exhausted the old 1 GB engine
        # fixture at four subscriptions on an 11-CPU host. Bound DuckDB query
        # parallelism as well as memory; use identical settings for both variants.
        engine = "printf 'SET threads=1;\\n' > /tmp/scale-pgduck.sql; cat /qualification/pgduck.sql >> /tmp/scale-pgduck.sql; exec pgduck_server --unix_socket_directory /var/run/pgduck --port 5332 --memory_limit 2GB --cache_dir /tmp/pgduck-cache --init_file_path /tmp/scale-pgduck.sql"
        text = text.replace('    entrypoint: [/qualification/pgduck.sh]', '    entrypoint: ' + json.dumps(['bash', '-ec', engine]))
        result['engine_settings'] = dict(memory_limit='2GB', threads=1)
        text = text.replace('      - max_worker_processes=24\n',
            '      - max_worker_processes=24\n      - -c\n      - track_commit_timestamp=on\n      - -c\n      - lake_replication.prototype_instance_id=scale-owner\n')
        text = text.replace('      - wal_level=logical\n', '      - track_commit_timestamp=on\n      - -c\n      - wal_level=logical\n')
        text = text.replace('max_replication_slots=8', 'max_replication_slots=32').replace('max_wal_senders=8', 'max_wal_senders=32')
        if not args.skip_scale:
            for dbs, subs in ((1, 1), (1, 4), (1, 16), (4, 4), (4, 16)):
                print(f'SCALE databases={dbs} subscriptions={subs} budget=24', flush=True)
                case = scale_case(dbs, subs, 24, image, text, root / f'scale-{dbs}db-{subs}sub-24slots')
                result['scale_cases'].append(case)
                write_json(root / 'results.json', result)
                if case['status'] != 'passed':
                    result['scale_cases'].append(scale_case(dbs, subs, 64, image, text, root / f'scale-{dbs}db-{subs}sub-64slots'))
                    write_json(root / 'results.json', result)
        if not args.skip_performance:
            tag = 'lake-replication-current-parent:' + q.RUN
            static = 'lake-replication-current-static:' + q.RUN
            q.command(['docker', 'tag', image, tag])
            build = q.command(['docker', 'build', '--build-arg', f'QUAL_IMAGE={tag}', '--tag', static,
                '--file', str(Path(__file__).with_name('Dockerfile.current-static')), str(ROOT)], timeout=600)
            (root / 'build-static.log').write_text(build.stdout + build.stderr)
            result['static_image_id'] = q.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', static]).stdout.strip()
            for profile, interval in (('steady', 500), ('catchup', 100)):
                for rep in range(1, args.repetitions + 1):
                    for variant in (('static', 'base') if rep % 2 else ('base', 'static')):
                        directory = root / f'{profile}-{rep:02d}-{variant}'
                        directory.mkdir()
                        print(f'PERFORMANCE {profile} pair={rep} {variant}', flush=True)
                        workload = argparse.Namespace(base_preload=False, transactions=120, warmup_transactions=10,
                            rows_per_transaction=100, payload_bytes=128, interval_ms=interval, observer='journal',
                            poll_ms=100, catchup_timeout=120, idle_seconds=5)
                        case = c.run_case(workload, variant, rep, image if variant == 'base' else static, text, directory)
                        case['profile'] = profile
                        result['cases'].append(case)
                        write_json(directory / 'results.json', case)
                        write_json(root / 'results.json', result)
        result['status'] = 'completed_with_findings' if any(s['status'] != 'passed' for s in result['scale_cases']) else 'passed'
    except Exception as error:
        result.update(status='error', error=str(error))
        print('ERROR: ' + str(error), flush=True)
    except KeyboardInterrupt:
        result.update(status='interrupted')
        raise
    finally:
        result['archive_provenance'] = q.EVIDENCE
        write_json(root / 'results.json', result)
        print('Evidence: ' + str(root), flush=True)
    return 1 if result['status'] == 'error' else 0


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Shared timing audits, bulk-copy coexistence cases and paired admission analysis."""
import hashlib
import importlib.util
import json
import threading
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('scale', Path(__file__).with_name('scale.py'))
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)
c, q = s.c, s.q
spec = importlib.util.spec_from_file_location('analysis', Path(__file__).with_name('analyze.py'))
a = importlib.util.module_from_spec(spec)
spec.loader.exec_module(a)


def timing_validation(work, tolerance_seconds=1.0):
    """Reject wall-clock steps against the independent monotonic observer.

    Source commits must fall inside their monotonic insertion intervals;
    publication commits must fall inside first-observation bounds. A generous
    one-second allowance covers local query/observer round trips, not host sleep.
    """
    errors=[]
    start=work['source_phase_start_epoch']
    rows=work['transactions']
    for index,row in enumerate(rows):
        end=rows[index+1]['insert_started_seconds'] if index+1<len(rows) else work['source_elapsed_seconds']
        elapsed=row['committed_epoch']-start
        if not row['insert_started_seconds']-tolerance_seconds <= elapsed <= end+tolerance_seconds:
            errors.append(f"source transaction {row['transaction']} clock differs from monotonic insertion window")
        published=row['publication_commit_epoch']-start
        if not row['not_seen_before_seconds']-tolerance_seconds <= published <= row['seen_by_seconds']+tolerance_seconds:
            errors.append(f"publication transaction {row['transaction']} clock differs from monotonic observation window")
    return dict(valid=not errors,tolerance_seconds=tolerance_seconds,errors=errors[:8])


def fairness_timing_validation(case, tolerance_seconds=1.0):
    trace=case['trace']
    if not trace:
        return dict(valid=False,errors=['no timing trace'])
    origin=trace[0]['copy']['observed_epoch']-trace[0]['elapsed_seconds']
    errors=[]
    for sample in trace:
        if abs(sample['copy']['observed_epoch']-origin-sample['elapsed_seconds'])>tolerance_seconds:
            errors.append('copy observer wall clock differs from monotonic trace')
    for stream in case['source_transactions']:
        for index,row in enumerate(stream):
            end=stream[index+1]['insert_started_seconds'] if index+1<len(stream) else trace[-1]['elapsed_seconds']
            elapsed=row['commit_epoch']-origin
            if not row['insert_started_seconds']-tolerance_seconds <= elapsed <= end+tolerance_seconds:
                errors.append('stream source clock differs from monotonic insertion window')
    return dict(valid=not errors,tolerance_seconds=tolerance_seconds,errors=errors[:8])


def compose_text():
    text = q.COMPOSE_FILE.read_text().replace('  destination:\n', '  destination:\n    init: true\n')
    text = text.replace('shared_preload_libraries=pg_extension_base,lake_replication', 'shared_preload_libraries=pg_extension_base')
    text = text.replace('      - -c\n      - lake_replication.database_name=analytics\n', '')
    text = text.replace('      - max_worker_processes=24\n', '      - max_worker_processes=64\n      - -c\n      - track_commit_timestamp=on\n      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n')
    text = text.replace('      - wal_level=logical\n', '      - track_commit_timestamp=on\n      - -c\n      - wal_level=logical\n')
    text = text.replace('max_replication_slots=8', 'max_replication_slots=32').replace('max_wal_senders=8', 'max_wal_senders=32')
    engine = "printf 'SET threads=1;\\n' > /tmp/measurement-pgduck.sql; cat /qualification/pgduck.sql >> /tmp/measurement-pgduck.sql; exec pgduck_server --unix_socket_directory /var/run/pgduck --port 5332 --memory_limit 2GB --cache_dir /tmp/pgduck-cache --init_file_path /tmp/measurement-pgduck.sql"
    return text.replace('    entrypoint: [/qualification/pgduck.sh]', '    entrypoint: ' + json.dumps(['bash', '-ec', engine]))


def fairness(args, variant, image, text, directory):
    directory.mkdir()
    q.COMPOSE_FILE = directory / 'compose.yml'
    q.COMPOSE_FILE.write_text(text)
    q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = image
    bulk_copies = getattr(args, 'bulk_copies', 1)
    admission_view = getattr(args, 'admission_view', 'lake_replication.prototype_writer_admission')
    copy_names = ['fair_copy'] + [f'fair_copy_{i}' for i in range(1, bulk_copies)]
    result = dict(variant=variant, image=image, copy_rows=args.copy_rows, bulk_copies=bulk_copies,
        stream_transactions=args.stream_transactions, stream_rows_per_transaction=20,
        offered_interval_ms=500, observer_interval_ms=500,
        limits=dict(p95_publication_ms=2000, max_publication_gap_seconds=5,
                    min_commits_during_copy=5, catchup_seconds=5, copy_timeout_seconds=180))
    clients = []
    producer = None
    stop = threading.Event()
    commits = [[], [], []]
    errors = []
    trace = []
    names = ['analytics', 'fair_stream_1', 'fair_stream_2']
    try:
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        for db in names[1:] + copy_names:
            q.sql('destination', 'analytics', f'CREATE DATABASE {db}')
            q.sql('destination', db, 'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
        connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
        source = c.Psql('source', 'application')
        clients.append(source)
        def seed_big():
            source.query('CREATE TABLE fair_big(id bigint PRIMARY KEY,payload text); '
                f"INSERT INTO fair_big SELECT n,repeat('b',128) FROM generate_series(1,{args.copy_rows}) n; "
                'CREATE PUBLICATION fair_copy_pub FOR TABLE fair_big; GRANT SELECT ON fair_big TO lake_source')
        # Dataset preparation precedes all measured slots. The optional old
        # ordering retains a separate empty-filtered-stream receiver reproducer.
        if not args.seed_after_subscriptions:
            seed_big()
        for i, db in enumerate(names):
            source.query(f'CREATE TABLE fair_items_{i}(id bigint PRIMARY KEY,tx integer,payload text); '
                f'CREATE PUBLICATION fair_pub_{i} FOR TABLE fair_items_{i}; GRANT SELECT ON fair_items_{i} TO lake_source')
            q.sql('destination', db, f"CREATE SUBSCRIPTION fair_sub_{i} CONNECTION '{connection}' PUBLICATION fair_pub_{i} WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
            q.check(db + ' is streaming before large copy', q.wait_for('destination', db,
                f"SELECT state FROM lake_replication.subscriptions WHERE subscription_name='fair_sub_{i}'", 'streaming'), 'streaming')
        if args.seed_after_subscriptions:
            seed_big()
        destinations = [c.Psql('destination', db) for db in names]
        copies = [c.Psql('destination', db) for db in copy_names]
        monitor = c.Psql('destination', 'analytics')
        clients.extend(destinations + copies + [monitor])
        result['before'] = s.inventory()
        started = time.monotonic()
        def produce():
            try:
                for tx in range(1, args.stream_transactions + 1):
                    delay = started + (tx - 1) * .5 - time.monotonic()
                    if delay > 0 and stop.wait(delay):
                        return
                    if stop.is_set():
                        return
                    for i in range(3):
                        began = time.monotonic() - started
                        source.query(f"INSERT INTO fair_items_{i} SELECT {tx}::bigint*20+n,{tx},repeat('x',128) FROM generate_series(1,20) n")
                        xid, epoch = source.query(f'SELECT xmin::text,extract(epoch FROM pg_xact_commit_timestamp(xmin)) FROM fair_items_{i} WHERE tx={tx} LIMIT 1')[0].split('|')
                        commits[i].append(dict(transaction=tx, xid=int(xid), commit_epoch=float(epoch), insert_started_seconds=began))
            except Exception as error:
                errors.append(str(error))
        producer = threading.Thread(target=produce)
        producer.start()
        stop.wait(5)
        for i, database in enumerate(copy_names):
            sub = 'fair_copy_sub' if i == 0 else f'fair_copy_sub_{i}'
            q.sql('destination', database, f"CREATE SUBSCRIPTION {sub} CONNECTION '{connection}' PUBLICATION fair_copy_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        deadline = time.monotonic() + 180 + args.stream_transactions * .5
        while time.monotonic() < deadline:
            if errors:
                raise RuntimeError(errors[0])
            sample = dict(elapsed_seconds=time.monotonic() - started, streams=[])
            for dest in destinations:
                values = dest.query("SELECT count(*),extract(epoch FROM clock_timestamp()) FROM lake_replication.transactions WHERE state='published'")[0].split('|')
                sample['streams'].append(dict(published_transactions=int(values[0]), observed_epoch=float(values[1])))
                diagnostic=dest.query("SELECT coalesce(last_error,'') FROM lake_replication.service_status")
                if diagnostic and 'has no changes' in diagnostic[0] and 'streamed transaction' in diagnostic[0]:
                    result['receiver_error']=diagnostic[0]
                    raise RuntimeError('empty filtered streamed transaction prevents receiver progress')
            sample['copies'] = []
            for copy in copies:
                values = copy.query("SELECT coalesce((SELECT state FROM lake_replication.subscriptions WHERE subscription_name LIKE 'fair_copy_sub%'),'absent'),coalesce(max(rows_copied),0),coalesce(min(extract(epoch FROM started_at)),0),coalesce(max(extract(epoch FROM pg_xact_commit_timestamp(xmin))),0),extract(epoch FROM clock_timestamp()) FROM lake_replication.initial_copies")[0].split('|')
                sample['copies'].append(dict(state=values[0], rows_copied=int(values[1]), started_epoch=float(values[2]), last_row_commit_epoch=float(values[3]), observed_epoch=float(values[4])))
            sample['copy'] = dict(state='streaming' if all(x['state']=='streaming' for x in sample['copies']) else 'copying',
                rows_copied=sum(x['rows_copied'] for x in sample['copies']),
                started_epoch=min(x['started_epoch'] for x in sample['copies']),
                last_row_commit_epoch=max(x['last_row_commit_epoch'] for x in sample['copies']),
                observed_epoch=max(x['observed_epoch'] for x in sample['copies']))
            if bulk_copies > 1:
                sample['active_copy_publications'] = int(monitor.query("SELECT count(*) FROM pg_stat_activity WHERE state='active' AND query LIKE 'SELECT lake_replication.finish_initial_sync_group(%'")[0])
            if variant in ('bounded', 'reserved', 'bulk_only'):
                sample['admission'] = json.loads(monitor.query(f"SELECT coalesce(jsonb_agg(t),'[]'::jsonb) FROM (SELECT * FROM {admission_view})t")[0])
                held = [row for row in sample['admission'] if row['admission_state'] == 'admitted']
                if len(held) > (2 if variant == 'reserved' else 1):
                    raise AssertionError('engine admission capacity exceeded')
                if variant == 'reserved' and len({row['lane'] for row in held}) != len(held):
                    raise AssertionError('multiple admitted writers in one lane')
            trace.append(sample)
            if not producer.is_alive() and all(row['published_transactions'] == args.stream_transactions for row in sample['streams']) and sample['copy']['state'] == 'streaming':
                break
            stop.wait(.5)
        else:
            raise TimeoutError('copy/streaming did not finish within the bounded observation window')
        producer.join(timeout=10)
        copy_start = trace[-1]['copy']['started_epoch']
        copy_end = trace[-1]['copy']['last_row_commit_epoch']
        if not (0 < copy_start < copy_end):
            raise AssertionError('no valid measured initial-copy interval')
        result.update(copy_start_epoch=copy_start, copy_commit_epoch=copy_end,
            copy_elapsed_seconds=copy_end-copy_start, streams=[], source_transactions=commits)
        for i, dest in enumerate(destinations):
            published = dest.query("SELECT xid::text,extract(epoch FROM pg_xact_commit_timestamp(xmin)) FROM lake_replication.transactions WHERE state='published' ORDER BY xid")
            mapping = {int(row.split('|')[0]): float(row.split('|')[1]) for row in published}
            if len(mapping) != args.stream_transactions or len(commits[i]) != args.stream_transactions:
                raise AssertionError('missing/duplicate measured streaming transactions')
            points = [dict(**tx, publication_commit_epoch=mapping[tx['xid']], lag_ms=1000*(mapping[tx['xid']]-tx['commit_epoch'])) for tx in commits[i]]
            if any(tx['lag_ms'] < 0 for tx in points):
                raise AssertionError('negative commit lag')
            during = [tx for tx in points if copy_start <= tx['publication_commit_epoch'] <= copy_end]
            ordered = sorted(tx['publication_commit_epoch'] for tx in points)
            # Boundaries include first source commit and the last publication;
            # the offered source is continuously active in between, then drains.
            times = [points[0]['commit_epoch']] + ordered
            gaps = [right-left for left,right in zip(times,times[1:])]
            p95 = c.percentile([tx['lag_ms'] for tx in points], .95)
            catchup = max(0, ordered[-1]-points[-1]['commit_epoch'])
            lateness = max(tx['insert_started_seconds']-(tx['transaction']-1)*.5 for tx in points)
            summary = dict(database=names[i], transactions=points,
                p95_publication_ms=p95, max_publication_gap_seconds=max(gaps),
                commits_during_copy=len(during), catchup_seconds=catchup,
                source_max_schedule_lateness_seconds=lateness)
            summary['checks'] = dict(p95_below_two_seconds=p95<2000,
                max_gap_within_five_seconds=max(gaps)<=5,
                at_least_five_commits_during_copy=len(during)>=5,
                catchup_within_five_seconds=catchup<=5,
                source_schedule_within_one_interval=lateness<=.5)
            result['streams'].append(summary)
            expected = args.stream_transactions*20
            actual = dest.query(f"SELECT count(*),count(DISTINCT id),bool_and((id>tx::bigint*20 AND id<=(tx::bigint+1)*20 AND tx BETWEEN 1 AND {args.stream_transactions} AND payload=repeat('x',128)) IS TRUE) FROM fair_items_{i}")[0]
            q.check(names[i] + ' exact final Iceberg rows', actual, f'{expected}|{expected}|t')
        n = args.copy_rows
        for i, copy in enumerate(copies):
            q.check(f'large copy {i} exact final Iceberg rows', copy.query("SELECT count(*),count(DISTINCT id),min(id),max(id),sum(id),bool_and(payload=repeat('b',128)) FROM fair_big")[0], f'{n}|{n}|1|{n}|{n*(n+1)//2}|t')
        result['after'] = s.inventory()
        result['checks'] = dict(all_stream_targets_pass=all(all(row['checks'].values()) for row in result['streams']),
            copy_within_three_minutes=copy_end-copy_start<=180,
            copy_progress_observed=sum(sample['copy']['rows_copied']>0 and sample['copy']['state']!='streaming' for sample in trace)>=2,
            waiting_observed=any(any(row['admission_state']=='waiting_for_writer' for row in sample.get('admission',[])) for sample in trace) if variant in ('bounded','reserved','bulk_only') and getattr(args, 'require_queue', True) else True)
        result['status'] = 'passed' if all(result['checks'].values()) else 'finding'
        if bulk_copies > 1:
            result['max_active_copy_publications'] = max(x['active_copy_publications'] for x in trace)
            result['checks']['publication_overlap_observed'] = result['max_active_copy_publications'] >= bulk_copies
            if not result['checks']['publication_overlap_observed']:
                result['status'] = 'finding'
    except Exception as error:
        result.update(status='finding' if result.get('receiver_error') else 'error', error=str(error))
        print('FAIRNESS ERROR: ' + str(error), flush=True)
    finally:
        stop.set()
        if producer:
            producer.join(timeout=125)
        result['trace'] = trace
        result['source_transactions'] = commits
        for client in clients:
            try:
                client.close()
            except Exception as error:
                result.setdefault('cleanup_errors', []).append(str(error))
        if result.get('cleanup_errors'):
            result['status'] = 'error'
        try:
            logs = q.compose('logs', '--no-color', check_result=False)
            log = logs.stdout + logs.stderr
            (directory / 'compose.log').write_text(log)
            result['log_sha256'] = hashlib.sha256(log.encode()).hexdigest()
            result['engine_oom'] = 'Out of Memory Error' in log
            result['unexpected_pg_crash'] = 'reinitializing' in log
            if result['engine_oom'] or result['unexpected_pg_crash']:
                result['status'] = 'finding'
        finally:
            q.compose('down', '-v', '--remove-orphans', timeout=300)
            s.write_json(directory / 'results.json', result)
    return result


def comparisons(cases):
    for case in cases:
        for phase in ('warmup','workload'):
            if not timing_validation(case[phase])['valid']:
                raise ValueError(f"clock discontinuity invalidates {case['profile']} pair {case['repetition']} {case['variant']} {phase}")
    results = {}
    for profile in ('steady', 'catchup'):
        pairs = {}
        for case in cases:
            if case['profile'] == profile:
                pairs.setdefault(case['repetition'], {})[case['variant']] = case
        complete = [pair for pair in pairs.values() if set(pair)=={'unbounded','bounded'}]
        if not complete:
            continue
        lag = [p['bounded']['workload']['publication_commit_lag_ms']['p95']/p['unbounded']['workload']['publication_commit_lag_ms']['p95'] for p in complete]
        throughput = [p['bounded']['workload']['publication_rows_per_second']/p['unbounded']['workload']['publication_rows_per_second'] for p in complete]
        results[profile] = dict(p95=a.assess(lag,1.10,False),
            throughput=a.assess(throughput,.95,True) if profile=='catchup' else dict(assessment='offered_load_limited', **a.paired_bootstrap(throughput)))
        if profile=='steady':
            checks=[]
            for pair in complete:
                for case in pair.values():
                    work=case['workload']; tx=work['transactions']; poll=work['last_poll_while_source_active']
                    backlog=tx[-1]['transaction']-max(tx[0]['transaction']-1,poll['maximum_transaction']) if poll else len(tx)
                    lateness=max(row['insert_started_seconds']-index*.5 for index,row in enumerate(tx))
                    checks.append(dict(variant=case['variant'],repetition=case['repetition'],
                        p95_below_two_seconds=work['publication_commit_lag_ms']['p95']<2000,
                        catchup_within_two_seconds=work['publication_catchup_seconds']<=2,
                        backlog_at_most_four=backlog<=4,source_schedule_within_one_interval=lateness<=.5))
            results[profile]['absolute_checks']=checks
            results[profile]['absolute_checks_all_pass']=all(all(row[k] for k in ('p95_below_two_seconds','catchup_within_two_seconds','backlog_at_most_four','source_schedule_within_one_interval')) for row in checks)
    return results

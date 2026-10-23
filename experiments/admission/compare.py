#!/usr/bin/env python3
"""Shared workload, visibility measurement and resource sampling helpers for admission experiments."""
import argparse
import importlib.util
import json
import queue
import statistics
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', ROOT / 'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)


class Psql:
    """One persistent psql session; keep Docker client startup outside timings."""
    def __init__(self, service, database):
        self.process = subprocess.Popen(
            ['docker', 'compose', '-p', q.PROJECT, '-f', str(q.COMPOSE_FILE),
             'exec', '-T', service, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
             '-U', 'postgres', '-d', database],
            env=q.ENV, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1)
        self.output = queue.Queue()
        self.errors = []
        def read():
            for line in self.process.stdout:
                self.output.put(line.rstrip('\n'))
            self.output.put(None)
        def read_errors():
            for line in self.process.stderr:
                self.errors.append(line)
        self.reader = threading.Thread(target=read, daemon=True)
        self.error_reader = threading.Thread(target=read_errors, daemon=True)
        self.reader.start()
        self.error_reader.start()
        self.sequence = 0
        self.query('SELECT 1')

    def query(self, statement, timeout=120):
        self.sequence += 1
        marker = f'query_finished_{self.sequence}'
        self.process.stdin.write(statement.rstrip(';') + ';\n\\echo ' + marker + '\n')
        self.process.stdin.flush()
        rows = []
        deadline = time.monotonic() + timeout
        while True:
            try:
                row = self.output.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty as error:
                raise TimeoutError('persistent psql query timed out') from error
            if row is None:
                raise RuntimeError('psql exited: ' + ''.join(self.errors)[-2000:])
            if row == marker:
                return rows
            rows.append(row)

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                self.process.wait(timeout=10)
        self.reader.join(timeout=2)
        self.error_reader.join(timeout=2)
        self.process.stdout.close()
        self.process.stderr.close()


def resources(service, launcher_pids=''):
    return json.loads(q.compose('exec', '-T', '--user', 'postgres', service, 'python3',
                               '-c', Path(__file__).with_name('sample.py').read_text(), launcher_pids).stdout)


def snapshot(client):
    wal, started = client.query("SELECT pg_current_wal_insert_lsn(), pg_postmaster_start_time()")[0].split('|')
    launcher_pids = client.query("SELECT coalesce(string_agg(w.pid::text,','),'') FROM extension_base.list_base_workers() w JOIN pg_extension e ON e.oid=w.extension_id WHERE e.extname='lake_replication'")[0]
    high, low = wal.split('/')
    return {'wal_bytes': (int(high, 16) << 32) + int(low, 16),
            'postmaster_start': started,
            'destination': resources('destination', launcher_pids), 'pgduck': resources('pgduck')}


def percentile(values, fraction):
    ordered = sorted(values)
    # Nearest rank; retain raw samples so other estimators can be applied later.
    import math
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def workload(args, source, destination, first_transaction):
    observed = {}
    visibility = {}
    commits = {}
    source_xids = {}
    polls = []
    timeline = []
    insert_started = {}
    stop = threading.Event()
    errors = []
    last_tx = first_transaction + args.transactions - 1
    query = ('SELECT coalesce(max(tx),0), extract(epoch FROM statement_timestamp()), '
             'extract(epoch FROM clock_timestamp()) '
             'FROM public.benchmark_items')
    if args.observer == 'journal':
        query = ("SELECT count(*), extract(epoch FROM statement_timestamp()), "
                 "extract(epoch FROM clock_timestamp()) FROM lake_replication.transactions "
                 "WHERE state='published' AND subscription_id=(SELECT subscription_id "
                 "FROM lake_replication.subscriptions WHERE subscription_name='benchmark_sub')")
    source_phase_epoch = float(source.query('SELECT extract(epoch FROM clock_timestamp())')[0])
    started = time.monotonic()
    def observe():
        last_unseen_started = started
        last_unseen_epoch = None
        highest_seen = first_transaction - 1
        try:
            while not stop.is_set():
                began = time.monotonic()
                maximum, snapshot_start, stamp = destination.query(query)[0].split('|')
                ended = time.monotonic()
                polls.append(ended - began)
                timeline.append({'query_started_seconds': began - started,
                                 'query_finished_seconds': ended - started,
                                 'statement_epoch': float(snapshot_start),
                                 'observed_epoch': float(stamp), 'maximum_transaction': int(maximum)})
                for tx in range(max(first_transaction, highest_seen + 1), min(int(maximum), last_tx) + 1):
                    observed[tx] = float(stamp)
                    visibility[tx] = {'not_seen_before_epoch': last_unseen_epoch,
                                      'not_seen_before_seconds': last_unseen_started - started,
                                      'seen_by_seconds': ended - started}
                highest_seen = max(highest_seen, int(maximum))
                if last_tx in observed:
                    return
                last_unseen_started = began
                last_unseen_epoch = float(snapshot_start)
                stop.wait(args.poll_ms / 1000)
        except Exception as error:
            errors.append(str(error))
            stop.set()
    reader = threading.Thread(target=observe)
    reader.start()
    offered_deadline = started
    try:
        for tx in range(first_transaction, last_tx + 1):
            if errors:
                raise RuntimeError(errors[0])
            delay = offered_deadline - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            # psql autocommits INSERT before reading its actual commit timestamp.
            insert_started[tx] = time.monotonic() - started
            source.query(f"INSERT INTO public.benchmark_items SELECT "
                         f"{tx}::bigint*{args.rows_per_transaction}+n,{tx},repeat('x',{args.payload_bytes}) "
                         f"FROM generate_series(1,{args.rows_per_transaction}) n")
            xid, stamp = source.query('SELECT xmin::text, extract(epoch FROM pg_xact_commit_timestamp(xmin)) '
                                      f'FROM public.benchmark_items WHERE tx={tx} LIMIT 1')[0].split('|')
            commits[tx] = float(stamp)
            source_xids[tx] = int(xid)
            offered_deadline += args.interval_ms / 1000
        source_done = time.monotonic()
        reader.join(timeout=args.catchup_timeout)
        if reader.is_alive():
            raise TimeoutError('replication did not catch up within the limit')
        if errors:
            raise RuntimeError(errors[0])
    finally:
        stop.set()
        reader.join(timeout=125)
    finished = time.monotonic()
    published = json.loads(destination.query(
        "SELECT coalesce(json_agg(json_build_object('xid',xid,'commit_epoch',"
        "extract(epoch FROM pg_xact_commit_timestamp(xmin)))),'[]'::json) "
        "FROM lake_replication.transactions WHERE state='published' AND xid IN ("
        + ','.join(str(xid) for xid in source_xids.values()) + ')')[0])
    publication_commits = {int(row['xid']): row['commit_epoch'] for row in published}
    if len(published) != args.transactions or any(publication_commits.get(xid) is None for xid in source_xids.values()):
        raise AssertionError('missing/duplicate publication commit timestamps for measured source transactions')
    publication_lags = [1000 * (publication_commits[source_xids[tx]] - commits[tx]) for tx in commits]
    if min(publication_lags) < 0:
        raise AssertionError('negative source-to-publication commit latency')
    lags = [1000 * (observed[tx] - commits[tx]) for tx in commits]
    lower_lags = [max(0, 1000 * ((visibility[tx]['not_seen_before_epoch'] or commits[tx]) - commits[tx]))
                  for tx in commits]
    if min(lags) < 0:
        raise AssertionError('negative measured commit lag; inspect clock/commit timestamp assumptions')
    for poll in timeline:
        visible = max(0, min(last_tx, poll['maximum_transaction']) - first_transaction + 1)
        committed_before = sum(stamp <= poll['statement_epoch'] for stamp in commits.values())
        committed_after = sum(stamp <= poll['observed_epoch'] for stamp in commits.values())
        poll['backlog_transactions_lower'] = max(0, committed_before - visible)
        poll['backlog_transactions_upper'] = max(0, committed_after - visible)
    final_visibility_lower = max(insert_started[last_tx], visibility[last_tx]['not_seen_before_seconds'])
    final_visibility_upper = visibility[last_tx]['seen_by_seconds']
    last_live_poll = next((poll for poll in reversed(timeline)
                           if poll['query_finished_seconds'] <= source_done - started), None)
    quarter = max(1, len(lags) // 4)
    return {
        'rows': args.transactions * args.rows_per_transaction,
        'source_elapsed_seconds': source_done - started,
        'visible_elapsed_seconds': finished - started,
        'rows_per_second_until_observed': args.transactions * args.rows_per_transaction / (finished - started),
        'source_phase_start_epoch': source_phase_epoch,
        'publication_rows_per_second': args.transactions * args.rows_per_transaction /
            (max(publication_commits.values()) - source_phase_epoch),
        'publication_catchup_seconds': max(0, max(publication_commits.values()) - max(commits.values())),
        'catchup_seconds': max(0, finished - source_done),
        'observed_commit_lag_ms': {'p50': percentile(lags, .5), 'p95': percentile(lags, .95), 'p99': percentile(lags, .99)},
        'publication_commit_lag_ms': {'p50': percentile(publication_lags, .5), 'p95': percentile(publication_lags, .95), 'p99': percentile(publication_lags, .99)},
        'first_quarter_p95_publication_commit_lag_ms': percentile(publication_lags[:max(1, len(publication_lags) // 4)], .95),
        'last_quarter_p95_publication_commit_lag_ms': percentile(publication_lags[-max(1, len(publication_lags) // 4):], .95),
        'p95_observation_lag_bounds_ms': {'lower': percentile(lower_lags, .95), 'upper': percentile(lags, .95)},
        'first_quarter_p95_observed_commit_lag_ms': percentile(lags[:quarter], .95),
        'last_quarter_p95_observed_commit_lag_ms': percentile(lags[-quarter:], .95),
        'observation_completion_seconds_bounds': {'lower': final_visibility_lower, 'upper': final_visibility_upper},
        'rows_per_second_until_observed_bounds': {
            'lower': args.transactions * args.rows_per_transaction / final_visibility_upper,
            'upper': args.transactions * args.rows_per_transaction / final_visibility_lower},
        'source_write_rows_per_second': args.transactions * args.rows_per_transaction / (source_done - started),
        'last_poll_while_source_active': last_live_poll,
        'observer_query_count': len(polls),
        'observer_query_total_seconds': sum(polls),
        'observation_timeline': timeline,
        'transactions': [{'transaction': tx, 'committed_epoch': commits[tx],
                          'insert_started_seconds': insert_started[tx],
                          'source_xid': source_xids[tx],
                          'publication_commit_epoch': publication_commits[source_xids[tx]],
                          'first_observed_epoch': observed[tx], **visibility[tx]} for tx in commits],
        'observer_query_ms': {'p50': percentile(polls, .5) * 1000, 'p95': percentile(polls, .95) * 1000},
        'observer_poll_ms': args.poll_ms,
        'observer_kind': args.observer,
    }


def run_case(args, variant, repetition, image, compose_text, directory):
    q.COMPOSE_FILE = directory / 'compose.yml'
    if variant == 'base':
        if not args.base_preload:
            compose_text = compose_text.replace(
                'shared_preload_libraries=pg_extension_base,lake_replication',
                'shared_preload_libraries=pg_extension_base')
        compose_text = compose_text.replace('      - -c\n      - lake_replication.database_name=analytics\n', '')
    q.COMPOSE_FILE.write_text(compose_text)
    q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = image
    source = destination = None
    try:
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        source = Psql('source', 'application')
        destination = Psql('destination', 'analytics')
        source.query('CREATE TABLE public.benchmark_items(id bigint PRIMARY KEY,tx integer NOT NULL,payload text NOT NULL); '
                     'CREATE PUBLICATION benchmark_pub FOR TABLE public.benchmark_items; '
                     'GRANT SELECT ON public.benchmark_items TO lake_source')
        connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
        destination.query(f"CREATE SUBSCRIPTION benchmark_sub CONNECTION '{connection}' PUBLICATION benchmark_pub "
                          "WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        if q.wait_for('destination', 'analytics',
                      "SELECT state FROM lake_replication.subscriptions WHERE subscription_name='benchmark_sub'",
                      'streaming', timeout=90) != 'streaming':
            raise AssertionError('initial copy did not complete')
        # Warm both the worker path and observer before measuring.
        warmup_args = argparse.Namespace(**vars(args))
        warmup_args.transactions = args.warmup_transactions or args.transactions
        warmup = workload(warmup_args, source, destination, 1)
        idle_before = snapshot(destination)
        time.sleep(args.idle_seconds)
        idle_after = snapshot(destination)
        idle_elapsed = (idle_after['destination']['monotonic_seconds'] -
                        idle_before['destination']['monotonic_seconds'])
        loaded_before = snapshot(destination)
        measured = workload(args, source, destination, warmup_args.transactions + 1)
        loaded_after = snapshot(destination)
        expected = (warmup_args.transactions + args.transactions) * args.rows_per_transaction
        total_transactions = warmup_args.transactions + args.transactions
        counts = destination.query(
            'SELECT count(*),count(DISTINCT id),bool_and(('
            f'tx BETWEEN 1 AND {total_transactions} AND id > tx::bigint*{args.rows_per_transaction} '
            f'AND id <= (tx::bigint+1)*{args.rows_per_transaction} '
            f"AND payload=repeat('x',{args.payload_bytes})) IS TRUE) FROM public.benchmark_items")[0]
        if counts != f'{expected}|{expected}|t':
            raise AssertionError(f'row correctness: expected {expected}, got {counts}')
        if loaded_before['postmaster_start'] != loaded_after['postmaster_start']:
            raise AssertionError('postmaster restarted during measurement')
        return {'variant': variant, 'poll_ms': args.poll_ms, 'repetition': repetition, 'image': image,
                'warmup': warmup, 'idle_before': idle_before, 'idle_after': idle_after,
                'idle_elapsed_seconds': idle_elapsed, 'loaded_before': loaded_before,
                'loaded_after': loaded_after, 'workload': measured, 'verified_rows': expected,
                'verified_ids_markers_and_payload': True}
    finally:
        # Evidence failures must never skip process/container cleanup.
        cleanup_errors = []
        try:
            if destination:
                (directory / 'worker.jsonl').write_text(q.worker_logs())
            logs = q.compose('logs', '--no-color', check_result=False)
            (directory / 'compose.log').write_text(logs.stdout + logs.stderr)
        except Exception as error:
            cleanup_errors.append(str(error))
        for client in (source, destination):
            try:
                if client:
                    client.close()
            except Exception as error:
                cleanup_errors.append(str(error))
        try:
            q.compose('down', '-v', '--remove-orphans', timeout=300)
        except Exception as error:
            cleanup_errors.append(str(error))
        if cleanup_errors:
            raise RuntimeError('fixture cleanup/evidence: ' + '; '.join(cleanup_errors))


def summarize(cases):
    result = {}
    groups = sorted({(case['variant'], case['workload']['observer_kind'], case['workload']['observer_poll_ms']) for case in cases})
    for variant, observer, poll_ms in groups:
        selected = [case for case in cases if case['variant'] == variant
                    and case['workload']['observer_kind'] == observer
                    and case['workload']['observer_poll_ms'] == poll_ms]
        def distribution(values):
            return {'median': statistics.median(values), 'min': min(values), 'max': max(values)}
        idle_cpu = [(case['idle_after']['destination']['cpu_usage_usec'] -
                     case['idle_before']['destination']['cpu_usage_usec']) /
                    case['idle_elapsed_seconds'] / 10000 for case in selected]
        wrapper_switches = []
        for case in selected:
            before = {p['pid']: p for p in case['idle_before']['destination']['processes'] if p['kind'] == 'wrapper'}
            for process in case['idle_after']['destination']['processes']:
                prior = before.get(process['pid'])
                if prior and prior['start_ticks'] == process['start_ticks']:
                    wrapper_switches.append((process['voluntary_switches'] - prior['voluntary_switches']) /
                                            case['idle_elapsed_seconds'])
        result[f'{variant}/{observer}/poll-{poll_ms:g}ms'] = {
            'repetitions': len(selected),
            'rows_per_second_until_observed': distribution([c['workload']['rows_per_second_until_observed'] for c in selected]),
            'publication_rows_per_second': distribution([c['workload']['publication_rows_per_second'] for c in selected]),
            'p95_observed_commit_lag_ms': distribution([c['workload']['observed_commit_lag_ms']['p95'] for c in selected]),
            'p95_observed_commit_lag_lower_bound_ms': distribution([c['workload']['p95_observation_lag_bounds_ms']['lower'] for c in selected]),
            'p95_publication_commit_lag_ms': distribution([c['workload']['publication_commit_lag_ms']['p95'] for c in selected]),
            'throughput_lower_bound': distribution([c['workload']['rows_per_second_until_observed_bounds']['lower'] for c in selected]),
            'throughput_upper_bound': distribution([c['workload']['rows_per_second_until_observed_bounds']['upper'] for c in selected]),
            'observer_query_total_seconds': distribution([c['workload']['observer_query_total_seconds'] for c in selected]),
            'destination_idle_cpu_percent_of_one_core': distribution(idle_cpu),
            'wrapper_idle_voluntary_switches_per_second': distribution(wrapper_switches) if wrapper_switches else None,
        }
    return result

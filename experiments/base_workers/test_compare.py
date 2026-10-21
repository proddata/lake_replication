"""Verify measurement bounds against a known delayed visibility model."""
import argparse
import importlib.util
import json
import re
import threading
import time
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('launcher_compare', Path(__file__).with_name('compare.py'))
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


class VisibilityModel:
    def __init__(self, delay):
        self.delay = delay
        self.commits = {}
        self.lock = threading.Lock()

    def write(self, statement):
        if statement == 'SELECT extract(epoch FROM clock_timestamp())':
            return [str(time.time())]
        if statement.startswith('INSERT'):
            tx = int(re.search(r'SELECT (\d+)::bigint', statement).group(1))
            with self.lock:
                self.commits[tx] = time.time()
            return []
        tx = int(re.search(r'WHERE tx=(\d+)', statement).group(1))
        return [f'{tx + 10000}|{self.commits[tx]}']

    def read(self, statement):
        if statement.startswith('SELECT coalesce(json_agg'):
            return [json.dumps([{'xid': tx + 10000, 'commit_epoch': stamp + self.delay}
                                for tx, stamp in self.commits.items()])]
        began = time.time()
        with self.lock:
            visible = [tx for tx, stamp in self.commits.items() if stamp + self.delay <= began]
        return [f'{max(visible, default=0)}|{began}|{time.time()}']


class Client:
    def __init__(self, query):
        self.query = query


class MeasurementTests(unittest.TestCase):
    def test_poll_bounds_contain_known_visibility_delay(self):
        model = VisibilityModel(.04)
        args = argparse.Namespace(transactions=5, rows_per_transaction=10,
                                  payload_bytes=16, interval_ms=2, poll_ms=10,
                                  catchup_timeout=5, observer='iceberg')
        result = compare.workload(args, Client(model.write), Client(model.read), 1)
        # Visibility occurs exactly 40ms after commit in the model, independently
        # of when the observer discovers it. Every per-transaction bound must contain it.
        for tx in result['transactions']:
            committed = tx['committed_epoch']
            last_negative = tx['not_seen_before_epoch']
            lower = max(0, ((last_negative or committed) - committed) * 1000)
            upper = (tx['first_observed_epoch'] - committed) * 1000
            self.assertLessEqual(lower, 40)
            self.assertGreaterEqual(upper, 40)
        self.assertLessEqual(result['p95_observation_lag_bounds_ms']['lower'], 40)
        self.assertGreaterEqual(result['p95_observation_lag_bounds_ms']['upper'], 40)
        self.assertEqual(result['observation_timeline'][-1]['backlog_transactions_upper'], 0)
        self.assertEqual(len(result['transactions']), 5)
        self.assertAlmostEqual(result['publication_commit_lag_ms']['p95'], 40, places=3)
        self.assertLessEqual(result['rows_per_second_until_observed_bounds']['lower'],
                             result['rows_per_second_until_observed_bounds']['upper'])

    def test_source_failure_stops_observer(self):
        model = VisibilityModel(.04)
        args = argparse.Namespace(transactions=2, rows_per_transaction=10,
                                  payload_bytes=16, interval_ms=2, poll_ms=10,
                                  catchup_timeout=5, observer='iceberg')
        def fail(statement):
            raise RuntimeError('injected source failure')
        before = set(threading.enumerate())
        with self.assertRaisesRegex(RuntimeError, 'injected source failure'):
            compare.workload(args, Client(fail), Client(model.read), 1)
        self.assertEqual(set(threading.enumerate()), before)


if __name__ == '__main__':
    unittest.main()

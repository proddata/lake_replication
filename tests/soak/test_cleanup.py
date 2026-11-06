"""Finalization must preserve failures and still attempt disposable teardown."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('soak_probe', Path(__file__).with_name('probe.py'))
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class CleanupTests(unittest.TestCase):
    def test_late_recovery_cannot_pass_the_catchup_deadline(self):
        runner = p.Runner.__new__(p.Runner)
        runner.producer = SimpleNamespace(error=None)
        runner.sql = Mock(return_value='ready')
        with patch.object(p.time, 'monotonic', side_effect=[0, 0, 2]):
            with self.assertRaises(TimeoutError):
                runner.wait('destination', 'SELECT 1', 'ready', timeout=1)

    def test_nonfinite_sampling_is_rejected_before_starting_a_fixture(self):
        result = subprocess.run([sys.executable, str(Path(p.__file__)), '--sample-seconds', 'nan'], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertIn('invalid duration', result.stderr)

    def test_docker_failures_preserve_primary_error_and_never_qualify_a_day(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = {}
            def hung(*args, **kwargs):
                self.assertLessEqual(kwargs['timeout'], 60)
                raise subprocess.TimeoutExpired(args, kwargs['timeout'])
            with patch.object(p.q, 'RESULTS', Path(directory)), patch.object(p.q, 'EVIDENCE', evidence), patch.object(p.q, 'compose', side_effect=hung) as compose:
                runner = p.Runner(SimpleNamespace(interval_seconds=.5, duration_seconds=86400))
                runner.begin = time.monotonic() - 86401
                runner.result.update(status='error', error='original data mismatch')
                runner.finish()
            result = json.loads((Path(directory) / 'results.json').read_text())
            self.assertEqual(result['status'], 'error')
            self.assertEqual(result['error'], 'original data mismatch')
            self.assertFalse(result['qualifies_24_hour_soak'])
            self.assertEqual([call.args[0] for call in compose.call_args_list], ['exec', 'logs', 'down'])
            self.assertEqual(len(result['cleanup_errors']), 3)

    def test_event_recorder_timeout_still_attempts_logs_and_teardown(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = {}
            success = subprocess.CompletedProcess('docker', 0, '', '')
            with patch.object(p.q, 'RESULTS', Path(directory)), patch.object(p.q, 'EVIDENCE', evidence), patch.object(p.q, 'compose', return_value=success) as compose:
                runner = p.Runner(SimpleNamespace(interval_seconds=.5, duration_seconds=600))
                runner.result.update(status='error', error='original failure')
                runner.events = Mock()
                runner.events.poll.return_value = None
                runner.events.wait.side_effect = subprocess.TimeoutExpired('events', 5)
                runner.finish()
            result = json.loads((Path(directory) / 'results.json').read_text())
            self.assertEqual(result['error'], 'original failure')
            self.assertFalse(result['qualifies_24_hour_soak'])
            self.assertEqual(result['cleanup_returncode'], 0)
            self.assertEqual([call.args[0] for call in compose.call_args_list], ['exec', 'logs', 'down'])
            self.assertEqual(result['cleanup_errors'][0]['operation'], 'stop Docker event recorder')

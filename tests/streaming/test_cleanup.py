"""A stuck Docker daemon must not erase the primary receiver-test failure."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('empty_filtered',Path(__file__).with_name('empty_filtered.py'))
p = importlib.util.module_from_spec(spec); spec.loader.exec_module(p)


class CleanupTests(unittest.TestCase):
    def test_all_docker_timeouts_preserve_primary_failure_and_attempt_teardown(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence={'status':'error','error':'original receiver kill timed out'}
            def hung(*args, **kwargs):
                self.assertLessEqual(kwargs['timeout'],60)
                raise subprocess.TimeoutExpired(['docker',args[0]],kwargs['timeout'])
            with patch.object(p.q,'RESULTS',Path(directory)), patch.object(p.q,'EVIDENCE',evidence), patch.object(p.q,'compose',side_effect=hung) as compose:
                p.finalize_evidence(True)
                result=json.loads((Path(directory)/'results.json').read_text())
            self.assertEqual(result['error'],'original receiver kill timed out')
            self.assertEqual(result['status'],'error')
            self.assertEqual([c.args[0] for c in compose.call_args_list],['exec','logs','down'])
            self.assertEqual(len(result['cleanup_errors']),3)
            self.assertIsNone(result['cleanup_returncode'])

    def test_successful_capture_and_cleanup_remain_a_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence={'status':'passed'}
            def success(*args,**kwargs):
                partial=json.loads((Path(directory)/'results.json').read_text())
                self.assertEqual(partial['status'],'finalizing')
                self.assertEqual(partial['test_status'],'passed')
                return subprocess.CompletedProcess(args,0,'fixture log\n','')
            with patch.object(p.q,'RESULTS',Path(directory)),patch.object(p.q,'EVIDENCE',evidence),patch.object(p.q,'compose',side_effect=success):
                p.finalize_evidence(True)
                result=json.loads((Path(directory)/'results.json').read_text())
            self.assertEqual(result['status'],'passed')
            self.assertEqual(result['cleanup_returncode'],0)
            self.assertIn('native_worker_logs_sha256',result)

    def test_local_exec_is_killed_after_bounded_terminate(self):
        process=Mock()
        process.poll.return_value=None
        process.communicate.side_effect=[subprocess.TimeoutExpired('docker exec',5),('','')]
        p.stop_exec(process)
        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        self.assertEqual([c.kwargs['timeout'] for c in process.communicate.call_args_list],[5,5])

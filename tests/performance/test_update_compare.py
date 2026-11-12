"""Keep update comparisons reproducible and reject stale SQL patch assumptions."""
import importlib.util
from pathlib import Path
import unittest
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location('update_compare',Path(__file__).with_name('update_compare.py'))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class UpdateComparisonTests(unittest.TestCase):
    def test_random_updates_are_distinct_reproducible_and_in_bounds(self):
        selected = probe.keys(1000000,10000,'random',17)
        self.assertEqual(selected,probe.keys(1000000,10000,'random',17))
        self.assertEqual(len(set(selected)),10000)
        self.assertTrue(all(1<=key<=1000000 for key in selected))
        self.assertNotEqual(selected,probe.keys(1000000,10000,'random',18))

    def test_clustered_keys_and_invalid_workloads(self):
        self.assertEqual(probe.keys(10,3,'clustered',1),[1,2,3])
        for rows,updates in [(10,11),(10,0)]:
            with self.assertRaises(ValueError):probe.keys(rows,updates,'random',1)

    def test_filter_copy_keeps_fallback_and_count_checks(self):
        sql = (probe.ROOT/'migrations/0024_bulk_admission.sql').read_text()
        begin=sql.index('CREATE OR REPLACE FUNCTION lake_replication.apply_change_run(')
        end=sql.index('\n$$;',begin)+4
        candidate=probe.filtered_definition(sql[begin:end])
        self.assertIn('FUNCTION update_probe.filtered_run(',candidate)
        self.assertIn('RETURN lake_replication.apply_change_run(',candidate)
        self.assertIn('AND destination.id = ANY ($3::bigint[])',candidate)
        self.assertIn('v_count <> v_expected OR v_distinct <> v_expected',candidate)
        self.assertIn('INTO v_count, v_distinct USING p_old, p_new, v_probe_keys;',candidate)
        with self.assertRaises(ValueError):probe.filtered_definition('SELECT 1')

    def test_commit_durations_use_unique_trial_markers(self):
        log='duration: 12.3 ms  statement: COMMIT /* update_probe:1 */;\n'
        log+='duration: 999 ms statement: COMMIT;\n'
        log+='duration: 4.5 ms statement: COMMIT /* update_probe:2 */;'
        self.assertEqual(probe.server_commits(log),{1:.0123,2:.0045})

    def test_nested_plan_keeps_file_counts_and_join_nodes(self):
        summary=probe.plan_summary([{'Plan':{'Node Type':'Hash Join','Plans':[
            {'Node Type':'Foreign Scan','Data Files Scanned':1,'Relation Name':'update_target'}]}}])
        self.assertEqual([node['Node Type'] for node in summary],['Hash Join','Foreign Scan'])
        self.assertEqual(summary[1]['Data Files Scanned'],1)

    def test_evidence_and_client_failures_cannot_skip_fixture_teardown(self):
        with tempfile.TemporaryDirectory() as directory:
            runner=probe.Runner.__new__(probe.Runner)
            runner.output=Path(directory)
            runner.report=dict(status='error',error='original measurement failure',trials=[])
            runner.client=Mock()
            runner.client.close.side_effect=RuntimeError('close failure')
            runner.args=SimpleNamespace(patterns=[],variants=[],run_sizes=[])
            calls=[]
            def compose(*args,**kwargs):
                calls.append(args)
                if args[0]=='logs':raise TimeoutError('log capture timed out')
                return SimpleNamespace(returncode=0)
            runner.q=SimpleNamespace(compose=compose)
            runner.finalize()
            self.assertEqual(calls[-1],('down','-v','--remove-orphans'))
            self.assertEqual(runner.report['error'],'original measurement failure')
            self.assertEqual(runner.report['cleanup_returncode'],0)
            self.assertEqual(len(runner.report['cleanup_errors']),2)


if __name__=='__main__':unittest.main()

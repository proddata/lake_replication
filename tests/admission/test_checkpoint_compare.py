"""A comparison must retain slow controls and reject invalid observations."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('checkpoint_compare',Path(__file__).with_name('checkpoint_compare.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def observation():
    stream=dict(database='stream',p95_publication_ms=1000,latency_phases_p95_ms={},
        source_max_schedule_lateness_seconds=0,checks=dict(p95_below_two_seconds=True,
        source_schedule_within_one_interval=True))
    case=dict(case='fairness',status='passed',copy_elapsed_seconds=150,cleanup_returncode=0,
        streams=[dict(copy.deepcopy(stream),database=str(i)) for i in range(3)],
        checks=dict(publication_overlap_observed=True,copy_within_three_minutes=True),
        timing_validation=dict(valid=True),engine_oom=False,unexpected_pg_crash=False)
    return dict(status='passed',checks=[dict(name='exact data',passed=True)],cases=[case])


class ComparisonTests(unittest.TestCase):
    def test_failed_control_latency_remains_a_finding_in_complete_comparison(self):
        original=observation();original['status']='completed_with_findings'
        original['cases'][0]['status']='finding'
        original['cases'][0]['streams'][0]['checks']['p95_below_two_seconds']=False
        control=m.summarize(original)
        self.assertTrue(control['measurement_valid'])
        self.assertFalse(control['workload_passed'])
        self.assertIn('0:p95_below_two_seconds',control['failed_gates'])
        runs=[dict(variant='original',**control),dict(variant='split',**m.summarize(observation())),
            dict(variant='original',**control)]
        result=m.compare(runs)
        self.assertTrue(result['candidate_workload_passed'])
        self.assertFalse(result['all_workloads_passed'])

    def test_source_schedule_miss_invalidates_even_low_latency(self):
        observed=observation();observed['cases'][0]['streams'][0]['checks']['source_schedule_within_one_interval']=False
        summary=m.summarize(observed)
        self.assertFalse(summary['measurement_valid'])
        self.assertFalse(summary['workload_passed'])

    def test_clock_invalidity_prevents_passing_comparison(self):
        observed=observation();observed['cases'][0]['timing_validation']['valid']=False
        self.assertFalse(m.summarize(observed)['workload_passed'])

    def test_root_finding_cannot_be_promoted_by_passed_case(self):
        observed=observation();observed['status']='completed_with_findings'
        self.assertFalse(m.summarize(observed)['workload_passed'])

    def test_fixture_and_exact_data_failures_are_errors(self):
        for field in ('status','checks'):
            observed=observation()
            if field=='status':observed[field]='error'
            else:observed[field][0]['passed']=False
            with self.subTest(field=field),self.assertRaises(ValueError):m.summarize(observed)

    def test_control_drift_and_split_ratio_use_both_originals(self):
        runs=[dict(variant=v,copy_elapsed_seconds=t,measurement_valid=True,workload_passed=True)
            for v,t in zip(m.SEQUENCE,[100,150,200])]
        result=m.compare(runs)
        self.assertEqual(result['original_copy_drift_ratio'],2)
        self.assertEqual(result['split_copy_ratio_to_original_mean'],1)
        with self.assertRaises(ValueError):m.compare(runs[::-1][:2])


if __name__=='__main__':unittest.main()

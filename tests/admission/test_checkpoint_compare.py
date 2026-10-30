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
        result=m.summarize(observed)
        self.assertFalse(result['workload_passed'])
        self.assertIn('observer_clock_validation',result['failed_gates'])

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

    def test_comparison_rejects_mixed_source_schedulers(self):
        runs=[dict(variant=v,copy_elapsed_seconds=100,measurement_valid=True,
            workload_passed=True,source_producer='independent_sessions') for v in m.SEQUENCE]
        runs[0]['source_producer']='legacy_serial_session'
        with self.assertRaises(ValueError):m.compare(runs)

    def test_copy_comparison_allows_only_declared_configuration_change(self):
        def run(rows):
            return dict(archive='same archive',image_id=f'wrapped-{rows}',inputs='same sources',
                prototype=dict(variant='split',sql_overlay=True,focused_operations=False,
                    sql_profiling=False,service_cpu_affinity='0-1',initial_copy_override=dict(
                    chunk_rows=rows,chunk_row_text_bytes=8388608,base_destination_image_id='base',worker_sha256='worker')))
        baseline=run(1000);candidate=run(2000)
        self.assertEqual(m.runtime_identity(baseline,'split','0-1',1000),
                         m.runtime_identity(candidate,'split','0-1',2000))
        candidate['prototype']['initial_copy_override']['worker_sha256']='different worker'
        self.assertNotEqual(m.runtime_identity(baseline,'split','0-1',1000),
                            m.runtime_identity(candidate,'split','0-1',2000))

    def test_copy_comparison_rejects_widened_byte_budget(self):
        observed=dict(archive='archive',image_id='image',inputs='sources',prototype=dict(
            variant='split',sql_overlay=True,focused_operations=False,sql_profiling=False,
            service_cpu_affinity='0-1',initial_copy_override=dict(chunk_rows=2000,
            chunk_row_text_bytes=16777216,base_destination_image_id='base',worker_sha256='worker')))
        with self.assertRaises(ValueError):m.runtime_identity(observed,'split','0-1',2000)
        with self.assertRaises(ValueError):m.runtime_identity(observed,'split','0-1')

    def test_chunk_control_treatment_control_order_and_ratio(self):
        sequence=('rows_1000','rows_2000','rows_1000')
        runs=[dict(variant=v,copy_elapsed_seconds=t,measurement_valid=True,workload_passed=True)
            for v,t in zip(sequence,[100,80,100])]
        self.assertEqual(m.compare(runs,sequence)['candidate_copy_ratio_to_control_mean'],.8)
        with self.assertRaises(ValueError):m.compare(runs)

    def test_comparison_rejects_mixed_clock_sampling_methods(self):
        runs=[dict(variant=v,copy_elapsed_seconds=100,measurement_valid=True,
            workload_passed=True,observer_clock_sampling='query_return_monotonic') for v in m.SEQUENCE]
        runs[0]['observer_clock_sampling']='legacy_batch_start'
        with self.assertRaises(ValueError):m.compare(runs)

    def test_storage_comparison_requires_declared_overlay_and_unchanged_runtime(self):
        base=dict(archive='same archive',image_id='same image',inputs='same sources',prototype=dict(
            variant='split',sql_overlay=True,focused_operations=False,sql_profiling=False,
            staging_profiling=False,chunk_storage=False,service_cpu_affinity='0-1'))
        candidate=copy.deepcopy(base);candidate['prototype']['chunk_storage']=True
        self.assertEqual(m.runtime_identity(base,'split','0-1'),
                         m.runtime_identity(candidate,'split','0-1',chunk_storage=True))
        with self.assertRaises(ValueError):m.runtime_identity(candidate,'split','0-1')
        with self.assertRaises(ValueError):m.runtime_identity(base,'split','0-1',chunk_storage=True)
        candidate['prototype']['staging_profiling']=True
        with self.assertRaises(ValueError):m.runtime_identity(candidate,'split','0-1',chunk_storage=True)

    def test_storage_comparison_retains_failed_candidate(self):
        sequence=('per_row','chunks','per_row')
        runs=[dict(variant=v,copy_elapsed_seconds=t,measurement_valid=True,workload_passed=i!=1)
            for i,(v,t) in enumerate(zip(sequence,[100,80,100]))]
        result=m.compare(runs,sequence)
        self.assertTrue(result['measurement_valid'])
        self.assertFalse(result['candidate_workload_passed'])
        self.assertEqual(result['candidate_copy_ratio_to_control_mean'],.8)

    def test_pacing_requires_matching_metadata_and_post_commit_configuration(self):
        def run(pause):
            return dict(archive='archive',image_id=f'wrapped-{pause}',inputs='sources',prototype=dict(
                variant='split',sql_overlay=True,focused_operations=False,sql_profiling=False,
                chunk_storage=True,pause_after_chunk_ms=pause,service_cpu_affinity='0-1',
                initial_copy_override=dict(chunk_rows=1000,chunk_row_text_bytes=8388608,
                    pause_after_chunk_ms=pause,pause_placement='after_managed_transaction_commit',
                    base_destination_image_id='base',worker_sha256='worker')))
        control=run(0);candidate=run(40)
        self.assertEqual(m.runtime_identity(control,'split','0-1',1000,True,0),
                         m.runtime_identity(candidate,'split','0-1',1000,True,40))
        with self.assertRaises(ValueError):m.runtime_identity(candidate,'split','0-1',1000,True,0)
        candidate['prototype']['initial_copy_override']['pause_after_chunk_ms']=0
        with self.assertRaises(ValueError):m.runtime_identity(candidate,'split','0-1',1000,True,40)
        candidate=run(40);candidate['prototype']['initial_copy_override']['pause_placement']='inside_transaction'
        with self.assertRaises(ValueError):m.runtime_identity(candidate,'split','0-1',1000,True,40)


if __name__=='__main__':unittest.main()

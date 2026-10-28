import copy
import importlib.util
import unittest
from pathlib import Path

spec=importlib.util.spec_from_file_location('admission_measure',Path(__file__).with_name('admission_measure.py'))
measure=importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure)


def normal():
    return dict(source_phase_start_epoch=1000,source_elapsed_seconds=.3,transactions=[
        dict(transaction=1,insert_started_seconds=0,committed_epoch=1000.02,
             publication_commit_epoch=1000.2,not_seen_before_seconds=.1,seen_by_seconds=.25),
        dict(transaction=2,insert_started_seconds=.1,committed_epoch=1000.12,
             publication_commit_epoch=1000.3,not_seen_before_seconds=.25,seen_by_seconds=.35)])


class AdmissionClockTests(unittest.TestCase):
    def test_consistent_clocks_are_valid(self):
        self.assertTrue(measure.timing_validation(normal())['valid'])

    def test_source_wall_clock_step_cannot_pass_comparison(self):
        work=normal()
        work['transactions'][1]['committed_epoch']+=570
        work['transactions'][1]['publication_commit_epoch']+=570
        self.assertFalse(measure.timing_validation(work)['valid'])
        case=dict(profile='catchup',repetition=1,variant='unbounded',warmup=normal(),workload=work)
        with self.assertRaisesRegex(ValueError,'clock discontinuity'):
            measure.comparisons([case])

    def test_destination_only_wall_clock_step_is_rejected(self):
        work=normal()
        work['transactions'][1]['publication_commit_epoch']+=2700
        self.assertFalse(measure.timing_validation(work)['valid'])

    def test_slow_sql_with_consistent_clocks_is_not_a_clock_step(self):
        work=normal();work['source_elapsed_seconds']=2.4
        first,second=work['transactions']
        first.update(committed_epoch=1002,publication_commit_epoch=1002.2,
                     not_seen_before_seconds=2.1,seen_by_seconds=2.3)
        second.update(insert_started_seconds=2.1,committed_epoch=1002.2,
                      publication_commit_epoch=1002.35,not_seen_before_seconds=2.3,seen_by_seconds=2.4)
        self.assertTrue(measure.timing_validation(work)['valid'])

    def test_copy_observer_clock_step_is_rejected(self):
        case=dict(trace=[dict(elapsed_seconds=5,copy=dict(observed_epoch=1005)),
                         dict(elapsed_seconds=6,copy=dict(observed_epoch=1006))],
                  source_transactions=[[dict(insert_started_seconds=0,commit_epoch=1000.02)]])
        self.assertTrue(measure.fairness_timing_validation(case)['valid'])
        stepped=copy.deepcopy(case);stepped['trace'][1]['copy']['observed_epoch']+=570
        self.assertFalse(measure.fairness_timing_validation(stepped)['valid'])

    def test_stream_only_observer_preserves_clock_audit_without_a_copy(self):
        case=dict(trace=[dict(elapsed_seconds=5,copy=None,observer_epoch=1005),
                         dict(elapsed_seconds=6,copy=None,observer_epoch=1006)],
                  source_transactions=[[dict(insert_started_seconds=0,commit_epoch=1000.02)]])
        self.assertTrue(measure.fairness_timing_validation(case)['valid'])
        case['trace'][1]['observer_epoch']+=570
        self.assertFalse(measure.fairness_timing_validation(case)['valid'])

"""Reject incomplete or invalid overhead cohorts before issuing a gate result."""
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import performance_probe as probe


def copies():
    return [dict(profile='single_copy', repetition=i, variant=v, status='passed',
                 timing_validation={'valid': True},copy_elapsed_seconds=10)
            for i in range(1,6) for v in probe.VARIANTS]


class PerformanceGateTests(unittest.TestCase):
    def test_complete_cohort_does_not_approve_production(self):
        result = probe.analyze(copies(), ['single_copy'])
        self.assertTrue(result['all_requested_gates_pass'])
        self.assertFalse(result['production_adoption_qualified'])

    def test_slow_copy_fails_despite_correct_data(self):
        cases = copies()
        for case in cases:
            if case['variant'] == 'bulk_only':
                case['copy_elapsed_seconds'] = 12
        self.assertFalse(probe.analyze(cases,['single_copy'])['all_requested_gates_pass'])

    def test_incomplete_and_duplicate_pairs_fail(self):
        cases = copies()
        with self.assertRaisesRegex(ValueError,'five complete pairs'):
            probe.analyze(cases[:-1],['single_copy'])
        with self.assertRaisesRegex(ValueError,'duplicate'):
            probe.analyze(cases+[copy.deepcopy(cases[0])],['single_copy'])

    def test_clock_or_case_failure_cannot_qualify(self):
        for key,value in [('status','finding'),('timing_validation',{'valid':False})]:
            cases = copies()
            cases[0][key] = value
            with self.assertRaisesRegex(ValueError,'invalid copy case'):
                probe.analyze(cases,['single_copy'])

"""Decision checks must not confuse eventual correctness with a passed gate."""
import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('launcher_analyze', Path(__file__).with_name('analyze.py'))
analyze = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analyze)


def cohort():
    cases = []
    for repetition in range(1, 6):
        for variant in ('static', 'base'):
            cases.append({'variant': variant, 'repetition': repetition,
                          'workload': {'observer_kind': 'journal', 'observer_poll_ms': 100,
                                       'publication_rows_per_second': 100 if variant == 'static' else 90,
                                       'publication_commit_lag_ms': {'p95': 100 if variant == 'static' else 105},
                                       'publication_catchup_seconds': .1,
                                       'first_quarter_p95_publication_commit_lag_ms': 100,
                                       'last_quarter_p95_publication_commit_lag_ms': 100,
                                       'last_poll_while_source_active': {'maximum_transaction': 2},
                                       'transactions': [{'transaction': 1, 'insert_started_seconds': 0},
                                                        {'transaction': 2, 'insert_started_seconds': .5}]}})
    return {'status': 'passed', 'parameters': {'interval_ms': 500}, 'cases': cases}


class GateTests(unittest.TestCase):
    def test_correct_rows_do_not_hide_throughput_regression(self):
        result = analyze.analyze(cohort(), 'catchup')
        self.assertEqual(result['publication_throughput_comparison']['assessment'], 'outside_proposed_margin')
        self.assertEqual(result['publication_p95_comparison']['assessment'], 'within_proposed_margin_in_this_cohort')
        self.assertFalse(result['adoption_qualified'])
        self.assertFalse(result['frozen_protocol_match'])

    def test_steady_backlog_fails_even_with_low_lag(self):
        evidence = cohort()
        for case in evidence['cases']:
            case['workload']['transactions'] = [{'transaction': tx, 'insert_started_seconds': (tx - 1) * .5}
                                                for tx in range(1, 11)]
            case['workload']['last_poll_while_source_active']['maximum_transaction'] = 4
        result = analyze.analyze(evidence, 'steady')
        self.assertFalse(result['steady_absolute_checks_all_pass'])
        self.assertTrue(all(c['absolute_checks']['p95_below_two_seconds'] for c in result['case_checks']))
        self.assertEqual(result['publication_throughput_comparison']['assessment'], 'offered_load_limited_not_a_capacity_gate')

    def test_wide_or_short_cohorts_are_not_equivalence(self):
        self.assertEqual(analyze.assess([.8, 1, 1, 1, 1], .95, True)['assessment'], 'inconclusive')
        self.assertEqual(analyze.assess([1, 1], .95, True)['assessment'], 'insufficient_repetitions')

    def test_incomplete_or_mixed_pairs_are_rejected(self):
        missing = cohort()
        missing['cases'].pop()
        with self.assertRaisesRegex(ValueError, 'both variants'):
            analyze.analyze(missing, 'steady')
        mixed = copy.deepcopy(cohort())
        mixed['cases'][0]['workload']['observer_kind'] = 'iceberg'
        with self.assertRaisesRegex(ValueError, 'one observer'):
            analyze.analyze(mixed, 'steady')


if __name__ == '__main__':
    unittest.main()

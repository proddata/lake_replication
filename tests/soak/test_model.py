"""Check soak accounting and fault witnesses without starting Docker."""
import unittest
from model import FAULTS, Progress, qualifies_soak, schedule, validate_events


class SoakModelTests(unittest.TestCase):
    def test_short_incomplete_and_cleanup_failed_runs_never_qualify_a_day(self):
        self.assertFalse(qualifies_soak('passed', 600, 650))
        self.assertFalse(qualifies_soak('passed', 86400, 86399))
        self.assertFalse(qualifies_soak('error', 86400, 86500))
        self.assertTrue(qualifies_soak('passed', 86400, 86500))

    def test_short_profile_runs_all_faults_once_and_long_profile_repeats_hourly(self):
        short = schedule(600)
        self.assertEqual([name for _, name in short], list(FAULTS))
        self.assertEqual([when for when, _ in short], [120, 240, 360, 480])
        long = schedule(86400)
        self.assertEqual(len(long), 96)
        self.assertEqual(long, sorted(long))
        self.assertLess(long[-1][0], 86400)

    def test_progress_rejects_reordering_and_future_or_regressed_destination(self):
        progress = Progress()
        with self.assertRaises(ValueError):
            progress.commit(2, 1)
        progress.commit(1, 1)
        with self.assertRaises(ValueError):
            progress.observe(2, 2)
        progress.observe(1, 2)
        with self.assertRaises(ValueError):
            progress.observe(0, 3)

    def test_observed_commits_are_removed_and_latency_buckets_do_not_double_count(self):
        progress = Progress()
        progress.commit(1, 0)
        progress.commit(2, 2)
        self.assertEqual(progress.observe(1, 4)['oldest_pending_seconds'], 2)
        progress.observe(1, 5)
        self.assertEqual(progress.completed, 1)
        self.assertEqual(progress.observe(2, 6)['pending'], 0)
        self.assertEqual(progress.buckets['5'], 2)
        self.assertEqual(progress.completed, 2)

    def test_slow_observer_has_a_bounded_pending_queue(self):
        progress = Progress()
        for n in range(1, 10001):
            progress.commit(n, n)
        with self.assertRaisesRegex(ValueError, 'accounting limit'):
            progress.commit(10001, 10001)
        progress.observe(10000, 10001)
        self.assertEqual(len(progress.pending), 0)

    def test_unexpected_container_death_and_any_oom_fail_even_with_planned_outages(self):
        expected = dict(source=0, destination=1, pgduck=0, storage=1)
        def event(service, action):
            return dict(Action=action, Actor=dict(Attributes={'com.docker.compose.service': service}))
        events = [event('destination', 'die'), event('storage', 'die'), event('source', 'exec_die')]
        self.assertEqual(validate_events(events, expected), expected)
        with self.assertRaisesRegex(ValueError, 'unexpected container exits'):
            validate_events(events + [event('pgduck', 'die')], expected)
        with self.assertRaisesRegex(ValueError, 'OOM'):
            validate_events(events + [event('storage', 'oom')], expected)


if __name__ == '__main__':
    unittest.main()

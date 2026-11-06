"""Maintenance must be explicit and must not accept warning-only vacuum failures."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from maintenance import Maintenance, POLICIES
from observe_references import object_file


class MaintenanceTests(unittest.TestCase):
    def test_independent_object_checks_are_scoped_to_the_private_fixture(self):
        self.assertEqual(object_file('s3://lake-qualification/iceberg/table/data.parquet'),
                         '/data/lake-qualification/iceberg/table/data.parquet/xl.meta')
        for uri in ['s3://other/iceberg/x', 'file:///etc/passwd', 's3://lake-qualification/iceberg/../../x',
                    's3://lake-qualification/iceberg/x\n/data/other', 's3://lake-qualification/.minio.sys/x']:
            with self.subTest(uri=uri), self.assertRaises(ValueError):
                object_file(uri)
    def runner(self, profile=None):
        args = SimpleNamespace() if profile is None else SimpleNamespace(maintenance_profile=profile)
        return SimpleNamespace(args=args, result={}, sql=Mock(), append=Mock())

    def test_default_profile_does_not_shorten_retention_or_issue_sql(self):
        runner = self.runner()
        maintenance = Maintenance(runner, Mock())
        self.assertFalse(maintenance.enabled)
        runner.sql.assert_not_called()
        self.assertEqual(runner.result['maintenance']['profile'], 'none')

    def test_profiles_separate_snapshot_expiry_from_the_recovery_window(self):
        self.assertEqual(POLICIES['snapshot-only']['snapshot_seconds'], POLICIES['bounded-test']['snapshot_seconds'])
        self.assertEqual(POLICIES['snapshot-only']['file_seconds'], 10 * 86400)
        self.assertEqual(POLICIES['bounded-test']['file_seconds'], 120)

    def test_warning_only_vacuum_failure_is_not_treated_as_success(self):
        runner = self.runner('bounded-test')
        runner.sql.return_value = SimpleNamespace(stdout='', stderr='WARNING: failed to expire snapshots')
        maintenance = Maintenance(runner, Mock())
        with self.assertRaisesRegex(RuntimeError, 'failed phase'):
            maintenance.vacuum('soak_items')
        self.assertEqual(runner.result['maintenance']['passes'], 0)
        runner.append.assert_called_once()

    def test_vacuum_rejects_targets_outside_the_fixture_before_sql(self):
        runner = self.runner('bounded-test')
        with self.assertRaisesRegex(ValueError, 'unexpected maintenance target'):
            Maintenance(runner, Mock()).vacuum('unrelated_table')
        runner.sql.assert_not_called()

    def test_targeted_vacuum_uses_the_supported_per_table_command(self):
        runner = self.runner('snapshot-only')
        runner.sql.return_value = SimpleNamespace(stdout='', stderr='INFO: pg_lake vacuum completed')
        Maintenance(runner, Mock()).vacuum('soak_items')
        command = runner.sql.call_args.args[1]
        self.assertIn('VACUUM (VERBOSE) soak_items', command)
        self.assertNotIn('VACUUM (ICEBERG', command)

    def test_admission_is_released_for_every_holder_after_vacuum_failure(self):
        runner = self.runner('bounded-test')
        holders = [Mock()]
        holders[0].query.return_value = ['1']
        maintenance = Maintenance(runner, Mock(side_effect=holders))
        with self.assertRaisesRegex(RuntimeError, 'vacuum failed'):
            with maintenance.publication_pause():
                raise RuntimeError('vacuum failed')
        for holder in holders:
            self.assertEqual(holder.query.call_args.args, ('ROLLBACK',))
            holder.close.assert_called_once()

    def test_cleanup_failure_preserves_the_primary_error_and_closes_other_holders(self):
        runner = self.runner('bounded-test')
        holders = [Mock()]
        holders[-1].query.side_effect = [['1'], RuntimeError('rollback failed')]
        maintenance = Maintenance(runner, Mock(side_effect=holders))
        with self.assertRaisesRegex(ValueError, 'original failure'):
            with maintenance.publication_pause():
                raise ValueError('original failure')
        for holder in holders:
            holder.close.assert_called_once()
        self.assertEqual(runner.result['maintenance_gate_cleanup_errors'], ['rollback failed'])

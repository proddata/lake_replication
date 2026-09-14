"""Offline checks for probe classification; these do not establish feasibility."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import probe


class NativeClassificationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.results = patch.object(probe, "RESULTS", Path(self.directory.name))
        self.results.start()
        self.addCleanup(self.results.stop)
        self.evidence = patch.object(probe, "EVIDENCE", {"native": {}})
        self.evidence.start()
        self.addCleanup(self.evidence.stop)

    def run_timeout(self, log, returncode=0):
        with patch.object(probe, "sql", return_value="{}"), \
             patch.object(probe, "wait_for", side_effect=[False, True]), \
             patch.object(probe, "command", return_value=SimpleNamespace(
                 stdout=log, stderr="", returncode=returncode)):
            return probe.native(True)

    def test_confirmed_async_rejection_is_blocked(self):
        self.assertFalse(self.run_timeout(
            'ERROR: cannot use relation "copy_iceberg" as logical replication target'))

    def test_unexplained_timeout_is_not_architecture_evidence(self):
        with self.assertRaisesRegex(RuntimeError, "without a confirmed"):
            self.run_timeout("worker starting")

    def test_schema_qualified_async_rejection_is_blocked(self):
        self.assertFalse(self.run_timeout(
            f'ERROR: cannot use relation "{probe.RUN}.copy_iceberg" as logical replication target'))

    def test_other_relation_rejection_is_not_evidence(self):
        with self.assertRaisesRegex(RuntimeError, "without a confirmed"):
            self.run_timeout(
                'ERROR: cannot use relation "other" as logical replication target')

    def test_log_failure_is_not_evidence(self):
        with self.assertRaisesRegex(RuntimeError, "without a confirmed"):
            self.run_timeout("", returncode=1)

    def test_confirmed_synchronous_rejection_is_blocked(self):
        def sql(database, statement):
            if "CREATE SUBSCRIPTION" in statement:
                raise RuntimeError('42809: cannot use relation "copy_iceberg" as logical replication target')
            return ""

        with patch.object(probe, "sql", side_effect=sql):
            self.assertFalse(probe.native(True))


if __name__ == "__main__":
    unittest.main()

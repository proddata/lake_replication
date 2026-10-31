"""Historical SQL digests generated independently from a209878 with make generate."""
import hashlib
import json
from pathlib import Path
import unittest


class HistoricalSqlTests(unittest.TestCase):
    def test_all_historical_install_and_upgrade_payloads_remain_frozen(self):
        directory=Path(__file__).resolve().parents[2]/'extensions/lake_replication'
        expected=json.loads(Path(__file__).with_name('frozen_sql.json').read_text())
        self.assertEqual(len(expected),10)
        for name,digest in expected.items():
            with self.subTest(name=name):
                self.assertEqual(hashlib.sha256((directory/name).read_bytes()).hexdigest(),digest)

"""Released SQL must remain byte-identical when new migrations are added."""
import hashlib
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
# SHA256 from the beta.2 exported archive (963e6dd2...) and the retained
# beta.3 archive built at 8bcd5b2 (4494a0ce...).
FROZEN = {'lake_replication--0.1.0-beta.3.sql':'fe02eb376e8467e3c69c51ce1e9c4f415810a5a54dcf2a402ec77742bdf3d35d',
 'lake_replication--0.1.0-beta.2--0.1.0-beta.3.sql':'9ab9461483a11815411da68b64618f3ee2665d3ba75ce2aab77fe2cfea508177',
 'lake_replication--0.1.0--0.1.0-beta.1.sql': '624ffc95db24c82b3f1b6558b77ef3da11f4ba7ea906f5efe15a4ed6fabb5ff3',
 'lake_replication--0.1.0--0.1.0-beta.2.sql': '4ef17b17feddafa22d1cfc2247b1b55f613c18d2d56c3f2dc6a6220b55fa589d',
 'lake_replication--0.1.0-beta.1--0.1.0-beta.2.sql': 'f75fc3576bb32a05b68251818ee3e70eb0b58ca4014ec01b34a8ece59117d145',
 'lake_replication--0.1.0-beta.1.sql': 'cfda40d4d12efa7a669b337349831d6b342b494aee07fd629b0b173136b6ca16',
 'lake_replication--0.1.0-beta.2.sql': '2079a45c0f1ae552059065a335b5cf6f40ad5cf2d0717bdca3589ab649f4160d',
 'lake_replication--0.1.0.sql': 'cc8110fdd40df700bed6fb11bb5276fae93b2adbff7e7c3c9b53ca610114107e'}


class ReleasedVersionTests(unittest.TestCase):
    def test_released_sql_is_unchanged(self):
        for name, expected in FROZEN.items():
            with self.subTest(version=name):
                actual = hashlib.sha256((ROOT/'extensions/lake_replication'/name).read_bytes()).hexdigest()
                self.assertEqual(actual, expected)

import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('pressure',Path(__file__).with_name('pressure.py'))
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)


class VisibilityTests(unittest.TestCase):
    def test_empty_destination_queue_cannot_hide_receiver_backlog(self):
        observed=p.sample(['0','1002','0','',''],[{'commit_epoch':1000}])
        self.assertEqual(observed['fixture_oldest_unpublished_age_seconds'],2)
        self.assertEqual(observed['fixture_pending_not_in_destination'],1)
        self.assertEqual(observed['receiver_health'],'unknown')

    def test_newer_publication_can_overtake_fixture_snapshot(self):
        observed=p.sample(['2','1002','0','',''],[{'commit_epoch':1000}])
        self.assertEqual(observed['fixture_unpublished_transactions'],0)
        self.assertEqual(observed['fixture_pending_not_in_destination'],0)

    def test_queue_and_fixture_backlog_remain_distinct(self):
        observed=p.sample(['1','1003','1','0.25','1'],[
            {'commit_epoch':1000},{'commit_epoch':1002},{'commit_epoch':1002.5}])
        self.assertEqual(observed['fixture_unpublished_transactions'],2)
        self.assertEqual(observed['fixture_pending_not_in_destination'],1)
        self.assertEqual(observed['oldest_pending_staged_age_seconds'],.25)

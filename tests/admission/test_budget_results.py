"""Reject apparent rate-limit passes with missing or multiplied accounting."""
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('budget_results',Path(__file__).with_name('budget_results.py'))
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)


class AccountingAuditTests(unittest.TestCase):
    def fixture(self):
        # Two literal one-row JSON chunks: 152 bytes each, plus [ and ].
        return {'timing_validation':{'valid':True},'copy_rows':1,'bulk_copies':2,'diagnostic':{'shared_byte_budget':{
            'rate_bytes_per_second':100,'burst_bytes':256,'stats':{'settings':{'rate_bytes_per_second':100,'burst_bytes':256},'worker_log_sha256':'0'*64,'credit':{
                'requests':2,'reserved_bytes':308,'available_bytes':0,
                'first_reserved_at':'2026-01-01T00:00:00+00:00',
                'refilled_at':'2026-01-01T00:00:01+00:00','max_wait_seconds':0.5}}}}}

    def test_shared_accounting_matches_fixed_json_fixture(self):
        result=a.audit(self.fixture())
        self.assertEqual(result['expected_bytes'],308)
        self.assertTrue(result['valid'])

    def test_per_copy_allowance_cannot_masquerade_as_shared_rate(self):
        case=self.fixture()
        case['diagnostic']['shared_byte_budget']['stats']['credit']['refilled_at']='2026-01-01T00:00:00+00:00'
        self.assertFalse(a.audit(case)['checks']['one_aggregate_reservation_envelope'])

    def test_configuration_mismatch_rejects_the_comparison(self):
        case=self.fixture()
        case['diagnostic']['shared_byte_budget']['stats']['settings']['rate_bytes_per_second']=200
        self.assertFalse(a.audit(case)['valid'])

    def test_clock_step_cannot_produce_a_valid_budget_measurement(self):
        case=self.fixture();case['timing_validation']['valid']=False
        self.assertFalse(a.audit(case)['valid'])

    def test_missing_and_duplicate_reservations_fail_the_audit(self):
        for requests in (1,3):
            case=self.fixture()
            case['diagnostic']['shared_byte_budget']['stats']['credit']['requests']=requests
            self.assertFalse(a.audit(case)['valid'])
        case=self.fixture()
        case['diagnostic']['shared_byte_budget']['stats']['credit']['reserved_bytes']-=1
        self.assertFalse(a.audit(case)['valid'])

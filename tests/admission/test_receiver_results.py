import copy
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('receiver_results',Path(__file__).with_name('receiver_results.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)


def fixture():
    row=dict(database='a',subscription_id=1,pid=100,started_epoch=1000,snapshot_epoch=1001,
             elapsed_seconds=1,durably_staged_lsn='0/10',last_message=None,last_message_age_at_snapshot_seconds=None)
    return dict(timing_validation={'valid':True},diagnostic={'receiver_progress':{'final':[row]}},trace=[{'receiver_progress':[row]}])


class ReceiverAuditTests(unittest.TestCase):
    def test_rapid_receiver_churn_must_not_multiply_subscription_records(self):
        case=fixture();old=copy.deepcopy(case['trace'][0]['receiver_progress'][0]);old['pid']=99
        case['trace'][0]['receiver_progress'].append(old)
        self.assertFalse(r.audit(case)['checks']['one_record_per_subscription'])

    def test_fresh_process_generation_cannot_regress_durable_frontier(self):
        case=fixture();point=copy.deepcopy(case['trace'][0]);point['receiver_progress'][0].update(pid=101,durably_staged_lsn='0/F')
        case['trace'].append(point)
        self.assertFalse(r.audit(case)['checks']['durable_frontier_never_regresses'])

    def test_clock_invalid_rejects_otherwise_valid_records(self):
        result=r.audit(fixture());self.assertTrue(result['valid'])
        case=fixture();case['timing_validation']['valid']=False
        self.assertFalse(r.audit(case)['valid'])

    def test_failed_final_capture_invalidates_otherwise_complete_trace(self):
        case=fixture();case['diagnostic']['receiver_progress']['final']={'error':'timeout'}
        self.assertFalse(r.audit(case)['valid'])

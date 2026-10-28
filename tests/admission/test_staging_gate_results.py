import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('gate_results',Path(__file__).with_name('staging_gate_results.py'))
g=importlib.util.module_from_spec(spec);spec.loader.exec_module(g)


def fixture():
    return {'copy_rows':1,'bulk_copies':2,'timing_validation':{'valid':True},
        'diagnostic':{'staging_gate':{'mode':'serial','final':{'fair_copy':{'chunks':1,'bytes':154},'fair_copy_1':{'chunks':1,'bytes':154}}}},
        'trace':[{'staging_gate':[{'state':'admitted'}]}]}


class GateAccountingTests(unittest.TestCase):
    def test_commit_accounting_must_cover_every_chunk_and_byte(self):
        case=fixture();self.assertTrue(g.audit(case)['valid'])
        case['diagnostic']['staging_gate']['final']['fair_copy_1']['bytes']=153
        self.assertFalse(g.audit(case)['valid'])
        case=fixture();case['diagnostic']['staging_gate']['final']['fair_copy_1']['chunks']=2
        self.assertFalse(g.audit(case)['valid'])

    def test_one_copy_cannot_supply_another_copys_missing_accounting(self):
        case=fixture();case['diagnostic']['staging_gate']['final']={'fair_copy':{'chunks':2,'bytes':308},'fair_copy_1':{'chunks':0,'bytes':0}}
        self.assertFalse(g.audit(case)['valid'])

    def test_two_cross_database_holders_cannot_masquerade_as_serialization(self):
        case=fixture();case['trace'][0]['staging_gate'].append({'state':'admitted'})
        self.assertFalse(g.audit(case)['valid'])
        case=fixture();case['timing_validation']['valid']=False
        self.assertFalse(g.audit(case)['valid'])

    def test_accounting_control_must_never_acquire_the_permit(self):
        case=fixture();case['diagnostic']['staging_gate']['mode']='accounting'
        self.assertFalse(g.audit(case)['valid'])
        case['trace'][0]['staging_gate']=[]
        self.assertTrue(g.audit(case)['valid'])

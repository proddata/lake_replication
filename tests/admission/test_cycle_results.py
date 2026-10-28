import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('cycles',Path(__file__).with_name('cycle_results.py'))
c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)


class MarkerTests(unittest.TestCase):
    def test_marker_must_fall_between_source_and_publication(self):
        row={'xid':1,'source_commit_epoch':1000,'journal_insert_epoch':1001,'publication_commit_epoch':1002}
        transactions=[{'database':name,'database_oid':i,'transactions':[row.copy()]} for i,name in enumerate(['a','b','c'])]
        import json
        events=[{'event':'transaction_staged','xid':1,'timestamp_unix_ms':1001001}]
        for index,phase in enumerate(['cycle_started','preflight_finished','receiver_spawned','publisher_started','publisher_finished','cycle_finished']):
            events.append({'event':phase,'component':'diagnostic_cycle','timestamp_unix_ms':1000000+index,'elapsed_us':index*1000,'pid':10,'cycle':'1'})
        logs={f'lake_replication_db{i}_1_supervisor.log':'\n'.join(json.dumps(e) for e in events) for i in range(3)}
        result=c.analyze({'timing_validation':{'valid':True},'stream_transactions':1},transactions,logs)
        self.assertTrue(result['valid'])
        self.assertEqual(result['streams'][0]['source_to_journal_insert_ms']['p50'],1000)
        transactions[0]['transactions'][0]['journal_insert_epoch']=1003
        self.assertFalse(c.analyze({'timing_validation':{'valid':True},'stream_transactions':1},transactions,logs)['valid'])

    def test_missing_supervisor_phases_rejects_an_otherwise_complete_capture(self):
        import json
        row={'xid':1,'source_commit_epoch':1000,'journal_insert_epoch':1001,'publication_commit_epoch':1002}
        transactions=[{'database':name,'database_oid':i,'transactions':[row.copy()]} for i,name in enumerate(['a','b','c'])]
        logs={f'lake_replication_db{i}_1_supervisor.log':json.dumps({'event':'transaction_staged','xid':1,'timestamp_unix_ms':1001001}) for i in range(3)}
        result=c.analyze({'timing_validation':{'valid':True},'stream_transactions':1},transactions,logs)
        self.assertFalse(result['valid'])
        self.assertFalse(result['checks']['cycle_events_captured'])

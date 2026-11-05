"""Reject internal spans that cannot belong to a measured publication commit."""
import copy
import unittest
from commit_profile_results import analyze


def fixture():
    prefix='destination-1 | lake-profile|analytics|lake_replication_worker/1/1|40|LOG: '
    messages=['duration: 1.000 ms','lake-commit|metadata_prepare|1.000|1',
        'lake-commit|iceberg_metadata|2.000|1','lake-commit|upload_connect|0.200|1',
        'lake-commit|upload_connect|0.300|1','lake-commit|upload_commands|3.500|2',
        'lake-commit|uploads|4.000|1','lake-commit|metadata_total|8.000|1','duration: 10.000 ms']
    matched=dict(valid=True,matched_publications=[dict(database='analytics',backend_pid=40,
        sequence=2,phase='commit',server_ms=10,batch_id='12',published_lsn='0/123')])
    return '\n'.join(prefix+message for message in messages),matched


class CommitResultsTests(unittest.TestCase):
    def test_nested_timers_match_the_same_backend_and_commit_message(self):
        log,matched=fixture();r=analyze(log,matched)
        self.assertTrue(r['valid'],r['errors']);row=r['publications'][0]
        self.assertEqual((row['batch_id'],row['metadata_total_ms'],row['upload_connect_sum_ms']),('12',8,.5))
        self.assertEqual((row['commit_remainder_ms'],row['metadata_remainder_ms']),(2,1))

    def test_wrong_message_or_backend_cannot_supply_missing_spans(self):
        log,matched=fixture()
        for key,value in [('sequence',1),('backend_pid',41)]:
            changed=copy.deepcopy(matched);changed['matched_publications'][0][key]=value
            self.assertFalse(analyze(log,changed)['valid'])

    def test_duplicate_metadata_parent_and_missing_upload_reject_attribution(self):
        log,matched=fixture()
        duplicate=log.replace('lake-commit|metadata_total|8.000|1','lake-commit|metadata_total|8.000|1\nlake-profile|analytics|worker|40|LOG: lake-commit|metadata_total|8.000|1')
        missing=log.replace('lake-commit|uploads|4.000|1','unrelated log')
        self.assertFalse(analyze(duplicate,matched)['valid'])
        self.assertFalse(analyze(missing,matched)['valid'])

    def test_children_cannot_exceed_enclosing_spans(self):
        log,matched=fixture()
        for old,new in [('metadata_total|8.000','metadata_total|20.000'),
                        ('metadata_prepare|1.000','metadata_prepare|12.000'),
                        ('upload_connect|0.200','upload_connect|10.000')]:
            self.assertFalse(analyze(log.replace(old,new),matched)['valid'])

    def test_missing_upload_connection_or_invalid_wire_match_rejects_result(self):
        log,matched=fixture()
        self.assertFalse(analyze(log.replace('upload_commands|3.500|2','upload_commands|3.500|3'),matched)['valid'])
        matched['valid']=False
        self.assertFalse(analyze(log,matched)['valid'])


if __name__=='__main__':unittest.main()

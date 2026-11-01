"""Do not attribute timings when a session or publication cannot be matched."""
import copy
import json
import unittest
from query_trace_results import correlate


def fixture():
    fields=dict(pid=30,connection='0',database='analytics',backend_pid='40',
        subscription_id='1',generation='1',role='publisher',status='ok',event='query_timing')
    events=[dict(event='publisher_spawn',child_pid='30',database='analytics',subscription_id='1',generation='1',started_unix_ms='1000',spawn_elapsed_us='100'),
        dict(fields,sequence='0',operation='connect',elapsed_us='4000',timestamp_unix_ms=1009)]
    operations=['query','next_batch','publish_batch','next_batch']
    for index,operation in enumerate(operations,1):
        events.append(dict(fields,sequence=str(index),operation=operation,elapsed_us='12000'))
    events.insert(5,dict(pid=30,event='batch_published',component='publisher',batch_id='12',published_lsn='0/123'))
    log='\n'.join(f'lake-profile|analytics|lake_replication_worker/1/1|40|LOG:  duration: {duration} ms' for duration in [1,2,10,1])
    return log,events


def run(log,events):return correlate(log,'\n'.join(json.dumps(e) for e in events))


class TraceResultsTests(unittest.TestCase):
    def test_complete_session_matches_actual_batch_and_both_prefix_formats(self):
        log,events=fixture()
        for text in [log,log.replace('|LOG:','| LOG:')]:
            result=run(text,events)
            self.assertTrue(result['valid'],result['errors'])
            row=result['matched_publications'][0]
            self.assertEqual((row['batch_id'],row['published_lsn'],row['server_ms']),('12','0/123',10))
            self.assertEqual((row['client_minus_server_ms'],row['startup_before_connect_ms']),(2,5))

    def test_missing_or_duplicate_client_message_rejects_session(self):
        log,events=fixture()
        for changed in [events[:3]+events[4:],events+[copy.deepcopy(events[2])]]:
            self.assertFalse(run(log,changed)['valid'])

    def test_missing_server_message_rejects_session(self):
        log,events=fixture()
        self.assertFalse(run('\n'.join(log.splitlines()[:-1]),events)['valid'])

    def test_unrelated_worker_cannot_claim_publication(self):
        log,events=fixture();events[5]['pid']=31
        self.assertFalse(run(log,events)['valid'])

    def test_error_or_inconsistent_backend_is_not_treated_as_valid_timing(self):
        log,events=fixture()
        for field,value in [('status','server_error'),('backend_pid','99')]:
            changed=copy.deepcopy(events);changed[4][field]=value
            self.assertFalse(run(log,changed)['valid'])

    def test_server_time_cannot_exceed_the_client_interval(self):
        log,events=fixture()
        self.assertFalse(run(log.replace('10 ms','100 ms'),events)['valid'])

    def test_missing_or_wrong_generation_spawn_cannot_claim_connection(self):
        log,events=fixture()
        self.assertFalse(run(log,events[1:])['valid'])
        events[0]['generation']='2'
        self.assertFalse(run(log,events)['valid'])

    def test_phase_timings_belong_to_one_batch_and_commit_summary_is_explicit(self):
        _,events=fixture();expanded=events[:4]
        for sequence,phase in enumerate(['begin','fence','statement','commit'],3):
            expanded.append(dict(events[4],sequence=str(sequence),phase=phase))
        expanded.extend([events[5],dict(events[6],sequence='7')])
        log='\n'.join(f'lake-profile|analytics|lake_replication_worker/1/1|40|LOG: duration: {value} ms' for value in [1,2,.1,.2,3,6,1])
        result=run(log,expanded)
        self.assertTrue(result['valid'],result['errors'])
        self.assertEqual(len(result['matched_publications']),1)
        self.assertEqual(result['databases']['analytics']['summary_phase'],'commit')
        self.assertEqual(result['databases']['analytics']['phases']['statement']['server_ms']['mean_ms'],3)
        self.assertEqual(result['matched_publications'][0]['batch_id'],'12')
        expanded[5]['phase']='statement'
        self.assertFalse(run(log,expanded)['valid'])


if __name__=='__main__':unittest.main()

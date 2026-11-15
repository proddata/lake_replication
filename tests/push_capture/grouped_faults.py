#!/usr/bin/env python3
"""Atomicity and receipt reconciliation of multi-transaction capture commits."""
import json
import subprocess
import sys
from native_faults import NativeFaultProbe


class GroupedFaultProbe(NativeFaultProbe):
    def kill_child(self,child):
        self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'],required=False)
        child.communicate(timeout=15)

    def exercise(self):
        self.evidence.update(scope='grouped source publication native crash and receipt reconciliation',
            unqualified=['production throughput','automatic failover'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql('UPDATE lake_capture.streams SET transactions_per_read=32')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        child=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        finally:self.kill_child(child)
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        for group,point in enumerate(['before_upload_completion','after_upload_completion','after_commit']):
            low=group*12+1;high=low+11
            for number in range(low,high+1):
                self.sql(f"BEGIN;INSERT INTO a VALUES({number},'{point}');INSERT INTO b VALUES({number},'{point}');COMMIT")
            failed=self.command(self.capture_args(LAKE_CAPTURE_TEST_CRASH=point),required=False)
            self.check(point+': grouped publication loses response',failed.returncode!=0,True)
            self.ready()
            expected=high if point=='after_commit' else low-1
            self.check(point+': every manifest in the group has the same outcome',self.value(
                "SELECT captured_sequence::text||':'||(SELECT count(*) FROM lake_capture_log.manifests)||':'||(SELECT count(*) FROM lake_capture.receipts) FROM lake_capture.streams"),f'{expected}:{expected}:{expected}')
            self.check(point+': no incomplete group becomes destination-visible',self.value(
                "SELECT (SELECT count(*) FROM lake_capture_log.events_a)::text||':'||(SELECT count(*) FROM lake_capture_log.events_b)"),f'{expected}:{expected}')
            if point=='after_commit':
                self.sql('UPDATE lake_capture.streams SET transactions_per_read=1')
            child=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(high))
                self.source_wait('''SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text
                    FROM lake_capture.streams s JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true')
            finally:self.kill_child(child)
            self.check(point+': repaired group uses one publication commit',self.value(
                f'SELECT count(DISTINCT xmin::text) FROM lake_capture.receipts WHERE sequence BETWEEN {low} AND {high}'),'1')
            self.check(point+': reconciliation preserves independent receipt identities',self.value(
                'SELECT count(DISTINCT batch_id) FROM lake_capture.receipts'),str(high))
            self.check(point+': recovery retains one data file per log and complete group',self.value('''
                SELECT count(*) FROM lake_table.files WHERE content=0 AND table_name IN (
                    'lake_capture_log.events_a'::regclass,'lake_capture_log.events_b'::regclass,
                    'lake_capture_log.manifests'::regclass)'''),str(3*(group+1)))
            self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-publisher'))
            self.importer(LAKE_PUSH_REPLAY_FROM=str(low),LAKE_PUSH_MAX_BATCHES='64')
            self.command(self.worker_args('lake-replication-publisher'))
            self.check(point+': complete group reaches both mirrors once',self.dest(
                "SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),f'{high}:{high}')


if __name__=='__main__':sys.exit(GroupedFaultProbe().run())

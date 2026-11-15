#!/usr/bin/env python3
"""Complete-transaction publication groups and bounded read-window fallback."""
import json
import subprocess
import sys
from live import LiveProbe


class GroupedProbe(LiveProbe):
    def stop_child(self,child):
        self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'],required=False)
        child.communicate(timeout=15)

    def confirmed(self):
        self.source_wait('''SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text
            FROM lake_capture.streams s JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true')

    def exercise(self):
        self.evidence.update(scope='opt-in bounded complete-transaction capture publication groups',
            unqualified=['production throughput','independently committed transport chunks'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql('UPDATE lake_capture.streams SET transactions_per_read=32,publication_interval_ms=50')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        for number in range(1,11):
            self.sql(f"BEGIN;INSERT INTO a VALUES({number},'row');INSERT INTO b VALUES({number},'paired');COMMIT")
        self.sql("BEGIN;UPDATE a SET id=101 WHERE id=1;DELETE FROM b WHERE id=2;INSERT INTO b VALUES(2,'reinsert');COMMIT")
        self.sql("BEGIN;UPDATE a SET note='discarded';TRUNCATE a,b;INSERT INTO a VALUES(99,'last');INSERT INTO b VALUES(100,'last');COMMIT")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        child=subprocess.Popen(self.capture_args(LAKE_CAPTURE_TEST_CHUNK_BYTES='128'),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','12')
            self.confirmed()
        finally:self.stop_child(child)
        self.check('twelve source commits share one publication transaction',self.value('SELECT count(DISTINCT xmin::text) FROM lake_capture.receipts'),'1')
        self.check('group preserves twelve independent manifests and reservations',self.value(
            "SELECT (SELECT count(*) FROM lake_capture_log.manifests)::text||':'||(SELECT count(*) FROM lake_capture.upload_attempts WHERE completed)"),'12:12')
        for relation in ['events_a','events_b','manifests']:
            self.check('one physical append holds all grouped '+relation,self.value(
                f"SELECT count(*) FROM lake_table.files WHERE content=0 AND table_name='lake_capture_log.{relation}'::regclass"),'1')
        self.check('group manifests preserve contiguous numeric transaction order',self.value(
            "SELECT string_agg(sequence::text,',' ORDER BY sequence) FROM lake_capture_log.manifests"),','.join(map(str,range(1,13))))
        self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-publisher'))
        self.check('grouped mutations and TRUNCATE preserve source commit order',self.dest(
            "SELECT (SELECT string_agg(id::text,',') FROM mirror.a)||':'||(SELECT string_agg(id::text,',') FROM mirror.b)"),'99:100')
        for number in range(200,203):
            self.sql(f"BEGIN;INSERT INTO a VALUES({number},'window');INSERT INTO b VALUES({number},'window');COMMIT")
        child=subprocess.Popen(self.capture_args(LAKE_CAPTURE_TEST_READ_EVENTS='4'),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','15')
            self.confirmed()
            self.check('adaptive read retry reacquires all private session locks',self.value("SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.application_name LIKE 'lake_capture/%' AND l.mode='AccessShareLock' AND l.relation IN ('lake_capture_log.events_a'::regclass,'lake_capture_log.events_b'::regclass,'lake_capture_log.controls'::regclass,'lake_capture_log.manifests'::regclass)"),'4')
        finally:self.stop_child(child)
        logs=self.command(self.compose+['logs','--no-color','source'])
        self.check('oversized read window retries smaller complete windows','capture read window budget exceeded' in logs.stdout+logs.stderr,True)
        self.check('read fallback consumes no speculative upload reservations',self.value('SELECT upload_attempt_count FROM lake_capture.streams'),'15')
        self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-publisher'))
        self.importer(LAKE_PUSH_REPLAY_FROM='1',LAKE_PUSH_MAX_BATCHES='64')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('grouped and split-window replay publishes every final row once',self.dest(
            "SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),'4:4')
        self.check('independent destination receipts survive source grouping',self.progress(),'15:15:15')
        self.sql('UPDATE lake_capture.streams SET max_generation_batches=16')
        for number in range(300,302):
            self.sql(f"BEGIN;INSERT INTO a VALUES({number},'budget');INSERT INTO b VALUES({number},'budget');COMMIT")
        child=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            child.communicate(timeout=30)
            self.check('cumulative group budget fails before any member is reserved',
                child.returncode!=0 and self.value('SELECT captured_sequence::text||\':\'||upload_attempt_count FROM lake_capture.streams')=='15:15',True)
        finally:
            if child.poll() is None:self.stop_child(child)
        self.sql('UPDATE lake_capture.streams SET max_generation_batches=17')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        child=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','17')
            self.confirmed()
        finally:self.stop_child(child)
        self.check('repaired cumulative budget reserves the complete group once',self.value('SELECT upload_attempt_count FROM lake_capture.streams'),'17')
        self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-publisher'))
        self.check('group budget repair publishes all protected transactions exactly once',self.dest(
            "SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),'6:6')


if __name__=='__main__':sys.exit(GroupedProbe().run())

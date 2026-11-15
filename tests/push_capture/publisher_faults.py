#!/usr/bin/env python3
"""Existing publisher's ambiguous commits while consuming push mutations."""
from datetime import datetime, timezone
import json
import sys
import time
from live import LiveProbe

class PublisherFaultProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='push journal -> unchanged publisher upload/COMMIT crash recovery',
            unqualified=['production mode selection','production failover'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        self.evidence['boundaries']={}
        changes=["INSERT INTO a VALUES(1,'insert');INSERT INTO b VALUES(1,'insert')",
            "UPDATE a SET id=2,note='key-change';UPDATE b SET id=2,note='key-change'",
            "TRUNCATE a,b;INSERT INTO a VALUES(3,'truncate');INSERT INTO b VALUES(3,'truncate')"]
        for sequence,(point,statement) in enumerate(zip(
                ['before_upload_completion','after_upload_completion','after_commit'],changes),1):
            self.sql('BEGIN;'+statement+';COMMIT')
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(sequence))
            self.importer()
            started=datetime.now(timezone.utc).isoformat()
            failed=self.command(self.worker_args('lake-replication-publisher',LAKE_LAB_CRASH_POINT=point),required=False)
            self.check(point+': publisher loses its success response',failed.returncode!=0,True)
            deadline=time.monotonic()+60
            while time.monotonic()<deadline:
                try:
                    if self.dest('SELECT 1')=='1':break
                except RuntimeError:pass
                time.sleep(.2)
            logs=self.command(self.compose+['logs','--no-color','--since',started,'destination'])
            diagnostic=logs.stdout+logs.stderr
            (self.output/(point+'.log')).write_text(diagnostic)
            self.check(point+': destination backend actually crashed',
                'was terminated by signal 9' in diagnostic and 'all server processes terminated; reinitializing' in diagnostic,True)
            expected=sequence if point=='after_commit' else sequence-1
            self.check(point+': publication checkpoint follows actual commit outcome',self.dest(
                f'SELECT published_sequence::text FROM lake_replication.push_status WHERE subscription_id={self.subscription}'),str(expected))
            self.check(point+': imported checkpoint remains durable',self.dest(
                f'SELECT imported_sequence::text FROM lake_replication.push_status WHERE subscription_id={self.subscription}'),str(sequence))
            self.command(self.worker_args('lake-replication-publisher'))
            self.importer(LAKE_PUSH_REPLAY_FROM=str(sequence))
            self.command(self.worker_args('lake-replication-publisher'))
            for table in 'ab':
                self.check(point+': '+table+' recovers exact mutations once',self.dest(
                    f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM mirror.{table} t'),self.value(
                    f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {table} t'))
            self.check(point+': source capture is independent of destination crash',self.value(
                'SELECT captured_sequence::text FROM lake_capture.streams'),str(sequence))
            self.evidence['boundaries'][point]=dict(published_before_recovery=expected,published_after_recovery=sequence)

if __name__=='__main__':sys.exit(PublisherFaultProbe().run())

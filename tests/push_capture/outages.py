#!/usr/bin/env python3
"""Asynchronous object-store outage, source-catalog loss and protected recovery."""
import json
import sys
import time
from live import LiveProbe

class OutageProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='source object-store and source-catalog outage recovery',
            unqualified=['credential rotation','physical disk full','production failover'])
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
        self.sql("BEGIN;INSERT INTO a VALUES(1,'first');INSERT INTO b VALUES(1,'first');COMMIT;")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.command(self.compose+['stop','storage'],timeout=30)
        started=time.monotonic()
        self.sql("BEGIN;INSERT INTO a VALUES(2,'while-storage-down');INSERT INTO b VALUES(2,'while-storage-down');COMMIT;")
        elapsed=time.monotonic()-started
        self.evidence['application_commit_during_storage_outage_seconds']=elapsed
        self.check('application COMMIT is independent of source object uploads',elapsed<10,True)
        deadline=time.monotonic()+50
        while time.monotonic()<deadline:
            if self.value("SELECT desired_state FROM lake_capture.streams")=='stopped':break
            time.sleep(.2)
        self.check('failed object upload stops within its statement budget',self.value('SELECT desired_state FROM lake_capture.streams'),'stopped')
        self.check('object-store outage leaves captured frontier and manifest unchanged',self.value(
            "SELECT captured_sequence::text||':'||(SELECT count(*) FROM lake_capture_log.manifests) FROM lake_capture.streams"),'1:1')
        self.check('failed upload keeps a durable incomplete attempt',self.value('SELECT count(*) FROM lake_capture.upload_attempts WHERE NOT completed'),'1')
        time.sleep(1)
        self.check('fatal upload failure cannot spin unlimited retries',self.value('SELECT upload_attempt_count::text FROM lake_capture.streams'),'2')
        self.command(self.compose+['start','storage'])
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2')
        self.command(self.compose+['stop','source'],timeout=30)
        failed=self.importer(required=False)
        self.check('source-catalog outage stops discovery without advancing import',failed.returncode!=0,True)
        self.check('catalog outage preserves destination imported frontier',self.dest(f'SELECT imported_sequence::text FROM lake_replication.push_bindings WHERE subscription_id={self.subscription}'),'1')
        self.command(self.compose+['start','source'])
        deadline=time.monotonic()+45
        while time.monotonic()<deadline:
            try:
                if self.value('SELECT 1')=='1':break
            except RuntimeError:pass
            time.sleep(.2)
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('catalog and object-store recovery publishes each committed row once',self.dest(
            "SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),'2:2')

if __name__=='__main__':sys.exit(OutageProbe().run())

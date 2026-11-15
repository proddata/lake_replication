#!/usr/bin/env python3
"""External text primary keys retain historical identities through mutations."""
import json
import sys
from live import LiveProbe

class ToastKeyProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='external text primary-key identity across unchanged, changed and deleted rows',
            unqualified=['replica identity full','non-primary identity indexes'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id text PRIMARY KEY,note text,active boolean);
            CREATE TABLE b(id bigint PRIMARY KEY,note text);
            ALTER TABLE a ALTER COLUMN id SET STORAGE EXTERNAL;
            INSERT INTO a VALUES(repeat(md5('original-key'),64),'initial',true);
            INSERT INTO b VALUES(1,'paired');''')
        toast=self.value("SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid='a'::regclass")
        self.check('text primary key is stored in the source TOAST relation',int(self.value('SELECT count(*) FROM '+toast))>0,True)
        self.stream=self.value("SELECT lake_capture.register_stream('a','b',NULL,'online_copy')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait("SELECT snapshot_state FROM lake_capture.streams",'complete')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination();self.importer();self.command(self.worker_args('lake-replication-publisher'))
        for sequence,statement,label in [(2,"UPDATE a SET note='nonkey-update',active=false",'unchanged external primary key'),
                (3,"UPDATE a SET id=repeat(md5('replacement-key'),64),note='key-change'",'changed external primary key'),
                (4,'DELETE FROM a','deleted external primary key')]:
            self.sql(statement);self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(sequence))
            self.importer();self.command(self.worker_args('lake-replication-publisher'))
            self.check(label+' preserves exact mirror identity/data',self.dest("SELECT coalesce(jsonb_agg(jsonb_build_array(md5(id),note,active)),'[]') FROM mirror.a"),self.value("SELECT coalesce(jsonb_agg(jsonb_build_array(md5(id),note,active)),'[]') FROM a"))
        self.importer(LAKE_PUSH_REPLAY_FROM='4')
        self.check('external-key deletion replay cannot duplicate or resurrect rows',self.dest('SELECT count(*) FROM mirror.a'),'0')

if __name__=='__main__':sys.exit(ToastKeyProbe().run())

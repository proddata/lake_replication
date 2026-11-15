#!/usr/bin/env python3
"""Native transport and log ordering beyond nine chunks and event ordinals."""
import json
import subprocess
import sys
from live import LiveProbe

class ChunkProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='native multi-digit transport chunk and event ordinal ordering',
            unqualified=['oversized transaction support','production throughput'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        child=subprocess.Popen(self.capture_args(LAKE_CAPTURE_TEST_CHUNK_BYTES='4096'),text=True,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
            self.identity=json.loads(self.value('''SELECT jsonb_build_object(
                'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
                'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
                'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
            self.bind_destination()
            self.sql("BEGIN;INSERT INTO a SELECT i,repeat(md5(i::text),256) FROM generate_series(1,22) i;INSERT INTO b VALUES(23,'paired');COMMIT")
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1')
            self.check('one native transaction survives more than ten transport chunks',self.value('SELECT count(*) FROM lake_capture_log.events_a'),'22')
            self.importer();self.command(self.worker_args('lake-replication-publisher'))
            for table in 'ab':
                self.check('numeric event ordering preserves every row '+table,self.dest(
                    f'SELECT jsonb_agg(jsonb_build_array(id,md5(note)) ORDER BY id) FROM mirror.{table}'),self.value(
                    f'SELECT jsonb_agg(jsonb_build_array(id,md5(note)) ORDER BY id) FROM {table}'))
            self.importer(LAKE_PUSH_REPLAY_FROM='1')
            self.check('chunk transport does not change batch replay identity',self.progress(),'1:1:1')
        finally:
            self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'],required=False)
            child.communicate(timeout=15)

if __name__=='__main__':sys.exit(ChunkProbe().run())

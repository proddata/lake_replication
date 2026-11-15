#!/usr/bin/env python3
"""Scalar/compound-key/identifier fidelity across paused copy and native WAL."""
import json
import sys
from live import LiveProbe


class ScalarProbe(LiveProbe):
    def exercise(self):
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint,part integer,active boolean,note text,
                PRIMARY KEY(id,part) INCLUDE(note));
            CREATE TABLE b("key's name" text PRIMARY KEY,note text);
            INSERT INTO a VALUES(-9223372036854775808,-2147483648,true,E'世界\\nquote''\\\\');
            INSERT INTO b VALUES('O''Reilly',NULL);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b',NULL,'paused_copy')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait("SELECT snapshot_state FROM lake_capture.streams",'complete')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        self.importer()
        self.command(self.worker_args('lake-replication-publisher'))
        self.compare('paused baseline')
        self.sql('''BEGIN;INSERT INTO a VALUES(9223372036854775807,2147483647,false,NULL);
            INSERT INTO b VALUES(E'世界\\nkey',E'\\\\N\\tO''Reilly');COMMIT;''')
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2')
        self.importer()
        self.command(self.worker_args('lake-replication-publisher'))
        self.compare('native INSERT')
        self.check('INCLUDE attributes are not mistaken for primary-key columns',self.value('''SELECT
            (columns->3->>'flags')::text FROM lake_capture_log.registrations WHERE source_relation='a' '''),'0')

    def compare(self,label):
        for name,order in [('a','id,part'),('b','"key\'s name"')]:
            expected=self.value(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY {order}) FROM public.{name} t')
            self.check(label+' preserves '+name+' scalar/key/identifier values',self.dest(
                f'SELECT jsonb_agg(to_jsonb(t) ORDER BY {order}) FROM mirror.{name} t'),expected)


if __name__=='__main__':sys.exit(ScalarProbe().run())

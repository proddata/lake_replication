#!/usr/bin/env python3
"""An empty online baseline can activate before ordered DDL-only commits."""
import json
import sys
from schema_live import SchemaLiveProbe


class EmptySchemaSnapshotProbe(SchemaLiveProbe):
    def exercise(self):
        self.evidence.update(scope='empty online source baseline followed by ordered DDL-only and row commits',
            unqualified=['unbounded snapshots','general catalogs'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b',NULL,'online_copy')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait("SELECT snapshot_state FROM lake_capture.streams",'complete')
        self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*)::text FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.enable_schema_auditing('{self.stream}')")
        self.identity=json.loads(self.value('''SELECT json_build_object('system_identifier',origin_system_identifier::text,
            'database_oid',origin_database_oid::text,'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),
            'table_a',table_a_id,'table_b',table_b_id) FROM lake_capture.streams'''))
        self.destination('CREATE EXTENSION lake_replication_push');self.overlay_installed=True;self.bind_destination()
        definitions=self.value("SELECT json_agg(json_build_object('table_id',table_id,'definition',definition_json)) FROM lake_capture.schema_definitions")
        self.dest(f"SELECT lake_replication.initialize_push_schema_history({self.subscription},'{definitions}'::jsonb)")
        self.importer(LAKE_PUSH_MAX_BATCHES='64')
        self.check('empty snapshot admits one ordered transaction with no row changes',self.dest('''SELECT
            (SELECT count(*) FROM lake_replication.transactions)::text||':'||(SELECT count(*) FROM lake_replication.changes)||':'||
            (SELECT jsonb_array_length(payload) FROM lake_replication.transactions)'''),'1:0:0')
        self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('empty ordered baseline advances its publication boundary',self.dest(f'''SELECT
            published_sequence::text||':'||(SELECT count(*) FROM mirror.a)||':'||(SELECT count(*) FROM mirror.b)
            FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'1:0:0')
        self.sql('BEGIN;ALTER TABLE a ADD COLUMN extra boolean;ALTER TABLE b RENAME COLUMN note TO renamed;COMMIT')
        self.capture_until(2);self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('DDL-only commit advances both empty mirror schemas atomically',self.dest(f'''SELECT
            published_sequence::text||':'||(SELECT count(*) FROM pg_attribute WHERE attrelid='mirror.a'::regclass AND attname='extra')||':'||
            (SELECT count(*) FROM pg_attribute WHERE attrelid='mirror.b'::regclass AND attname='renamed')
            FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'2:1:1')
        self.sql("BEGIN;INSERT INTO a VALUES(1,'after empty baseline',true);INSERT INTO b VALUES(2,'after ddl');COMMIT")
        self.capture_until(3);self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('later rows use the schemas published after the empty baseline',self.dest("SELECT (SELECT count(*) FROM mirror.a WHERE extra)::text||':'||(SELECT count(*) FROM mirror.b WHERE renamed='after ddl')"),'1:1')


if __name__=='__main__':sys.exit(EmptySchemaSnapshotProbe().run())

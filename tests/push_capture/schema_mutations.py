#!/usr/bin/env python3
"""Aborted DDL subtransactions and mutations after primary-key column rename."""
import sys
from schema_live import SchemaLiveProbe


class SchemaMutationProbe(SchemaLiveProbe):
    def exercise(self):
        super().exercise()
        self.evidence.update(scope='native schema subtransaction aborts and key-column rename mutation ordering',
            unqualified=['production sizing','general catalogs'])
        self.sql("BEGIN;SAVEPOINT before_ddl;ALTER TABLE a RENAME COLUMN label TO aborted_name;"
            "INSERT INTO a VALUES(90,'aborted',false,NULL);ROLLBACK TO before_ddl;"
            "UPDATE a SET label='after subabort' WHERE id=1;ALTER TABLE b RENAME COLUMN qty TO quantity;COMMIT")
        self.capture_until(5);self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('aborted DDL cannot leak a definition or advance the surviving table version',self.value('''SELECT
            (SELECT max(schema_version) FROM lake_capture.schema_definitions WHERE table_id=s.table_a_id)::text||':'||
            (SELECT max(schema_version) FROM lake_capture.schema_definitions WHERE table_id=s.table_b_id)
            FROM lake_capture.streams s'''),'4:3')
        self.check('subtransaction rollback removes its data while retaining later valid changes',self.dest("SELECT count(*) FILTER(WHERE id=90)::text||':'||count(*) FILTER(WHERE id=1 AND label='after subabort')::text FROM mirror.a"),'0:1')
        self.sql("BEGIN;ALTER TABLE a RENAME COLUMN id TO key_id;UPDATE a SET key_id=10 WHERE key_id=1;"
            "DELETE FROM a WHERE key_id=2;INSERT INTO a VALUES(2,'reinsert after key rename',true,'same stable column ids');COMMIT")
        self.capture_until(6);self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('key-column rename preserves exact update/delete/reinsert semantics',self.dest(
            'SELECT jsonb_agg(to_jsonb(t) ORDER BY key_id) FROM mirror.a t'),self.value('SELECT jsonb_agg(to_jsonb(t) ORDER BY key_id) FROM a t'))
        self.check('renamed key retains its stable identity and NOT NULL metadata',self.dest("SELECT attnum::text||':'||attnotnull::text FROM pg_attribute WHERE attrelid='mirror.a'::regclass AND attname='key_id'"),'1:true')
        self.importer(LAKE_PUSH_REPLAY_FROM='4',LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('historical replay uses retained old key names without reapplying DDL',self.dest('SELECT jsonb_agg(to_jsonb(t) ORDER BY key_id) FROM mirror.a t'),self.value('SELECT jsonb_agg(to_jsonb(t) ORDER BY key_id) FROM a t'))
        self.sql("""BEGIN;ALTER TABLE a RENAME COLUMN label TO "label""雪";INSERT INTO a VALUES(99,'quoted identifier',true,'UTF8');COMMIT""")
        self.capture_until(7);self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('quoted UTF8 rename preserves identifier and row values end to end',self.dest(
            'SELECT jsonb_agg(to_jsonb(t) ORDER BY key_id) FROM mirror.a t'),self.value('SELECT jsonb_agg(to_jsonb(t) ORDER BY key_id) FROM a t'))



if __name__=='__main__':sys.exit(SchemaMutationProbe().run())

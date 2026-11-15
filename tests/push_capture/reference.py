#!/usr/bin/env python3
"""Independent pgoutput comparison for interleaved mutable source transactions."""
import json
import sys
from live import LiveProbe
from session import Session


class ReferenceProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='native capture versus independent pgoutput with interleaving and mutations',
            unqualified=['ordered DDL','production throughput','all PostgreSQL data types'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            ALTER TABLE a ALTER COLUMN note SET STORAGE EXTERNAL;
            CREATE PUBLICATION reference_pub FOR TABLE a,b;
            SELECT pg_create_logical_replication_slot('reference_slot','pgoutput');''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql('UPDATE lake_capture.streams SET decode_work_mem_kb=64,max_decode_spill_bytes=8388608')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        columns=self.bind_destination()
        reference=int(self.dest(f'''SELECT lake_replication.create_subscription(
            {self.identity['system_identifier']},{self.identity['database_oid']},{self.identity['timeline']},
            'reference_pull','reference_slot',ARRAY['reference_pub'])'''))
        self.dest('CREATE SCHEMA reference')
        for name in 'ab':
            oid=self.value(f"SELECT '{name}'::regclass::oid")
            version=int(self.dest(f"SELECT lake_replication.register_relation({reference},{oid},'public','{name}','d','{columns}'::jsonb)"))
            self.dest(f'''CREATE TABLE reference.{name}(id bigint,note text) USING iceberg;
                SELECT lake_replication.assign_destination({reference},{oid},{version},'reference.{name}'::regclass);''')
        first=Session(self.sql_args());second=Session(self.sql_args())
        try:
            first.query("BEGIN;INSERT INTO a SELECT i,repeat(md5(i::text),64) FROM generate_series(1,500)i")
            second.query("BEGIN;INSERT INTO b VALUES(1,'commits-first');SAVEPOINT undone;INSERT INTO b VALUES(999,'subabort');ROLLBACK TO undone;COMMIT")
            first.query('COMMIT')
            self.sql("BEGIN;UPDATE a SET id=1001 WHERE id=1;UPDATE a SET note=NULL WHERE id=2;DELETE FROM a WHERE id=3;INSERT INTO a VALUES(3,'reinsert');UPDATE b SET id=2;COMMIT")
            first.query("BEGIN;INSERT INTO a VALUES(9999,'aborted-interleaved')")
            second.query("BEGIN;INSERT INTO b VALUES(3,'survives-abort');COMMIT")
            first.query('ROLLBACK')
        finally:first.close();second.close()
        # Capture is deliberately behind when incompatible source DDL is tried.
        denied=self.sql('ALTER TABLE a RENAME COLUMN note TO unsafe',required=False)
        self.check('frozen-schema guard rejects DDL while decoding is behind',denied.returncode!=0 and 'schema is frozen' in denied.stderr,True)
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','4')
        self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-publisher'))
        self.command(self.worker_args('lake-replication-receiver',LAKE_SUBSCRIPTION_ID=str(reference),
            LAKE_SLOT='reference_slot',LAKE_PUBLICATION='reference_pub',LAKE_MAX_TRANSACTIONS='4'))
        self.command(self.worker_args('lake-replication-publisher',LAKE_SUBSCRIPTION_ID=str(reference)))
        self.compare('interleaved commit/abort and mutable historical TOAST')
        self.check('manifest order follows commit rather than transaction start',self.value(
            'SELECT (SELECT count(*) FROM lake_capture_log.events_a WHERE sequence=1)::text||\':\'||(SELECT count(*) FROM lake_capture_log.events_b WHERE sequence=1)'),'0:1')
        self.sql("BEGIN;UPDATE a SET note='discarded';TRUNCATE a,b;INSERT INTO a VALUES(7,'after-barrier');INSERT INTO b VALUES(8,'paired');COMMIT")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','5')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.command(self.worker_args('lake-replication-receiver',LAKE_SUBSCRIPTION_ID=str(reference),
            LAKE_SLOT='reference_slot',LAKE_PUBLICATION='reference_pub',LAKE_MAX_TRANSACTIONS='1'))
        self.command(self.worker_args('lake-replication-publisher',LAKE_SUBSCRIPTION_ID=str(reference)))
        self.compare('multi-table TRUNCATE and same-transaction replacement')
        self.importer(LAKE_PUSH_REPLAY_FROM='1',LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-publisher'))
        self.compare('historical replay after later destructive transactions')

    def compare(self,label):
        for name in 'ab':
            query="SELECT count(*)::text||':'||md5(coalesce(jsonb_agg(jsonb_build_array(id,md5(note)) ORDER BY id)::text,'[]')) FROM "
            actual=self.dest(query+'mirror.'+name)
            self.check(label+' agrees with pgoutput '+name,actual,self.dest(query+'reference.'+name))
            self.check(label+' agrees with source '+name,actual,self.value(query+name))


if __name__=='__main__':sys.exit(ReferenceProbe().run())

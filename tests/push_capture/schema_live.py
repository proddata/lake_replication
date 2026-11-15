#!/usr/bin/env python3
"""Real source DDL audit -> native logs -> ordered importer/publisher -> ACK."""
import json
import subprocess
import sys
from live import LiveProbe


class SchemaLiveProbe(LiveProbe):
    def capture_until(self,sequence):
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        child=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(sequence),timeout=60)
            self.source_wait('''SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text FROM lake_capture.streams s
                JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true')
        finally:
            self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
            stdout,stderr=child.communicate(timeout=20)
            self.evidence.setdefault('capture_runs',[]).append(dict(returncode=child.returncode,stdout=stdout,stderr=stderr))

    def exercise(self):
        self.evidence.update(scope='native source schema changes through ordered journal/publication and source retention ACK',
            unqualified=['managed schema activation','publication backend schema crash boundaries','public schema activation'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        initial=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        finally:
            self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')");initial.communicate(timeout=20)
        self.sql(f"SELECT lake_capture.enable_schema_auditing('{self.stream}');UPDATE lake_capture.streams SET transactions_per_read=8")
        self.identity=json.loads(self.value('''SELECT json_build_object('system_identifier',origin_system_identifier::text,
            'database_oid',origin_database_oid::text,'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),
            'table_a',table_a_id,'table_b',table_b_id) FROM lake_capture.streams'''))
        self.destination('CREATE EXTENSION lake_replication_push');self.overlay_installed=True;self.bind_destination()
        definitions=self.value("SELECT json_agg(json_build_object('table_id',table_id,'definition',definition_json)) FROM lake_capture.schema_definitions")
        self.dest(f"SELECT lake_replication.initialize_push_schema_history({self.subscription},'{definitions.replace(chr(39),chr(39)*2)}'::jsonb)")
        consumer=self.value(f"SELECT lake_capture.register_consumer('{self.stream}',true)")
        self.sql("BEGIN;INSERT INTO a VALUES(1,'old');ALTER TABLE a ADD COLUMN extra boolean;"
            "INSERT INTO a VALUES(2,'new',true);ALTER TABLE a RENAME COLUMN note TO label;"
            "UPDATE a SET label='renamed' WHERE id=1;INSERT INTO b VALUES(7,'other');COMMIT")
        self.sql('ALTER TABLE b ADD COLUMN qty integer')
        # Requests require a running source binding; the actor is deliberately not started yet.
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.sql(f"BEGIN;INSERT INTO a VALUES(3,'after ddl',false);SELECT lake_capture.request_progress('{self.stream}');UPDATE b SET qty=42 WHERE id=7;COMMIT")
        self.capture_until(3)
        self.check('native schema and progress commits retain three independent source receipts',self.value('''SELECT
            count(*)::text||':'||max(table_a_version)::text||':'||max(table_b_version)::text FROM lake_capture.receipts'''),'3:3:2')
        self.importer(LAKE_PUSH_MAX_BATCHES='64')
        self.check('native importer admits every ordered source commit without publishing rows',self.dest(f'''SELECT
            imported_sequence::text||':'||published_sequence::text||':'||(SELECT count(*) FROM mirror.a)
            FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'3:0:0')
        self.check('imported DDL cannot mutate the visible destination schema early',self.dest("SELECT count(*) FROM pg_attribute WHERE attrelid='mirror.a'::regclass AND attnum>0 AND NOT attisdropped"),'2')
        ordinary=self.command(self.worker_args('lake-replication-publisher'),required=False)
        self.check('ordinary publisher cannot publish the ordered native journal',ordinary.returncode!=0,True)
        self.command(self.worker_args('lake-replication-push-publisher'))
        for table in ['a','b']:
            self.check('ordered native publication reproduces source '+table,self.dest(
                f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM mirror.{table} t'),self.value(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {table} t'))
        self.check('schema, data and receipt frontier become published together',self.dest(f'''SELECT
            published_sequence::text||':'||(SELECT published_version FROM lake_replication.push_schema_tables WHERE table_id='{self.identity['table_a']}')||':'||
            (SELECT published_version FROM lake_replication.push_schema_tables WHERE table_id='{self.identity['table_b']}')
            FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'3:3:2')
        self.importer(LAKE_PUSH_REPLAY_FROM='1',LAKE_PUSH_MAX_BATCHES='64')
        self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('native replay preserves complete schema history and original journal cardinality',self.dest('''SELECT
            (SELECT count(*) FROM lake_replication.push_schema_versions)::text||':'||(SELECT count(*) FROM lake_replication.transactions)||':'||
            (SELECT count(*) FROM mirror.a)||':'||(SELECT count(*) FROM mirror.b)'''),'5:3:3:1')
        receipt=json.loads(self.dest(f"SELECT json_build_object('end',end_lsn,'digest',digest) FROM lake_replication.push_receipts WHERE subscription_id={self.subscription} AND sequence=3"))
        self.sql(f"SELECT lake_capture.acknowledge_publication('{self.stream}','{consumer}',1,3,'{receipt['end']}','{receipt['digest']}')")
        self.check('published ordered receipt authorizes source log pruning',self.value(f"SELECT lake_capture.prune_logs('{self.stream}')"),'3')
        self.check('pruning retains immutable definitions and schema audit history',self.value('''SELECT
            (SELECT count(*) FROM lake_capture.schema_definitions)::text||':'||(SELECT count(*) FROM lake_capture.schema_events)'''),'5:3')
        self.sql("BEGIN;ALTER TABLE a ADD COLUMN appended text;INSERT INTO a VALUES(4,'after prune',true,'new column');COMMIT")
        self.capture_until(4)
        self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-push-publisher'))
        self.check('capture and import resume from retained schema receipts after pruning',self.dest('SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM mirror.a t'),
            self.value('SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM a t'))
        self.check('post-prune schema evolution advances the same coherent source generation',self.dest(f'''SELECT
            capture_generation::text||':'||published_sequence::text FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'1:4')


if __name__=='__main__':sys.exit(SchemaLiveProbe().run())

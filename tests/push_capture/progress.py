#!/usr/bin/env python3
"""Owner-authorized complete-commit progress barriers without selected changes."""
import json
import sys
import time
from live import LiveProbe, ROOT
from session import Session


class ProgressProbe(LiveProbe):
    def control(self,statement,required=True,role='capture_operator'):
        args=self.sql_args();args[args.index('postgres')]=role
        return self.command(args,input=statement,required=required)

    def progress_before_older_transaction_commits(self):
        writer=Session(self.sql_args())
        before=self.value('SELECT pg_current_wal_insert_lsn()::text')
        try:
            writer.query("BEGIN;INSERT INTO a VALUES(10,'started before progress')")
            request=self.control(f"SELECT lake_capture.request_progress('{self.stream}')").stdout.strip()
            self.check('non-superuser request receives its durable captured sequence',self.source_wait(
                f"SELECT captured_sequence::text FROM lake_capture.progress_requests WHERE request_id='{request}'",'1'),True)
            self.check('progress manifest has a control and no selected row events',self.value('''SELECT
                (SELECT count(*) FROM lake_capture_log.manifests)::text||':'||
                (SELECT count(*) FROM lake_capture_log.controls)||':'||
                ((SELECT count(*) FROM lake_capture_log.events_a)+(SELECT count(*) FROM lake_capture_log.events_b))'''),'1:1:0')
            self.check('request completion and source receipt share one durable boundary',self.value('''SELECT
                (p.captured_end_lsn=r.end_lsn AND p.captured_sequence=s.captured_sequence
                    AND p.captured_end_lsn=s.captured_end_lsn)::text
                FROM lake_capture.progress_requests p JOIN lake_capture.receipts r USING(stream_id)
                JOIN lake_capture.streams s USING(stream_id)'''),'true')
            self.check('progress confirms a complete boundary while retaining older unfinished WAL',self.source_wait(f'''
                SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn AND r.restart_lsn<='{before}'::pg_lsn)::text
                FROM lake_capture.streams s JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true'),True)
            writer.query('COMMIT')
        finally:writer.close()
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2')

    def exercise(self):
        self.evidence.update(scope='bounded explicit complete-commit source progress barriers',
            unqualified=['automatic idle heartbeats','unbounded generation lifetime','ordered schema evolution'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            CREATE TABLE unselected(id bigint PRIMARY KEY);
            CREATE ROLE capture_operator LOGIN REPLICATION;CREATE ROLE untrusted_login LOGIN;''')
        self.command(self.sql_args()+['-v','capture_login=capture_operator'],input=(ROOT/'deploy/push/source-grants.sql').read_text())
        self.sql('GRANT SELECT,MAINTAIN ON a,b TO capture_operator')
        self.stream=self.control("SELECT lake_capture.register_stream('a','b')").stdout.strip()
        self.sql('UPDATE lake_capture.streams SET max_progress_requests=2')
        self.control(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        consumer=self.control(f"SELECT lake_capture.register_consumer('{self.stream}')").stdout.strip()
        denied=self.control(f"SELECT lake_capture.request_progress('{self.stream}')",False,'untrusted_login')
        self.check('ordinary source login cannot request capture progress',denied.returncode!=0,True)
        self.sql('INSERT INTO unselected VALUES(1)')
        self.progress_before_older_transaction_commits()
        self.importer()
        self.check('control-only import stages an empty durable journal transaction',self.dest(f'''
            SELECT jsonb_array_length(payload)::text||':'||(SELECT imported_sequence
                FROM lake_replication.push_bindings WHERE subscription_id={self.subscription})
            FROM lake_replication.transactions WHERE subscription_id={self.subscription}'''),'0:1')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('unchanged publisher advances the control-only publication boundary',self.dest(f'''
            SELECT published_sequence::text||':'||(SELECT count(*) FROM mirror.a)||':'||(SELECT count(*) FROM mirror.b)
            FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'1:0:0')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('older-started transaction publishes only after its later commit',self.dest(f'''
            SELECT published_sequence::text||':'||(SELECT count(*) FROM mirror.a)
            FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'2:1')
        self.control(f"BEGIN;SELECT lake_capture.request_progress('{self.stream}');ROLLBACK")
        self.check('aborted request leaves no row or consumed request budget',self.value('''SELECT
            progress_request_count::text||':'||(SELECT count(*) FROM lake_capture.progress_requests)
            FROM lake_capture.streams'''),'1:1')
        self.sql(f"BEGIN;INSERT INTO a VALUES(1,'before');SET ROLE capture_operator;SELECT lake_capture.request_progress('{self.stream}');RESET ROLE;INSERT INTO b VALUES(2,'after');COMMIT")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','3')
        self.check('mixed source transaction preserves the request between row ordinals',self.value(
            'SELECT ordinal FROM lake_capture_log.controls WHERE sequence=3'),'1')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.importer(LAKE_PUSH_REPLAY_FROM='1',LAKE_PUSH_MAX_BATCHES='64')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('progress and mixed-transaction replay publish every row once',self.dest(f'''
            SELECT published_sequence::text||':'||(SELECT count(*) FROM mirror.a)||':'||(SELECT count(*) FROM mirror.b)
            FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'3:2:1')
        exhausted=self.control(f"SELECT lake_capture.request_progress('{self.stream}')",False)
        self.check('request budget fails before creating another source marker',
            exhausted.returncode!=0 and 'progress request budget exhausted' in exhausted.stderr
            and self.value('SELECT progress_request_count::text||\':\'||upload_attempt_count FROM lake_capture.streams')=='2:3',True)
        self.source_wait('''SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text FROM lake_capture.streams s
            JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true')
        time.sleep(.5)
        self.check('capture receipts and progress completion cannot generate more progress',self.value(
            'SELECT captured_sequence FROM lake_capture.streams'),'3')
        receipt=json.loads(self.dest(f"SELECT jsonb_build_object('end',end_lsn,'digest',digest) FROM lake_replication.push_receipts WHERE subscription_id={self.subscription} AND sequence=3"))
        self.control(f"SELECT lake_capture.acknowledge_publication('{self.stream}','{consumer}',1,3,'{receipt['end']}','{receipt['digest']}')")
        self.check('actual publication acknowledgement permits control-log pruning',self.control(
            f"SELECT lake_capture.prune_logs('{self.stream}')").stdout.strip(),'3')
        self.check('request completion receipts remain after logical log pruning',self.value(
            'SELECT count(*) FROM lake_capture.progress_requests WHERE captured_sequence IS NOT NULL'),'2')
        self.control(f"SELECT lake_capture.stop_stream('{self.stream}')")
        denied=self.control(f"SELECT lake_capture.request_progress('{self.stream}')",False)
        self.check('stopped source cannot enqueue progress',denied.returncode!=0,True)
        self.control(f"SELECT lake_capture.reset_stream('{self.stream}')")
        self.check('controlled reset clears request rows and their generation budget',self.source_wait('''SELECT
            (capture_generation=2 AND progress_request_count=0 AND NOT EXISTS(SELECT FROM lake_capture.progress_requests))::text
            FROM lake_capture.streams''','true'),True)


if __name__=='__main__':sys.exit(ProgressProbe().run())

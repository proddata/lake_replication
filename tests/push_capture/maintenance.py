#!/usr/bin/env python3
"""Reader-gated compaction, snapshot expiry and delayed physical deletion."""
import json
import sys
import subprocess
from retention import RetentionProbe
from session import Session

class MaintenanceProbe(RetentionProbe):
    def exercise(self):
        self.evidence.update(scope='reader-gated private source-log maintenance and finite maintenance attempts',
            unqualified=['uncoordinated external readers','physical byte quota','production benchmark'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.sql("BEGIN;INSERT INTO a VALUES(1,'first');INSERT INTO b VALUES(1,'first');COMMIT;")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination();self.importer();self.command(self.worker_args('lake-replication-publisher'))
        consumer=self.value(f"SELECT lake_capture.register_consumer('{self.stream}')")
        self.acknowledge(consumer)
        reader=Session(self.sql_args())
        try:
            reader.query('SELECT pg_advisory_lock_shared(1280593921,1);BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;SELECT count(*) FROM lake_capture.receipts')
            self.sql("BEGIN;INSERT INTO a VALUES(2,'second');INSERT INTO b VALUES(2,'second');COMMIT;")
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2')
            self.source_wait('''SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text FROM lake_capture.streams s
                JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true')
            self.command(self.compose+['exec','-T','source','psql','-XqAt','-U','postgres','-d','capture','-c',
                "SELECT extension_base.deregister_worker('lake_replication capture')"])
            self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
            self.check('logical prune remains publication-bounded while a reader is pinned',self.prune(),'1')
            self.command(self.capture_args(LAKE_CAPTURE_MAINTENANCE_ONLY='1'))
            self.check('maintenance waits outside pinned source read views without consuming attempts',self.value(
                'SELECT maintenance_attempt_count::text FROM lake_capture.streams'),'0')
            self.check('a pinned reader opening Iceberg logs after pruning still sees its complete old group',
                reader.query("SELECT (SELECT count(*) FROM lake_capture_log.events_a)::text||':'||(SELECT count(*) FROM lake_capture_log.events_b)||':'||(SELECT count(*) FROM lake_capture_log.manifests)"),['1:1:1'])
            reader.query('COMMIT;SELECT pg_advisory_unlock_shared(1280593921,1)')
        finally:reader.close()
        self.sql('UPDATE lake_capture.streams SET max_maintenance_attempts=2')
        queued=self.value("SELECT count(*) FROM lake_engine.deletion_queue WHERE table_name IN (SELECT oid FROM pg_class WHERE relnamespace='lake_capture_log'::regnamespace)")
        self.check('retired snapshots queue physical cleanup separately from logical pruning',int(queued)>0,True)
        witness=self.value("SELECT path FROM lake_engine.deletion_queue WHERE table_name IN (SELECT oid FROM pg_class WHERE relnamespace='lake_capture_log'::regnamespace) LIMIT 1")
        if not witness.startswith('s3://lake-push/'):raise RuntimeError('witness outside private fixture')
        def exists():
            result=self.command(self.compose+['exec','-T','storage','test','-f','/data/'+witness.removeprefix('s3://')+'/xl.meta'],required=False)
            if result.returncode not in (0,1):raise RuntimeError('could not inspect retention witness')
            return result.returncode==0
        self.evidence['physical_cleanup_witness']=witness
        self.check('retired physical object exists before delayed maintenance',exists(),True)
        self.command(self.capture_args(LAKE_CAPTURE_MAINTENANCE_ONLY='1'))
        self.check('maintenance commits one durable attempt before compaction',self.value('SELECT maintenance_attempt_count::text FROM lake_capture.streams'),'1')
        self.check('default ten-day retention keeps the retired physical object',exists(),True)
        self.sql("ALTER ROLE postgres SET pg_lake_engine.orphaned_file_retention_period='0s'")
        self.command(self.capture_args(LAKE_CAPTURE_MAINTENANCE_ONLY='1'))
        self.check('expired retention removes the physical object independently of queue bookkeeping',exists(),False)
        self.check('eligible private log deletion queue drains after the retention delay',self.value("SELECT count(*) FROM lake_engine.deletion_queue WHERE table_name IN (SELECT oid FROM pg_class WHERE relnamespace='lake_capture_log'::regnamespace)"),'0')
        self.check('maintenance preserves all unpruned live log rows',self.value("SELECT (SELECT count(*) FROM lake_capture_log.events_a)::text||':'||(SELECT count(*) FROM lake_capture_log.events_b)||':'||(SELECT count(*) FROM lake_capture_log.manifests)"),'1:1:1')
        self.command(self.capture_args(LAKE_CAPTURE_MAINTENANCE_ONLY='1'))
        self.check('generation maintenance quota fences repeated object work',self.value('SELECT maintenance_attempt_count::text FROM lake_capture.streams'),'2')
        gate=Session(self.sql_args())
        importer=None
        try:
            gate.query('SELECT pg_advisory_lock(1280593921,1)')
            importer=subprocess.Popen(self.worker_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            self.check('importer waits for maintenance before pinning its read snapshot',self.source_wait(
                "SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%pg_advisory_lock_shared%' AND wait_event_type='Lock'",'1'),True)
            self.check('maintenance gate leaves imported frontier unchanged',self.progress(),'1:1:1')
            gate.query('SELECT pg_advisory_unlock(1280593921,1)')
            _,error=importer.communicate(timeout=20)
            if importer.returncode:raise RuntimeError(error)
        finally:
            gate.close()
            if importer is not None and importer.poll() is None:importer.kill();importer.communicate(timeout=10)
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('import and publication remain exact after physical maintenance',self.dest("SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),'2:2')

if __name__=='__main__':sys.exit(MaintenanceProbe().run())

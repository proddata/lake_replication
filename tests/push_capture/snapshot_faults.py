#!/usr/bin/env python3
"""Native upload/COMMIT crashes around paused and online snapshot activation."""
from datetime import datetime, timezone
import json
import subprocess
import sys
from native_faults import NativeFaultProbe


class SnapshotFaultProbe(NativeFaultProbe):
    def __init__(self,mode):
        super().__init__();self.mode=mode

    def exercise(self):
        self.evidence.update(scope=self.mode+' snapshot activation upload/COMMIT crash recovery',
            unqualified=['unbounded or independently resumable snapshot chunks','ordered DDL'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            INSERT INTO a VALUES(1,'baseline'),(2,NULL);INSERT INTO b VALUES(3,'paired');''')
        self.stream=self.value(f"SELECT lake_capture.register_stream('a','b',NULL,'{self.mode}')")
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination('incomplete')
        old_subscription=self.subscription
        for generation,point in enumerate(['before_upload_completion','after_upload_completion','after_commit'],1):
            started=datetime.now(timezone.utc).isoformat()
            failed=self.command(self.capture_args(LAKE_CAPTURE_TEST_CRASH=point),required=False)
            self.check(point+': snapshot driver loses response',failed.returncode!=0,True)
            self.ready()
            logs=self.command(self.compose+['logs','--no-color','--since',started,'source'])
            diagnostic=logs.stdout+logs.stderr
            self.check(point+': actual snapshot backend crash recovered','was terminated by signal 9' in diagnostic,True)
            (self.output/(point+'.log')).write_text(diagnostic)
            committed=point=='after_commit'
            self.check(point+': snapshot and manifest activation are atomic',self.value(
                "SELECT snapshot_state||':'||captured_sequence||':'||(SELECT count(*) FROM lake_capture_log.manifests)||':'||(SELECT count(*) FROM lake_capture_log.controls) FROM lake_capture.streams"),
                'complete:1:1:4' if committed else 'building:0:0:0')
            if committed:break
            retry=self.command(self.capture_args(),required=False)
            self.check(point+': incomplete snapshot requires controlled recopy',retry.returncode!=0 and 'controlled reset and recopy' in retry.stderr,True)
            self.importer()
            self.check(point+': no incomplete snapshot reaches destination journal',self.progress(),'0:0:0')
            self.sql('SELECT lake_capture.start_manager()')
            self.sql(f"SELECT lake_capture.reset_stream('{self.stream}')")
            self.source_wait(f"SELECT (capture_generation={generation+1} AND desired_state='stopped' AND snapshot_state='none')::text FROM lake_capture.streams",'true')
            self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
            self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
            # Rebind the still-empty test consumer explicitly for the next
            # incomplete generation; production consumers require a new mirror.
            if generation==1:
                self.dest(f'UPDATE lake_replication.push_bindings SET capture_generation=2 WHERE subscription_id={self.subscription}')
        stale=self.importer(required=False)
        self.check('old incomplete generation cannot adopt the committed baseline',stale.returncode!=0 and 'generation mismatch' in stale.stderr,True)
        self.bind_destination('complete',capture_generation=3)
        self.sql("BEGIN;UPDATE a SET note='after-boundary' WHERE id=1;DELETE FROM a WHERE id=2;INSERT INTO b VALUES(4,'new');COMMIT")
        resumed=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2')
        finally:
            self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'],required=False)
            resumed.communicate(timeout=15)
        self.check('lost snapshot commit response never appends another baseline',self.value('SELECT count(*) FROM lake_capture_log.controls'),'4')
        self.importer()
        self.check('committed baseline stays private until publication',self.dest('SELECT count(*) FROM complete.a'),'0')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('first publication activates the historical coherent snapshot',self.dest(
            "SELECT (SELECT count(*) FROM complete.a)::text||':'||(SELECT count(*) FROM complete.b)||':'||(SELECT note FROM complete.a WHERE id=1)"),'2:1:baseline')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('post-snapshot WAL replays exact mutations after activation',self.dest(
            "SELECT (SELECT count(*) FROM complete.a)::text||':'||(SELECT count(*) FROM complete.b)||':'||(SELECT note FROM complete.a WHERE id=1)"),'1:2:after-boundary')
        self.importer(LAKE_PUSH_REPLAY_FROM='1',LAKE_PUSH_MAX_BATCHES='64')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('snapshot replay cannot restore rows deleted by later WAL',self.dest('SELECT count(*) FROM complete.a'),'1')
        self.check('incomplete mirror remains private across all recopy generations',self.dest('SELECT count(*) FROM incomplete.a'),'0')
        self.check('old consumer frontier never advances implicitly',self.dest(f'SELECT imported_sequence FROM lake_replication.push_bindings WHERE subscription_id={old_subscription}'),'0')


if __name__=='__main__':
    for mode in ['paused_copy','online_copy']:
        if SnapshotFaultProbe(mode).run():sys.exit(1)

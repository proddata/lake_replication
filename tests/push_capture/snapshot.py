#!/usr/bin/env python3
"""Bounded paused baseline, write handoff and interrupted-generation rebuild."""
import json
import subprocess
import sys
import time
from live import LiveProbe


class SnapshotProbe(LiveProbe):
    online=False
    def __init__(self):
        super().__init__()
        self.evidence.update(scope='bounded paused source snapshot and private destination generations',
            unqualified=['online snapshot','unbounded/chunked snapshot generations','mutations',
                         'scoped source roles','retention','public activation/API'])

    def stop_child(self,child):
        self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'],required=False)
        child.communicate(timeout=15)

    def stopped_snapshot(self):
        child=subprocess.Popen(self.capture_args(LAKE_CAPTURE_TEST_SNAPSHOT_STOP='before-publication'),
            text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        script="""from pathlib import Path
found=0
for p in Path('/proc').glob('[0-9]*/cmdline'):
 try:
  if p.read_bytes().split(b'\\0')[0].endswith(b'lake-replication-capture'):
   state=(p.parent/'status').read_text().split('State:',1)[1].splitlines()[0]
   found+=int(state.strip().startswith('T'))
 except (FileNotFoundError,PermissionError,IndexError): pass
print(found)
"""
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if child.poll() is not None:
                _,error=child.communicate();raise RuntimeError(error)
            if self.command(self.compose+['exec','-T','source','python3','-c',script]).stdout.strip()=='1':
                return child
            time.sleep(.1)
        self.stop_child(child)
        raise RuntimeError('snapshot did not reach prepublication barrier')

    def read_identity(self):
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))

    def exercise(self):
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text); CREATE TABLE b(id bigint PRIMARY KEY,note text);
            INSERT INTO a VALUES(1,'baseline'),(2,NULL); INSERT INTO b VALUES(3,'baseline');''')
        mode='online_copy' if self.online else 'paused_copy'
        self.stream=self.value(f"SELECT lake_capture.register_stream('a','b',NULL,'{mode}')")
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.read_identity()
        child=self.stopped_snapshot()
        writer=None;private_ddl=None
        try:
            self.check('incomplete baseline exposes no committed manifest',self.value(
                'SELECT count(*) FROM lake_capture_log.manifests'),'0')
            self.bind_destination()
            self.importer()
            self.check('incomplete snapshot cannot advance destination admission',self.progress(),'0:0:0')
            # Fault-only bypass: normal private DDL is rejected before taking
            # relation locks. Exercise a queued exclusive lock deliberately.
            self.sql('ALTER EVENT TRIGGER lake_capture_ddl_start DISABLE')
            private_ddl=subprocess.Popen(self.sql_args(),text=True,stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            private_ddl.stdin.write("SET application_name='snapshot_private_ddl';SET lock_timeout='45s';ALTER FOREIGN TABLE lake_capture_log.events_a OPTIONS (SET max_snapshot_age '0');\n")
            private_ddl.stdin.close();private_ddl.stdin=None
            self.check('private DDL queues behind the snapshot session pin',self.source_wait(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name='snapshot_private_ddl' AND wait_event_type='Lock'",'1'),True)
            self.sql('ALTER EVENT TRIGGER lake_capture_ddl_start ENABLE')
            writer=subprocess.Popen(self.sql_args(),text=True,stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            changes="BEGIN;INSERT INTO a VALUES(4,'after-boundary');INSERT INTO b VALUES(5,'after-boundary');COMMIT;"
            if self.online:
                changes="BEGIN;UPDATE a SET note='after-snapshot' WHERE id=1;DELETE FROM a WHERE id=2;INSERT INTO a VALUES(4,'after-boundary'),(6,'new');UPDATE b SET id=30;INSERT INTO b VALUES(5,'after-boundary');COMMIT;"
            writer.stdin.write("SET application_name='snapshot_writer';"+changes+"\n")
            writer.stdin.close();writer.stdin=None
            if self.online:
                writer.communicate(timeout=10)
                self.check('online baseline permits concurrent committed application mutations',writer.returncode,0)
                self.check('concurrent writes do not activate the incomplete baseline',self.value(
                    'SELECT count(*) FROM lake_capture_log.manifests'),'0')
            else:
                self.check('paused baseline blocks concurrent application writes at its boundary',self.source_wait(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name='snapshot_writer' AND wait_event_type='Lock'",'1'),True)
            self.command(self.compose+['exec','-T','source','pkill','-CONT','-f','(^|/)lake-replication-capture$'])
            writer.communicate(timeout=30)
            self.check('blocked writer commits after snapshot publication',writer.returncode,0)
            self.check('baseline and post-boundary transaction are separate captures',self.source_wait(
                'SELECT captured_sequence::text FROM lake_capture.streams','2'),True)
            self.sql("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name='snapshot_private_ddl'")
            _,ddl_error=private_ddl.communicate(timeout=15)
            self.check('snapshot publishes with queued DDL before releasing new readers',private_ddl.returncode!=0 and '57014' in ddl_error,True)
            self.importer()
            self.check('complete snapshot enters journal as one atomic baseline',self.progress(),'1:1:1')
            self.check('baseline rows remain private until publisher commit',self.dest(
                'SELECT (SELECT count(*) FROM mirror.a)+(SELECT count(*) FROM mirror.b)'),'0')
            self.command(self.worker_args('lake-replication-publisher'))
            self.check('publisher activates both baseline tables coherently',self.dest(
                'SELECT (SELECT count(*) FROM mirror.a)::text||\':\'||(SELECT count(*) FROM mirror.b)'),'2:1')
            self.check('baseline uses historical pre-mutation rows',self.dest("SELECT note FROM mirror.a WHERE id=1"),'baseline')
            self.check('snapshot activation frontier is published',self.dest(f'''SELECT
                (c.published_lsn>=b.snapshot_end_lsn)::text FROM lake_replication.checkpoints c
                JOIN lake_replication.push_bindings b USING(subscription_id) WHERE subscription_id={self.subscription}'''),'true')
            self.importer()
            self.command(self.worker_args('lake-replication-publisher'))
            self.check('post-snapshot handoff has neither missing nor duplicate rows',self.dest(
                'SELECT (SELECT count(*) FROM mirror.a)::text||\':\'||(SELECT count(*) FROM mirror.b)'),'3:2')
            for table in 'ab':
                self.check('snapshot handoff has exact historical mutation result '+table,self.dest(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM mirror.{table} t'),self.value(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {table} t'))
            self.importer(LAKE_PUSH_REPLAY_FROM='1',LAKE_PUSH_MAX_BATCHES='2')
            self.check('snapshot and streaming replay are idempotent',self.progress(),'2:2:2')
        finally:
            self.stop_child(child)
            if private_ddl is not None:
                if private_ddl.poll() is None:
                    self.sql("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name='snapshot_private_ddl'")
                private_ddl.communicate(timeout=20)
            self.sql('ALTER EVENT TRIGGER lake_capture_ddl_start ENABLE')
            if writer is not None and writer.poll() is None:
                writer.kill();writer.communicate(timeout=10)
        old_subscription=self.subscription
        self.sql('SELECT lake_capture.start_manager()')
        self.sql(f"SELECT lake_capture.reset_stream('{self.stream}')")
        self.source_wait("SELECT (capture_generation=2 AND snapshot_state='none' AND desired_state='stopped')::text FROM lake_capture.streams",'true')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        interrupted=self.stopped_snapshot()
        self.stop_child(interrupted)
        failed=self.command(self.capture_args(),required=False)
        self.check('lost snapshot cannot resume from unrelated current rows',
                   failed.returncode!=0 and 'controlled reset and recopy' in failed.stderr,True)
        self.check('interrupted snapshot generation exposes no complete batch',self.value(
            'SELECT count(*) FROM lake_capture_log.manifests'),'0')
        self.check('incomplete recopy leaves the previous mirror intact',self.dest(
            'SELECT (SELECT count(*) FROM mirror.a)::text||\':\'||(SELECT count(*) FROM mirror.b)'),'3:2')
        self.sql('SELECT lake_capture.start_manager()')
        self.sql(f"SELECT lake_capture.reset_stream('{self.stream}')")
        # The interrupted driver intentionally records a stopped diagnostic;
        # reset clears it only once a new private generation has been prepared.
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if self.value('SELECT capture_generation::text FROM lake_capture.streams')=='3':break
            time.sleep(.1)
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.check('controlled rebuild recopies a fresh coherent generation',self.source_wait(
            "SELECT (capture_generation=3 AND snapshot_state='complete')::text FROM lake_capture.streams",'true'),True)
        stale=self.importer(required=False,LAKE_PUSH_REPLAY_FROM='1')
        self.check('old consumer cannot switch snapshot generations implicitly',stale.returncode!=0 and 'generation mismatch' in stale.stderr,True)
        self.read_identity()
        self.bind_destination('mirror_next',capture_generation=3)
        self.importer()
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('fresh private destination generation materializes the recopy',self.dest(
            'SELECT (SELECT count(*) FROM mirror_next.a)::text||\':\'||(SELECT count(*) FROM mirror_next.b)'),'3:2')
        self.check('recopy never mutates the previously published generation',self.dest(
            'SELECT (SELECT count(*) FROM mirror.a)::text||\':\'||(SELECT count(*) FROM mirror.b)'),'3:2')


if __name__=='__main__':sys.exit(SnapshotProbe().run())

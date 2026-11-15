#!/usr/bin/env python3
"""Real upload/commit crashes around owner-requested zero-row progress."""
from datetime import datetime, timezone
import json
import subprocess
import sys
from native_faults import NativeFaultProbe


class ProgressFaultProbe(NativeFaultProbe):
    def stop_child(self,child):
        self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'],required=False)
        child.communicate(timeout=15)

    def exercise(self):
        self.evidence.update(scope='control-only source progress upload/commit crash recovery',
            unqualified=['automatic idle heartbeats','unbounded generations'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        child=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        finally:self.stop_child(child)
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        for sequence,point in enumerate(['before_upload_completion','after_upload_completion','after_commit'],1):
            request=self.value(f"SELECT lake_capture.request_progress('{self.stream}')")
            started=datetime.now(timezone.utc).isoformat()
            failed=self.command(self.capture_args(LAKE_CAPTURE_TEST_CRASH=point),required=False)
            self.check(point+': progress actor loses its success response',failed.returncode!=0,True)
            self.ready()
            logs=self.command(self.compose+['logs','--no-color','--since',started,'source'])
            diagnostic=logs.stdout+logs.stderr
            (self.output/(point+'.log')).write_text(diagnostic)
            self.check(point+': source backend actually crashed',
                'was terminated by signal 9' in diagnostic and 'all server processes terminated; reinitializing' in diagnostic,True)
            expected=sequence if point=='after_commit' else sequence-1
            self.check(point+': manifest, control and checkpoint share the commit outcome',self.value('''SELECT
                captured_sequence::text||':'||(SELECT count(*) FROM lake_capture_log.manifests)||':'||
                (SELECT count(*) FROM lake_capture_log.controls) FROM lake_capture.streams'''),f'{expected}:{expected}:{expected}')
            self.check(point+': request completion follows the same commit outcome',self.value(f'''
                SELECT coalesce(captured_sequence::text,'pending') FROM lake_capture.progress_requests
                WHERE request_id='{request}' '''),str(sequence) if point=='after_commit' else 'pending')
            child=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(sequence))
                self.source_wait('''SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text FROM lake_capture.streams s
                    JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true')
            finally:self.stop_child(child)
            self.importer(LAKE_PUSH_MAX_BATCHES='64');self.command(self.worker_args('lake-replication-publisher'))
            self.importer(LAKE_PUSH_REPLAY_FROM='1',LAKE_PUSH_MAX_BATCHES='64')
            self.command(self.worker_args('lake-replication-publisher'))
            self.check(point+': replay publishes each progress boundary once with no rows',self.dest(f'''
                SELECT published_sequence::text||':'||(SELECT count(*) FROM mirror.a)||':'||(SELECT count(*) FROM mirror.b)
                FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),f'{sequence}:0:0')
            self.check(point+': recovery completes the original request',self.value(f'''
                SELECT captured_sequence FROM lake_capture.progress_requests WHERE request_id='{request}' '''),str(sequence))
        self.check('crash replay never creates replacement progress requests',self.value('''SELECT
            progress_request_count::text||':'||(SELECT count(*) FROM lake_capture.progress_requests)
            FROM lake_capture.streams'''),'3:3')
        self.check('lost commit response never appends duplicate committed progress files',self.value('''
            SELECT count(*) FROM lake_table.files WHERE content=0 AND table_name IN (
                'lake_capture_log.controls'::regclass,'lake_capture_log.manifests'::regclass)'''),'6')


if __name__=='__main__':sys.exit(ProgressFaultProbe().run())

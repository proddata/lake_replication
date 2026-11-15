#!/usr/bin/env python3
"""Native capture crash -> slot reconciliation -> importer/publisher recovery."""
from datetime import datetime, timezone
import json
import subprocess
import sys
import time

from live import LiveProbe, ROOT


class NativeFaultProbe(LiveProbe):
    def __init__(self):
        super().__init__()
        self.evidence.update(scope='native capture upload/COMMIT crashes and slot durability through existing publisher')

    def ready(self):
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            try:
                if self.value('SELECT 1')=='1':return
            except RuntimeError:pass
            time.sleep(.2)
        raise RuntimeError('source did not recover')

    def exercise(self):
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE public.a(id bigint PRIMARY KEY,note text);
            CREATE TABLE public.b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('public.a','public.b')")
        # Keep the real lifecycle manager out of fault timing. The same driver
        # runs with its normal generation/lease checks; explicit restart after
        # a postmaster crash is required by the source clone/restore fence.
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        initial=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.check('native fault fixture establishes protected empty boundary',self.source_wait(
                'SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true'),True)
        finally:
            self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'])
            initial.communicate(timeout=10)
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        self.evidence['boundaries']={}
        for sequence,point in enumerate(['before_upload_completion','after_upload_completion','after_commit'],1):
            self.sql(f"BEGIN; INSERT INTO a VALUES({sequence},'{point}'); INSERT INTO b VALUES({sequence},'{point}'); COMMIT;")
            source_end=self.value('''SELECT encode(substring(data FROM 27 FOR 8),'hex')
                FROM lake_capture.streams s CROSS JOIN LATERAL pg_logical_slot_peek_binary_changes(
                    s.slot_name,NULL,NULL,'table-a-oid',s.table_a::oid::text,'table-b-oid',s.table_b::oid::text,
                    'table-a-id',s.table_a_id::text,'table-b-id',s.table_b_id::text)
                WHERE get_byte(data,45)>0 LIMIT 1''')
            source_end=int(source_end,16)
            source_end=f'{source_end>>32:X}/{source_end&0xffffffff:X}'
            started=datetime.now(timezone.utc).isoformat()
            failed=self.command(self.capture_args(LAKE_CAPTURE_TEST_CRASH=point),required=False)
            self.check(point+': native driver loses success response',failed.returncode!=0,True)
            self.ready()
            logs=self.command(self.compose+['logs','--no-color','--since',started,'source'])
            diagnostic=logs.stdout+logs.stderr
            (self.output/(point+'.log')).write_text(diagnostic)
            self.check(point+': actual capture backend SIGKILL caused recovery',
                       'was terminated by signal 9' in diagnostic and
                       'all server processes terminated; reinitializing' in diagnostic,True)
            committed=point=='after_commit'
            expected=sequence if committed else sequence-1
            self.check(point+': source manifest/checkpoint commit outcome',self.value(
                "SELECT captured_sequence::text||':'||(SELECT count(*) FROM lake_capture_log.manifests) FROM lake_capture.streams"),f'{expected}:{expected}')
            self.check(point+': slot never advances past durable selected capture',self.value('''SELECT
                (r.confirmed_flush_lsn<''' + f"'{source_end}'::pg_lsn" + ''')::text
                FROM lake_capture.streams s JOIN pg_replication_slots r ON r.slot_name=s.slot_name'''),'true')
            if committed:
                # Keep the committed receipt ahead of slot confirmation while
                # restarting the entire server. Reconciliation must survive
                # both the lost success response and a new postmaster binding.
                self.command(self.compose+['restart','source'],timeout=90)
                self.ready()
                self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
                self.check('full source restart retains committed capture receipt',self.value(
                    'SELECT captured_sequence::text||\':\'||(SELECT count(*) FROM lake_capture.receipts) FROM lake_capture.streams'),f'{sequence}:{sequence}')
                self.check('full source restart preserves unconfirmed replay boundary',self.value(
                    f"SELECT (confirmed_flush_lsn<'{source_end}'::pg_lsn)::text FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'true')
                stale=self.command(self.capture_args(),required=False)
                self.check('new postmaster refuses implicit stale capture binding',stale.returncode!=0,True)
                self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
            resumed=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                self.check(point+': native capture recovers complete batch exactly once',self.source_wait(
                    'SELECT captured_sequence::text FROM lake_capture.streams',str(sequence)),True)
                self.check(point+': slot advances only after capture/reconciliation',self.source_wait('''SELECT
                    (r.confirmed_flush_lsn>=s.captured_end_lsn)::text FROM lake_capture.streams s
                    JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true'),True)
            finally:
                self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'])
                resumed.communicate(timeout=15)
            self.check(point+': recovered logs have no duplicate live events',self.value(
                'SELECT (SELECT count(*) FROM lake_capture_log.events_a)::text||\':\'||(SELECT count(*) FROM lake_capture_log.events_b)'),f'{sequence}:{sequence}')
            self.importer()
            self.importer(LAKE_PUSH_REPLAY_FROM=str(sequence))
            self.command(self.worker_args('lake-replication-publisher'))
            self.check(point+': recovered native batch reaches final mirrors once',self.dest(
                'SELECT (SELECT count(*) FROM mirror.a)::text||\':\'||(SELECT count(*) FROM mirror.b)'),f'{sequence}:{sequence}')
            self.evidence['boundaries'][point]=dict(captured_before_reconciliation=expected,
                captured_after_reconciliation=sequence,driver_returncode=failed.returncode,
                driver_stderr=failed.stderr)


if __name__=='__main__':
    sys.exit(NativeFaultProbe().run())

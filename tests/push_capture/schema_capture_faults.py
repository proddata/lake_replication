#!/usr/bin/env python3
"""Real capture backend crashes with historical schema receipts ahead of confirmation."""
from datetime import datetime,timezone
import subprocess
import sys
from native_faults import NativeFaultProbe


class SchemaCaptureFaultProbe(NativeFaultProbe):
    def exercise(self):
        self.evidence.update(scope='source schema capture crashes and receipt-backed confirmed-boundary replay',
            unqualified=['schema import and ordered destination publication','public schema enablement'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        initial=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        finally:
            self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
            initial.communicate(timeout=20)
        self.sql(f"SELECT lake_capture.verify_schema_history('{self.stream}');UPDATE lake_capture.streams SET schema_auditing=true;SELECT lake_capture.start_stream('{self.stream}')")
        column='note'
        for sequence,point in enumerate(['before_upload_completion','after_upload_completion','after_commit'],1):
            if sequence==1:ddl='ALTER TABLE a ADD COLUMN extra boolean'
            else:
                new_column=f'column_{sequence}'
                ddl=f'ALTER TABLE a RENAME COLUMN {column} TO {new_column}'
            self.sql(f"BEGIN;INSERT INTO a(id,{column}) VALUES({sequence},'{point}');{ddl};INSERT INTO b VALUES({sequence},'{point}');COMMIT")
            if sequence>1:column=new_column
            before=self.value("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE plugin='lake_replication_capture'")
            started=datetime.now(timezone.utc).isoformat()
            failed=self.command(self.capture_args(LAKE_CAPTURE_TEST_CRASH=point),required=False)
            self.check(point+': schema capture loses its success response',failed.returncode!=0,True)
            self.ready()
            logs=self.command(self.compose+['logs','--no-color','--since',started,'source'])
            diagnostic=logs.stdout+logs.stderr
            (self.output/(point+'.log')).write_text(diagnostic)
            self.check(point+': real backend crash restarts the source children',
                'was terminated by signal 9' in diagnostic and 'all server processes terminated; reinitializing' in diagnostic,True)
            committed=point=='after_commit';expected=sequence if committed else sequence-1
            self.check(point+': schema receipt and manifest share the commit outcome',self.value('''SELECT
                captured_sequence::text||':'||(SELECT count(*) FROM lake_capture.receipts)||':'||
                (SELECT count(*) FROM lake_capture_log.manifests) FROM lake_capture.streams'''),f'{expected}:{expected}:{expected}')
            self.check(point+': crash cannot advance beyond the previous confirmed schema boundary',self.value(
                f"SELECT (confirmed_flush_lsn<='{before}'::pg_lsn)::text FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'true')
            self.check(point+': receipt retains only its committed post-schema state',self.value('''SELECT
                coalesce((SELECT table_a_version FROM lake_capture.receipts ORDER BY sequence DESC LIMIT 1),1)'''),str(expected+1))
            if committed:
                self.check('lost COMMIT response leaves captured schema ahead of confirmed history',self.value('''SELECT
                    (s.captured_end_lsn>r.confirmed_flush_lsn)::text FROM lake_capture.streams s
                    JOIN pg_replication_slots r ON r.slot_name=s.slot_name'''),'true')
                self.command(self.compose+['restart','source'],timeout=90);self.ready()
                stale=self.command(self.capture_args(),required=False)
                self.check('postmaster restart requires an explicit source rebind',stale.returncode!=0,True)
                self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
            resumed=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(sequence),timeout=60)
                self.source_wait('''SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text
                    FROM lake_capture.streams s JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true')
            finally:
                self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'])
                stdout,stderr=resumed.communicate(timeout=20)
                self.evidence.setdefault('recovered_actors',[]).append(dict(point=point,stdout=stdout,stderr=stderr,returncode=resumed.returncode))
            self.check(point+': replay captures each row and Schema control exactly once',self.value('''SELECT
                (SELECT count(*) FROM lake_capture_log.events_a)::text||':'||
                (SELECT count(*) FROM lake_capture_log.events_b)||':'||
                (SELECT count(*) FROM lake_capture_log.controls)'''),f'{sequence}:{sequence}:{sequence}')
            self.check(point+': replay commits the correct version without consulting latest heads',self.value(
                f"SELECT table_a_version::text||':'||table_b_version FROM lake_capture.receipts WHERE sequence={sequence}"),f'{sequence+1}:1')
        self.check('source crash recovery preserves the immutable audited schema history',self.value('''SELECT
            (SELECT count(*) FROM lake_capture.schema_events)::text||':'||
            (SELECT count(*) FROM lake_capture.schema_definitions)'''),'3:5')


if __name__=='__main__':sys.exit(SchemaCaptureFaultProbe().run())

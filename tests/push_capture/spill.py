#!/usr/bin/env python3
"""Pre-commit native spill quota and actual backend crash with unfinished work."""
import json
import re
import subprocess
import sys
import time
from live import LiveProbe
from session import Session

class SpillProbe(LiveProbe):
    def spill_files(self):
        slot=self.value('SELECT slot_name FROM lake_capture.streams')
        script="from pathlib import Path\nimport json\np=Path('/var/lib/postgresql/18/data/pg_replslot')/"+repr(slot)+"\nsizes=[]\nfor x in p.glob('*.spill'):\n try:sizes.append(x.stat().st_size)\n except FileNotFoundError:pass\nprint(json.dumps({'files':len(sizes),'bytes':sum(sizes)}))"
        return json.loads(self.command(self.compose+['exec','-T','source','python3','-c',script]).stdout)

    def completed_spill_cleanup(self):
        # Capturing a batch does not prevent the next SQL peek from already
        # spilling again while it rereads from the slot's older restart point.
        # Check eventual cleanup between completed native decoding contexts.
        samples=[];deadline=time.monotonic()+10
        while True:
            sample=self.spill_files();samples.append(sample)
            if sample['files']==0 or time.monotonic()>=deadline:
                self.evidence['spill_cleanup_samples']=samples
                return sample['files']
            time.sleep(.1)

    def ready(self):
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            try:
                if self.value('SELECT 1')=='1':return
            except RuntimeError:pass
            time.sleep(.2)
        raise RuntimeError('source did not recover')

    def exercise(self):
        self.evidence.update(scope='native pre-commit decoder spill quota and unfinished spill crash recovery',
            unqualified=['absolute filesystem byte quota','streaming output-plugin protocol','physical disk-full injection'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            ALTER TABLE a ALTER COLUMN note SET STORAGE EXTERNAL;''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql('UPDATE lake_capture.streams SET max_decode_spill_bytes=65536,decode_work_mem_kb=64')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        writer=Session(self.sql_args())
        try:
            writer.query("BEGIN;INSERT INTO a SELECT i,repeat(md5(i::text),64) FROM generate_series(1,2000) i")
            self.check('unfinished transaction stops native decoding at its spill budget',self.source_wait(
                "SELECT (desired_state='stopped' AND last_error LIKE '%decode spill budget exceeded%')::text FROM lake_capture.streams",'true'),True)
            diagnostic=self.value('SELECT last_error FROM lake_capture.streams')
            spilled=int(re.search(r'([0-9]+) bytes',diagnostic)[1])
            self.evidence['spill_high_water_diagnostic']=diagnostic
            self.check('spill check sees actual native serialized bytes before commit',65536<spilled<=65536*3,True)
            self.evidence['files_after_guard']=self.spill_files()
            self.check('spill failure cannot admit rows, manifests or upload attempts',self.value("SELECT captured_sequence::text||':'||upload_attempt_count||':'||(SELECT count(*) FROM lake_capture_log.manifests) FROM lake_capture.streams"),'0:0:0')
            writer.query('COMMIT')
        finally:writer.close()
        self.sql('UPDATE lake_capture.streams SET max_decode_spill_bytes=8388608')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.check('repaired budget replays the protected complete source transaction',self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1'),True)
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('spill repair publishes every row exactly once',self.dest('SELECT count(*) FROM mirror.a'),'2000')
        self.check('successful replay confirms its complete source boundary',self.source_wait('''
            SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text FROM lake_capture.streams s
            JOIN pg_replication_slots r ON r.slot_name=s.slot_name''','true'),True)
        self.check('completed decoding sessions clean native provisional spill files',self.completed_spill_cleanup(),0)
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        child=subprocess.Popen(self.capture_args(LAKE_CAPTURE_TEST_SPILL_STOP='1'),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        writer=Session(self.sql_args())
        try:
            writer.query("BEGIN;INSERT INTO a SELECT i,repeat(md5(i::text),64) FROM generate_series(5000,6999) i")
            pid=None;deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                pid=self.value("SELECT pid FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%' LIMIT 1")
                if pid:
                    state=self.command(self.compose+['exec','-T','source','python3','-c',"from pathlib import Path;print(Path('/proc/"+pid+"/status').read_text().split('State:',1)[1].splitlines()[0].strip())"]).stdout.strip()
                    if state.startswith('T'):break
                if child.poll() is not None:raise RuntimeError(child.communicate()[1])
                time.sleep(.1)
            else:raise RuntimeError('native decoder did not reach unfinished spill barrier')
            files=self.spill_files();self.evidence['files_before_backend_crash']=files
            self.check('crash barrier holds actual native spill files for unfinished work',files['bytes']>0,True)
            self.check('unfinished spooled work remains outside complete source batches',self.value('SELECT captured_sequence::text FROM lake_capture.streams'),'1')
            self.command(self.compose+['exec','-T','source','kill','-KILL',pid])
            child.communicate(timeout=20);self.ready()
        finally:
            writer.close()
            if child.poll() is None:child.kill();child.communicate(timeout=10)
        self.check('PostgreSQL crash recovery removes provisional spill files',self.spill_files()['files'],0)
        self.check('unfinished source transaction rolls back across decoder backend crash',self.value('SELECT count(*) FROM a WHERE id>=5000'),'0')
        self.check('decoder crash preserves the complete source checkpoint',self.value('SELECT captured_sequence::text FROM lake_capture.streams'),'1')
        self.sql('SELECT lake_capture.start_manager()')
        self.sql("BEGIN;INSERT INTO a VALUES(3000,'after-crash');INSERT INTO b VALUES(3001,'paired');COMMIT")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('post-crash native replay publishes only complete committed transactions',self.dest("SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),'2001:1')

if __name__=='__main__':sys.exit(SpillProbe().run())

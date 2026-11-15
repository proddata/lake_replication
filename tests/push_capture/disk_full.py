#!/usr/bin/env python3
"""Real ENOSPC on disposable slot spill storage, with protected WAL replay."""
import json
import sys
from live import LiveProbe, ROOT
from session import Session


class DiskFullProbe(LiveProbe):
    def __init__(self):
        super().__init__()
        self.compose += ['-f', str(ROOT/'deploy/push/spill-disk.compose.yml')]

    def exercise(self):
        self.evidence.update(scope='physical ENOSPC on native decoder spill storage and repair',
            unqualified=['full PGDATA/WAL disk outage','power loss of tmpfs slot storage','production disk quotas'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            ALTER TABLE a ALTER COLUMN note SET STORAGE EXTERNAL;''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql('UPDATE lake_capture.streams SET max_decode_spill_bytes=8388608,decode_work_mem_kb=64')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT active::text FROM pg_replication_slots WHERE plugin='lake_replication_capture'",'false')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        slot=self.value('SELECT slot_name FROM lake_capture.streams')
        # The server stays up. Only this stopped, disposable slot directory is
        # relocated; neither application storage nor WAL shares the fault disk.
        script="""
from pathlib import Path
import json,os,shutil
root=Path('/capture-spill-test');slot=Path('/var/lib/postgresql/18/data/pg_replslot')/SLOT
shutil.copytree(slot,root/'slot')
free=shutil.disk_usage(root).free
with (root/'pressure').open('wb') as output:os.posix_fallocate(output.fileno(),0,free-131072)
print(json.dumps({'disk_bytes':shutil.disk_usage(root).total,'remaining_bytes':shutil.disk_usage(root).free}))
""".replace('SLOT',repr(slot))
        result=self.command(self.compose+['exec','-T','--user','postgres','source','python3','-c',script])
        self.evidence['fault_disk']=json.loads(result.stdout)
        # Use a real mount, not a slot symlink: PostgreSQL deliberately skips
        # symlinks during stale spill cleanup. Reusing those files would retain
        # pointers from an expired decoding context and invalidate this test.
        self.command(self.compose+['exec','-T','--user','root','source','mount','--bind',
            '/capture-spill-test/slot','/var/lib/postgresql/18/data/pg_replslot/'+slot])
        self.check('fault disk has bounded physical free space',self.evidence['fault_disk']['remaining_bytes']<=131072,True)
        writer_start=self.value('SELECT pg_current_wal_lsn()')
        writer=Session(self.sql_args())
        try:
            writer.query("BEGIN;INSERT INTO a SELECT i,repeat(md5(i::text),64) FROM generate_series(1,2000)i")
            writer_end=self.value('SELECT pg_current_wal_lsn()')
            self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
            self.source_wait("SELECT (desired_state='stopped' AND last_error LIKE '%No space left on device%')::text FROM lake_capture.streams",'true')
            self.evidence['disk_full_diagnostic']=self.value('SELECT last_error FROM lake_capture.streams')
            self.check('actual native spill ENOSPC stops capture',
                'C=53100' in self.evidence['disk_full_diagnostic'] and
                'could not write to data file for XID' in self.evidence['disk_full_diagnostic'],True)
            self.check('disk-full cannot advance complete capture or reserve uploads',self.value("SELECT captured_sequence::text||':'||upload_attempt_count||':'||(SELECT count(*) FROM lake_capture_log.manifests) FROM lake_capture.streams"),'0:0:0')
            self.check('disk-full retains history and never confirms the failed transaction',self.value(
                f"SELECT (restart_lsn<='{writer_start}'::pg_lsn AND confirmed_flush_lsn<'{writer_end}'::pg_lsn AND invalidation_reason IS NULL)::text FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'true')
            writer.query('COMMIT')
        finally:writer.close()
        self.check('application can commit while isolated spill disk is full',self.value('SELECT count(*) FROM a'),'2000')
        self.command(self.compose+['exec','-T','--user','postgres','source','rm','/capture-spill-test/pressure'])
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('space repair replays protected WAL exactly once',self.dest('SELECT count(*)::text||\':\'||count(DISTINCT id) FROM mirror.a'),'2000:2000')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('repeated import cannot duplicate repaired transaction',self.dest('SELECT count(*) FROM mirror.a'),'2000')


if __name__=='__main__':sys.exit(DiskFullProbe().run())

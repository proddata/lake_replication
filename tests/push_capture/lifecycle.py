#!/usr/bin/env python3
"""Source clone/restart fencing, schema guard and fail-closed decoder limits."""
import subprocess
import sys
import time
from live import LiveProbe


class LifecycleProbe(LiveProbe):
    def __init__(self):
        super().__init__()
        self.evidence['scope']='native optional source lifecycle/clone/unsupported-change safety'

    def until(self,query,expected,timeout=40):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            try:
                if self.value(query)==expected:return True
            except RuntimeError:pass
            time.sleep(.1)
        raise RuntimeError('source condition timed out: '+query)

    def clone(self,statement,required=True):
        args=self.sql_args();args[args.index('capture')]='capture_clone'
        return self.command(args,input=statement,required=required)

    def register(self):
        self.stream=self.value("SELECT lake_capture.register_stream('public.a','public.b')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")

    def drop_stream(self):
        self.sql(f"SELECT lake_capture.drop_stream('{self.stream}')")
        self.until('SELECT count(*) FROM lake_capture.streams','0')
        self.check('source cleanup leaves no owned slot',self.value(
            "SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'0')

    def exercise(self):
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE public.a(id bigint PRIMARY KEY,note text);
            CREATE TABLE public.b(id bigint PRIMARY KEY,note text);
            INSERT INTO a VALUES(1,'existing');''')
        self.sql('ALTER TABLE a ENABLE ROW LEVEL SECURITY')
        rls=self.sql("SELECT lake_capture.register_stream('a','b')",required=False)
        self.check('registration rejects a filtered RLS baseline',rls.returncode!=0 and 'row-level security' in rls.stderr,True)
        self.sql('ALTER TABLE a DISABLE ROW LEVEL SECURITY')
        self.register()
        self.check('nonempty activation stops with snapshot diagnostic',self.until(
            "SELECT (desired_state='stopped' AND last_error LIKE '%existing rows require snapshot support%')::text FROM lake_capture.streams",'true'),True)
        self.check('failed empty activation creates no slot',self.value('SELECT count(*) FROM pg_replication_slots'),'0')
        renamed=self.sql('ALTER SCHEMA public RENAME TO unsafe_capture_schema',required=False)
        self.check('selected namespace renames remain frozen while stopped',renamed.returncode!=0 and 'schema is frozen' in renamed.stderr,True)
        self.check('DDL stays guarded while stream is stopped',self.sql(
            'ALTER TABLE a ADD COLUMN unsafe text',required=False).returncode!=0,True)
        self.check('an unselected child cannot change selected-table membership',self.sql(
            'CREATE TABLE unsafe_child() INHERITS(a)',required=False).returncode!=0,True)
        self.sql('''CREATE ROLE unrelated_source_owner NOLOGIN;
            CREATE SCHEMA unrelated AUTHORIZATION unrelated_source_owner;
            SET ROLE unrelated_source_owner; CREATE TABLE unrelated.items(id integer);
            ALTER TABLE unrelated.items ADD COLUMN note text; DROP TABLE unrelated.items; RESET ROLE;
            CREATE EXTENSION hstore; DROP EXTENSION hstore;''')
        self.check('unrelated roles retain ordinary source DDL privileges',self.value(
            "SELECT to_regclass('unrelated.items') IS NULL"),'t')
        blocked=self.sql('DROP EXTENSION lake_replication_capture CASCADE',required=False)
        self.check('capture dependency drop cannot orphan its owned slot',
                   blocked.returncode!=0 and 'owned slot cleanup' in blocked.stderr,True)
        self.sql('DELETE FROM a')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.until('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.command(self.compose+['restart','source'],timeout=90)
        self.check('full postmaster restart fences copied/running capture state',self.until(
            "SELECT (desired_state='stopped' AND last_error LIKE '%postmaster changed%')::text FROM lake_capture.streams",'true'),True)
        self.sql("INSERT INTO a VALUES(1,'after-postmaster-restart')")
        self.check('restart does not implicitly resume source uploads',self.value(
            'SELECT captured_sequence::text FROM lake_capture.streams'),'0')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.check('explicit same-source rebind replays retained WAL',self.source_wait(
            'SELECT captured_sequence::text FROM lake_capture.streams','1'),True)
        self.command(self.compose+['exec','-T','source','pg_dump','-U','postgres','-Fc','-d','capture','-f','/tmp/capture-clone.dump'])
        self.sql('CREATE DATABASE capture_clone')
        self.command(self.compose+['exec','-T','source','pg_restore','-U','postgres','--exit-on-error','-d','capture_clone','/tmp/capture-clone.dump'])
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if self.clone("SELECT desired_state FROM lake_capture.streams").stdout.strip()=='stopped':break
            time.sleep(.1)
        self.check('logical restore starts source capture stopped',self.clone(
            'SELECT desired_state FROM lake_capture.streams').stdout.strip(),'stopped')
        rejected=self.clone(f"SELECT lake_capture.start_stream('{self.stream}')",required=False)
        self.check('restored source cannot adopt original binding',rejected.returncode!=0 and 'new stream/snapshot' in rejected.stderr,True)
        self.clone(f"SELECT lake_capture.drop_stream('{self.stream}')")
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if self.clone('SELECT count(*) FROM lake_capture.streams').stdout.strip()=='0':break
            time.sleep(.1)
        self.check('copied cleanup cannot remove original capture slot',self.value(
            "SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture' AND database=current_database()"),'1')
        self.sql('DROP DATABASE capture_clone WITH (FORCE)')
        self.sql("UPDATE a SET note='supported' WHERE id=1")
        self.check('same-source restart retains mutable WAL continuity',self.source_wait(
            'SELECT captured_sequence::text FROM lake_capture.streams','2'),True)
        self.sql(f"BEGIN;SELECT lake_capture.reset_stream('{self.stream}');ROLLBACK;")
        self.check('rolled-back reset preserves the capture generation',self.value(
            'SELECT capture_generation::text FROM lake_capture.streams'),'1')
        self.sql(f"SELECT lake_capture.reset_stream('{self.stream}')")
        self.until("SELECT (desired_state='stopped' AND capture_generation=2 AND baseline_lsn IS NULL)::text FROM lake_capture.streams",'true')
        self.check('controlled reset fences the old generation and drops its slot',self.value(
            'SELECT count(*) FROM pg_replication_slots'),'0')
        self.check('controlled reset clears source progress and private logs atomically',self.value('''SELECT
            captured_sequence::text||':'||(SELECT count(*) FROM lake_capture_log.manifests)||':'||
            (SELECT count(*) FROM lake_capture.receipts)||':'||(SELECT count(*) FROM lake_capture.upload_attempts)
            FROM lake_capture.streams'''),'0:0:0:0')
        self.drop_stream()
        for operation in ['oversize','history']:
            self.sql('TRUNCATE a,b')
            self.register()
            self.until('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
            if operation=='history':
                self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
                self.until("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
                self.sql('SELECT pg_drop_replication_slot(slot_name) FROM lake_capture.streams')
                self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
                expected='%history lost%'
            elif operation=='oversize':
                self.sql("INSERT INTO a VALUES(1,repeat('z',8388608))")
                expected='%byte budget exceeded%'
            self.check(operation+' fails closed with a decoder diagnostic',self.until(
                f"SELECT (desired_state='stopped' AND last_error LIKE '{expected}')::text FROM lake_capture.streams",'true'),True)
            self.check(operation+' leaves captured sequence unchanged',self.value(
                'SELECT captured_sequence::text FROM lake_capture.streams'),'0' if operation in ('oversize','history') else '1')
            if operation=='history':
                self.check('missing history never creates a replacement slot silently',self.value(
                    'SELECT count(*) FROM pg_replication_slots'),'0')
            self.drop_stream()
        self.sql('DROP EXTENSION lake_replication_capture CASCADE')


if __name__=='__main__':
    sys.exit(LifecycleProbe().run())

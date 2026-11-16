#!/usr/bin/env python3
"""Streamed direct publication: atomic chunks, subabort, spill limits and WAL replay."""
import os
import re
import subprocess
import sys
import time
from datetime import datetime,timezone
from wal_direct import DirectProbe
from session import Session

class StreamingProbe(DirectProbe):
    def worker(self,**settings):
        return super().worker(**dict(dict(LAKE_DIRECT_STREAMING='1',LAKE_DIRECT_DEBUG_STREAMING=('0' if os.environ.get('WAL_DIRECT_STREAMING_CASE')=='ordinary' else '1'),LAKE_DIRECT_CHUNK_BYTES='262144'),**settings))

    def start(self,name,count=1,**settings):
        path=self.output/(name+'.log');log=path.open('w')
        process=subprocess.Popen(self.worker(LAKE_DIRECT_MAX_COMMITS=str(count),**settings),stdout=log,stderr=log,text=True)
        self.wait_log(process,path,'direct_started')
        return process,log,path

    def wait_log(self,process,path,text):
        deadline=time.monotonic()+40
        while text not in path.read_text():
            if process.poll() is not None or time.monotonic()>deadline:raise RuntimeError('missing '+text+': '+path.read_text())
            time.sleep(.05)

    def wait_spill(self,p,path):
        deadline=time.monotonic()+40
        while not any(int(n)>=500 for n in re.findall(r'direct_spilled xid=\d+ rows=(\d+)',path.read_text())):
            if p.poll() is not None or time.monotonic()>deadline:raise RuntimeError('missing nonempty spill: '+path.read_text()[-3000:])
            time.sleep(.05)

    def stop(self,p,log,path):
        if p.poll() is None:
            pid=re.search(r'direct_started pid=(\d+)',path.read_text()).group(1)
            self.command(self.compose+['exec','-T','postgres','kill','-KILL',pid]);p.wait(timeout=10)
        log.close()

    def finish(self,p,log,path):
        try:p.wait(timeout=120);self.check('streamed worker exits cleanly',p.returncode,0)
        finally:
            if p.poll() is None:self.stop(p,log,path)
            else:log.close()

    def exercise(self):
        if os.environ.get('WAL_DIRECT_STREAMING_CASE')=='ordinary':
            self.evidence.update(scope='Protocol 4 without debug forcing: ordinary transaction framing, mutations and chunked commit',unqualified=['power loss','managed lifecycle','production sizing'])
            self.bootstrap();self.sql('ALTER DATABASE capture SET lake_replication.group_independent_inserts=on')
            self.sql("INSERT INTO a VALUES(1,repeat('toast',3000),'{\"n\":123456789012345678901234567890123456789}');INSERT INTO b VALUES(1,'first')")
            result=self.command(self.worker(LAKE_DIRECT_MAX_COMMITS='2'));self.check('nondebug small commits use ordinary framing','direct_spilled' not in result.stderr,True);self.equal()
            self.sql("BEGIN;UPDATE a SET id=2;DELETE FROM b;INSERT INTO b VALUES(2,'new');COMMIT");self.command(self.worker());self.equal()
            self.sql("BEGIN;TRUNCATE a,b;INSERT INTO a SELECT i,repeat(md5(i::text),64),NULL FROM generate_series(1,5000) i;COMMIT")
            self.command(self.worker(),timeout=180);self.equal();self.journal_empty()
            self.check('unforced protocol chunks oversized transaction',int(self.value('SELECT last_large_chunks FROM lake_direct.stream'))>1,True)
            return
        self.evidence.update(scope=__doc__,unqualified=['production sizing','power loss','managed lifecycle','storage outage'])
        self.bootstrap();self.sql('ALTER DATABASE capture SET lake_replication.group_independent_inserts=on')
        if os.environ.get('WAL_DIRECT_STREAMING_CRASH_POINT'):
            point=os.environ['WAL_DIRECT_STREAMING_CRASH_POINT'];before=self.state();started=datetime.now(timezone.utc).isoformat()
            self.sql("INSERT INTO a SELECT i,repeat('crash',100),NULL FROM generate_series(1,2000) i")
            failed=self.command(self.worker(LAKE_DIRECT_CRASH_POINT=point),required=False,timeout=120)
            self.check(point+': lost publication response',failed.returncode!=0,True);self.wait('SELECT 1','1')
            logs=self.command(self.compose+['logs','--no-color','--since',started,'postgres'])
            self.check(point+': real shared-server crash','was terminated by signal 9' in logs.stdout+logs.stderr,True)
            self.check(point+': receipt follows actual commit outcome',self.state()!=before,point=='after_commit')
            if point=='after_commit':self.sql("INSERT INTO b VALUES(1,'marker')")
            self.command(self.worker(),timeout=120);self.equal();self.journal_empty()
            self.check(point+': one source transaction retained once',self.value('SELECT count(*) FROM iceberg_mirror.a'),'2000')
            return
        # More than both the ordinary 4096-change and 8MiB transaction caps.
        self.sql("INSERT INTO a SELECT i,repeat(md5(i::text),64),jsonb_build_object('n',123456789012345678901234567890123456789) FROM generate_series(1,5000) i")
        result=self.command(self.worker(),timeout=180)
        (self.output/'large.log').write_text(result.stdout+result.stderr)
        self.check('large transaction has multiple atomic chunks',int(self.value('SELECT last_large_chunks FROM lake_direct.stream'))>1,True)
        self.check('large transaction exceeds ordinary byte cap',int(self.value('SELECT last_large_bytes FROM lake_direct.stream'))>8388608,True)
        self.check('one source commit produces one publication',self.value('SELECT commits::text||\':\'||publications FROM lake_direct.stream'),'1:1');self.equal();self.journal_empty()
        self.check('JSONB precision survives streamed chunks',self.value("SELECT document->>'n' FROM iceberg_mirror.a LIMIT 1"),'123456789012345678901234567890123456789')
        # Stream parent and child segments before rollback. Both top abort and
        # savepoint rollback must discard only their corresponding changes.
        session=Session(self.sql_args());p,log,path=self.start('subabort')
        try:
            session.query("BEGIN;INSERT INTO b SELECT i,'parent' FROM generate_series(10000,11000) i;SAVEPOINT child;INSERT INTO b SELECT i,'child' FROM generate_series(20000,21000) i")
            self.wait_spill(p,path);session.query("SAVEPOINT grandchild;INSERT INTO b SELECT i,'nested' FROM generate_series(22000,22100) i;RELEASE grandchild;ROLLBACK TO child;INSERT INTO b VALUES(30000,'after child');COMMIT")
            self.finish(p,log,path)
        finally:session.close();self.stop(p,log,path)
        self.equal();self.check('rolled back child never reaches mirror',self.value('SELECT count(*) FROM iceberg_mirror.b WHERE id BETWEEN 20000 AND 21000'),'0')
        p,log,path=self.start('top-abort');session=Session(self.sql_args());before=self.state()
        try:
            session.query("BEGIN;INSERT INTO b SELECT i,'abort' FROM generate_series(40000,41000) i")
            self.wait_spill(p,path);session.query('ROLLBACK');self.sql("INSERT INTO b VALUES(42000,'marker')");self.finish(p,log,path)
        finally:session.close();self.stop(p,log,path)
        self.equal();self.check('top abort is absent',self.value('SELECT count(*) FROM iceberg_mirror.b WHERE id BETWEEN 40000 AND 41000'),'0')
        # L remains unfinished when S commits. Kill and recover from the durable
        # replay floor, rather than S's later publication receipt.
        session=Session(self.sql_args());before=self.state();p,log,path=self.start('interleave',2)
        try:
            session.query("BEGIN;INSERT INTO b SELECT i,'long' FROM generate_series(50000,51000) i")
            self.wait_spill(p,path);self.sql("INSERT INTO b VALUES(52000,'short')")
            self.wait('SELECT count(*) FROM iceberg_mirror.b WHERE id=52000','1')
            self.check('interleaved commit keeps old replay floor',self.value('SELECT replay_lsn::text FROM lake_direct.stream'),before)
            self.check('interleaved commit does not acknowledge unfinished WAL',self.value("SELECT confirmed_flush_lsn<=replay_lsn FROM pg_replication_slots r,lake_direct.stream WHERE r.slot_name='wal_direct_slot'"),'t')
            ordinary=self.command(super().worker(),required=False);self.check('ordinary mode refuses unfinished stream history',ordinary.returncode!=0,True)
            self.stop(p,log,path);p,log,path=self.start('interleave-restart');session.query('COMMIT');self.finish(p,log,path)
        finally:session.close();self.stop(p,log,path)
        self.equal();self.check('replay skips already published short transaction',self.value('SELECT count(*) FROM iceberg_mirror.b WHERE id=52000'),'1')
        self.check('recovery discards stale disposable files','stale_files_removed=1' in path.read_text(),True)
        self.check('recovery advances replay floor when no transaction remains',self.value('SELECT replay_lsn=published_lsn FROM lake_direct.stream'),'t')
        # Spill budget failure preserves both receipt and source acknowledgement.
        self.sql("INSERT INTO b SELECT i,repeat('budget',100) FROM generate_series(60000,61000) i")
        before=self.state();feedback=self.feedback();failed=self.command(self.worker(LAKE_DIRECT_SPILL_BYTES='1024'),required=False)
        self.check('spill quota fails closed',failed.returncode!=0,True);self.check('quota preserves receipt',self.state(),before);self.check('quota preserves feedback',self.feedback(),feedback)
        self.command(self.worker(),timeout=120);self.equal();self.journal_empty()
        # An actual full filesystem, isolated from PostgreSQL's data/WAL disks.
        self.sql("INSERT INTO b SELECT i,repeat('disk',512) FROM generate_series(90000,95000) i")
        before=self.state();feedback=self.feedback();container=self.command(self.compose+['ps','-q','postgres']).stdout.strip()
        args=['docker','run','--rm','--network','container:'+container,'--tmpfs','/spool:rw,size=1m,mode=0700']
        settings=dict(LAKE_DIRECT_EXPERIMENT='1',LAKE_DIRECT_ADDRESS='127.0.0.1:5432',LAKE_DIRECT_DATABASE='capture',LAKE_DIRECT_USER='postgres',LAKE_DIRECT_MAX_COMMITS='1',LAKE_DIRECT_STREAMING='1',LAKE_DIRECT_DEBUG_STREAMING='1',LAKE_DIRECT_SPILL_DIRECTORY='/spool')
        for key,value in settings.items():args+=['-e',key+'='+value]
        args+=[os.environ['PUSH_PG_LAKE_IMAGE'],'lake-replication-direct']
        failed=self.command(args,required=False,timeout=120)
        self.check('real spill ENOSPC fails closed',failed.returncode!=0 and 'No space left on device' in failed.stderr,True)
        self.check('ENOSPC preserves receipt',self.state(),before);self.check('ENOSPC preserves WAL feedback',self.feedback(),feedback)
        self.command(self.worker(),timeout=120);self.equal()
        # A failure in a later chunk rolls back all earlier target changes.
        self.sql("BEGIN;INSERT INTO b SELECT i,repeat('atomic',100) FROM generate_series(70000,71000) i;UPDATE b SET note='changed' WHERE id=10000;COMMIT")
        self.sql('DELETE FROM iceberg_mirror.b WHERE id=10000');before=self.state()
        failed=self.command(self.worker(),required=False,timeout=120)
        self.check('later chunk failure stops publication',failed.returncode!=0,True)
        self.check('later chunk failure preserves receipt',self.state(),before)
        self.check('earlier chunks roll back atomically',self.value('SELECT count(*) FROM iceberg_mirror.b WHERE id BETWEEN 70000 AND 71000'),'0')
        self.sql("INSERT INTO iceberg_mirror.b VALUES(10000,'parent')");self.command(self.worker(),timeout=120);self.equal()
        # Change a valid JSON byte in a spooled record after decoding. The
        # checksum, not JSON parsing, must prevent untrusted data publication.
        session=Session(self.sql_args());before=self.state();p,log,path=self.start('corruption')
        try:
            session.query("BEGIN;INSERT INTO b SELECT i,'corrupt' FROM generate_series(80000,81000) i")
            self.wait_spill(p,path)
            directory=re.search(r'direct_spill_open directory=(\S+)',path.read_text()).group(1)
            code="from pathlib import Path;p=next(Path("+repr(directory)+").glob('tx-*.bin'));f=p.open('r+b');f.seek(p.read_bytes().index(b'corrupt'));f.write(b'C');f.close()"
            self.command(self.compose+['exec','-T','postgres','python3','-c',code]);session.query('COMMIT');p.wait(timeout=30)
            self.check('spill corruption fails checksum',p.returncode!=0 and 'checksum mismatch' in path.read_text(),True)
            self.check('spill corruption preserves receipt',self.state(),before)
            self.check('corrupt transaction remains invisible',self.value('SELECT count(*) FROM iceberg_mirror.b WHERE id BETWEEN 80000 AND 81000'),'0')
        finally:session.close();self.stop(p,log,path)
        self.command(self.worker(),timeout=120);self.equal()
        # Lost response following final Iceberg COMMIT: replay skips its changes.
        self.sql("INSERT INTO b VALUES(62000,'response')");before=self.state();failed=self.command(self.worker(LAKE_DIRECT_EXIT_BEFORE_FEEDBACK='1'),required=False)
        self.check('response loss occurs after receipt',failed.returncode!=0 and self.state()!=before,True)
        self.sql("INSERT INTO b VALUES(62001,'after response')");self.command(self.worker());self.equal();self.journal_empty()

if __name__=='__main__':sys.exit(StreamingProbe().run())

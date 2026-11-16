#!/usr/bin/env python3
"""Bounded direct batching: ordering, pressure, idle flush, stop/replay and crashes."""
from datetime import datetime,timezone
import os
import re
import subprocess
import sys
import time
from wal_direct import DirectProbe
from session import Session


class CoalescingProbe(DirectProbe):
    def counts(self):return tuple(map(int,self.value('SELECT commits::text||\':\'||publications FROM lake_direct.stream').split(':')))

    def run_group(self,count,**settings):
        result=self.command(self.worker(LAKE_DIRECT_MAX_COMMITS=str(count),LAKE_DIRECT_PUBLISH_INTERVAL_MS='60000',**settings))
        self.equal();self.journal_empty();return result

    def background(self,name,count,interval):
        log=(self.output/(name+'.log')).open('w')
        process=subprocess.Popen(self.worker(LAKE_DIRECT_MAX_COMMITS=str(count),LAKE_DIRECT_PUBLISH_INTERVAL_MS=str(interval)),stdout=log,stderr=log,text=True)
        return process,log,self.output/(name+'.log')

    def finish(self,process,log):
        try:
            process.wait(timeout=15)
            self.check('background worker finishes cleanly',process.returncode,0)
        finally:
            if process.poll() is None:process.kill();process.wait()
            log.close()

    def exercise(self):
        self.evidence.update(scope=__doc__,unqualified=['nonempty snapshot','streaming/spill','managed lifecycle','power loss','production memory/WAL budgets'])
        self.bootstrap();self.sql('ALTER DATABASE capture SET lake_replication.group_independent_inserts=on')
        for i in range(1,4):self.sql(f"BEGIN;INSERT INTO a VALUES({i},'first','{{}}');INSERT INTO b VALUES({i},'first');COMMIT")
        result=self.run_group(3)
        self.check('three source commits publish atomically once',self.counts(),(3,1))
        self.check('worker reports grouped source transaction count','transactions=3' in result.stderr,True)
        for table in 'ab':self.check('pure INSERT grouping writes one file '+table,self.value(f"SELECT count(*) FROM lake_table.files WHERE table_name='iceberg_mirror.{table}'::regclass AND content=0"),'1')
        self.sql("BEGIN;UPDATE a SET id=10,note='key change' WHERE id=1;UPDATE b SET note='changed' WHERE id=1;COMMIT")
        self.sql('BEGIN;DELETE FROM a WHERE id=2;TRUNCATE b;COMMIT')
        self.sql("BEGIN;INSERT INTO a VALUES(4,'after truncate',NULL);INSERT INTO b VALUES(4,'after truncate');COMMIT")
        self.run_group(3);self.check('mixed transaction order and TRUNCATE share one commit',self.counts(),(6,2))
        for i in range(5,8):self.sql(f"BEGIN;INSERT INTO a VALUES({i},'pressure',NULL);INSERT INTO b VALUES({i},'pressure');COMMIT")
        result=self.run_group(3,LAKE_DIRECT_MAX_ROWS='3')
        self.check('row pressure flushes only complete transactions',self.counts(),(9,5))
        self.check('row pressure is observable','reason=buffer_pressure' in result.stderr,True)
        for i in [15,16]:self.sql(f"INSERT INTO a VALUES({i},repeat('x',1000),NULL)")
        result=self.run_group(2,LAKE_DIRECT_MAX_BYTES='2000')
        self.check('byte pressure bounds the aggregate batch',self.counts(),(11,7))
        self.check('byte pressure is observable','reason=buffer_pressure' in result.stderr,True)
        # Idle publication cannot wait for another application commit.
        process,log,path=self.background('idle',2,250)
        try:
            self.sql("INSERT INTO b VALUES(20,'idle')")
            self.wait('SELECT commits FROM lake_direct.stream','12')
            self.check('idle flush leaves decoder running',process.poll(),None)
            self.check('idle flush publishes exactly once',self.counts(),(12,8))
            self.check('idle flush reason is observable','reason=interval' in path.read_text(),True)
            self.sql("INSERT INTO b VALUES(21,'exit')")
            self.finish(process,log)
        finally:
            if process.poll() is None:process.kill();process.wait()
            if not log.closed:log.close()
        self.check('process budget flushes remaining work',self.counts(),(13,9));self.equal()
        # Hold the first publication past its own deadline. The next window
        # must start fresh after the first COMMIT, not drain immediately.
        barrier=Session(self.sql_args());barrier.query('BEGIN;SELECT id FROM lake_direct.stream FOR UPDATE')
        process,log,path=self.background('fresh-window',3,2000)
        try:
            self.sql("INSERT INTO b VALUES(30,'blocked first')")
            self.wait("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%SELECT lake_direct.publish(%'",'1')
            self.sql("INSERT INTO b VALUES(31,'during publication')")
            barrier.query('COMMIT');self.wait('SELECT commits FROM lake_direct.stream','14')
            time.sleep(.4)
            self.check('next live publication receives a fresh window',self.counts(),(14,10))
            self.sql("INSERT INTO b VALUES(32,'joins second window')")
            self.finish(process,log)
        finally:
            barrier.close()
            if process.poll() is None:process.kill();process.wait()
            if not log.closed:log.close()
        self.check('followup transactions coalesce together',self.counts(),(16,11));self.equal()
        # Kill the worker after decoding but before any final-table COMMIT.
        before=self.state();feedback=self.feedback()
        process,log,path=self.background('stop-buffered',2,60000)
        try:
            self.sql("INSERT INTO b VALUES(40,'buffered at stop')")
            deadline=time.monotonic()+15
            while 'direct_buffered' not in path.read_text():
                if process.poll() is not None or time.monotonic()>deadline:raise RuntimeError('worker did not buffer the transaction')
                time.sleep(.05)
            self.check('buffered transaction does not advance receipt',self.state(),before)
            self.check('buffered transaction does not acknowledge WAL',self.feedback(),feedback)
            pid=re.search(r'direct_started pid=(\d+)',path.read_text()).group(1)
            self.command(self.compose+['exec','-T','postgres','kill','-TERM',pid]);process.wait(timeout=10)
            self.check('stopped worker loses only provisional buffers',self.state(),before)
        finally:
            if process.poll() is None:process.kill();process.wait()
            log.close()
        self.run_group(1);self.check('restart replays stopped transaction once',self.value('SELECT count(*) FROM iceberg_mirror.b WHERE id=40'),'1')
        # A later source transaction failing apply rolls back earlier ones in
        # the same publication and keeps the entire source range replayable.
        before=self.state();self.sql("DELETE FROM iceberg_mirror.b WHERE id=6;BEGIN;UPDATE a SET note='atomic batch' WHERE id=5;COMMIT;BEGIN;UPDATE b SET note='atomic batch' WHERE id=6;COMMIT")
        failed=self.command(self.worker(LAKE_DIRECT_MAX_COMMITS='2',LAKE_DIRECT_PUBLISH_INTERVAL_MS='60000'),required=False)
        self.check('later failed transaction aborts entire group',failed.returncode!=0,True)
        self.check('failed group leaves receipt unchanged',self.state(),before)
        self.check('earlier transaction is rolled back with group',self.value("SELECT note FROM iceberg_mirror.a WHERE id=5"),'pressure')
        self.sql("INSERT INTO iceberg_mirror.b VALUES(6,'pressure')");self.run_group(2)
        if os.environ.get('WAL_DIRECT_CRASH_TESTS')=='1':
            for offset,point in enumerate(['before_upload_completion','after_upload_completion','after_commit']):
                before=self.state();counts=self.counts();started=datetime.now(timezone.utc).isoformat()
                keys=list(range(100+offset*10,103+offset*10))
                for i in keys:self.sql(f"BEGIN;INSERT INTO a VALUES({i},'grouped crash','{{}}');INSERT INTO b VALUES({i},'grouped crash');COMMIT")
                failed=self.command(self.worker(LAKE_DIRECT_MAX_COMMITS='3',LAKE_DIRECT_PUBLISH_INTERVAL_MS='60000',LAKE_DIRECT_CRASH_POINT=point),required=False)
                self.check(point+': grouped publication loses response',failed.returncode!=0,True)
                self.wait('SELECT 1','1')
                logs=self.command(self.compose+['logs','--no-color','--since',started,'postgres'])
                self.check(point+': shared PostgreSQL actually crashes','was terminated by signal 9' in logs.stdout+logs.stderr,True)
                self.check(point+': receipt follows atomic commit outcome',self.state()!=before,point=='after_commit')
                self.check(point+': counters follow atomic commit outcome',self.counts(),(counts[0]+3,counts[1]+1) if point=='after_commit' else counts)
                if point=='after_commit':
                    self.sql("INSERT INTO b VALUES(1000,'after committed crash')");self.run_group(1)
                else:self.run_group(3)
                self.check(point+': exact grouped replay',self.value('SELECT count(*) FROM iceberg_mirror.a WHERE id IN ('+','.join(map(str,keys))+')'),'3')
        before=self.counts()
        for i in [2001,2002,2003]:self.sql(f"INSERT INTO b VALUES({i},'transaction cap')")
        result=self.run_group(3,LAKE_DIRECT_BATCH_MAX_TRANSACTIONS='2')
        self.check('transaction cap flushes before the deadline',self.counts(),(before[0]+3,before[1]+2))
        self.check('transaction cap has an explicit flush reason','reason=transactions' in result.stderr,True)
        self.check('feedback never exceeds final publication',self.value("SELECT confirmed_flush_lsn<=published_lsn FROM pg_replication_slots r,lake_direct.stream s WHERE r.slot_name='wal_direct_slot'"),'t')
        self.sql("SELECT pg_drop_replication_slot('wal_direct_slot')")


if __name__=='__main__':sys.exit(CoalescingProbe().run())

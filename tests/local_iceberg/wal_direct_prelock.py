#!/usr/bin/env python3
"""Native target locks precede publication writes, across buffered and streamed input."""
import os
import subprocess
import sys
from wal_direct import DirectProbe
from session import Session

class PrelockProbe(DirectProbe):
    def exercise(self):
        if os.environ.get('WAL_DIRECT_PRELOCK')!='1':raise RuntimeError('prelock fixture requires WAL_DIRECT_PRELOCK=1')
        self.bootstrap()
        self.evidence['scope']=__doc__
        self.sql("BEGIN;INSERT INTO a VALUES(1,'seed',NULL);INSERT INTO b VALUES(1,'seed');COMMIT")
        self.publish()
        self.sql('CREATE ROLE prelock_unprivileged')
        self.check('native lock bridge is private',self.value("SELECT has_function_privilege('prelock_unprivileged','lake_direct.lock_target(regclass)','EXECUTE')"),'f')
        self.sql("GRANT USAGE ON SCHEMA lake_direct,iceberg_mirror TO prelock_unprivileged;GRANT EXECUTE ON FUNCTION lake_direct.lock_target(regclass) TO prelock_unprivileged")
        denied=self.sql("SET ROLE prelock_unprivileged;SELECT lake_direct.lock_target('iceberg_mirror.a'::regclass)",required=False)
        self.check('unprivileged lock bridge call fails',denied.returncode!=0 and 'requires superuser' in denied.stderr,True)
        for number,streaming in [(2,False),(3,True)]:
            owner=Session(self.sql_args());log=(self.output/f'blocked-{number}.log').open('w');process=None
            before=self.state();ack=self.feedback()
            try:
                # The publisher must acquire A then wait for B BEFORE inserting
                # into A. This checks full-mapping order even for chunked replay.
                owner.query("BEGIN;SELECT lake_direct.lock_target('iceberg_mirror.b'::regclass)")
                self.sql(f"BEGIN;INSERT INTO a VALUES({number},'pending',NULL);INSERT INTO b VALUES({number},'pending');UPDATE a SET note='updated' WHERE id=1;COMMIT")
                settings=dict(LAKE_DIRECT_STREAMING='1',LAKE_DIRECT_CHUNK_BYTES='256') if streaming else {}
                process=subprocess.Popen(self.worker(**settings),stdout=log,stderr=log,text=True)
                self.wait("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event='advisory' AND query LIKE '%lake_direct.%'",'1')
                self.check('prelock blocks before first target writes '+str(number),self.value(f"SELECT count(*) FROM iceberg_mirror.a WHERE id={number}"),'0')
                self.check('blocked checkpoint stays unchanged '+str(number),self.state(),before)
                self.check('blocked feedback stays unchanged '+str(number),self.feedback(),ack)
                # Row-lock all existing file metadata without waiting: the
                # blocked publisher must not own any conflicting catalog rows.
                owner.query("SET LOCAL lock_timeout='1s';SELECT id FROM lake_table.files WHERE table_name IN ('iceberg_mirror.a'::regclass,'iceberg_mirror.b'::regclass) FOR UPDATE NOWAIT")
                self.check('file metadata remains lockable before publication '+str(number),True,True)
                held=self.value("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objsubid=101 AND objid='iceberg_mirror.a'::regclass::oid AND granted")
                self.check('lower OID native target lock is already held '+str(number),held,'1')
                owner.query('ROLLBACK');process.wait(timeout=60)
                self.check('publication completes after maintenance lock release '+str(number),process.returncode,0)
            finally:
                owner.close()
                if process and process.poll() is None:process.kill();process.wait()
                log.close()
            self.check('coordinated wait needs no deadlock replay '+str(number),'direct_retry' in (self.output/f'blocked-{number}.log').read_text(),False)
            if streaming:self.check('streamed input applies several chunks',int(self.value('SELECT last_large_chunks FROM lake_direct.stream'))>1,True)
            self.equal();self.journal_empty()
            self.check('commit releases native target locks '+str(number),self.value("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objsubid=101"),'0')

if __name__=='__main__':sys.exit(PrelockProbe().run())

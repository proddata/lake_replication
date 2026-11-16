#!/usr/bin/env python3
"""Whole-publication deadlock retry, including streamed rewind and COMMIT abort."""
import re
import subprocess
import sys
from wal_direct import DirectProbe
from session import Session

class RetryProbe(DirectProbe):
    def wrapper(self,condition,code='40P01',gate=False):
        lock="IF attempt=1 THEN PERFORM pg_advisory_xact_lock(9191,1);END IF;" if gate else ''
        self.sql(f"""CREATE OR REPLACE FUNCTION lake_direct.apply_payload(p_changes jsonb) RETURNS bigint
        LANGUAGE plpgsql SET search_path=pg_catalog,lake_direct AS $$
        DECLARE affected bigint;attempt bigint;
        BEGIN
         affected:=lake_direct.retry_original_apply(p_changes);attempt:=nextval('lake_direct.retry_attempts');
         {lock}
         IF {condition} THEN RAISE EXCEPTION USING ERRCODE='{code}',MESSAGE='retry fixture C=40P01';END IF;
         RETURN affected;
        END $$;ALTER SEQUENCE lake_direct.retry_attempts RESTART WITH 1""")

    def ack_safe(self,before):
        self.check('feedback never exceeds the previously committed boundary',self.value(f"SELECT confirmed_flush_lsn<='{before}'::pg_lsn FROM pg_replication_slots WHERE slot_name='wal_direct_slot'"),'t')

    def record(self,name,result):
        (self.output/(name+'.log')).write_text(result.stdout+result.stderr)
        return result.stderr.count('direct_retry sqlstate=40P01')

    def exercise(self):
        self.evidence.update(scope=__doc__,unqualified=['production retry sizing','arbitrary error retry','power loss','managed direct lifecycle'])
        self.bootstrap();self.sql("ALTER DATABASE capture SET lake_replication.group_independent_inserts=on;ALTER DATABASE capture SET deadlock_timeout='5s'")
        self.sql("ALTER FUNCTION lake_direct.apply_payload(jsonb) RENAME TO retry_original_apply;CREATE SEQUENCE lake_direct.retry_attempts")
        # Actual PostgreSQL lock cycle: the worker owns the stream row and
        # waits for our advisory lock; we then request its stream row lock.
        self.wrapper('false',gate=True);owner=Session(self.sql_args());log=(self.output/'actual-deadlock.log').open('w')
        owner.query("SET deadlock_timeout='20s';SELECT pg_advisory_lock(9191,1)")
        before=self.state();feedback=self.feedback()
        self.sql("INSERT INTO b VALUES(1,'first');INSERT INTO b VALUES(2,'second')")
        process=subprocess.Popen(self.worker(LAKE_DIRECT_MAX_COMMITS='2',LAKE_DIRECT_PUBLISH_INTERVAL_MS='60000'),stdout=log,stderr=log,text=True)
        try:
            self.wait("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event='advisory' AND query LIKE '%SELECT lake_direct.publish(%'",'1')
            self.check('uncommitted first attempt is invisible',self.value('SELECT count(*) FROM iceberg_mirror.b'),'0')
            self.check('blocked first attempt preserves checkpoint',self.state(),before)
            self.check('blocked first attempt preserves WAL acknowledgement',self.feedback(),feedback)
            owner.query('SELECT id FROM lake_direct.stream FOR UPDATE');owner.query('SELECT pg_advisory_unlock(9191,1)')
            process.wait(timeout=30);self.check('actual deadlock retries successfully',process.returncode,0)
        finally:
            owner.close()
            if process.poll() is None:process.kill();process.wait()
            log.close()
        text=(self.output/'actual-deadlock.log').read_text()
        self.check('actual lock cycle emits one retry',text.count('direct_retry sqlstate=40P01'),1)
        self.check('real deadlock replays the full batch',self.value('SELECT last_value FROM lake_direct.retry_attempts'),'2')
        self.check('two source commits have one final publication',self.value("SELECT commits::text||':'||publications FROM lake_direct.stream"),'2:1');self.equal()
        logs=self.command(self.compose+['logs','--no-color','postgres'])
        self.check('server observed a real deadlock','deadlock detected' in logs.stdout+logs.stderr,True)
        # Third streamed apply chunk aborts after writing. Every earlier chunk
        # must roll back; the second attempt must rewind the complete spill.
        self.wrapper('attempt=3')
        self.sql("INSERT INTO a SELECT i,repeat('large',100),jsonb_build_object('n',123456789012345678901234567890123456789) FROM generate_series(100,5099) i")
        result=self.command(self.worker(LAKE_DIRECT_STREAMING='1',LAKE_DIRECT_CHUNK_BYTES='65536'),timeout=180)
        self.check('late streamed chunk retries exactly once',self.record('streamed-rewind',result),1)
        chunks=int(self.value('SELECT last_large_chunks FROM lake_direct.stream'))
        self.check('streamed case spans several chunks',chunks>3,True)
        self.check('retry replays all earlier chunks from zero',int(self.value('SELECT last_value FROM lake_direct.retry_attempts')),chunks+3)
        self.check('streamed retry has one committed receipt',self.value("SELECT commits::text||':'||publications FROM lake_direct.stream"),'3:2');self.equal();self.journal_empty()
        # A deferred trigger raises at COMMIT, not during apply. The server
        # confirms abort; it differs from losing the COMMIT response.
        self.wrapper('false');self.sql("""CREATE FUNCTION lake_direct.abort_commit() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF nextval('lake_direct.commit_attempts')=1 THEN RAISE EXCEPTION USING ERRCODE='40P01',MESSAGE='deferred abort';END IF;RETURN NEW;END $$;
        CREATE SEQUENCE lake_direct.commit_attempts;
        CREATE CONSTRAINT TRIGGER retry_commit AFTER UPDATE ON lake_direct.stream DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.published_lsn>OLD.published_lsn) EXECUTE FUNCTION lake_direct.abort_commit()""")
        self.sql("INSERT INTO b VALUES(3,'commit abort')");result=self.command(self.worker(LAKE_DIRECT_TRACE_PHASES='1'))
        self.check('confirmed COMMIT abort retries once',self.record('commit-abort',result),1)
        self.check('COMMIT retry is atomic',self.value('SELECT last_value FROM lake_direct.commit_attempts'),'2')
        self.equal()
        self.sql("ALTER SEQUENCE lake_direct.commit_attempts RESTART WITH 1;INSERT INTO b VALUES(30,'streamed commit abort')")
        result=self.command(self.worker(LAKE_DIRECT_STREAMING='1'))
        self.check('streamed COMMIT abort retries once',self.record('streamed-commit-abort',result),1)
        self.check('streamed COMMIT retries one complete transaction',self.value('SELECT last_value FROM lake_direct.commit_attempts'),'2')
        self.equal();self.sql('DROP TRIGGER retry_commit ON lake_direct.stream')
        # Exhaustion and disable both stop, with the failed range replayable.
        for retries,attempts,identifier in [('2',3,4),('0',1,5)]:
            self.wrapper('true');self.sql(f"INSERT INTO b VALUES({identifier},'budget')");before=self.state()
            failed=self.command(self.worker(LAKE_DIRECT_DEADLOCK_RETRIES=retries),required=False)
            self.check('bounded retry exhaustion stops worker '+retries,failed.returncode!=0,True)
            self.check('exact bounded attempt count '+retries,int(self.value('SELECT last_value FROM lake_direct.retry_attempts')),attempts)
            self.check('bounded retry log count '+retries,self.record('exhaustion-'+retries,failed),attempts-1)
            self.check('exhaustion preserves checkpoint '+retries,self.state(),before);self.ack_safe(before)
            self.check('exhaustion leaves target unchanged '+retries,self.value(f'SELECT count(*) FROM iceberg_mirror.b WHERE id={identifier}'),'0')
            self.wrapper('false');self.command(self.worker());self.equal()
        # An unrelated SQLSTATE with a spoofed message must never be retried.
        self.wrapper('true',code='23505');self.sql("INSERT INTO b VALUES(6,'other error')");before=self.state()
        failed=self.command(self.worker(),required=False)
        self.check('other SQLSTATE stops without retry',failed.returncode!=0 and self.record('other-error',failed)==0,True)
        self.check('message text cannot authorize retry',self.value('SELECT last_value FROM lake_direct.retry_attempts'),'1')
        self.check('other SQLSTATE preserves checkpoint',self.state(),before);self.ack_safe(before)
        self.wrapper('false');self.command(self.worker());self.equal()
        failed=self.command(self.worker(LAKE_DIRECT_DEADLOCK_RETRIES='9'),required=False)
        self.check('invalid retry budget is rejected before streaming',failed.returncode!=0,True)
        # Successful COMMIT followed by lost response must still exit. Native
        # crash fixtures exercise actual transport loss independently.
        self.sql("INSERT INTO b VALUES(7,'response loss')");before=self.state()
        failed=self.command(self.worker(LAKE_DIRECT_EXIT_BEFORE_FEEDBACK='1'),required=False)
        self.check('response loss is not retried',failed.returncode!=0 and self.record('response-loss',failed)==0,True)
        self.check('response loss follows committed receipt',self.state()!=before,True)
        self.sql("INSERT INTO b VALUES(8,'after response')");self.command(self.worker());self.equal();self.journal_empty()

if __name__=='__main__':sys.exit(RetryProbe().run())

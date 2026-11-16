#!/usr/bin/env python3
"""Bounded exported-snapshot bootstrap directly into final Iceberg tables."""
from datetime import datetime,timezone
import os
import re
import subprocess
import sys
import time
from wal_direct import DirectProbe,enable_prelock


class SnapshotProbe(DirectProbe):
    def initializer(self,**settings):
        settings={'LAKE_DIRECT_SLOT':'wal_direct_slot','LAKE_DIRECT_PUBLICATION':'wal_direct_pub','LAKE_DIRECT_TARGET_SCHEMA':'iceberg_mirror',**settings}
        args=self.worker(**settings);args[-1]='lake-replication-direct-init'
        return args

    def wait_log(self,path,text,timeout=20):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            if text in path.read_text():return
            time.sleep(.05)
        raise RuntimeError('missing log marker '+text+': '+path.read_text()[-1500:])

    def start_init(self,name,**settings):
        path=self.output/(name+'.log');log=path.open('w')
        process=subprocess.Popen(self.initializer(**settings),stdout=log,stderr=log,text=True)
        return process,log,path

    def run_direct(self,count):
        self.command(self.worker(LAKE_DIRECT_MAX_COMMITS=str(count),LAKE_DIRECT_PUBLISH_INTERVAL_MS='250'))
        self.equal();self.journal_empty()

    def assert_no_copy_store(self):
        self.check('snapshot writes no PostgreSQL row-data staging',self.value('SELECT (SELECT count(*) FROM lake_replication.initial_copy_rows)+(SELECT count(*) FROM lake_replication.initial_copy_chunks)+(SELECT count(*) FROM lake_replication.transactions)+(SELECT count(*) FROM lake_replication.changes)'),'0')

    def prepare(self):
        self.sql((self.root/'experiments/wal_direct/install.sql').read_text())
        enable_prelock(self)
        self.sql('''CREATE TABLE a(id bigint PRIMARY KEY,note text,document jsonb);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            INSERT INTO a SELECT i,repeat('a',256),jsonb_build_object('n',123456789012345678901234567890123456789::numeric,'id',i) FROM generate_series(1,1000)i;
            INSERT INTO b SELECT i,repeat('b',256) FROM generate_series(1,1000)i;
            CREATE PUBLICATION wal_direct_pub FOR TABLE a,b''')

    @property
    def root(self):
        from probe import ROOT
        return ROOT

    def recover_marker(self):
        self.sql("INSERT INTO b VALUES(3000,'after retry')");self.run_direct(1)
        self.check('post-bootstrap writes continue exactly once',self.value('SELECT commits FROM lake_direct.stream'),'1')
        self.assert_no_copy_store()

    def interrupted(self):
        process,log,path=self.start_init('interrupted',LAKE_DIRECT_SNAPSHOT_TABLE_PAUSE_MS='5000')
        try:
            self.wait_log(path,'direct_snapshot_acquired')
            self.wait("SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%SELECT lake_direct.bootstrap(%' AND wait_event='PgSleep'",'1')
            feedback=self.feedback()
            self.sql("BEGIN;INSERT INTO a VALUES(2001,'during interruption','{}');UPDATE b SET note='during interruption' WHERE id=1;COMMIT")
            self.check('uncommitted snapshot stays invisible',self.value("SELECT to_regclass('iceberg_mirror.a') IS NULL"),'t')
            pid=re.search(r'direct_snapshot_acquired pid=(\d+)',path.read_text()).group(1)
            self.command(self.compose+['exec','-T','postgres','kill','-TERM',pid]);process.wait(timeout=10)
            self.check('interrupted initializer exits',process.returncode!=0,True)
        finally:
            if process.poll() is None:process.kill();process.wait()
            log.close()
        self.wait("SELECT count(*) FROM pg_stat_activity WHERE application_name='lake_direct_snapshot_control'",'0')
        self.check('interruption rolls back activation',self.value('SELECT count(*) FROM lake_direct.stream'),'0')
        self.check('interruption rolls back all destination tables',self.value("SELECT to_regclass('iceberg_mirror.a') IS NULL AND to_regclass('iceberg_mirror.b') IS NULL"),'t')
        self.check('interruption rolls back copy audit',self.value('SELECT count(*) FROM lake_direct.snapshot_tables'),'0')
        self.check('interruption does not acknowledge WAL',self.feedback(),feedback)
        self.command(self.initializer());self.equal();self.assert_no_copy_store();self.recover_marker()

    def lost_response(self):
        failed=self.command(self.initializer(LAKE_DIRECT_EXIT_AFTER_SNAPSHOT_COMMIT='1'),required=False)
        self.check('snapshot can lose response after COMMIT',failed.returncode!=0,True)
        self.check('snapshot activation survived response loss',self.value('SELECT count(*) FROM lake_direct.stream'),'1')
        before=self.state();self.equal()
        result=self.command(self.initializer())
        self.check('retry reconciles committed snapshot','direct_snapshot_already_ready' in result.stderr,True)
        self.check('retry preserves original snapshot boundary',self.state(),before)
        self.check('retry preserves exactly one snapshot copy',self.value('SELECT sum(rows_copied) FROM lake_direct.snapshot_tables'),'2000')
        self.equal();self.recover_marker()

    def crashed(self,point):
        original={table:self.value(f"SELECT md5(string_agg(row_to_json(t)::text,',' ORDER BY id)) FROM {table} t") for table in 'ab'}
        started=datetime.now(timezone.utc).isoformat()
        failed=self.command(self.initializer(LAKE_DIRECT_CRASH_POINT=point),required=False)
        self.check(point+': initializer loses response',failed.returncode!=0,True)
        self.wait('SELECT 1','1')
        logs=self.command(self.compose+['logs','--no-color','--since',started,'postgres'])
        self.check(point+': shared PostgreSQL actually crashes','was terminated by signal 9' in logs.stdout+logs.stderr,True)
        committed=point=='after_commit'
        self.check(point+': activation follows actual COMMIT outcome',self.value('SELECT count(*) FROM lake_direct.stream'),'1' if committed else '0')
        self.check(point+': snapshot audit follows actual COMMIT outcome',self.value('SELECT count(*) FROM lake_direct.snapshot_tables'),'2' if committed else '0')
        if not committed:self.check(point+': destinations remain unpublished',self.value("SELECT to_regclass('iceberg_mirror.a') IS NULL AND to_regclass('iceberg_mirror.b') IS NULL"),'t')
        for table in 'ab':self.check(point+': committed source heap survives '+table,self.value(f"SELECT md5(string_agg(row_to_json(t)::text,',' ORDER BY id)) FROM {table} t"),original[table])
        self.command(self.initializer());self.equal();self.assert_no_copy_store();self.recover_marker()

    def existing_destination(self):
        self.sql("CREATE SCHEMA iceberg_mirror;CREATE TABLE iceberg_mirror.a(id bigint,note text,document jsonb) USING iceberg;INSERT INTO iceberg_mirror.a VALUES(42,'existing','{}')")
        failed=self.command(self.initializer(),required=False)
        self.check('existing destination refuses initialization',failed.returncode!=0,True)
        self.check('existing destination remains untouched',self.value('SELECT id::text||note FROM iceberg_mirror.a'),'42existing')
        self.check('refusal never activates stream',self.value('SELECT count(*) FROM lake_direct.stream'),'0')
        self.check('refusal never creates remaining target',self.value("SELECT to_regclass('iceberg_mirror.b') IS NULL"),'t')
        self.assert_no_copy_store()

    def guards(self):
        failed=self.command(self.initializer(LAKE_DIRECT_PUBLICATION='a'*64),required=False)
        self.check('overlong identifiers are refused before slot creation',failed.returncode!=0 and self.value('SELECT count(*) FROM pg_replication_slots')=='0',True)
        for kind in ['numeric','numeric(39,2)','numeric(4,-1)']:
            self.sql(f'ALTER TABLE a ADD COLUMN amount {kind}')
            failed=self.command(self.initializer(),required=False)
            self.check('unsupported decimal contract refuses '+kind,failed.returncode!=0,True)
            self.check('decimal refusal leaves targets absent '+kind,self.value("SELECT to_regclass('iceberg_mirror.a') IS NULL"),'t')
            self.sql('ALTER TABLE a DROP COLUMN amount')
        self.sql('ALTER PUBLICATION wal_direct_pub SET TABLE a WHERE(id>1),b')
        failed=self.command(self.initializer(),required=False)
        self.check('filtered snapshot publication is refused',failed.returncode!=0,True)
        self.sql('ALTER PUBLICATION wal_direct_pub SET TABLE a,b;ALTER TABLE a ENABLE ROW LEVEL SECURITY')
        failed=self.command(self.initializer(),required=False)
        self.check('row-security source is refused',failed.returncode!=0,True)
        self.sql('ALTER TABLE a DISABLE ROW LEVEL SECURITY;ALTER TABLE a REPLICA IDENTITY NOTHING')
        failed=self.command(self.initializer(),required=False)
        self.check('source without usable identity is refused',failed.returncode!=0,True)
        self.sql('ALTER TABLE a REPLICA IDENTITY DEFAULT;ALTER TABLE a ADD COLUMN amount numeric(20,2);UPDATE a SET amount=1234567890123456.78')
        self.command(self.initializer());self.equal();self.assert_no_copy_store()
        self.check('bounded decimals copy without rounding',self.value('SELECT amount FROM iceberg_mirror.a WHERE id=1'),'1234567890123456.78')
        self.sql("BEGIN;UPDATE a SET amount=999.11 WHERE id=1;INSERT INTO b VALUES(3000,'after guarded initialization');COMMIT")
        self.run_direct(1)
        self.check('copied decimal row continues through WAL',self.value('SELECT amount FROM iceberg_mirror.a WHERE id=1'),'999.11')

    def lost_schema_guard(self):
        process,log,path=self.start_init('lost-schema-guard',LAKE_DIRECT_SNAPSHOT_PAUSE_MS='5000')
        try:
            self.wait_log(path,'direct_snapshot_acquired')
            pid=self.value("SELECT pid FROM pg_stat_activity WHERE application_name='lake_direct_snapshot_schema_guard'")
            self.check('source guard is terminated explicitly',self.value(f'SELECT pg_terminate_backend({int(pid)})'),'t')
            self.wait("SELECT count(*) FROM pg_stat_activity WHERE application_name='lake_direct_snapshot_schema_guard'",'0')
            self.sql("ALTER TABLE a ALTER COLUMN note TYPE varchar(400) USING (note||'')")
            process.wait(timeout=30)
            self.check('lost pre-export source guard aborts activation',process.returncode!=0,True)
        finally:
            if process.poll() is None:process.kill();process.wait()
            log.close()
        self.check('guard loss never publishes a stream',self.value('SELECT count(*) FROM lake_direct.stream'),'0')
        self.check('guard loss never exposes a partial table',self.value("SELECT to_regclass('iceberg_mirror.a') IS NULL AND to_regclass('iceberg_mirror.b') IS NULL"),'t')
        self.check('source data survives the intervening rewrite',self.value("SELECT (SELECT count(*) FROM a)::text||':'||(SELECT count(*) FROM b)"),'1000:1000')
        self.command(self.initializer());self.equal();self.assert_no_copy_store();self.recover_marker()

    def exercise(self):
        self.evidence.update(scope=__doc__,unqualified=['general resumable large copy','arbitrary DDL','streaming/spill','managed lifecycle','power loss','production memory sizing'])
        self.prepare()
        case=os.environ.get('WAL_DIRECT_SNAPSHOT_CASE','normal')
        if case!='normal':
            if case=='stop':self.interrupted()
            elif case=='response':self.lost_response()
            elif case=='existing':self.existing_destination()
            elif case=='guards':self.guards()
            elif case=='guard_loss':self.lost_schema_guard()
            elif case.startswith('crash:'):self.crashed(case.split(':',1)[1])
            else:raise ValueError('unknown snapshot case')
            self.sql("SELECT pg_drop_replication_slot('wal_direct_slot')");return
        # Budget failure leaves a retained slot, but no visible destination/data/checkpoint.
        failed=self.command(self.initializer(LAKE_DIRECT_SNAPSHOT_MAX_ROWS='50'),required=False)
        self.check('snapshot row budget refuses oversized copy',failed.returncode!=0,True)
        self.check('failed snapshot does not activate a stream',self.value('SELECT count(*) FROM lake_direct.stream'),'0')
        self.check('failed snapshot leaves destination absent',self.value("SELECT to_regclass('iceberg_mirror.a') IS NULL"),'t')
        self.check('failed snapshot retains persistent replay history',self.value("SELECT count(*) FROM pg_replication_slots WHERE slot_name='wal_direct_slot' AND invalidation_reason IS NULL"),'1')
        self.assert_no_copy_store()
        failed=self.command(self.initializer(LAKE_DIRECT_SNAPSHOT_MAX_BYTES='10'),required=False)
        self.check('snapshot byte budget refuses oversized copy',failed.returncode!=0,True)
        self.check('budget rollback removes provisional snapshot metadata',self.value('SELECT count(*) FROM lake_direct.snapshot_tables'),'0')
        # Retry uses a fresh temporary exported snapshot while the original slot retains WAL.
        process,log,path=self.start_init('concurrent',LAKE_DIRECT_SNAPSHOT_PAUSE_MS='2500',LAKE_DIRECT_SNAPSHOT_TABLE_PAUSE_MS='1500')
        try:
            self.wait_log(path,'direct_snapshot_acquired')
            ddl=self.sql("SET lock_timeout='200ms';ALTER TABLE a ADD COLUMN unexpected text",required=False)
            self.check('DDL is fenced from before snapshot export',ddl.returncode!=0 and 'lock timeout' in ddl.stderr,True)
            self.sql("BEGIN;UPDATE a SET id=2001,note='during snapshot' WHERE id=1;DELETE FROM b WHERE id=1;INSERT INTO b VALUES(2001,'during snapshot');COMMIT;BEGIN;INSERT INTO a VALUES(2002,'second transaction','null');COMMIT;BEGIN;INSERT INTO b VALUES(9999,'aborted');ROLLBACK")
            self.check('snapshot remains unactivated during concurrent writes',self.value('SELECT count(*) FROM lake_direct.stream'),'0')
            self.check('application commits proceed during snapshot',self.value("SELECT note FROM a WHERE id=2001"),'during snapshot')
            self.wait("SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%SELECT lake_direct.bootstrap(%' AND wait_event='PgSleep'",'1')
            self.check('partially copied first table is invisible',self.value("SELECT to_regclass('iceberg_mirror.a') IS NULL"),'t')
            self.check('snapshot metadata is invisible before activation',self.value('SELECT count(*) FROM lake_direct.snapshot_tables'),'0')
            process.wait(timeout=60);self.check('snapshot retry completes successfully',process.returncode,0)
        finally:
            if process.poll() is None:process.kill();process.wait()
            log.close()
        self.check('one consistent snapshot includes both original tables',self.value("SELECT (SELECT count(*) FROM iceberg_mirror.a)::text||':'||(SELECT count(*) FROM iceberg_mirror.b)"),'1000:1000')
        self.check('initial mirrors use pre-concurrent-update values',self.value("SELECT note=repeat('a',256) FROM iceberg_mirror.a WHERE id=1"),'t')
        self.check('initial mirror excludes post-snapshot row',self.value('SELECT count(*) FROM iceberg_mirror.a WHERE id=2002'),'0')
        self.check('snapshot row audit matches copied data',self.value('SELECT rows_copied::text||\':\'||table_count FROM lake_direct.snapshot_state'),'2000:2')
        self.check('initial publication and checkpoint share one boundary',self.value('SELECT (consistent_lsn=published_lsn AND commits=0 AND publications=0)::text FROM lake_direct.snapshot_state,lake_direct.stream'),'true')
        self.check('JSONB precision survives native snapshot copy',self.value("SELECT document->>'n' FROM iceberg_mirror.a WHERE id=1"),'123456789012345678901234567890123456789')
        self.assert_no_copy_store()
        self.sql("INSERT INTO b VALUES(3000,'after activation')");self.run_direct(3)
        self.check('snapshot-to-WAL boundary applies every transaction once',self.value('SELECT commits FROM lake_direct.stream'),'3')
        self.check('aborted transaction never appears',self.value('SELECT count(*) FROM iceberg_mirror.b WHERE id=9999'),'0')
        checkpoint=self.state();before=self.value('SELECT sum(rows_copied) FROM lake_direct.snapshot_tables')
        self.command(self.initializer())
        self.check('repeated initializer does not overwrite an active stream',self.state(),checkpoint)
        self.check('repeated initializer does not copy rows again',self.value('SELECT sum(rows_copied) FROM lake_direct.snapshot_tables'),before)
        self.equal();self.assert_no_copy_store()
        self.sql("SELECT pg_drop_replication_slot('wal_direct_slot')")


if __name__=='__main__':sys.exit(SnapshotProbe().run())

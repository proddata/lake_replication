#!/usr/bin/env python3
"""Frozen-schema, empty-bootstrap direct publication proof; no bulk journal."""
import json
import os
from datetime import datetime,timezone
import sys
from probe import LocalIcebergProbe,ROOT
from session import Session


def enable_prelock(probe):
    if os.environ.get('WAL_DIRECT_PRELOCK')!='1':return
    image=os.environ['PUSH_PG_LAKE_IMAGE']
    probe.evidence['prelock_native_runtime']=json.loads(probe.command([
        'docker','run','--rm','--entrypoint','cat',image,
        '/usr/share/pg_lake/lake-direct-prelock-prototype.json']).stdout)
    probe.sql((ROOT/'experiments/wal_direct/prelock/install.sql').read_text())
    probe.evidence['prelock_targets']=True


class DirectProbe(LocalIcebergProbe):
    def worker(self,**settings):
        env=dict(LAKE_DIRECT_EXPERIMENT='1',LAKE_DIRECT_ADDRESS='127.0.0.1:5432',
            LAKE_DIRECT_DATABASE='capture',LAKE_DIRECT_USER='postgres',LAKE_DIRECT_MAX_COMMITS='1')
        env.update(settings)
        args=self.compose+['exec','-T']
        for key,value in env.items():args+=['-e',key+'='+value]
        return args+['postgres','lake-replication-direct']

    def provenance(self):
        super().provenance()
        import hashlib
        for path in (ROOT/'experiments/wal_direct').rglob('*'):
            if path.is_file():self.evidence['files_sha256'][str(path.relative_to(ROOT))]=hashlib.sha256(path.read_bytes()).hexdigest()

    def state(self):
        return self.value('SELECT published_lsn FROM lake_direct.stream')

    def feedback(self):
        return self.value("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='wal_direct_slot'")

    def journal_empty(self):
        self.check('no bulk PostgreSQL journal rows',self.value('SELECT (SELECT count(*) FROM lake_replication.transactions)+(SELECT count(*) FROM lake_replication.changes)+(SELECT count(*) FROM lake_replication.streamed_changes)'),'0')

    def publish(self,**settings):
        self.command(self.worker(**settings));self.equal();self.journal_empty()

    def bootstrap(self):
        self.sql((ROOT/'experiments/wal_direct/install.sql').read_text())
        enable_prelock(self)
        self.sql('''CREATE TABLE a(id bigint PRIMARY KEY,note text,document jsonb);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            CREATE SCHEMA iceberg_mirror;CREATE TABLE iceberg_mirror.a(id bigint NOT NULL,note text,document jsonb) USING iceberg;
            CREATE TABLE iceberg_mirror.b(id bigint NOT NULL,note text) USING iceberg;
            CREATE PUBLICATION wal_direct_pub FOR TABLE a,b;''')
        # Empty tables, no concurrent writers during bootstrap; snapshot is out of scope.
        self.sql("SELECT pg_create_logical_replication_slot('wal_direct_slot','pgoutput')")
        self.sql("""INSERT INTO lake_direct.stream(id,slot_name,publication,system_identifier,database_oid,timeline,published_lsn)
            SELECT 1,'wal_direct_slot','wal_direct_pub',(pg_control_system()).system_identifier::text,
              (SELECT oid FROM pg_database WHERE datname=current_database()),(pg_control_checkpoint()).timeline_id,confirmed_flush_lsn
            FROM pg_replication_slots WHERE slot_name='wal_direct_slot';
            INSERT INTO lake_direct.mapping SELECT 'a'::regclass,'iceberg_mirror.a'::regclass,lake_direct.descriptor('a'),lake_direct.shape('a'),lake_direct.shape('iceberg_mirror.a');
            INSERT INTO lake_direct.mapping SELECT 'b'::regclass,'iceberg_mirror.b'::regclass,lake_direct.descriptor('b'),lake_direct.shape('b'),lake_direct.shape('iceberg_mirror.b');""")

    def exercise(self):
        self.evidence.update(scope='experimental WAL-backed direct final-Iceberg publication, empty bootstrap, frozen schemas',
            unqualified=['nonempty snapshot bootstrap','streamed/oversized transactions','managed lifecycle','power loss','production sizing','performance'])
        self.bootstrap()
        owner=Session(self.sql_args())
        try:
            owner.query('SELECT pg_advisory_lock(1818321765,1)')
            failed=self.command(self.worker(),required=False)
            self.check('second direct owner is rejected',failed.returncode!=0,True)
        finally:owner.close()
        before=self.state()
        self.sql('ALTER PUBLICATION wal_direct_pub DROP TABLE b')
        failed=self.command(self.worker(),required=False)
        self.check('publication changes fail closed',failed.returncode!=0,True)
        self.check('publication mismatch preserves receipt',self.state(),before)
        self.sql('ALTER PUBLICATION wal_direct_pub ADD TABLE b')
        self.sql("BEGIN;INSERT INTO a VALUES(1,'first','{\"n\":123456789012345678901234567890123456789}');INSERT INTO b VALUES(1,'first');COMMIT;BEGIN;INSERT INTO b VALUES(99,'aborted');ROLLBACK")
        self.publish()
        self.check('JSONB numeric precision survives',self.value("SELECT document->>'n' FROM iceberg_mirror.a"),'123456789012345678901234567890123456789')
        self.sql("BEGIN;UPDATE a SET id=2,note='updated',document='null';DELETE FROM b;INSERT INTO b VALUES(2,'reinsert');COMMIT")
        self.publish()
        self.sql("BEGIN;TRUNCATE a,b;INSERT INTO a VALUES(3,'after truncate',NULL);INSERT INTO b VALUES(3,'after truncate');COMMIT")
        self.publish()
        # Lost worker response after the authoritative publication COMMIT.
        previous=self.state();feedback_before=self.feedback();self.sql("BEGIN;UPDATE a SET id=4;UPDATE b SET id=4;COMMIT")
        failed=self.command(self.worker(LAKE_DIRECT_EXIT_BEFORE_FEEDBACK='1'),required=False)
        self.check('worker exits after durable publication',failed.returncode!=0,True)
        self.check('publication receipt commits before feedback',self.state()!=previous,True)
        self.equal();self.journal_empty()
        self.check('worker exit leaves slot acknowledgement unchanged',self.feedback(),feedback_before)
        self.check('slot feedback does not exceed publication receipt',self.value("SELECT confirmed_flush_lsn<=published_lsn FROM pg_replication_slots r,lake_direct.stream s WHERE r.slot_name='wal_direct_slot'"),'t')
        self.sql("BEGIN;INSERT INTO a VALUES(5,'after response loss','{}');INSERT INTO b VALUES(5,'after response loss');COMMIT")
        self.publish()
        self.check('lost response retry does not duplicate INSERTs',self.value('SELECT count(*) FROM iceberg_mirror.a'),'2')
        # Reject over-budget transactions without partial mirror/receipt changes.
        before=self.state();feedback_before=self.feedback();self.sql("INSERT INTO b SELECT i,'bounded' FROM generate_series(10,12) i")
        failed=self.command(self.worker(LAKE_DIRECT_MAX_ROWS='2'),required=False)
        self.check('oversized transaction fails closed',failed.returncode!=0,True)
        self.check('oversized transaction leaves receipt unchanged',self.state(),before)
        self.check('oversized transaction does not acknowledge WAL',self.feedback(),feedback_before)
        self.check('oversized transaction leaves mirrors unchanged',self.value('SELECT count(*) FROM iceberg_mirror.b'),'2')
        self.publish()
        before=self.state();feedback_before=self.feedback()
        self.sql("INSERT INTO b VALUES(15,repeat('x',4096))")
        failed=self.command(self.worker(LAKE_DIRECT_MAX_BYTES='1024'),required=False)
        self.check('oversized logical message fails closed',failed.returncode!=0,True)
        self.check('oversized logical message preserves receipt',self.state(),before)
        self.check('oversized logical message does not acknowledge WAL',self.feedback(),feedback_before)
        self.publish()
        # Sink error after preceding changes must roll back the complete publication.
        before=self.state();self.sql("DELETE FROM iceberg_mirror.b WHERE id=4;BEGIN;UPDATE a SET note='atomic';UPDATE b SET note='atomic' WHERE id=4;COMMIT")
        failed=self.command(self.worker(),required=False)
        self.check('missing mutation target aborts publication',failed.returncode!=0,True)
        self.check('failed publication leaves receipt unchanged',self.state(),before)
        self.check('failed publication rolls back earlier table writes',self.value("SELECT count(*) FROM iceberg_mirror.a WHERE note='atomic'"),'0')
        self.sql("INSERT INTO iceberg_mirror.b VALUES(4,'after truncate')");self.publish()
        # Actual shared-server restart preserves final contents and direct receipt.
        before=self.state();self.command(self.compose+['restart','postgres']);self.wait('SELECT 1','1')
        self.check('server restart preserves direct receipt',self.state(),before)
        self.sql("INSERT INTO b VALUES(20,'after restart')");self.publish()
        if os.environ.get('WAL_DIRECT_CRASH_TESTS')=='1':
            for number,point in enumerate(['before_upload_completion','after_upload_completion','after_commit'],100):
                before=self.state();started=datetime.now(timezone.utc).isoformat()
                self.sql(f"BEGIN;INSERT INTO a VALUES({number},'crash','{{}}');INSERT INTO b VALUES({number},'crash');COMMIT")
                failed=self.command(self.worker(LAKE_DIRECT_CRASH_POINT=point),required=False)
                self.check(point+': publication response lost',failed.returncode!=0,True)
                self.wait('SELECT 1','1')
                logs=self.command(self.compose+['logs','--no-color','--since',started,'postgres'])
                self.check(point+': shared PostgreSQL crashes and recovers','was terminated by signal 9' in logs.stdout+logs.stderr,True)
                self.check(point+': receipt follows actual commit outcome',self.state()!=before,point=='after_commit')
                if point=='after_commit':
                    # A new source transaction lets the resumed decoder leave its read loop.
                    self.sql(f"INSERT INTO b VALUES({number+1000},'after committed crash')")
                self.publish()
                self.check(point+': exactly one committed row',self.value(f'SELECT count(*) FROM iceberg_mirror.a WHERE id={number}'),'1')
        # Frozen schema failure: do not silently acknowledge unsupported DDL.
        before=self.state();self.sql("ALTER TABLE a ADD COLUMN extra text;INSERT INTO a VALUES(30,'schema change','{}','new')")
        failed=self.command(self.worker(),required=False)
        self.check('changed schema fails closed',failed.returncode!=0,True)
        self.check('changed schema does not advance receipt',self.state(),before)
        self.journal_empty()
        self.sql("SELECT pg_drop_replication_slot('wal_direct_slot');SELECT pg_create_logical_replication_slot('wal_direct_slot','pgoutput')")
        failed=self.command(self.worker(),required=False)
        self.check('replacement slot cannot bridge missing history',failed.returncode!=0,True)
        self.check('replacement slot does not advance receipt',self.state(),before)
        self.sql("SELECT pg_drop_replication_slot('wal_direct_slot')")


if __name__=='__main__':sys.exit(DirectProbe().run())

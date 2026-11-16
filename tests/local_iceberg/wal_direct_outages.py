#!/usr/bin/env python3
"""Direct publication during storage outage and actual slot WAL-history loss."""
import json
import os
import sys
from probe import ROOT
from wal_direct_streaming import StreamingProbe
from wal_direct_snapshot import SnapshotProbe

class OutageProbe(StreamingProbe):
    def __init__(self):
        super().__init__();self.compose+=['-f',str(ROOT/'deploy/local/wal-retention.compose.yml')]

    def worker(self,**settings):
        return super().worker(**dict(dict(LAKE_DIRECT_STREAMING=os.environ.get('WAL_DIRECT_OUTAGE_STREAMING','1'),LAKE_DIRECT_DEBUG_STREAMING='0',LAKE_DIRECT_PUBLISH_INTERVAL_MS='250'),**settings))

    def exercise(self):
        self.evidence.update(scope=__doc__,protocol_streaming=os.environ.get('WAL_DIRECT_OUTAGE_STREAMING','1'),unqualified=['production outage duration','automatic supervision/rebuild','physical disk exhaustion','power loss','retention limit as a hard disk ceiling'])
        self.bootstrap();self.sql('ALTER DATABASE capture SET lake_replication.group_independent_inserts=on')
        self.sql("INSERT INTO a VALUES(1,'baseline',NULL)");self.command(self.worker());self.equal()
        before=self.state();feedback=self.feedback();retained_before=int(self.value("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name='wal_direct_slot'"))
        p,log,path=self.start('storage-outage',5)
        self.command(self.compose+['stop','storage'],timeout=30)
        try:
            for i in range(10,15):self.sql(f"SET statement_timeout='3s';BEGIN;INSERT INTO a VALUES({i},repeat('outage',100),NULL);INSERT INTO b VALUES({i},'outage');COMMIT")
            self.check('application commits continue while storage is down',self.value('SELECT (SELECT count(*) FROM a)::text||\':\'||(SELECT count(*) FROM b)'),'6:5')
            p.wait(timeout=100);self.check('storage outage stops publisher with an error',p.returncode!=0,True)
            self.check('outage does not advance direct checkpoint',self.state(),before);self.check('outage does not acknowledge source WAL',self.feedback(),feedback)
            self.check('outage leaves mirrors at last complete publication',self.value('SELECT (SELECT count(*) FROM iceberg_mirror.a)::text||\':\'||(SELECT count(*) FROM iceberg_mirror.b)'),'1:0')
            retained=int(self.value("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint FROM pg_replication_slots WHERE slot_name='wal_direct_slot'"))
            self.check('retained WAL grows while publication is unavailable',retained>retained_before,True)
            self.evidence['storage_outage']=dict(retained_before=retained_before,retained_after=retained,checkpoint=before,feedback=feedback)
            self.journal_empty()
        finally:
            self.command(self.compose+['start','storage'],timeout=30);self.stop(p,log,path)
        self.command(self.worker(LAKE_DIRECT_MAX_COMMITS='5'),timeout=150);self.equal();self.journal_empty()
        # Retention is enforced by PostgreSQL at checkpoints. It can invalidate
        # required history; stopping a consumer alone cannot bound WAL growth.
        self.check('fixture has a one-MiB retention setting',self.value('SHOW max_slot_wal_keep_size'),'1MB')
        before=self.state();feedback=self.feedback();self.sql("INSERT INTO b VALUES(20,'pending when history is removed')")
        latest=''
        for attempt in range(8):
            self.sql("DO $$ BEGIN FOR counter IN 1..24 LOOP PERFORM pg_logical_emit_message(false,'direct_retention',repeat('x',1048576),true); END LOOP;END $$;CHECKPOINT")
            latest=self.value("SELECT coalesce(wal_status,'')||':'||coalesce(invalidation_reason,'') FROM pg_replication_slots WHERE slot_name='wal_direct_slot'")
            if latest=='lost:wal_removed':break
        self.evidence['retention_overrun']=dict(emitted_message_bytes=(attempt+1)*24*1048576,slot_state=latest)
        self.check('required WAL is actually removed',latest,'lost:wal_removed')
        failed=self.command(self.worker(),required=False);self.check('lost history fails closed',failed.returncode!=0 and 'cannot resume' in failed.stderr,True)
        self.check('lost history preserves last checkpoint',self.state(),before);self.check('lost history preserves source confirmation',self.feedback(),feedback)
        self.check('worker retains invalidated slot for diagnosis',self.value("SELECT count(*) FROM pg_replication_slots WHERE slot_name='wal_direct_slot' AND invalidation_reason='wal_removed'"),'1')
        self.sql("SET statement_timeout='3s';INSERT INTO b VALUES(21,'application survives history loss')")
        self.check('application remains writable after WAL loss',self.value('SELECT count(*) FROM b'),'7')
        self.check('old mirror remains at complete publication',self.value('SELECT count(*) FROM iceberg_mirror.b'),'5')
        # Explicit operator rebuild in this disposable fixture. Preserve old
        # mirrors, remove invalid control/slot, install fresh experiment state,
        # and export a new snapshot. The worker never performs this itself.
        self.sql("ALTER SCHEMA iceberg_mirror RENAME TO previous_mirror;SELECT pg_drop_replication_slot('wal_direct_slot');DROP SCHEMA lake_direct CASCADE")
        self.sql((ROOT/'experiments/wal_direct/install.sql').read_text())
        enable_prelock(self)
        self.command(SnapshotProbe.initializer(self),timeout=180);self.equal();self.journal_empty()
        self.check('explicit fresh snapshot preserves old complete mirrors',self.value('SELECT count(*) FROM previous_mirror.b'),'5')
        self.sql("INSERT INTO b VALUES(22,'after explicit rebuild')");self.command(self.worker());self.equal()
        self.check('rebuilt stream continues from fresh history',self.value('SELECT count(*) FROM iceberg_mirror.b'),'8')

if __name__=='__main__':sys.exit(OutageProbe().run())

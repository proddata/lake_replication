#!/usr/bin/env python3
"""Object-store outage backpressures the local mirror, not application commits."""
import sys
from probe import LocalIcebergProbe


class LocalOutageProbe(LocalIcebergProbe):
    def exercise(self):
        self.evidence.update(scope='single-instance object-store outage, bounded local journal and replay recovery',
            unqualified=['physical WAL disk exhaustion','hard memory limits','unlimited outage duration'])
        self.setup()
        before=int(self.value('SELECT total_failures FROM lake_replication.service_status'))
        self.command(self.compose+['stop','storage'],timeout=30)
        try:
            for i in range(10,30):
                self.sql(f"SET statement_timeout='3s';BEGIN;INSERT INTO a VALUES({i},'during storage outage');INSERT INTO b VALUES({i},'during storage outage');COMMIT")
            self.check('application heap commits survive object-store unavailability',self.value('SELECT (SELECT count(*) FROM a)::text||\':\'||(SELECT count(*) FROM b)'),'21:21')
            self.check('mirror outage is reported in service failure history',self.wait(f'SELECT (total_failures>{before})::text FROM lake_replication.service_status','true',timeout=90),'true')
            self.check('receiver obeys the local committed journal transaction budget',self.value("SELECT (count(*)<=max(p.max_transactions))::text FROM lake_replication.transactions t JOIN lake_replication.journal_limits p USING(subscription_id) WHERE t.state<>'published'"),'true')
            self.check('receiver obeys the local committed journal byte budget',self.value("SELECT (coalesce(sum(t.journal_payload_bytes),0)<=max(p.max_payload_bytes))::text FROM lake_replication.transactions t JOIN lake_replication.journal_limits p USING(subscription_id) WHERE t.state<>'published'"),'true')
            self.check('local slot retains recovery authority during the outage',self.value("SELECT count(*) FROM pg_replication_slots WHERE database=current_database() AND plugin='pgoutput'"),'1')
            self.evidence['outage_state']=self.value("SELECT json_build_object('staged',staged_lsn,'published',published_lsn,'wal_retained_bytes',(SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn) FROM pg_replication_slots WHERE plugin='pgoutput')) FROM lake_replication.checkpoints")
        finally:self.command(self.compose+['start','storage'],timeout=30)
        self.wait('SELECT count(*) FROM iceberg_mirror.a','21',timeout=120);self.wait('SELECT count(*) FROM iceberg_mirror.b','21',timeout=120)
        self.equal()
        self.check('object-store recovery returns the local subscription to healthy',self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'healthy',timeout=90),'healthy')
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('outage recovery cleanup removes its local slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(LocalOutageProbe().run())

#!/usr/bin/env python3
"""Local mirror compaction and checkpoint-bounded journal cleanup."""
import sys
from probe import LocalIcebergProbe


class LocalMaintenanceProbe(LocalIcebergProbe):
    def files(self,table):
        return int(self.value(f"SELECT count(*) FROM lake_iceberg.files((SELECT metadata_location FROM iceberg_tables WHERE table_namespace='iceberg_mirror' AND table_name='{table}'))"))

    def exercise(self):
        self.evidence.update(scope='single-instance mirror data-file compaction and safe bounded journal cleanup',
            unqualified=['immediate physical object deletion','external catalog readers','production maintenance sizing'])
        self.setup()
        self.sql('ALTER FOREIGN TABLE iceberg_mirror.a OPTIONS(ADD autovacuum_enabled \'false\');ALTER FOREIGN TABLE iceberg_mirror.b OPTIONS(ADD autovacuum_enabled \'false\')')
        for i in range(3,13):
            self.sql(f"BEGIN;INSERT INTO a VALUES({i},'small commit');INSERT INTO b VALUES({i},'small commit');COMMIT")
            self.wait('SELECT max(id) FROM iceberg_mirror.a',str(i));self.wait('SELECT max(id) FROM iceberg_mirror.b',str(i))
        self.equal()
        before={table:self.files(table) for table in 'ab'}
        self.check('small local publications produce multiple data files',all(v>1 for v in before.values()),True)
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        sub=self.value("SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='local_iceberg'")
        checkpoint=self.value(f'SELECT published_lsn::text||\':\'||acknowledged_lsn::text FROM lake_replication.checkpoints WHERE subscription_id={sub}')
        for table in 'ab':
            self.sql(f'VACUUM iceberg_mirror.{table}')
            self.check('native VACUUM compacts current mirror files '+table,self.files(table),1)
        self.equal()
        self.sql(f"SELECT lake_replication.configure_journal_maintenance({sub},true,'0 seconds','1 second',3)")
        total=int(self.value(f'SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={sub}'))
        self.check('published local journal retains cleanup candidates',total>3,True)
        deleted=int(self.value(f'SELECT transactions_deleted FROM lake_replication.run_journal_maintenance({sub},true)'))
        self.check('one journal cleanup obeys the configured transaction bound',deleted,3)
        for _ in range(20):
            remaining=int(self.value(f'SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={sub}'))
            if not remaining:break
            self.value(f'SELECT transactions_deleted FROM lake_replication.run_journal_maintenance({sub},true)')
        self.check('published acknowledged journal history is reclaimable',self.value(f'SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={sub}'),'0')
        self.check('maintenance preserves durable publication and ACK boundaries',self.value(f'SELECT published_lsn::text||\':\'||acknowledged_lsn::text FROM lake_replication.checkpoints WHERE subscription_id={sub}'),checkpoint)
        self.evidence['compaction']=dict(before_current_data_files=before,after_current_data_files={t:self.files(t) for t in 'ab'},journal_transactions_before=total)
        self.sql('ALTER SUBSCRIPTION local_iceberg ENABLE')
        self.sql("BEGIN;UPDATE a SET note='after maintenance' WHERE id=1;DELETE FROM b WHERE id=3;INSERT INTO b VALUES(13,'after maintenance');COMMIT")
        self.wait("SELECT note FROM iceberg_mirror.a WHERE id=1",'after maintenance');self.wait('SELECT max(id) FROM iceberg_mirror.b','13');self.equal()
        self.check('maintenance does not alter application-table selection',self.value("SELECT string_agg(schemaname||'.'||tablename,',' ORDER BY tablename) FROM pg_publication_tables WHERE pubname='local_iceberg_pub'"),'public.a,public.b')
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('post-maintenance cleanup releases the local slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(LocalMaintenanceProbe().run())

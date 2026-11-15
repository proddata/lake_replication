#!/usr/bin/env python3
"""Opt-in publication coalescing preserves ingestion, stop and restart fences."""
import sys
import time
from probe import LocalIcebergProbe


class CoalescingProbe(LocalIcebergProbe):
    def exercise(self):
        self.evidence.update(scope='bounded opt-in local publication coalescing with managed stop/start',
            unqualified=['hard real-time publication deadlines','production sizing'])
        self.setup(mirror=False,seed=False)
        self.check('default publication behaviour remains immediate',self.value("SHOW lake_replication.publish_interval_ms"),'0')
        denied=self.sql("SELECT lake_replication.destination_system_identifier();SET ROLE local_worker;SET lake_replication.publish_interval_ms=1",required=False)
        self.check('unprivileged worker cannot change publication policy',denied.returncode!=0 and 'permission denied to set parameter' in denied.stderr,True)
        rejected=self.sql('SET lake_replication.publish_interval_ms=60001',required=False)
        self.check('publication coalescing is bounded to sixty seconds',rejected.returncode!=0,True)
        self.sql('ALTER DATABASE capture SET lake_replication.publish_interval_ms=60000')
        self.start_subscription()
        self.wait_initial_copy()
        before=int(self.value('SELECT count(*) FROM lake_replication.batches'))
        for i in range(1,3):self.sql(f"BEGIN;INSERT INTO a VALUES({i},'queued');INSERT INTO b VALUES({i},'queued');COMMIT")
        self.check('receiver admits all transactions while publication is delayed',self.wait("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'",'2'),'2')
        time.sleep(1)
        self.check('opt-in interval actually holds back small publications',self.value('SELECT (SELECT count(*) FROM iceberg_mirror.a)::text||\':\'||(SELECT count(*) FROM iceberg_mirror.b)'),'0:0')
        began=time.monotonic();self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        elapsed=time.monotonic()-began
        self.check('stop remains responsive during a sixty-second publication delay',elapsed<10,True)
        self.evidence['stop_elapsed_seconds']=elapsed
        self.check('stop preserves admitted work without publishing it',self.value("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'"),'2')
        self.sql('ALTER DATABASE capture SET lake_replication.publish_interval_ms=1000;ALTER SUBSCRIPTION local_iceberg ENABLE')
        self.wait('SELECT count(*) FROM iceberg_mirror.a','2');self.wait('SELECT count(*) FROM iceberg_mirror.b','2');self.equal()
        self.check('restart publishes the two queued transactions in one batch',int(self.value('SELECT count(*) FROM lake_replication.batches'))-before,1)
        self.check('published transaction receipts are complete',self.value("SELECT count(*) FROM lake_replication.transactions WHERE state='published'"),'2')
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        self.sql('ALTER DATABASE capture SET lake_replication.publish_interval_ms=60000;UPDATE lake_replication.journal_limits SET max_transactions=2;ALTER SUBSCRIPTION local_iceberg ENABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'healthy')
        began=time.monotonic()
        self.sql("BEGIN;INSERT INTO a VALUES(3,'full journal');INSERT INTO b VALUES(3,'full journal');COMMIT;BEGIN;INSERT INTO a VALUES(4,'full journal');INSERT INTO b VALUES(4,'full journal');COMMIT")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','4',timeout=15)
        self.check('full journal overrides the configured sixty-second delay',time.monotonic()-began<15,True)
        self.equal()
        self.sql('UPDATE lake_replication.journal_limits SET max_transactions=32,max_payload_bytes=(SELECT max(journal_payload_bytes)+16 FROM lake_replication.transactions)')
        began=time.monotonic()
        self.sql("BEGIN;INSERT INTO a VALUES(5,'full journal');INSERT INTO b VALUES(5,'full journal');COMMIT")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','5',timeout=15)
        self.check('journal byte pressure also overrides the sixty-second delay',time.monotonic()-began<15,True)
        self.equal()
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('coalesced managed cleanup removes the local slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(CoalescingProbe().run())

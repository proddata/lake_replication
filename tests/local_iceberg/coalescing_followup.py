#!/usr/bin/env python3
"""A new arrival during publication must obey the next coalescing window."""
import sys
import time
from probe import LocalIcebergProbe
from session import Session


class FollowupCoalescingProbe(LocalIcebergProbe):
    def exercise(self):
        self.evidence.update(scope=__doc__,unqualified=['hard real-time deadlines','production capacity'])
        self.setup(mirror=False,seed=False)
        self.sql('ALTER DATABASE capture SET lake_replication.publish_interval_ms=60000')
        self.start_subscription();self.wait_initial_copy()
        self.sql('''CREATE FUNCTION public.hold_first_publication() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.id<=2 THEN PERFORM pg_advisory_xact_lock(772001);END IF;RETURN NEW;END $$;
            CREATE TRIGGER first_publication BEFORE INSERT ON iceberg_mirror.a
            FOR EACH ROW EXECUTE FUNCTION public.hold_first_publication();
            UPDATE lake_replication.journal_limits SET max_transactions=2''')
        barrier=Session(self.sql_args())
        try:
            barrier.query('SELECT pg_advisory_lock(772001)')
            for i in [1,2]:self.sql(f"BEGIN;INSERT INTO a VALUES({i},'first batch');INSERT INTO b VALUES({i},'first batch');COMMIT")
            self.check('journal pressure starts a publication behind the barrier',self.wait("SELECT count(*) FROM pg_stat_activity WHERE wait_event='advisory' AND application_name LIKE 'lake_replication_worker/%'",'1'),'1')
            self.sql('UPDATE lake_replication.journal_limits SET max_transactions=32')
            self.sql("BEGIN;INSERT INTO a VALUES(3,'arrives during publication');INSERT INTO b VALUES(3,'arrives during publication');COMMIT")
            # The first publication's state changes are uncommitted behind the
            # barrier, so all three receipts still appear staged to this reader.
            self.check('new source transaction is staged while publication is blocked',self.wait("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'",'3'),'3')
            barrier.query('SELECT pg_advisory_unlock(772001)')
            self.wait("SELECT (count(*)>=2)::text FROM lake_replication.transactions WHERE state='published'",'true')
            time.sleep(2)
            self.check('new arrival waits instead of bypassing coalescing in the child',self.value('SELECT count(*) FROM iceberg_mirror.a'),'2')
            self.check('follow-up source transaction remains durably staged',self.value("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'"),'1')
            self.sql('UPDATE lake_replication.journal_limits SET max_transactions=2')
            self.sql("BEGIN;INSERT INTO a VALUES(4,'pressure again');INSERT INTO b VALUES(4,'pressure again');COMMIT")
            self.check('follow-up pressure flushes without waiting sixty seconds',self.wait('SELECT count(*) FROM iceberg_mirror.a','4',timeout=15),'4')
            self.equal()
            self.sql("BEGIN;INSERT INTO a VALUES(5,'stop while waiting again');INSERT INTO b VALUES(5,'stop while waiting again');COMMIT")
            self.wait("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'",'1')
            began=time.monotonic();self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
            self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
            self.check('stop remains responsive between publications',time.monotonic()-began<10,True)
            self.check('stop retains follow-up work',self.value("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'"),'1')
            self.sql('ALTER DATABASE capture SET lake_replication.publish_interval_ms=0;ALTER SUBSCRIPTION local_iceberg ENABLE')
            self.wait('SELECT count(*) FROM iceberg_mirror.a','5');self.wait('SELECT count(*) FROM iceberg_mirror.b','5');self.equal()
            self.check('all source receipts publish exactly once',self.value("SELECT count(*) FROM lake_replication.transactions WHERE state='published'"),'5')
            self.sql('DROP SUBSCRIPTION local_iceberg')
            self.check('follow-up cleanup releases the local slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')
        finally:barrier.close()


if __name__=='__main__':sys.exit(FollowupCoalescingProbe().run())

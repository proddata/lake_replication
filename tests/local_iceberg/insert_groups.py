#!/usr/bin/env python3
"""Guarded INSERT grouping: file reduction, barriers and concurrent DDL."""
from concurrent.futures import ThreadPoolExecutor
import sys
from probe import LocalIcebergProbe
from session import Session


class InsertGroupingProbe(LocalIcebergProbe):
    def files(self,table):
        return int(self.value(f"SELECT count(*) FROM lake_table.files WHERE table_name='iceberg_mirror.{table}'::regclass AND content=0"))

    def stop(self):
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')

    def enable(self,interval):
        self.sql(f'ALTER DATABASE capture SET lake_replication.publish_interval_ms={interval};ALTER SUBSCRIPTION local_iceberg ENABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'healthy')

    def eligible(self):
        return self.value("SELECT lake_replication.lock_insert_group_targets(ARRAY['iceberg_mirror.a'::regclass,'iceberg_mirror.b'::regclass])::text")

    def exercise(self):
        self.evidence.update(scope='opt-in pure INSERT grouping with native target eligibility, barriers, ACLs and DDL race fences',
            unqualified=['REST catalogs','targets with side-effecting rules/defaults/triggers','global row order within an independent INSERT-only publication'])
        self.setup(mirror=False,seed=False)
        self.check('independent INSERT grouping is off by default',self.value('SHOW lake_replication.group_independent_inserts'),'off')
        self.sql('ALTER TABLE a ADD COLUMN document jsonb;ALTER DATABASE capture SET lake_replication.group_independent_inserts=on;ALTER DATABASE capture SET lake_replication.publish_interval_ms=60000')
        self.start_subscription();self.wait_initial_copy()
        self.sql('UPDATE lake_replication.journal_limits SET max_transactions=32')
        self.check('new local Iceberg targets are eligible',self.eligible(),'true')
        before={t:self.files(t) for t in 'ab'}
        for i in range(1,11):self.sql(f"BEGIN;INSERT INTO a VALUES({i},'n{i}',jsonb_build_object('n',{i}));INSERT INTO b VALUES({i},'n{i}');COMMIT")
        self.wait("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'",'10')
        self.stop();self.enable(0)
        self.wait('SELECT count(*) FROM iceberg_mirror.a','10');self.wait('SELECT count(*) FROM iceberg_mirror.b','10');self.equal()
        for table in 'ab':self.check('ten source INSERT transactions produce one new file for '+table,self.files(table)-before[table],1)
        self.sql("BEGIN;UPDATE a SET note='updated' WHERE id=1;DELETE FROM b WHERE id=1;INSERT INTO b VALUES(1,'reinserted');TRUNCATE a;INSERT INTO a VALUES(20,'after truncate','[]');COMMIT")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','1');self.wait('SELECT id FROM iceberg_mirror.a','20');self.wait('SELECT note FROM iceberg_mirror.b WHERE id=1','reinserted');self.equal()
        self.stop()
        self.sql('''CREATE TABLE public.local_insert_order(sequence bigserial,relation text,id bigint);
            GRANT INSERT,SELECT ON public.local_insert_order TO local_worker;
            GRANT USAGE ON SEQUENCE public.local_insert_order_sequence_seq TO local_worker;
            CREATE FUNCTION public.record_local_insert() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
            BEGIN INSERT INTO public.local_insert_order(relation,id) VALUES(TG_TABLE_NAME,NEW.id);RETURN NEW;END $$;
            CREATE TRIGGER observe_insert BEFORE INSERT ON iceberg_mirror.a FOR EACH ROW EXECUTE FUNCTION public.record_local_insert();
            CREATE TRIGGER observe_insert BEFORE INSERT ON iceberg_mirror.b FOR EACH ROW EXECUTE FUNCTION public.record_local_insert();''')
        self.check('user triggers make grouping ineligible',self.eligible(),'false')
        self.enable(60000)
        for i in range(30,33):self.sql(f"BEGIN;INSERT INTO a VALUES({i},'triggered',NULL);INSERT INTO b VALUES({i},'triggered');COMMIT")
        self.wait("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'",'3')
        self.stop();self.enable(0)
        self.wait('SELECT count(*) FROM iceberg_mirror.a','4')
        self.check('triggered writes retain global source operation order',self.value("SELECT string_agg(relation||id::text,',' ORDER BY sequence) FROM public.local_insert_order"),'a30,b30,a31,b31,a32,b32')
        self.equal();self.stop()
        self.sql('DROP TRIGGER observe_insert ON iceberg_mirror.a;DROP TRIGGER observe_insert ON iceberg_mirror.b')
        self.sql("ALTER FOREIGN TABLE iceberg_mirror.a ALTER COLUMN note SET DEFAULT 'default'")
        self.check('destination defaults make grouping ineligible',self.eligible(),'false')
        self.sql('ALTER FOREIGN TABLE iceberg_mirror.a ALTER COLUMN note DROP DEFAULT')
        self.check('removing side effects restores eligibility',self.eligible(),'true')
        self.sql('CREATE ROLE local_lock_only;GRANT USAGE ON SCHEMA lake_replication,iceberg_mirror TO local_lock_only;GRANT EXECUTE ON FUNCTION lake_replication.lock_insert_group_targets(regclass[]) TO local_lock_only')
        denied=self.sql("SET ROLE local_lock_only;SELECT lake_replication.lock_insert_group_targets(ARRAY['iceberg_mirror.a'::regclass])",required=False)
        self.check('native lock helper independently enforces target INSERT privilege',denied.returncode!=0 and 'permission denied' in denied.stderr,True)
        old=Session(self.sql_args());ddl=Session(self.sql_args())
        try:
            old.query("SET application_name='local_group_old';BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT 1 FROM iceberg_mirror.a LIMIT 0;SELECT count(*) FROM pg_trigger WHERE tgrelid='iceberg_mirror.a'::regclass")
            ddl.query('BEGIN;CREATE TRIGGER ddl_guard BEFORE INSERT ON iceberg_mirror.a FOR EACH ROW EXECUTE FUNCTION public.record_local_insert()')
            with ThreadPoolExecutor(max_workers=1) as executor:
                future=executor.submit(old.query,"SELECT lake_replication.lock_insert_group_targets(ARRAY['iceberg_mirror.a'::regclass,'iceberg_mirror.b'::regclass])::text")
                self.wait("SELECT count(*) FROM pg_stat_activity WHERE application_name='local_group_old' AND wait_event_type='Lock'",'1')
                ddl.query('COMMIT')
                self.check('old snapshots see trigger eligibility after a DDL lock wait',future.result(timeout=15),['false'])
            old.query('COMMIT');self.sql('DROP TRIGGER ddl_guard ON iceberg_mirror.a')
            old.query("BEGIN;SELECT lake_replication.lock_insert_group_targets(ARRAY['iceberg_mirror.a'::regclass,'iceberg_mirror.b'::regclass])::text")
            with ThreadPoolExecutor(max_workers=1) as executor:
                future=executor.submit(ddl.query,"SET application_name='local_group_new';CREATE TRIGGER ddl_guard BEFORE INSERT ON iceberg_mirror.b FOR EACH ROW EXECUTE FUNCTION public.record_local_insert()")
                self.wait("SELECT count(*) FROM pg_stat_activity WHERE application_name='local_group_new' AND wait_event_type='Lock'",'1')
                self.check('target locks fence DDL until the publication transaction ends',future.done(),False)
                old.query('COMMIT');future.result(timeout=15)
            self.sql('DROP TRIGGER ddl_guard ON iceberg_mirror.b')
        finally:old.close();ddl.close()
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('grouped INSERT cleanup releases its local slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(InsertGroupingProbe().run())

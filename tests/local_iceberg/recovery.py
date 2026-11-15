#!/usr/bin/env python3
"""Managed local initial-copy interruption and streaming connection recovery."""
import sys
import time
from probe import LocalIcebergProbe


class ManagedLocalRecovery(LocalIcebergProbe):
    def exercise(self):
        self.evidence.update(scope='single-instance managed initial-copy interruption with concurrent heap writes and streaming disconnect',
            unqualified=['power-loss recovery','all resource contention scenarios','production sizing'])
        self.setup(mirror=False,seed=False)
        self.sql("INSERT INTO a SELECT i,'snapshot-a-'||i FROM generate_series(1,6000) i;INSERT INTO b SELECT i,'snapshot-b-'||i FROM generate_series(1,6000) i")
        self.sql('ALTER SYSTEM SET lake_replication.initial_copy_pause_ms=1000');self.sql('SELECT pg_reload_conf()')
        self.start_subscription()
        sub=self.value("SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='local_iceberg'")
        staged=f"SELECT EXISTS(SELECT FROM lake_replication.initial_copies WHERE subscription_id={sub} AND state='copying' AND rows_copied>0)::text"
        self.check('managed local runtime stages a partial initial snapshot',self.wait(staged,'true'),'true')
        before=self.value('SELECT pg_postmaster_start_time()')
        old=int(self.value(f'SELECT supervisor_pid FROM lake_replication.service_status WHERE subscription_id={sub}'))
        self.check('initial copy runs as the dedicated unprivileged runtime',self.value("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE usename='local_worker')::text"),'true')
        self.sql("BEGIN;UPDATE a SET note='updated during snapshot' WHERE id=1;DELETE FROM b WHERE id=3;INSERT INTO a VALUES(7001,'during snapshot');INSERT INTO b VALUES(7001,'during snapshot');COMMIT")
        self.command(self.compose+['exec','-T','postgres','kill','-KILL',str(old)])
        self.check('launcher replaces the interrupted local supervisor',self.wait(f'SELECT (supervisor_pid IS NOT NULL AND supervisor_pid<>{old})::text FROM lake_replication.service_status WHERE subscription_id={sub}','true'),'true')
        self.check('worker interruption does not restart application PostgreSQL',self.value('SELECT pg_postmaster_start_time()'),before)
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'healthy',timeout=120)
        self.wait('SELECT count(*) FROM iceberg_mirror.a','6001',timeout=120)
        self.wait('SELECT count(*) FROM iceberg_mirror.b','6000',timeout=120)
        self.wait("SELECT note FROM iceberg_mirror.a WHERE id=1",'updated during snapshot')
        self.equal()
        self.check('abandoned snapshot rows do not create duplicate mirror keys',self.value('SELECT (SELECT count(*)=count(DISTINCT id) FROM iceberg_mirror.a)::text||\':\'||(SELECT count(*)=count(DISTINCT id) FROM iceberg_mirror.b)::text'),'true:true')
        self.check('initial copy retains only its persistent local slot',self.value("SELECT count(*) FROM pg_replication_slots WHERE database=current_database() AND plugin='pgoutput'"),'1')
        self.sql('ALTER SYSTEM RESET lake_replication.initial_copy_pause_ms');self.sql('SELECT pg_reload_conf()')
        self.wait("SELECT count(*) FROM pg_stat_activity WHERE backend_type='walsender' AND usename='local_source'",'1')
        pid=int(self.value("SELECT pid FROM pg_stat_activity WHERE backend_type='walsender' AND usename='local_source'"))
        self.check('disconnect the actual local replication connection',self.value(f'SELECT pg_terminate_backend({pid},5000)'),'t')
        self.sql("BEGIN;UPDATE a SET note='after disconnect' WHERE id=1;DELETE FROM b WHERE id=4;INSERT INTO b VALUES(7002,'after disconnect');COMMIT")
        self.wait("SELECT note FROM iceberg_mirror.a WHERE id=1",'after disconnect',timeout=90)
        self.wait("SELECT note FROM iceberg_mirror.b WHERE id=7002",'after disconnect',timeout=90)
        self.equal()
        self.check('stream reconnect keeps exactly one local sender',self.wait("SELECT count(*) FROM pg_stat_activity WHERE backend_type='walsender' AND usename='local_source'",'1'),'1')
        self.check('stream reconnect preserves application postmaster',self.value('SELECT pg_postmaster_start_time()'),before)
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('managed local cleanup removes its persistent slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE database=current_database() AND plugin='pgoutput'",'0'),'0')
        self.check('managed local cleanup preserves heap and Iceberg contents',self.value('SELECT (SELECT count(*) FROM a)::text||\':\'||(SELECT count(*) FROM iceberg_mirror.a)'),'6001:6001')


if __name__=='__main__':sys.exit(ManagedLocalRecovery().run())

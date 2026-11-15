#!/usr/bin/env python3
"""A local slot losing retained WAL fails closed and supports a fresh snapshot."""
import sys
from probe import LocalIcebergProbe,ROOT


class LocalWalRetentionProbe(LocalIcebergProbe):
    def __init__(self):
        super().__init__()
        self.compose+=['-f',str(ROOT/'deploy/local/wal-retention.compose.yml')]

    def exercise(self):
        self.evidence.update(
            scope='single-instance WAL retention overrun, fail-closed checkpoints and explicit snapshot rebuild',
            unqualified=['physical PGDATA/WAL ENOSPC', 'power-loss recovery', 'production WAL sizing'])
        self.setup()
        self.sql("INSERT INTO a VALUES(3,'before retention overrun')")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','2')
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        slot=self.value("SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
        before=self.value("SELECT staged_lsn||':'||published_lsn||':'||acknowledged_lsn FROM lake_replication.checkpoints")
        confirmed=self.value(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{slot}'")
        self.check('private fixture starts with a one-MiB WAL retention limit',self.value('SHOW max_slot_wal_keep_size'),'1MB')
        self.sql("INSERT INTO a VALUES(4,'must survive the rebuild');INSERT INTO b VALUES(5,'must not be skipped')")
        latest=''
        for attempt in range(8):
            # Bounded to 192 MiB of WAL in this disposable private PGDATA.
            # CHECKPOINT enforces retention; no host filesystem is filled.
            self.sql("""DO $$ BEGIN FOR counter IN 1..24 LOOP
                PERFORM pg_logical_emit_message(false,'local_retention',repeat('x',1048576),true);
                END LOOP;END $$;CHECKPOINT""")
            latest=self.value(f"SELECT coalesce(wal_status,'')||':'||coalesce(invalidation_reason,'') FROM pg_replication_slots WHERE slot_name='{slot}'")
            if latest=='lost:wal_removed':break
        self.evidence['wal_retention_overrun']=dict(emitted_message_bytes=(attempt+1)*24*1048576,slot_state=latest)
        self.check('PostgreSQL removes required WAL from the inactive local slot',latest,'lost:wal_removed')
        self.sql('ALTER SUBSCRIPTION local_iceberg ENABLE')
        self.check('local mirror reports lost WAL as critical',self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'critical'),'critical')
        self.check('local source error names lost WAL and its cause',self.wait("SELECT (last_source_error LIKE '%lost WAL%' AND last_source_error LIKE '%wal_removed%')::text FROM lake_replication.service_status",'true'),'true')
        self.check('lost history does not advance local checkpoints',self.value("SELECT staged_lsn||':'||published_lsn||':'||acknowledged_lsn FROM lake_replication.checkpoints"),before)
        self.check('lost history does not advance source confirmation',self.value(f"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='{slot}'"),confirmed)
        self.check('local worker does not replace the invalidated slot',self.value(f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{slot}' AND invalidation_reason='wal_removed'"),'1')
        self.check('mirror retains its last complete contents',self.value("SELECT (SELECT count(*) FROM iceberg_mirror.a)::text||':'||(SELECT count(*) FROM iceberg_mirror.b)"),'2:1')
        self.sql("SET statement_timeout='3s';INSERT INTO a VALUES(6,'application remains writable')")
        self.check('application commits continue after local WAL loss',self.value('SELECT count(*) FROM a'),'4')
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('drop removes the invalidated owned local slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')
        # Keep the last complete mirror while a fresh target is snapshotted.
        self.sql('ALTER SCHEMA iceberg_mirror RENAME TO previous_mirror;CREATE SCHEMA iceberg_mirror')
        self.command(self.sql_args()+['-v','worker_login=local_worker','-v','destination_schema=iceberg_mirror'],
            input=(ROOT/'deploy/install/worker-grants.sql').read_text())
        self.start_subscription();self.wait_initial_copy()
        self.wait('SELECT count(*) FROM iceberg_mirror.a','4')
        self.wait('SELECT count(*) FROM iceberg_mirror.b','2');self.equal()
        self.sql("INSERT INTO b VALUES(7,'streaming resumes after rebuild')")
        self.wait('SELECT count(*) FROM iceberg_mirror.b','3');self.equal()
        self.check('fresh snapshot restores healthy local streaming',self.value("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'"),'healthy')
        self.check('rebuild preserves the previous complete mirror for readers',self.value("SELECT (SELECT count(*) FROM previous_mirror.a)::text||':'||(SELECT count(*) FROM previous_mirror.b)"),'2:1')
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('rebuilt local subscription cleans up its slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(LocalWalRetentionProbe().run())

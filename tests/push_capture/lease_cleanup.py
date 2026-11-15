#!/usr/bin/env python3
"""Cleanup waits for manual capture/maintenance leases, including idle slots."""
import sys
from live import LiveProbe
from session import Session


class LeaseCleanupProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='source reset/drop coordination with manual actor lease holders',
            unqualified=['automatic failover','untrusted capture operators'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        for operation in ['drop','reset']:
            self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
            self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
            self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
            self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
            self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
            self.source_wait("SELECT active::text FROM pg_replication_slots WHERE plugin='lake_replication_capture'",'false')
            actor=Session(self.sql_args())
            try:
                actor.query(f"SELECT pg_advisory_lock(hashtextextended('lake_capture/{self.stream}',0))")
                self.sql(f"SELECT lake_capture.{operation}_stream('{self.stream}')")
                # A committed tuple-lock xmax proves the manager completed a
                # cleanup attempt. This fixture has one locker and no XID wrap.
                self.source_wait(f"SELECT (desired_state='{operation}' AND CASE WHEN xmax='0'::xid THEN false ELSE pg_xact_status(xmax::text::xid8)='committed' END)::text FROM lake_capture.streams",'true')
                self.check(operation+': manager waits for a lease even though the slot is idle',self.value('SELECT count(*) FROM lake_capture.streams'),'1')
                self.check(operation+': waiting cleanup retains protected source WAL',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'1')
                self.check(operation+': waiting cleanup preserves the current generation',self.value('SELECT capture_generation FROM lake_capture.streams'),'1')
                actor.query(f"SELECT pg_advisory_unlock(hashtextextended('lake_capture/{self.stream}',0))")
            finally:actor.close()
            if operation=='drop':
                self.source_wait('SELECT count(*) FROM lake_capture.streams','0')
            else:
                self.source_wait("SELECT (capture_generation=2 AND desired_state='stopped' AND baseline_lsn IS NULL)::text FROM lake_capture.streams",'true')
                self.sql(f"SELECT lake_capture.drop_stream('{self.stream}')")
                self.source_wait('SELECT count(*) FROM lake_capture.streams','0')
            self.check(operation+': released lease permits owned-slot cleanup',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'0')


if __name__=='__main__':sys.exit(LeaseCleanupProbe().run())

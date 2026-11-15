#!/usr/bin/env python3
"""Oversized baseline fails before rows, manifests or upload reservations."""
import sys
import time
from live import LiveProbe

class SnapshotLimitProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='bounded paused/online snapshot rejection and controlled slot cleanup',
            unqualified=['unbounded/chunked baseline support'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            INSERT INTO a SELECT i,'over-row-budget' FROM generate_series(1,100000) i;''')
        for mode in ['paused_copy','online_copy']:
            self.stream=self.value(f"SELECT lake_capture.register_stream('a','b',NULL,'{mode}')")
            self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
            deadline=time.monotonic()+35
            while time.monotonic()<deadline:
                if self.value('SELECT desired_state FROM lake_capture.streams')=='stopped':break
                time.sleep(.1)
            self.check(mode+' rejects an oversized row inventory explicitly',self.value("SELECT (desired_state='stopped' AND last_error LIKE '%row budget%')::text FROM lake_capture.streams"),'true')
            self.check(mode+' cannot publish a partial baseline or reserve uploads',self.value('''SELECT
                captured_sequence::text||':'||(SELECT count(*) FROM lake_capture_log.manifests)||':'||upload_attempt_count
                FROM lake_capture.streams'''),'0:0:0')
            self.sql(f"SELECT lake_capture.drop_stream('{self.stream}')")
            self.source_wait('SELECT count(*) FROM lake_capture.streams','0')
            self.check(mode+' controlled cleanup removes any provisional slot',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'0')

if __name__=='__main__':sys.exit(SnapshotLimitProbe().run())

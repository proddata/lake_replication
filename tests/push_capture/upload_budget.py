#!/usr/bin/env python3
"""Pre-upload metadata failure must not upload or spin on a fatal generation."""
import sys
import time
from live import LiveProbe


class UploadBudgetProbe(LiveProbe):
    def exercise(self):
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.sql('''CREATE FUNCTION public.fail_upload_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected metadata disk full before upload' USING ERRCODE='53100'; END $$;
            CREATE TRIGGER fail_upload_reservation BEFORE INSERT ON lake_capture.upload_attempts
                FOR EACH ROW EXECUTE FUNCTION public.fail_upload_reservation();
            CREATE FUNCTION public.fail_error_status() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.desired_state='stopped' THEN RAISE EXCEPTION 'injected metadata status write failure' USING ERRCODE='53100'; END IF;RETURN NEW;END $$;
            CREATE TRIGGER fail_error_status BEFORE UPDATE ON lake_capture.streams
                FOR EACH ROW EXECUTE FUNCTION public.fail_error_status();''')
        self.sql("BEGIN;INSERT INTO a VALUES(1,'pending');INSERT INTO b VALUES(1,'pending');COMMIT;")
        self.check('failed reservation terminates its source child',self.source_wait(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0'),True)
        time.sleep(2)
        self.check('fatal generation is not respawned when error status cannot persist',self.value(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'"),'0')
        self.check('failed pre-upload reservation creates no log batch or checkpoint',self.value('''SELECT
            captured_sequence::text||':'||upload_attempt_count||':'||(SELECT count(*) FROM lake_capture_log.manifests)
            FROM lake_capture.streams'''),'0:0:0')
        self.check('failed reservation creates no provisional upload records',self.value('''SELECT count(*) FROM lake_engine.in_progress_files
            WHERE path LIKE '%lake_capture_log%' '''),'0')
        self.sql('DROP TRIGGER fail_upload_reservation ON lake_capture.upload_attempts;DROP TRIGGER fail_error_status ON lake_capture.streams')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.check('explicit repaired generation retries protected source WAL',self.source_wait(
            'SELECT captured_sequence::text FROM lake_capture.streams','1'),True)
        self.check('successful capture consumes exactly one durable reservation',self.value('''SELECT
            upload_attempt_count::text||':'||(SELECT count(*) FROM lake_capture.upload_attempts WHERE completed)
            FROM lake_capture.streams'''),'1:1')


if __name__=='__main__':sys.exit(UploadBudgetProbe().run())

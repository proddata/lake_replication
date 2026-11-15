#!/usr/bin/env python3
"""Durable capture failure counters, SQLSTATE classification and restart history."""
import sys
from live import LiveProbe, ROOT


class TelemetryProbe(LiveProbe):
    def control(self,statement):
        args=self.sql_args();args[args.index('postgres')]='capture_operator'
        return self.command(args,input=statement).stdout.strip()

    def capture_args(self,**settings):
        settings.setdefault('LAKE_SOURCE_USER','capture_operator')
        return super().capture_args(**settings)

    def exercise(self):
        self.evidence.update(scope='best-effort durable capture failure telemetry',
            unqualified=['crash/disconnection failure counts','upload-specific failure counts',
                         'resource telemetry','production monitoring'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            CREATE ROLE capture_operator LOGIN REPLICATION;''')
        self.command(self.sql_args()+['-v','capture_login=capture_operator'],
            input=(ROOT/'deploy/push/source-grants.sql').read_text())
        self.sql('GRANT SELECT,MAINTAIN ON a,b TO capture_operator')
        self.stream=self.control("SELECT lake_capture.register_stream('a','b')")
        self.control(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.control(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql("BEGIN;INSERT INTO a VALUES(1,'pending');INSERT INTO b VALUES(1,'pending');COMMIT")
        for count,code in enumerate(['53100','23505','22023'],1):
            self.sql(f'''CREATE OR REPLACE FUNCTION public.fail_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'injected failure with misleading C=23505' USING ERRCODE='{code}';END $$;''')
            if count==1:
                self.sql('''CREATE TRIGGER fail_reservation BEFORE INSERT ON lake_capture.upload_attempts
                    FOR EACH ROW EXECUTE FUNCTION public.fail_reservation()''')
            self.control(f"SELECT lake_capture.start_stream('{self.stream}')")
            failed=self.command(self.capture_args(),required=False)
            self.check(code+': failed capture exits',failed.returncode!=0,True)
            self.check(code+': failure history records native SQLSTATE and occurrence',self.value(f'''
                SELECT (capture_failure_count={count} AND capture_conflict_count={int(count>=2)}
                    AND last_failure_sqlstate='{code}' AND last_failure_at IS NOT NULL
                    AND desired_state='stopped')::text FROM lake_capture.status'''),'true')
            self.check(code+': telemetry cannot advance protected data',self.value('''
                SELECT captured_sequence::text||':'||upload_attempt_count FROM lake_capture.status'''),'0:0')
        self.sql('DROP TRIGGER fail_reservation ON lake_capture.upload_attempts')
        old=self.value('SELECT config_generation FROM lake_capture.streams')
        self.control(f"SELECT lake_capture.start_stream('{self.stream}')")
        stale=self.command(self.capture_args(LAKE_CONFIG_GENERATION=old),required=False)
        self.check('obsolete worker exits without changing newer failure history',
            stale.returncode!=0 and self.value('SELECT capture_failure_count FROM lake_capture.status')=='3',True)
        self.sql("SELECT lake_capture.start_manager()")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1')
        self.check('successful restart clears active error and preserves cumulative history',self.value('''
            SELECT (last_error IS NULL AND capture_failure_count=3 AND capture_conflict_count=1
                AND last_failure_sqlstate='22023' AND last_failure_at IS NOT NULL)::text
                FROM lake_capture.status'''),'true')
        self.control(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.check('ordinary stop does not count as a capture failure',self.value(
            'SELECT capture_failure_count FROM lake_capture.status'),'3')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.sql('''UPDATE lake_capture.streams SET capture_failure_count=9223372036854775807,
            capture_conflict_count=9223372036854775807;
            CREATE OR REPLACE FUNCTION public.fail_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected conflict at counter limit' USING ERRCODE='23505';END $$;
            CREATE TRIGGER fail_reservation BEFORE INSERT ON lake_capture.upload_attempts
                FOR EACH ROW EXECUTE FUNCTION public.fail_reservation();
            INSERT INTO a VALUES(2,'pending at counter limit')''')
        self.control(f"SELECT lake_capture.start_stream('{self.stream}')")
        failed=self.command(self.capture_args(),required=False)
        self.check('counter saturation cannot prevent persisting a capture failure',
            failed.returncode!=0 and self.value('''SELECT (desired_state='stopped'
                AND capture_failure_count=9223372036854775807 AND capture_conflict_count=9223372036854775807
                AND last_failure_sqlstate='23505' AND captured_sequence=1)::text
                FROM lake_capture.status''')=='true',True)


if __name__=='__main__':sys.exit(TelemetryProbe().run())

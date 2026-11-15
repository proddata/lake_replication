#!/usr/bin/env python3
"""Actual private storage identities, mutable defaults and fail-closed drift."""
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from live import LiveProbe
from session import Session


class LocationProbe(LiveProbe):
    def exercise(self):
        self.evidence.update(scope='catalog-pinned private log destinations and drift fencing',
            unqualified=['automatic storage relocation','external catalog rebind'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SET pg_lake_iceberg.default_location_prefix='s3://lake-push/unused-default';SELECT lake_capture.register_stream('a','b')")
        self.check('registration persists all four actual private log roots',self.value(
            'SELECT (log_locations=lake_capture.current_log_locations() AND (SELECT count(*) FROM jsonb_object_keys(log_locations))=4)::text FROM lake_capture.streams'),'true')
        self.check('registration records storage rather than a changed session default',self.value(
            "SELECT (object_prefix LIKE 's3://lake-push/iceberg/%' AND object_prefix NOT LIKE '%unused-default%')::text FROM lake_capture.streams"),'true')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        self.sql("BEGIN;INSERT INTO a VALUES(1,'first');INSERT INTO b VALUES(1,'first');COMMIT")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('capture session holds all four private relation locks',self.value("SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.application_name LIKE 'lake_capture/%' AND l.mode='AccessShareLock' AND l.relation IN ('lake_capture_log.events_a'::regclass,'lake_capture_log.events_b'::regclass,'lake_capture_log.controls'::regclass,'lake_capture_log.manifests'::regclass)"),'4')
        alter="ALTER FOREIGN TABLE lake_capture_log.events_a OPTIONS (SET max_snapshot_age '0')"
        denied=self.sql("SET lock_timeout='200ms';"+alter,required=False)
        self.check('private DDL is rejected before it can queue behind capture',denied.returncode!=0 and 'private schema is frozen' in denied.stderr,True)
        self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        stopped=self.sql("SET lock_timeout='2s';"+alter,required=False)
        self.check('private schema remains frozen while the stream is stopped',stopped.returncode!=0 and 'private schema is frozen' in stopped.stderr,True)
        self.check('stopped capture releases every private relation lock',self.value("SELECT count(*) FROM pg_locks WHERE mode='AccessShareLock' AND relation IN ('lake_capture_log.events_a'::regclass,'lake_capture_log.events_b'::regclass,'lake_capture_log.controls'::regclass,'lake_capture_log.manifests'::regclass)"),'0')
        for statement in ['ALTER SCHEMA lake_capture_log RENAME TO unsafe_logs','ALTER TABLE lake_capture.streams ADD COLUMN unsafe integer','DROP FOREIGN TABLE lake_capture_log.events_a',"SET search_path=lake_capture_log,public;ALTER FOREIGN TABLE events_a OPTIONS (SET max_snapshot_age '0')"]:
            denied=self.sql(statement,required=False)
            self.check('private DDL fails closed: '+statement,denied.returncode!=0 and 'private schema is frozen' in denied.stderr,True)
        self.sql("CREATE TABLE unrelated_log(id bigint) USING iceberg;ALTER FOREIGN TABLE unrelated_log OPTIONS (ADD max_snapshot_age '0');DROP FOREIGN TABLE unrelated_log")
        self.check('unrelated foreign-table DDL remains independent',self.value("SELECT to_regclass('unrelated_log') IS NULL"),'t')
        self.sql('CREATE TABLE public.streams(id integer);SET search_path=public,lake_capture;ALTER TABLE streams ADD COLUMN note text;DROP TABLE streams')
        self.check('private DDL guard preserves caller search-path resolution',self.value("SELECT to_regclass('public.streams') IS NULL"),'t')
        self.sql('CREATE ROLE ddl_owner NOLOGIN;GRANT CREATE ON DATABASE capture TO ddl_owner')
        identity=self.value('SET ROLE ddl_owner;CREATE EXTENSION hstore;DROP EXTENSION hstore;SELECT current_user')
        self.check('metadata guard restores the unrelated extension owner identity',identity,'ddl_owner')
        self.sql('CREATE SCHEMA shadow;CREATE TABLE shadow.pg_extension(LIKE pg_catalog.pg_extension);CREATE TABLE shadow.pg_depend(LIKE pg_catalog.pg_depend)')
        denied=self.sql('SET search_path=shadow,pg_catalog;DROP EXTENSION lake_replication_capture CASCADE',required=False)
        self.check('dependency guard ignores caller catalog shadows',denied.returncode!=0 and 'owned slot cleanup' in denied.stderr,True)
        options=json.loads(self.value("SELECT array_to_json(ftoptions) FROM pg_foreign_table WHERE ftrelid='lake_capture_log.events_a'::regclass"))
        literal=lambda value:"E'"+value.replace('\\','\\\\').replace("'","''")+"'"
        def option_update(values):
            return "UPDATE pg_foreign_table SET ftoptions=ARRAY["+','.join(map(literal,values))+"] WHERE ftrelid='lake_capture_log.events_a'::regclass"
        def set_options(values):self.sql(option_update(values))
        # Superuser-only corruption injection in a disposable database, with no
        # storage operation at the altered root. Ordinary operators cannot
        # rewrite the recorded expectation or own the private foreign tables.
        changed=[option+'/unexpected-relocation' if option.startswith('location=') else option for option in options]
        try:
            writer=Session(self.sql_args());reader=Session(self.sql_args())
            try:
                self.sql('ALTER EVENT TRIGGER lake_capture_ddl_start DISABLE')
                writer.query('BEGIN;'+alter+';'+option_update(changed))
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending=pool.submit(reader.query,"SET application_name='log_location_waiter';SELECT lake_capture.lock_private_logs(false)",10)
                    try:self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name='log_location_waiter' AND wait_event_type='Lock'",'1')
                    finally:writer.query('COMMIT')
                    observed=json.loads(pending.result(timeout=10)[-1])
                self.check('catalog check sees DDL committed after its statement snapshot',observed['events_a'].endswith('/unexpected-relocation'),True)
            finally:
                writer.close();reader.close()
                self.sql('ALTER EVENT TRIGGER lake_capture_ddl_start ENABLE')
            self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
            failed=self.command(self.capture_args(LAKE_CAPTURE_MAINTENANCE_ONLY='1'),required=False)
            self.check('maintenance refuses private storage drift before object I/O',failed.returncode!=0 and 'private log locations changed' in failed.stderr,True)
            self.check('drift consumes no maintenance reservation',self.value('SELECT maintenance_attempt_count FROM lake_capture.streams'),'0')
            self.sql("BEGIN;INSERT INTO a VALUES(2,'pending');INSERT INTO b VALUES(2,'pending');COMMIT")
            self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
            failed=self.command(self.capture_args(),required=False)
            self.check('capture refuses an altered private log destination',failed.returncode!=0 and 'private log locations changed' in failed.stderr,True)
            self.check('storage drift preserves capture and upload frontiers',self.value('SELECT captured_sequence::text||\':\'||upload_attempt_count FROM lake_capture.streams'),'1:1')
            failed=self.sql(f"SELECT lake_capture.prune_logs('{self.stream}')",required=False)
            self.check('logical pruning also rejects altered storage roots',failed.returncode!=0 and 'private log locations changed' in failed.stderr,True)
            self.sql('SELECT lake_capture.start_manager()')
            self.sql(f"SELECT lake_capture.drop_stream('{self.stream}')")
            self.source_wait("SELECT (desired_state='stopped' AND last_error LIKE '%repair before reset or drop%')::text FROM lake_capture.streams",'true')
            self.check('cleanup refuses storage drift before dropping protected WAL',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'1')
            self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        finally:set_options(options)
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        child=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2')
        finally:
            self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'],required=False)
            child.communicate(timeout=15)
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        self.check('storage repair replays protected source changes once',self.dest(
            "SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),'2:2')


if __name__=='__main__':sys.exit(LocationProbe().run())

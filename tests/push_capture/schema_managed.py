#!/usr/bin/env python3
"""Controlled schema activation and managed publication with unprivileged logins."""
import json
import sys
from managed import ManagedPushProbe
from live import ROOT
from session import Session


class ManagedSchemaProbe(ManagedPushProbe):
    def control(self,statement,required=True,role='capture_operator'):
        args=self.sql_args();args[args.index('postgres')]=role
        return self.command(args,input=statement,required=required)

    def exercise(self):
        self.evidence.update(scope='controlled source/destination ordered schema mode with dedicated managed runtime',
            unqualified=['backend schema publication crash recovery','production sizing','general release'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE ROLE application_owner LOGIN;GRANT USAGE,CREATE ON SCHEMA public TO application_owner;
            CREATE ROLE capture_operator LOGIN REPLICATION;CREATE ROLE capture_reader LOGIN;
            CREATE TABLE a(id bigint PRIMARY KEY,note text NOT NULL);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            INSERT INTO a VALUES(1,'baseline');INSERT INTO b VALUES(2,'baseline');
            ALTER TABLE a OWNER TO application_owner;ALTER TABLE b OWNER TO application_owner;
            GRANT SELECT,MAINTAIN ON a,b TO capture_operator;
            GRANT USAGE ON SCHEMA lake_capture_log TO capture_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA lake_capture_log TO capture_reader;GRANT lake_read TO capture_reader;''')
        self.command(self.sql_args()+['-v','capture_login=capture_operator'],input=(ROOT/'deploy/push/source-grants.sql').read_text())
        self.stream=self.control("SELECT lake_capture.register_stream('a','b',NULL,'online_copy')").stdout.strip()
        failed=self.control(f"SELECT lake_capture.enable_schema_auditing('{self.stream}')",False)
        self.check('source activation refuses an incomplete baseline',failed.returncode!=0 and 'stopped initialized' in failed.stderr,True)
        self.control(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait("SELECT snapshot_state FROM lake_capture.streams",'complete')
        self.control(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*)::text FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.control(f"SELECT lake_capture.register_consumer('{self.stream}')").stdout.strip()
        failed=self.control(f"SELECT lake_capture.enable_schema_auditing('{self.stream}')",False)
        self.check('existing frozen consumers prevent an in-place schema mode switch',
            failed.returncode!=0 and 'existing mirrors require a new stream' in failed.stderr,True)
        self.control(f"SELECT lake_capture.drop_stream('{self.stream}')")
        self.source_wait('SELECT count(*)::text FROM lake_capture.streams','0')
        self.stream=self.control("SELECT lake_capture.register_stream('a','b',NULL,'online_copy')").stdout.strip()
        self.control(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait("SELECT snapshot_state FROM lake_capture.streams",'complete')
        self.control(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*)::text FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        lease=Session(self.sql_args())
        try:
            lease.query(f"SELECT pg_advisory_lock(hashtextextended('lake_capture/{self.stream}',0))")
            failed=self.control(f"SELECT lake_capture.enable_schema_auditing('{self.stream}')",False)
            self.check('schema activation cannot race an older decoder lease',
                failed.returncode!=0 and 'capture actor lease' in failed.stderr,True)
        finally:lease.close()
        self.sql('CREATE ROLE other_capture_owner LOGIN REPLICATION;GRANT USAGE ON SCHEMA lake_capture TO other_capture_owner;GRANT EXECUTE ON FUNCTION lake_capture.enable_schema_auditing(uuid) TO other_capture_owner')
        failed=self.control(f"SELECT lake_capture.enable_schema_auditing('{self.stream}')",False,'other_capture_owner')
        self.check('schema activation cannot adopt another capture owner binding',
            failed.returncode!=0 and 'owning capture LOGIN' in failed.stderr,True)
        self.control(f"SELECT lake_capture.enable_schema_auditing('{self.stream}')")
        self.check('owning replication login activates schema through the guarded API',self.value('SELECT schema_auditing::text FROM lake_capture.streams'),'true')
        failed=self.control(f"SELECT lake_capture.register_consumer('{self.stream}')",False)
        self.check('ordered streams reject undeclared consumer capabilities',failed.returncode!=0 and 'schema-capable consumer' in failed.stderr,True)
        consumer=self.control(f"SELECT lake_capture.register_consumer('{self.stream}',true)").stdout.strip()
        self.check('registered ordered consumer retains its declared capability',self.value(f"SELECT supports_schema::text FROM lake_capture.consumers WHERE consumer_id='{consumer}'"),'true')
        failed=self.control("UPDATE lake_capture.streams SET schema_auditing=false",False)
        self.check('capture operator cannot bypass the activation protocol by direct flag writes',failed.returncode!=0 and 'permission denied' in failed.stderr,True)
        self.control(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.destination('CREATE EXTENSION lake_replication_push;CREATE ROLE push_runtime LOGIN;CREATE SCHEMA push_worker_bootstrap')
        args=self.sql_args();args[args.index('source')]='destination';args[args.index('capture')]='analytics'
        self.command(args+['-v','worker_login=push_runtime','-v','destination_schema=push_worker_bootstrap'],input=(ROOT/'deploy/install/worker-grants.sql').read_text())
        self.write_credentials()
        config=json.loads(self.value('''SELECT jsonb_build_object('mode','iceberg_push','schema_mode','ordered','stream_id',stream_id,
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'capture_generation',capture_generation) FROM lake_capture.streams'''))
        config.update(destination_user='push_runtime',source_address='source:5432',source_database='capture',conninfo='host=source port=5432 dbname=capture user=capture_reader sslmode=disable')
        registry=self.value('''SELECT jsonb_agg(to_jsonb(r)||jsonb_build_object('definition',d.definition_json) ORDER BY r.table_id)
            FROM lake_capture_log.registrations r JOIN lake_capture.schema_definitions d USING(stream_id,table_id)
            WHERE d.schema_version=1''')
        literal=lambda value:"E'"+str(value).replace('\\','\\\\').replace("'","''")+"'"
        missing=json.loads(registry)
        del missing[0]['definition']
        failed=self.destination(f"SELECT lake_replication.create_push_subscription('missing_definition',{literal(json.dumps(config))}::jsonb,{literal(json.dumps(missing))}::jsonb,'missing_mirror','/tmp/managed-push.env')",required=False)
        self.check('ordered creation requires retained initial definitions',
            failed.returncode!=0 and 'requires retained initial definitions' in failed.stderr,True)
        self.check('invalid ordered creation leaves no mirror or subscription',self.dest("SELECT (to_regnamespace('missing_mirror') IS NULL AND NOT EXISTS(SELECT FROM pg_subscription WHERE subname='missing_definition'))::text"),'true')
        self.subscription=int(self.dest(f"SELECT lake_replication.create_push_subscription('managed_schema',{literal(json.dumps(config))}::jsonb,{literal(registry)}::jsonb,'managed_mirror','/tmp/managed-push.env')"))
        self.check('explicit ordered destination initializes fresh and stopped',self.dest(f'''SELECT schema_enabled::text||':'||desired_state
            FROM lake_replication.push_bindings JOIN lake_replication.worker_launch_config USING(subscription_id)
            WHERE subscription_id={self.subscription}'''),'true:stopped')
        self.check('ordered initial mirror preserves non-key NOT NULL metadata',self.dest("SELECT attnotnull::text FROM pg_attribute WHERE attrelid='managed_mirror.a'::regclass AND attname='note'"),'true')
        self.destination('ALTER SUBSCRIPTION managed_schema ENABLE')
        self.check('managed ordered importer and publisher activate the online baseline',self.wait('SELECT count(*) FROM managed_mirror.a','1'),True)
        self.check('ordered supervisor runs as the dedicated non-superuser login',self.dest(f"SELECT EXISTS(SELECT FROM pg_stat_activity WHERE application_name='lake-replication-supervisor:{self.subscription}' AND usename='push_runtime')"),'t')
        self.control("BEGIN;UPDATE a SET note='before ddl' WHERE id=1;ALTER TABLE a ADD COLUMN extra boolean;"
            "INSERT INTO a VALUES(3,'new row',true);ALTER TABLE a RENAME COLUMN note TO label;"
            "UPDATE a SET label='after ddl' WHERE id=1;ALTER TABLE b ADD COLUMN qty integer;UPDATE b SET qty=42;COMMIT",role='application_owner')
        self.check('managed runtime publishes mixed DML-DDL-DML through the optional publisher',self.wait(
            "SELECT count(*) FROM pg_attribute WHERE attrelid='managed_mirror.a'::regclass AND attname='label' AND NOT attisdropped",'1') and self.wait('SELECT count(*) FROM managed_mirror.a','2'),True)
        for table in ['a','b']:
            self.check('unprivileged managed publication matches source '+table,self.dest(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM managed_mirror.{table} t'),self.value(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {table} t'))
        self.check('runtime has DDL capability only through scoped publication, not ownership',self.dest("SELECT (pg_get_userbyid(relowner)<>'push_runtime')::text FROM pg_class WHERE oid='managed_mirror.a'::regclass"),'true')
        self.destination('ALTER SUBSCRIPTION managed_schema DISABLE')
        self.wait(f"SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_replication_worker/{self.subscription}/%' OR application_name='lake-replication-supervisor:{self.subscription}'",'0')
        self.control("ALTER TABLE a RENAME COLUMN label TO detail;INSERT INTO a VALUES(4,'while destination stopped',false)",role='application_owner')
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','4')
        self.check('stopped destination retains its previously published schema',self.dest("SELECT count(*) FROM pg_attribute WHERE attrelid='managed_mirror.a'::regclass AND attname='label' AND NOT attisdropped"),'1')
        self.destination('ALTER SUBSCRIPTION managed_schema ENABLE')
        self.check('restart replays DDL-only and following data from retained history',self.wait(
            "SELECT count(*) FROM pg_attribute WHERE attrelid='managed_mirror.a'::regclass AND attname='detail' AND NOT attisdropped",'1') and self.wait('SELECT count(*) FROM managed_mirror.a','3'),True)
        self.control("BEGIN;TRUNCATE a,b;INSERT INTO a VALUES(5,'after truncate',true);INSERT INTO b VALUES(6,'after truncate',9);COMMIT",role='application_owner')
        self.check('managed TRUNCATE uses the evolved versions of both tables',self.wait(
            "SELECT (SELECT min(id) FROM managed_mirror.a)::text||':'||(SELECT min(id) FROM managed_mirror.b)",'5:6'),True)
        self.destination('DROP SUBSCRIPTION managed_schema')
        self.check('managed cleanup removes every optional schema registry and marker',self.dest("SELECT (SELECT count(*) FROM lake_replication.push_schema_tables)::text||':'||(SELECT count(*) FROM lake_replication.push_schema_versions)||':'||(SELECT count(*) FROM lake_replication.push_schema_transactions)"),'0:0:0')
        self.destination('DROP EXTENSION lake_replication_push')
        self.check('ordered companion removal preserves the source capture slot',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'1')


if __name__=='__main__':sys.exit(ManagedSchemaProbe().run())

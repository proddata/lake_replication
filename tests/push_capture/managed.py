#!/usr/bin/env python3
"""Optional destination API uses the existing native managed worker lifecycle."""
import json
import sys
import time
from live import LiveProbe, ROOT

class ManagedPushProbe(LiveProbe):
    def write_credentials(self):
        code="from pathlib import Path;import os,pwd;p=Path('/tmp/managed-push.env');p.write_text('LAKE_SOURCE_USER=capture_reader\\nLAKE_DESTINATION_USER=push_runtime\\nLAKE_SOURCE_TLS_MODE=disable\\nLAKE_DESTINATION_TLS_MODE=disable\\n');os.chmod(p,0o600);u=pwd.getpwnam('postgres');os.chown(p,u.pw_uid,u.pw_gid)"
        self.command(self.compose+['exec','-T','destination','python3','-c',code])

    def restore_probe(self):
        self.command(self.compose+['exec','-T','destination','pg_dump','-U','postgres','-Fc','-d','analytics','-f','/tmp/push-restore.dump'])
        self.destination('CREATE DATABASE push_clone')
        full=self.command(self.compose+['exec','-T','destination','pg_restore','-U','postgres','--exit-on-error','-d','push_clone','/tmp/push-restore.dump'],required=False)
        self.check('full restore refuses adoption of a live writable Iceberg location',
            full.returncode!=0 and 'is not empty' in full.stderr,True)
        self.destination('DROP DATABASE push_clone WITH (FORCE);CREATE DATABASE push_clone')
        self.command(self.compose+['exec','-T','destination','pg_dump','-U','postgres','-Fc','--exclude-schema=managed_mirror','-d','analytics','-f','/tmp/push-control.dump'])
        self.command(self.compose+['exec','-T','destination','pg_restore','-U','postgres','--exit-on-error','--section=pre-data','-d','push_clone','/tmp/push-control.dump'])
        args=self.sql_args();args[args.index('source')]='destination';args[args.index('capture')]='push_clone'
        def clone(query):return self.command(args,input=query).stdout.strip()
        clone('CREATE SCHEMA managed_mirror;CREATE TABLE managed_mirror.a(id bigint NOT NULL,note text);CREATE TABLE managed_mirror.b(id bigint NOT NULL,note text)')
        for section in ['data','post-data']:
            restore=self.compose+['exec','-T','destination','pg_restore','-U','postgres','--exit-on-error','--section='+section]
            if section=='data':restore+=['--disable-triggers']
            self.command(restore+['-d','push_clone','/tmp/push-control.dump'])
        self.check('control-only restore does not claim to restore Iceberg mirror data',clone('SELECT count(*) FROM managed_mirror.a'),'0')
        self.evidence['restored_worker_state']=json.loads(clone("""SELECT jsonb_build_object(
            'registered',(SELECT jsonb_agg(to_jsonb(w)) FROM extension_base.workers w),
            'active',(SELECT jsonb_agg(to_jsonb(w)) FROM extension_base.list_base_workers() w),
            'binding_matches',(SELECT lake_replication.worker_binding_matches(subscription_id) FROM lake_replication.worker_launch_config),
            'subscription',(SELECT jsonb_build_object('state',state,'last_error',last_error) FROM lake_replication.subscriptions))"""))
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if clone("SELECT desired_state FROM lake_replication.worker_launch_config")=='stopped':break
            time.sleep(.1)
        self.check('restored push configuration starts stopped on the inherited destination',clone(
            'SELECT desired_state FROM lake_replication.worker_launch_config'),'stopped')
        self.check('restore preserves push stream identity and imported frontier',clone(
            f"SELECT (stream_id='{self.stream}'::uuid AND imported_sequence=2)::text FROM lake_replication.push_bindings"),'true')
        clone("UPDATE lake_replication.worker_launch_config SET desired_state='running'")
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            if clone('SELECT desired_state FROM lake_replication.worker_launch_config')=='stopped':break
            time.sleep(.1)
        self.check('attempted copied push enable is fenced before any importer launches',clone(
            'SELECT desired_state FROM lake_replication.worker_launch_config'),'stopped')
        self.check('copied destination never takes authority over the source slot',self.value(
            "SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'1')
        self.destination('DROP DATABASE push_clone WITH (FORCE)')

    def exercise(self):
        self.evidence.update(scope='optional destination API and native managed push importer/publisher',
            unqualified=['production release packaging','ordered DDL','automatic failover'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            INSERT INTO a VALUES(1,'baseline');INSERT INTO b VALUES(2,'baseline');
            CREATE ROLE capture_reader LOGIN;
            GRANT USAGE ON SCHEMA lake_capture_log TO capture_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA lake_capture_log TO capture_reader;
            GRANT lake_read TO capture_reader;''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b',NULL,'online_copy')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait("SELECT snapshot_state FROM lake_capture.streams",'complete')
        self.destination('CREATE EXTENSION lake_replication_push;CREATE ROLE push_runtime LOGIN;CREATE SCHEMA push_worker_bootstrap')
        grant_args=self.sql_args();grant_args[grant_args.index('source')]='destination';grant_args[grant_args.index('capture')]='analytics'
        self.command(grant_args+['-v','worker_login=push_runtime','-v','destination_schema=push_worker_bootstrap'],input=(ROOT/'deploy/install/worker-grants.sql').read_text())
        self.write_credentials()
        config=json.loads(self.value('''SELECT jsonb_build_object('mode','iceberg_push','stream_id',stream_id,
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'capture_generation',capture_generation)
            FROM lake_capture.streams'''))
        config.update(destination_user='push_runtime',source_address='source:5432',source_database='capture',conninfo='host=source port=5432 dbname=capture user=capture_reader sslmode=disable')
        registry=self.value('SELECT jsonb_agg(to_jsonb(r) ORDER BY table_id) FROM lake_capture_log.registrations r')
        literal=lambda value:"E'"+str(value).replace('\\','\\\\').replace("'","''")+"'"
        create=lambda name,schema: f"SELECT lake_replication.create_push_subscription('{name}',{literal(json.dumps(config))}::jsonb,{literal(registry)}::jsonb,'{schema}','/tmp/managed-push.env')"
        self.destination('BEGIN;'+create('aborted_push','aborted_mirror')+';ROLLBACK')
        self.check('rolled-back optional API creates no durable launch or mirror',self.dest("SELECT (SELECT count(*) FROM lake_replication.worker_launch_config)::text||':'||(to_regnamespace('aborted_mirror') IS NULL)::text"),'0:true')
        self.subscription=int(self.dest(create('managed_push','managed_mirror')))
        denied=self.destination(create('second_writer','managed_mirror'),required=False)
        self.check('optional API rejects a second writer adopting an existing mirror schema',denied.returncode!=0,True)
        self.check('failed mirror adoption leaves no subscription or binding residue',self.dest(
            "SELECT count(*) FROM lake_replication.subscriptions WHERE subscription_name='second_writer'"),'0')
        config['mode']='logical_pull'
        denied=self.destination(create('implicit_switch','wrong_mode'),required=False)
        config['mode']='iceberg_push'
        self.check('optional API refuses an implicit logical-to-push mode switch',denied.returncode!=0 and 'explicit iceberg_push' in denied.stderr,True)
        self.check('new optional subscription has explicit mode and starts stopped',self.dest(f"SELECT m.source_mode||':'||c.desired_state FROM lake_replication.subscription_modes m JOIN lake_replication.worker_launch_config c USING(subscription_id) WHERE subscription_id={self.subscription}"),'iceberg_push:stopped')
        self.check('destination has no capture-slot cleanup authority',self.dest(f'SELECT count(*) FROM lake_replication.pending_cleanup WHERE subscription_id={self.subscription}'),'0')
        self.check('destination mirror cannot activate before managed start',self.dest('SELECT count(*) FROM managed_mirror.a'),'0')
        denied=self.destination('DROP EXTENSION lake_replication_push CASCADE',required=False)
        self.check('optional transport removal requires subscription cleanup',denied.returncode!=0 and 'drop push subscriptions' in denied.stderr,True)
        self.destination('ALTER SUBSCRIPTION managed_push ENABLE')
        self.check('existing native launcher manages push baseline and publisher',self.wait('SELECT count(*) FROM managed_mirror.a','1'),True)
        self.check('managed push authenticates with the dedicated destination LOGIN',self.dest(f"SELECT EXISTS(SELECT FROM pg_stat_activity WHERE application_name='lake-replication-supervisor:{self.subscription}' AND usename='push_runtime')"),'t')
        self.check('managed push uses a positive generation fence',self.dest(f'SELECT (config_generation>0)::text FROM lake_replication.worker_launch_config WHERE subscription_id={self.subscription}'),'true')
        self.sql("BEGIN;UPDATE a SET id=10,note='managed-update';DELETE FROM b;INSERT INTO b VALUES(20,'reinsert');COMMIT")
        self.check('managed optional mode streams mutations through the existing publisher',self.wait("SELECT (SELECT min(id) FROM managed_mirror.a)::text||':'||(SELECT min(id) FROM managed_mirror.b)",'10:20'),True)
        self.restore_probe()
        self.sql("CREATE TABLE pull_only(id bigint PRIMARY KEY,note text);INSERT INTO pull_only VALUES(100,'logical');CREATE PUBLICATION managed_pull_pub FOR TABLE pull_only")
        self.destination("CREATE SCHEMA managed_pull_mirror;GRANT USAGE,CREATE ON SCHEMA managed_pull_mirror TO push_runtime")
        self.destination("CREATE SUBSCRIPTION managed_pull CONNECTION 'host=source port=5432 dbname=capture user=postgres sslmode=disable' PUBLICATION managed_pull_pub WITH(lake_target='iceberg',lake_target_schema='managed_pull_mirror',lake_credential_file='/tmp/managed-push.env')")
        self.check('default logical pull initializes beside a managed push mirror',self.wait("SELECT to_regclass('managed_pull_mirror.pull_only') IS NOT NULL",'t') and self.wait('SELECT count(*) FROM managed_pull_mirror.pull_only','1'),True)
        self.check('existing subscription syntax retains the logical_pull mode',self.dest("SELECT source_mode FROM lake_replication.subscription_modes WHERE subscription_name='managed_pull'"),'logical_pull')
        self.destination('ALTER SUBSCRIPTION managed_push DISABLE')
        self.check('managed disable reaps push children',self.wait(f"SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_replication_worker/{self.subscription}/%' OR application_name='lake-replication-supervisor:{self.subscription}'",'0'),True)
        self.sql("BEGIN;INSERT INTO a VALUES(11,'paused-destination');INSERT INTO pull_only VALUES(101,'while-push-stopped');COMMIT")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','3')
        self.check('logical pull continues independently while push is stopped',self.wait('SELECT count(*) FROM managed_pull_mirror.pull_only','2'),True)
        self.check('stopped destination leaves published mirror unchanged',self.dest('SELECT count(*) FROM managed_mirror.a'),'1')
        self.destination('ALTER SUBSCRIPTION managed_push ENABLE')
        self.check('managed enable resumes protected source logs',self.wait('SELECT count(*) FROM managed_mirror.a','2'),True)
        self.sql("BEGIN;TRUNCATE a,b;INSERT INTO a VALUES(12,'runtime-truncate');INSERT INTO b VALUES(21,'runtime-truncate');COMMIT")
        self.check('dedicated destination worker applies a two-table TRUNCATE barrier',self.wait("SELECT (SELECT min(id) FROM managed_mirror.a)::text||':'||(SELECT min(id) FROM managed_mirror.b)",'12:21'),True)
        runtime=self.sql_args();runtime[runtime.index('source')]='destination';runtime[runtime.index('capture')]='analytics';runtime[runtime.index('postgres')]='push_runtime'
        denied=self.command(runtime,input='UPDATE lake_replication.push_bindings SET stream_id=gen_random_uuid()',required=False)
        self.check('runtime destination credentials cannot rewrite push stream identity',denied.returncode!=0,True)
        self.destination('DROP SUBSCRIPTION managed_push')
        self.check('managed drop removes only destination push metadata',self.dest('SELECT count(*) FROM lake_replication.push_bindings'),'0')
        self.check('destination drop leaves the source-owned capture slot alive',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'1')
        self.destination('DROP EXTENSION lake_replication_push')
        self.check('optional destination companion removes independently',self.dest("SELECT count(*) FROM pg_extension WHERE extname='lake_replication_push'"),'0')
        self.sql("INSERT INTO pull_only VALUES(102,'after-push-removal')")
        self.check('logical pull continues after optional destination removal',self.wait('SELECT count(*) FROM managed_pull_mirror.pull_only','3'),True)
        self.destination('DROP SUBSCRIPTION managed_pull')
        self.check('logical cleanup preserves the source companion slot',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'1')


if __name__=='__main__':sys.exit(ManagedPushProbe().run())

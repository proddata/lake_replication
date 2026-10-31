"""Packaged beta.5 SQL upgrade, cross-store integrity, restore and copy admission."""
import importlib.util
import json
from pathlib import Path
from urllib.parse import quote


def run(q,c):
    spec=importlib.util.spec_from_file_location('checkpoint_operations',Path(__file__).with_name('checkpoint_split_operations.py'))
    checks=importlib.util.module_from_spec(spec);spec.loader.exec_module(checks)
    checks.run(q,c)
    upgrade_and_restore(q,c)
    leases(q,c)
    spec=importlib.util.spec_from_file_location('copy_bounds',Path(__file__).with_name('copy_chunk_bounds.py'))
    bounds=importlib.util.module_from_spec(spec);spec.loader.exec_module(bounds)
    bounds.run(q,c,1000)


def upgrade_and_restore(q,c):
    db='beta5_upgrade';restore='beta5_restore';reader='beta5_reader';editor='beta5_editor'
    def sql(text,database=db):return q.sql('destination',database,text)
    def rejected(name,text,needle,database=db):
        r=q.sql_result('destination',database,text,check_result=False)
        q.check(name,r.returncode!=0 and needle in r.stderr,True)
    q.sql('destination','analytics',f'CREATE ROLE {reader}; CREATE ROLE {editor}; CREATE DATABASE {db}')
    try:
        sql("CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication VERSION '0.1.0-beta.4'")
        sid=int(sql("SELECT lake_replication.create_subscription(42,1,1,'frontier','frontier',ARRAY['pub'])"))
        copy=int(sql("SELECT lake_replication.create_subscription(42,1,1,'copy','copy',ARRAY['pub'])"))
        sql(f"""SELECT lake_replication.stage_transaction({sid},1,'0/8','0/8','0/8','2026-01-01','[]');
          SELECT lake_replication.finish_batch(lake_replication.prepare_batch({sid},'first','0/8','0/8'));
          SELECT lake_replication.stage_transaction({sid},2,'0/10','0/10','0/10','2026-01-01','[]');
          SELECT lake_replication.mark_acknowledged({sid},'0/10');
          CREATE TABLE public.copy_target(id bigint);
          INSERT INTO lake_replication.relations(subscription_id,remote_relation_id,schema_version,source_namespace,source_relation,replica_identity,columns)
           VALUES({copy},42,1,'public','copy_target','d','[]');
          INSERT INTO lake_replication.initial_copies(subscription_id,remote_relation_id,relation_schema_version,destination_relation,consistent_lsn,snapshot_name,state,overwrite_destination)
           VALUES({copy},42,1,'public.copy_target','0/1','saved-snapshot','copying',false);
          SELECT lake_replication.append_initial_copy_rows({copy},42,1,0,'[{{"id":1}},{{"id":2}}]');
          GRANT USAGE ON SCHEMA lake_replication TO {reader},{editor};
          GRANT SELECT(published_lsn) ON lake_replication.checkpoints TO {reader} WITH GRANT OPTION;
          GRANT SELECT,DELETE ON lake_replication.initial_copy_rows TO {editor};
          GRANT UPDATE(row_data) ON lake_replication.initial_copy_rows TO {editor};""")
        def digest(database=db):
            return sql("SELECT jsonb_build_array((SELECT jsonb_agg(to_jsonb(c) ORDER BY subscription_id) FROM lake_replication.checkpoints c),(SELECT jsonb_agg(to_jsonb(r) ORDER BY row_ordinal) FROM lake_replication.initial_copy_rows r),(SELECT jsonb_agg(jsonb_build_array(xid,state) ORDER BY xid) FROM lake_replication.transactions))::text",database)
        before=digest()
        sql('CREATE VIEW public.checkpoint_consumer AS SELECT * FROM lake_replication.checkpoints')
        rejected('upgrade refuses unhandled user view dependencies',"ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.5'",'custom dependencies')
        sql('DROP VIEW public.checkpoint_consumer; ALTER TABLE lake_replication.initial_copy_rows ENABLE ROW LEVEL SECURITY')
        rejected('upgrade refuses custom staging row security',"ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.5'",'custom dependencies')
        sql('ALTER TABLE lake_replication.initial_copy_rows DISABLE ROW LEVEL SECURITY')
        sql(f'ALTER TABLE lake_replication.initial_copy_rows OWNER TO {editor}')
        rejected('upgrade refuses changed runtime table ownership',"ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.5'",'custom dependencies')
        sql(f'ALTER TABLE lake_replication.initial_copy_rows OWNER TO postgres; GRANT SELECT,DELETE ON lake_replication.initial_copy_rows TO {editor}; GRANT UPDATE(row_data) ON lake_replication.initial_copy_rows TO {editor}')
        sql(f'SET ROLE {reader}; GRANT SELECT(published_lsn) ON lake_replication.checkpoints TO {editor}')
        rejected('upgrade refuses to flatten custom grant chains',"ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.5'",'custom dependencies')
        sql(f'SET ROLE {reader}; REVOKE SELECT(published_lsn) ON lake_replication.checkpoints FROM {editor}')
        rejected('upgrade refuses enabled configuration',f"BEGIN; INSERT INTO lake_replication.worker_launch_config(subscription_id,slot_name,publication_name,source_database,destination_database,source_address,destination_address,destination_schema) VALUES({sid},'x','x','application','{db}','source:5432','/var/run/postgresql:5432','public'); ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.5'",'stop subscriptions')
        sql("BEGIN; ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.5'; ROLLBACK")
        q.check('beta5 rollback preserves beta4 version and checkpoint table',sql("SELECT extversion||':'||(SELECT relkind::text FROM pg_class WHERE oid='lake_replication.checkpoints'::regclass) FROM pg_extension WHERE extname='lake_replication'"),'0.1.0-beta.4:r')
        q.check('beta5 rollback preserves all old frontiers and staged rows',digest(),before)
        sql("ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.5'")
        q.check('beta5 migration preserves populated frontiers, rows and journal',digest(),before)
        rejected('grant transfer is unavailable outside extension DDL',"SELECT lake_replication.migration_grant('GRANT SELECT ON lake_replication.checkpoints TO PUBLIC')",'only valid during')
        q.check('beta5 removes the obsolete ingress publication column',sql("SELECT count(*) FROM pg_attribute WHERE attrelid='lake_replication.checkpoint_ingress'::regclass AND attname='published_lsn' AND NOT attisdropped"),'0')
        q.check('publication-only column grant survives view migration',sql(f"SET ROLE {reader}; SELECT published_lsn FROM lake_replication.checkpoints ORDER BY published_lsn DESC LIMIT 1").splitlines()[-1],'0/8')
        rejected('column reader cannot read private ingress',f'SET ROLE {reader}; SELECT staged_lsn FROM lake_replication.checkpoints','permission denied')
        rejected('management role gains no checkpoint mutation privilege','SET ROLE lake_replication_admin; UPDATE lake_replication.checkpoints SET published_lsn=published_lsn','permission denied')
        rejected('temporary control tables cannot spoof the worker fence',f"SET ROLE lake_replication_admin; CREATE TEMP TABLE service_status(subscription_id bigint,desired_state text); INSERT INTO service_status VALUES({copy},'running'); CREATE TEMP TABLE worker_launch_config(subscription_id bigint,config_generation bigint,desired_state text); INSERT INTO worker_launch_config VALUES({copy},1,'running'); CREATE TEMP TABLE journal_limits(subscription_id bigint PRIMARY KEY); SELECT lake_replication.assert_worker({copy},1)",'stopped or obsolete')
        rejected('physical ingress cannot regress',f"UPDATE lake_replication.checkpoint_ingress SET received_lsn='0/0',staged_lsn='0/0',acknowledged_lsn='0/0' WHERE subscription_id={sid}",'cannot regress')
        rejected('physical publication cannot outrun durable staging',f"UPDATE lake_replication.checkpoint_publication SET published_lsn='0/20' WHERE subscription_id={sid}",'exceeds durable staging')
        rejected('physical publication cannot regress',f"UPDATE lake_replication.checkpoint_publication SET published_lsn='0/0' WHERE subscription_id={sid}",'cannot regress')
        rejected('publication half cannot be deleted independently',f'DELETE FROM lake_replication.checkpoint_publication WHERE subscription_id={sid}','foreign key')
        rejected('checkpoint explicit NULL is not silently defaulted',f'UPDATE lake_replication.checkpoints SET received_lsn=NULL WHERE subscription_id={sid}','null value')
        sql(f"SELECT lake_replication.append_initial_copy_rows({copy},42,1,2,'[{{\"id\":3}},{{\"id\":4}}]')")
        q.check('native-style appends use chunks alongside preserved legacy rows',sql('SELECT (SELECT count(*) FROM lake_replication.initial_copy_legacy_rows)||\':\'||(SELECT count(*) FROM lake_replication.initial_copy_chunks)||\':\'||(SELECT count(*) FROM lake_replication.initial_copy_rows)'),'2:1:4')
        rejected('chunk append cannot overlap an old row',f"SELECT lake_replication.append_initial_copy_rows({copy},42,1,1,'[{{}}]')",'overlaps a staged row')
        rejected('legacy row insert cannot overlap a new chunk',f"INSERT INTO lake_replication.initial_copy_rows VALUES({copy},42,1,2,'{{}}')",'overlaps a staged chunk')
        q.check('temporary tables cannot redirect privileged row edits',sql(f"SET ROLE {editor}; CREATE TEMP TABLE initial_copy_legacy_rows AS SELECT * FROM lake_replication.initial_copy_legacy_rows WITH NO DATA; UPDATE lake_replication.initial_copy_rows SET row_data='{{\"id\":44}}' WHERE row_ordinal=3; SELECT count(*) FROM pg_temp.initial_copy_legacy_rows").splitlines()[-1],'0')
        q.check('legacy row edit materializes chunks atomically without losing peers',sql("SELECT string_agg(row_data->>'id',',' ORDER BY row_ordinal) FROM lake_replication.initial_copy_rows"),'1,2,3,44')
        rejected('legacy column editor cannot mutate row identity',f'SET ROLE {editor}; UPDATE lake_replication.initial_copy_rows SET row_ordinal=9','permission denied')
        sql(f"BEGIN; SET ROLE {editor}; DELETE FROM lake_replication.initial_copy_rows WHERE row_ordinal=3; ROLLBACK")
        q.check('legacy row deletion rollback preserves all rows',sql('SELECT count(*) FROM lake_replication.initial_copy_rows'),'4')
        # Configured chunk sizes beyond the native default remain valid SQL inputs.
        q.check('nondefault 2000-row chunk remains supported',sql(f"SELECT lake_replication.append_initial_copy_rows({copy},42,1,4,(SELECT jsonb_agg(jsonb_build_object('id',n)) FROM generate_series(5,2004) n))"),'2000')
        boundary=c.Psql('destination',db)
        try:
            boundary.query(f"BEGIN; SELECT lake_replication.append_initial_copy_rows({copy},42,1,9223372036854775807,'[{{}}]')")
            q.check('maximum bigint ordinal survives the logical row projection',boundary.query('SELECT max(row_ordinal) FROM lake_replication.initial_copy_rows'),['9223372036854775807'])
            boundary.query('ROLLBACK')
        finally:boundary.close()
        rejected('chunk-to-chunk ranges exclude partial overlaps',f"SELECT lake_replication.append_initial_copy_rows({copy},42,1,100,'[{{}}]')",'exclusion constraint')
        # Repeatable-read must not admit a stale cross-store overlap after waiting.
        owner=c.Psql('destination',db);stale=c.Psql('destination',db)
        try:
            stale.query('BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM lake_replication.initial_copy_chunks')
            owner.query(f"BEGIN; SELECT lake_replication.append_initial_copy_rows({copy},42,1,2004,'[{{\"id\":2005}}]'); COMMIT")
            try:stale.query(f"INSERT INTO lake_replication.initial_copy_rows VALUES({copy},42,1,2004,'{{}}')")
            except RuntimeError as error:q.check('repeatable-read cross-store write skew is rejected','serialize' in str(error),True)
            else:q.check('repeatable-read cross-store write skew is rejected',False,True)
        finally:owner.close();stale.close()
        sql('SELECT lake_replication.validate_runtime_state()')
        snapshot=digest()
        dump='/tmp/beta5-upgrade.dump'
        q.compose('exec','-T','destination','pg_dump','-U','postgres','-Fc','-f',dump,db)
        q.sql('destination','analytics',f'CREATE DATABASE {restore}')
        for section in ['pre-data','data','post-data']:
            args=['exec','-T','destination','pg_restore','--exit-on-error','--section='+section]
            if section=='data':args.append('--disable-triggers')
            q.compose(*args,'-U','postgres','--dbname',restore,dump)
        q.check('logical restore preserves split checkpoints and both staging stores',digest(restore),snapshot)
        sql('SELECT lake_replication.validate_runtime_state()',restore)
        q.check('logical restore preserves checkpoint column grants',sql(f"SET ROLE {reader}; SELECT published_lsn FROM lake_replication.checkpoints ORDER BY published_lsn DESC LIMIT 1",restore).splitlines()[-1],'0/8')
        q.check('logical restore preserves staged-row view permissions',sql(f"SET ROLE {editor}; SELECT count(*) FROM lake_replication.initial_copy_rows",restore).splitlines()[-1],'2005')
        sql(f'SET ROLE {reader}; GRANT SELECT(published_lsn) ON lake_replication.checkpoints TO {editor}',restore)
        q.check('logical restore preserves checkpoint grant options',sql(f"SET ROLE {editor}; SELECT published_lsn FROM lake_replication.checkpoints ORDER BY published_lsn DESC LIMIT 1",restore).splitlines()[-1],'0/8')
        rejected('restore validator detects a missing checkpoint half',f"SET session_replication_role=replica; DELETE FROM lake_replication.checkpoint_publication WHERE subscription_id={sid}; SET session_replication_role=origin; SELECT lake_replication.validate_runtime_state()",'checkpoint state',restore)
        # The failed command rolls back its corruption. Re-check with an
        # idempotent pair repair under the same disabled-trigger restore contract.
        sql(f"SET session_replication_role=replica; INSERT INTO lake_replication.checkpoint_publication SELECT subscription_id,'0/8',clock_timestamp() FROM lake_replication.checkpoint_ingress WHERE subscription_id={sid} ON CONFLICT(subscription_id) DO NOTHING; SET session_replication_role=origin; SELECT lake_replication.validate_runtime_state()",restore)
        q.check('new physical stores are registered for logical backup',sql("SELECT count(*) FROM pg_extension e,LATERAL unnest(e.extconfig) c WHERE e.extname='lake_replication' AND c IN ('lake_replication.checkpoint_ingress'::regclass,'lake_replication.checkpoint_publication'::regclass,'lake_replication.initial_copy_legacy_rows'::regclass,'lake_replication.initial_copy_chunks'::regclass)"),'4')
    finally:
        for database in [restore,db]:q.sql('destination','analytics',f'DROP DATABASE IF EXISTS {database} WITH (FORCE)')
        q.sql('destination','analytics',f'DROP ROLE {reader}; DROP ROLE {editor}')


def leases(q,c):
    other='lease_other';native='lease_native';owners=[]
    q.sql('destination','analytics',f'CREATE DATABASE {other}; CREATE DATABASE {native}')
    for db in [other,native]:q.sql('destination',db,'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    try:
        for db in ['analytics',other,'analytics',other]:
            owner=c.Psql('destination',db);owners.append(owner);owner.query('SELECT lake_replication.admit_initial_copy()')
        q.check('initial-copy pool is instance-wide and has four leases',q.sql('destination','analytics','SELECT count(*) FROM lake_replication.initial_copy_admission'),'4')
        owners[0].query('BEGIN; SAVEPOINT work; SELECT lake_replication.admit_initial_copy(); ROLLBACK TO work; ROLLBACK')
        q.check('initial-copy session lease survives transaction rollback without nesting',q.sql('destination','analytics','SELECT count(*) FROM lake_replication.initial_copy_admission'),'4')
        r=q.sql_result('destination',other,"SET statement_timeout='200ms'; SELECT lake_replication.admit_initial_copy()",check_result=False)
        q.check('fifth initial-copy pipeline waits with cancellation support',r.returncode!=0 and 'statement timeout' in r.stderr,True)
        r=q.sql_result('destination',other,'SET ROLE pg_read_all_data; SELECT lake_replication.admit_initial_copy()',check_result=False)
        q.check('read-only role cannot occupy initial-copy capacity',r.returncode!=0 and 'permission denied' in r.stderr,True)
        q.sql('source','application','CREATE TABLE lease_src(id bigint PRIMARY KEY); INSERT INTO lease_src VALUES(1),(2),(3); CREATE PUBLICATION lease_pub FOR TABLE lease_src; GRANT SELECT ON lease_src TO lake_source')
        connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
        q.sql('destination',native,f"CREATE SUBSCRIPTION lease_sub CONNECTION '{connection}' PUBLICATION lease_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        waiting="SELECT count(*) FROM lake_replication.initial_copy_admission WHERE database_name='lease_native' AND admission_state='waiting_for_initial_copy'"
        q.check('native initial copy waits before source snapshot admission',q.wait_for('destination','analytics',waiting,'1'),'1')
        q.check('queued native copy has not created a source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='lease_sub'"),'0')
        q.sql('destination',native,'ALTER SUBSCRIPTION lease_sub DISABLE')
        q.check('stop cancels an initial-copy lease waiter',q.wait_for('destination','analytics',"SELECT count(*) FROM lake_replication.initial_copy_admission WHERE database_name='lease_native'",'0',timeout=5),'0')
        q.sql('destination',native,'ALTER SUBSCRIPTION lease_sub ENABLE')
        q.check('restarted copy queues while all leases remain occupied',q.wait_for('destination','analytics',waiting,'1'),'1')
        owners.pop(0).close()
        q.check('queued native copy resumes when capacity is free',q.wait_for('destination',native,'SELECT state FROM lake_replication.subscriptions','streaming'),'streaming')
        q.check('admitted native copy publishes exact data',q.sql('destination',native,"SELECT count(*)||':'||sum(id) FROM lease_src"),'3:6')
        q.check('completed native copy releases its lease',q.wait_for('destination','analytics',"SELECT count(*) FROM lake_replication.initial_copy_admission WHERE database_name='lease_native'",'0'),'0')
        q.check('backend disconnect releases initial-copy session capacity',q.wait_for('destination','analytics','SELECT count(*) FROM lake_replication.initial_copy_admission','3'),'3')
        q.check('production default pacing is visible through PostgreSQL',q.sql('destination',native,"SHOW lake_replication.initial_copy_pause_ms"),'40')
        q.sql('destination',native,'DROP SUBSCRIPTION lease_sub')
        q.check('lease test removes source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='lease_sub'"),'0')
        q.sql('source','application','DROP PUBLICATION lease_pub; DROP TABLE lease_src')
    finally:
        for owner in owners:owner.close()
        for db in [native,other]:q.sql('destination','analytics',f'DROP DATABASE {db} WITH (FORCE)')

#!/usr/bin/env python3
"""Qualify a non-superuser runtime grant recipe against a native archive."""
import hashlib
import importlib.util
import json
from pathlib import Path
import secrets
import tarfile
import tempfile
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('grants_qualification', ROOT/'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
WORKER = 'grants_runtime_worker'
SCHEMA = 'worker_lake'


def literal(value):
    return "E'"+str(value).replace('\\', '\\\\').replace("'", "''")+"'"


def destination(statement):
    return q.sql('destination', 'analytics', statement)


def source(statement):
    return q.sql('source', 'application', statement)


def worker_result(statement):
    return q.compose('exec', '-T', 'destination', 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                     '-U', WORKER, '-d', 'analytics', input='\\set VERBOSITY verbose\n'+statement,
                     check_result=False)


def worker(statement):
    result = worker_result(statement)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def denied(name, statement):
    result = worker_result(statement)
    q.check(name, result.returncode != 0 and '42501' in result.stderr, True)


def supervisor(identity):
    return int(destination(f'SELECT supervisor_pid FROM lake_replication.service_status WHERE subscription_id={identity}'))


def wait_replacement(identity, previous):
    q.check('a distinct supervisor replaces the killed process', q.wait_for('destination', 'analytics',
            f'SELECT (supervisor_pid <> {previous})::text FROM lake_replication.service_status WHERE subscription_id={identity}',
            'true', timeout=30), 'true')


def write_credentials():
    q.compose('run', '--rm', '--no-deps', '--entrypoint', 'sh', 'credentials-init', '-ec', '''
set -eu
umask 077
printf '%s\\n' 'LAKE_DESTINATION_USER=grants_runtime_worker' \
  "LAKE_DESTINATION_PASSWORD=${QUAL_DESTINATION_PASSWORD}" 'LAKE_DESTINATION_TLS_MODE=disable' \
  > /credentials/grants-worker.env
printf '%s\\n' 'LAKE_DESTINATION_USER=grants_runtime_worker' \
  "LAKE_DESTINATION_PASSWORD=${QUAL_ROTATED_SOURCE_PASSWORD}" 'LAKE_DESTINATION_TLS_MODE=disable' \
  > /credentials/grants-worker-rotated.env
chown postgres:postgres /credentials/grants-worker*.env
chmod 0600 /credentials/grants-worker*.env
''')


def apply_recipe(recipe, role=WORKER, schema=SCHEMA, succeeds=True):
    args=['exec','-T','destination','psql','-XAt','-v','ON_ERROR_STOP=1',
          '-U','postgres','-d','analytics']
    if role is not None:args+=['-v','worker_login='+role]
    if schema is not None:args+=['-v','destination_schema='+schema]
    result = q.compose(*args,input=recipe,check_result=False)
    if succeeds and result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    return result


def negative_checks():
    q.check('worker has no privileged role attributes', worker(
            "SELECT (NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls))::text "
            'FROM pg_roles WHERE rolname=current_user'), 'true')
    q.check('worker is not a replication administrator', worker(
            "SELECT pg_has_role(current_user,'lake_replication_admin','MEMBER')::text"), 'false')
    for name, statement in [
        ('unrelated application reads denied', 'SELECT * FROM unrelated.secret'),
        ('unrelated application writes denied', "UPDATE unrelated.secret SET value='bad'"),
        ('unrelated DDL denied', 'ALTER TABLE unrelated.secret ADD COLUMN bad integer'),
        ('unrelated table removal denied', 'DROP TABLE unrelated.secret'),
        ('unrelated schema creation denied', 'CREATE TABLE unrelated.bad(id integer)'),
        ('database schema creation denied', 'CREATE SCHEMA unauthorized_schema'),
        ('raw source credential reads denied', 'SELECT * FROM lake_replication.source_connections'),
        ('password catalog reads denied', 'SELECT * FROM pg_authid'),
        ('server file reads denied', "SELECT pg_read_file('/etc/passwd')"),
        ('role creation denied', 'CREATE ROLE unauthorized_role'),
        ('superuser impersonation denied', 'SET ROLE postgres'),
        ('administrator grant denied', 'GRANT lake_replication_admin TO grants_runtime_worker'),
        ('server configuration denied', "ALTER SYSTEM SET work_mem='8MB'"),
        ('launch configuration mutation denied', "UPDATE lake_replication.worker_launch_config SET desired_state='stopped'"),
        ('source identity replacement denied', 'UPDATE lake_replication.sources SET system_identifier=0'),
        ('source registration denied', 'INSERT INTO lake_replication.sources(system_identifier,database_oid,timeline) VALUES(1,1,1)'),
        ('cleanup metadata reads denied', 'SELECT * FROM lake_replication.pending_cleanup'),
        ('maintenance policy replacement denied', "UPDATE lake_replication.journal_maintenance_policies SET retention_interval='0 seconds'"),
    ]:
        denied(name, statement)
    destination("CREATE TABLE unrelated.future_secret(value text); INSERT INTO unrelated.future_secret VALUES('future')")
    denied('future application tables are not granted', 'SELECT * FROM unrelated.future_secret')


def exercise(recipe):
    destination(f'CREATE ROLE {WORKER} LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE '
                'NOREPLICATION NOBYPASSRLS PASSWORD '+literal(q.ADMIN_PASSWORD))
    destination("CREATE SCHEMA worker_lake; CREATE SCHEMA unrelated; CREATE TABLE unrelated.secret(value text); "
                "INSERT INTO unrelated.secret VALUES('private')")
    denied('unprovisioned worker cannot use the control schema', 'SELECT * FROM lake_replication.subscriptions')
    # Prove the recipe does not depend on database-wide PUBLIC privileges.
    destination('REVOKE CONNECT, TEMPORARY ON DATABASE analytics FROM PUBLIC')
    q.check('recipe rejects superuser login', apply_recipe(recipe, role='postgres', succeeds=False).returncode != 0, True)
    q.check('recipe rejects shared public destination schema', apply_recipe(recipe, schema='public', succeeds=False).returncode != 0, True)
    q.check('recipe rejects a missing worker parameter', apply_recipe(recipe,role=None,succeeds=False).returncode != 0,True)
    q.check('recipe rejects a missing schema parameter', apply_recipe(recipe,schema=None,succeeds=False).returncode != 0,True)
    destination('CREATE ROLE rejected_runtime LOGIN INHERIT; CREATE ROLE rejected_noinherit LOGIN NOINHERIT; '
                'GRANT lake_replication_admin TO rejected_runtime')
    q.check('recipe rejects administrator membership',apply_recipe(recipe,role='rejected_runtime',succeeds=False).returncode != 0,True)
    destination('REVOKE lake_replication_admin FROM rejected_runtime; GRANT pg_read_all_data TO rejected_runtime')
    q.check('recipe rejects inherited broad data access',apply_recipe(recipe,role='rejected_runtime',succeeds=False).returncode != 0,True)
    q.check('recipe rejects NOINHERIT login',apply_recipe(recipe,role='rejected_noinherit',succeeds=False).returncode != 0,True)
    q.check('invalid recipes grant no write capabilities',destination(
            "SELECT pg_has_role('rejected_runtime','lake_read_write','MEMBER')::text"),'false')
    q.check('recipe treats a quoted role parameter as data',apply_recipe(recipe,
            role="absent'; DROP TABLE unrelated.secret; --",succeeds=False).returncode != 0,True)
    apply_recipe(recipe)
    apply_recipe(recipe)
    q.check('recipe can be applied twice', worker('SELECT current_user'), WORKER)
    q.check('recipe explicitly grants required database TEMPORARY', worker(
            "SELECT has_database_privilege(current_user,current_database(),'TEMPORARY')::text"), 'true')
    negative_checks()
    write_credentials()
    source("CREATE TABLE grants_items(id bigint PRIMARY KEY,payload text NOT NULL); "
           "INSERT INTO grants_items SELECT g,'snapshot-'||g FROM generate_series(1,6000) g; "
           "CREATE PUBLICATION grants_pub FOR TABLE grants_items; GRANT SELECT ON grants_items TO lake_source")
    # Deliberately slow this private copy so an actual interrupted snapshot is
    # observable before publication. Production settings are not changed.
    destination("ALTER SYSTEM SET lake_replication.initial_copy_pause_ms=1000")
    destination('SELECT pg_reload_conf()')
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application?sslmode=disable"
    destination('CREATE SUBSCRIPTION grants_sub CONNECTION '+literal(connection)+
                " PUBLICATION grants_pub WITH(lake_target='iceberg',lake_target_schema='worker_lake',"
                "lake_credential_file='/credentials/grants-worker.env')")
    identity=int(destination("SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='grants_sub'"))
    postmaster=destination('SELECT pg_postmaster_start_time()')
    for name, sql in [
        ('native subscription pause denied', 'ALTER SUBSCRIPTION grants_sub DISABLE'),
        ('native subscription removal denied', 'DROP SUBSCRIPTION grants_sub'),
        ('native subscription creation denied', 'CREATE SUBSCRIPTION unauthorized_sub CONNECTION '+literal(connection)+
         " PUBLICATION grants_pub WITH(lake_target='iceberg')"),
    ]:
        denied(name, sql)
    q.check('non-superuser stages an initial-copy chunk', q.wait_for('destination','analytics',
            f"SELECT EXISTS(SELECT FROM lake_replication.initial_copies WHERE subscription_id={identity} "
            "AND state='copying' AND rows_copied>0)::text", 'true', timeout=45), 'true')
    previous=supervisor(identity)
    q.compose('exec','-T','destination','kill','-KILL',str(previous))
    wait_replacement(identity,previous)
    source("UPDATE grants_items SET payload='after-copy-interruption' WHERE id=1")
    q.check('non-superuser recovers interrupted copy',q.wait_for('destination','analytics',
            f"SELECT state FROM lake_replication.subscriptions WHERE subscription_id={identity}", 'streaming', timeout=90),'streaming')
    q.check('recovered copy contains exactly 6000 rows',destination('SELECT count(*) FROM worker_lake.grants_items'),'6000')
    q.check('new write survives interrupted copy',q.wait_for('destination','analytics',
            'SELECT payload FROM worker_lake.grants_items WHERE id=1','after-copy-interruption'),'after-copy-interruption')
    q.check('worker owns its destination table',destination(
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='worker_lake.grants_items'::regclass"),WORKER)
    destination("ALTER SYSTEM RESET lake_replication.initial_copy_pause_ms")
    destination('SELECT pg_reload_conf()')
    source("BEGIN; UPDATE grants_items SET payload='updated' WHERE id=2; DELETE FROM grants_items WHERE id=3; "
           "INSERT INTO grants_items VALUES(7001,'inserted'); COMMIT")
    q.check('non-superuser applies insert update delete',q.wait_for('destination','analytics',
            "SELECT (SELECT payload FROM worker_lake.grants_items WHERE id=2)||':'||"
            "(SELECT count(*) FROM worker_lake.grants_items WHERE id=3)||':'||"
            "(SELECT payload FROM worker_lake.grants_items WHERE id=7001)", 'updated:0:inserted'),'updated:0:inserted')
    source('ALTER TABLE grants_items ADD COLUMN extra text; '
           "UPDATE grants_items SET extra='schema-value' WHERE id=2")
    q.check('non-superuser adds a nullable source column',q.wait_for('destination','analytics',
            "SELECT extra FROM worker_lake.grants_items WHERE id=2",'schema-value'),'schema-value')
    previous=supervisor(identity)
    q.compose('exec','-T','destination','kill','-KILL',str(previous))
    wait_replacement(identity,previous)
    source("INSERT INTO grants_items VALUES(7002,'after-stream-crash','recovered')")
    q.check('non-superuser resumes after streaming crash',q.wait_for('destination','analytics',
            'SELECT payload FROM worker_lake.grants_items WHERE id=7002','after-stream-crash'),'after-stream-crash')
    q.compose('restart','source',timeout=90)
    source("INSERT INTO grants_items VALUES(7003,'after-source-restart',NULL)")
    q.check('non-superuser reconnects after source restart',q.wait_for('destination','analytics',
            'SELECT payload FROM worker_lake.grants_items WHERE id=7003','after-source-restart',timeout=90),'after-source-restart')
    q.check('faults preserve destination postmaster',destination('SELECT pg_postmaster_start_time()'),postmaster)
    destination('ALTER SUBSCRIPTION grants_sub DISABLE')
    q.check('non-superuser subscription stops',q.wait_for('destination','analytics',
            f'SELECT health_state FROM lake_replication.subscription_status WHERE subscription_id={identity}', 'stopped'),'stopped')
    destination(f'ALTER ROLE {WORKER} PASSWORD '+literal(q.ROTATED_SOURCE_PASSWORD))
    destination("ALTER SUBSCRIPTION grants_sub SET(lake_credential_file='/credentials/grants-worker-rotated.env')")
    destination('ALTER SUBSCRIPTION grants_sub ENABLE')
    source("INSERT INTO grants_items VALUES(7004,'after-password-rotation',NULL)")
    q.check('non-superuser reloads rotated credentials',q.wait_for('destination','analytics',
            'SELECT payload FROM worker_lake.grants_items WHERE id=7004','after-password-rotation'),'after-password-rotation')
    # Force one eligible cleanup with the invoker privileges in this recipe.
    destination(f"SELECT lake_replication.configure_journal_maintenance({identity},true,'0 seconds','1 second',1000)")
    q.check('worker can run checkpoint-safe journal maintenance',worker(
            f'SELECT did_run FROM lake_replication.run_journal_maintenance({identity},true)'), 't')
    source('TRUNCATE grants_items; '+"INSERT INTO grants_items VALUES(8001,'after-truncate','final')")
    q.check('non-superuser publishes truncate and later insert',q.wait_for('destination','analytics',
            "SELECT count(*)||':'||min(payload)||':'||min(extra) FROM worker_lake.grants_items",'1:after-truncate:final'),'1:after-truncate:final')
    q.compose('restart','destination',timeout=90)
    q.check('non-superuser restarts with persisted grants',q.wait_for('destination','analytics',
            f"SELECT health_state FROM lake_replication.subscription_status WHERE subscription_id={identity}",'healthy',timeout=90),'healthy')
    source("INSERT INTO grants_items VALUES(8002,'after-destination-restart',NULL)")
    q.check('non-superuser streams after destination restart',q.wait_for('destination','analytics',
            'SELECT payload FROM worker_lake.grants_items WHERE id=8002','after-destination-restart'),'after-destination-restart')
    q.check('worker sessions keep the dedicated identity',destination(
            f"SELECT count(*)>0 FROM pg_stat_activity WHERE usename='{WORKER}' AND datname='analytics'"),'t')
    destination('DROP SUBSCRIPTION grants_sub')
    q.check('operator DROP removes non-superuser source slot',q.wait_for('source','application',
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name='grants_sub'",'0'),'0')
    q.check('unrelated application data is unchanged',destination('SELECT value FROM unrelated.secret'),'private')
    q.check('worker still lacks administrator membership',worker(
            "SELECT pg_has_role(current_user,'lake_replication_admin','MEMBER')::text"),'false')


def main():
    token=secrets.token_hex(4)
    q.PROJECT+='-grants-'+token;q.SOURCE_IMAGE+='-grants-'+token;q.DESTINATION_IMAGE+='-grants-'+token
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE,QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    q.RESULTS=ROOT/'.qualification'/(q.RUN+'_worker_grants_'+token)
    q.RESULTS.mkdir(parents=True)
    q.EVIDENCE.update(status='running',scope=__doc__,release_qualified=False,subscription_isolation_qualified=False,project=q.PROJECT)
    q.EVIDENCE['inputs']={}
    for p in [Path(__file__),Path(q.__file__)]:
        target=q.RESULTS/'sources'/p.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(p.read_bytes())
        q.EVIDENCE['inputs'][str(p.relative_to(ROOT))]=hashlib.sha256(p.read_bytes()).hexdigest()
    try:
        archive=q.archive_path();q.verify_archive(archive)
        with tarfile.open(archive) as bundle:
            members=[m for m in bundle if m.name.endswith('/WORKER_GRANTS.sql')]
            if len(members)!=1:raise ValueError('archive must contain WORKER_GRANTS.sql')
            recipe=bundle.extractfile(members[0]).read().decode()
        q.check('packaged recipe matches current source',recipe,(ROOT/'deploy/install/worker-grants.sql').read_text())
        (q.RESULTS/'WORKER_GRANTS.sql').write_text(recipe)
        q.EVIDENCE['recipe_sha256']=hashlib.sha256(recipe.encode()).hexdigest()
        with tempfile.TemporaryDirectory(prefix='lake-worker-grants-') as context:q.build_images(archive,Path(context))
        q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
        exercise(recipe)
        q.EVIDENCE['status']='passed'
    except Exception as error:
        q.EVIDENCE.update(status='error',error=str(error));print('ERROR:',error,flush=True)
    finally:
        try:
            logs=q.compose('logs','--no-color',check_result=False);(q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
            try:(q.RESULTS/'worker.log').write_text(q.worker_logs())
            except Exception:pass
        finally:
            cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
            q.EVIDENCE['cleanup_returncode']=cleanup.returncode
            if cleanup.returncode:q.EVIDENCE['status']='error'
            (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2,sort_keys=True)+'\n')
            print('Evidence:',q.RESULTS,flush=True)
    return int(q.EVIDENCE['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

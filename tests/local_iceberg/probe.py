#!/usr/bin/env python3
"""One PostgreSQL database mirrors its own heaps directly into Iceberg."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import time

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tests/push_capture'))
spec=importlib.util.spec_from_file_location('source_fixture_probe',ROOT/'tests/push_capture/probe.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
Probe=fixture.Probe


class LocalIcebergProbe(Probe):
    def __init__(self):
        super().__init__()
        self.compose=['docker','compose','-p',self.project,'-f',str(ROOT/'deploy/local/compose.yml')]
        self.evidence.update(scope='one PostgreSQL instance and database: local WAL -> local journal -> final Iceberg tables',
            unqualified=['production resource sizing','power-loss recovery','all schema operations'])

    def sql_args(self):
        args=super().sql_args();args[args.index('source')]='postgres';return args

    def provenance(self):
        super().provenance()
        for directory in ['deploy/local','tests/local_iceberg','extensions/lake_replication','migrations','worker/src']:
            for path in (ROOT/directory).rglob('*'):
                if path.is_file() and '__pycache__' not in path.parts and path.suffix not in ('.o','.so','.bc'):
                    self.evidence['files_sha256'][str(path.relative_to(ROOT))]=hashlib.sha256(path.read_bytes()).hexdigest()

        for name in ['Cargo.toml','Cargo.lock','worker/Cargo.toml']:
            self.evidence['files_sha256'][name]=hashlib.sha256((ROOT/name).read_bytes()).hexdigest()

    def wait(self,query,expected,timeout=60):
        end=time.monotonic()+timeout;latest=''
        while time.monotonic()<end:
            response=self.sql(query,required=False)
            latest=response.stdout.strip() if response.returncode==0 else response.stderr[-1000:]
            if response.returncode==0 and latest==expected:return latest
            time.sleep(.2)
        raise RuntimeError(f'local mirror timed out: {latest!r} != {expected!r}')

    def equal(self):
        for table in ['a','b']:
            self.check('local Iceberg mirror equals heap '+table,self.value(f'''SELECT NOT EXISTS(
                (TABLE public.{table} EXCEPT ALL TABLE iceberg_mirror.{table}) UNION ALL
                (TABLE iceberg_mirror.{table} EXCEPT ALL TABLE public.{table}))'''),'t')

    def setup(self,mirror=True,seed=True):
        self.sql('''CREATE ROLE local_source LOGIN REPLICATION;CREATE ROLE local_worker LOGIN;CREATE SCHEMA iceberg_mirror;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            CREATE PUBLICATION local_iceberg_pub FOR TABLE a,b;
            GRANT SELECT ON a,b TO local_source;''')
        if seed:self.sql("INSERT INTO a VALUES(1,'baseline');INSERT INTO b VALUES(2,'baseline')")
        self.command(self.sql_args()+['-v','worker_login=local_worker','-v','destination_schema=iceberg_mirror'],
            input=(ROOT/'deploy/install/worker-grants.sql').read_text())
        code="from pathlib import Path;import os,pwd;p=Path('/tmp/local-iceberg.env');p.write_text('LAKE_SOURCE_USER=local_source\\nLAKE_DESTINATION_USER=local_worker\\nLAKE_SOURCE_TLS_MODE=disable\\nLAKE_DESTINATION_TLS_MODE=disable\\n');os.chmod(p,0o600);u=pwd.getpwnam('postgres');os.chown(p,u.pw_uid,u.pw_gid)"
        self.command(self.compose+['exec','-T','postgres','python3','-c',code])
        if not mirror:return
        self.start_subscription()
        self.wait('SELECT count(*) FROM iceberg_mirror.a','1' if seed else '0')
        self.wait('SELECT count(*) FROM iceberg_mirror.b','1' if seed else '0')
        self.wait_initial_copy()

    def start_subscription(self):
        self.sql("""CREATE SUBSCRIPTION local_iceberg CONNECTION
            'host=127.0.0.1 port=5432 dbname=capture user=local_source sslmode=disable'
            PUBLICATION local_iceberg_pub WITH(lake_target='iceberg',lake_target_schema='iceberg_mirror',
            lake_credential_file='/tmp/local-iceberg.env')""")

    def wait_initial_copy(self):
        self.wait("SELECT count(*) FROM lake_replication.initial_copies c JOIN lake_replication.subscriptions s USING(subscription_id) WHERE s.subscription_name='local_iceberg' AND c.state='ready'",'2')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'healthy')

    def exercise(self):
        self.check('only one PostgreSQL service is configured',self.command(self.compose+['config','--services']).stdout.splitlines().count('postgres'),1)
        self.check('no remote destination service is configured','destination' in self.command(self.compose+['config','--services']).stdout.splitlines(),False)
        self.setup()
        self.equal()
        self.check('journal and application share the same database identity',self.value('''SELECT
            (origin.system_identifier=lake_replication.destination_system_identifier() AND
            origin.database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()))::text
            FROM lake_replication.subscriptions s JOIN lake_replication.sources origin USING(source_id) WHERE subscription_name='local_iceberg' '''),'true')
        self.check('source capture and push importer extensions are unnecessary',self.value("SELECT count(*) FROM pg_extension WHERE extname IN ('lake_replication_capture','lake_replication_push')"),'0')
        self.check('no intermediate Iceberg capture-log schema exists',self.value("SELECT (to_regnamespace('lake_capture_log') IS NULL)::text"),'true')
        self.check('local worker has no superuser or replication privileges',self.value("SELECT (NOT rolsuper AND NOT rolreplication)::text FROM pg_roles WHERE rolname='local_worker'"),'true')
        self.sql("BEGIN;INSERT INTO a VALUES(3,'streamed');UPDATE b SET note='updated' WHERE id=2;COMMIT;BEGIN;INSERT INTO a VALUES(99,'aborted');ROLLBACK")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','2');self.wait("SELECT note FROM iceberg_mirror.b WHERE id=2",'updated');self.equal()
        self.sql("BEGIN;UPDATE a SET id=4,note='new key' WHERE id=3;DELETE FROM b WHERE id=2;INSERT INTO b VALUES(5,'reinserted');COMMIT")
        self.wait('SELECT max(id) FROM iceberg_mirror.a','4');self.wait('SELECT max(id) FROM iceberg_mirror.b','5');self.equal()
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        self.sql("INSERT INTO a VALUES(6,'while stopped')")
        self.check('disabled local mirror retains its old contents',self.value('SELECT count(*) FROM iceberg_mirror.a'),'2')
        self.sql('ALTER SUBSCRIPTION local_iceberg ENABLE');self.wait('SELECT count(*) FROM iceberg_mirror.a','3')
        self.sql('ALTER TABLE a ADD COLUMN extra text');self.sql("UPDATE a SET extra='schema evolution'")
        self.wait("SELECT count(*) FROM pg_attribute WHERE attrelid='iceberg_mirror.a'::regclass AND attname='extra' AND NOT attisdropped",'1')
        self.wait("SELECT count(*) FROM iceberg_mirror.a WHERE extra='schema evolution'",'3');self.equal()
        self.command(self.compose+['restart','postgres'],timeout=90)
        self.wait('SELECT 1','1')
        self.sql("INSERT INTO b VALUES(7,'after restart')");self.wait('SELECT count(*) FROM iceberg_mirror.b','2');self.equal()
        self.sql("TRUNCATE a,b;INSERT INTO a VALUES(10,'after truncate','v2');INSERT INTO b VALUES(11,'after truncate')")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','1');self.wait('SELECT min(id) FROM iceberg_mirror.a','10');self.wait('SELECT min(id) FROM iceberg_mirror.b','11');self.equal()
        self.check('mirror writes cannot feed back into capture selection',self.value("SELECT string_agg(schemaname||'.'||tablename,',' ORDER BY tablename) FROM pg_publication_tables WHERE pubname='local_iceberg_pub'"),'public.a,public.b')
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0')
        self.check('drop preserves source and final Iceberg rows',self.value('SELECT (SELECT count(*) FROM a)::text||\':\'||(SELECT count(*) FROM iceberg_mirror.a)'),'1:1')


if __name__=='__main__':sys.exit(LocalIcebergProbe().run())

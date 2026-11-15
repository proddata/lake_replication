#!/usr/bin/env python3
"""Destination retained schema metadata and fail-closed publication guards."""
import json
import sys
from live import LiveProbe
from session import Session


class DestinationHistoryProbe(LiveProbe):
    def literal(self,value):return "'"+json.dumps(value).replace("'","''")+"'::jsonb"

    def as_role(self,role,statement,required=True):
        args=self.sql_args();args[args.index('source')]='destination';args[args.index('capture')]='analytics';args[args.index('postgres')]=role
        return self.command(args,input=statement,required=required)

    def initialize(self,definitions,required=True,role='postgres'):
        return self.as_role(role,f'SELECT lake_replication.initialize_push_schema_history({self.subscription},{self.literal(definitions)})',required)

    def state(self):
        return self.dest(f'''SELECT schema_enabled::text||':'||
            (SELECT count(*) FROM lake_replication.push_schema_tables WHERE subscription_id={self.subscription})||':'||
            (SELECT count(*) FROM lake_replication.push_schema_versions WHERE subscription_id={self.subscription})
            FROM lake_replication.push_bindings WHERE subscription_id={self.subscription}''')

    def exercise(self):
        self.evidence.update(scope='destination initial schema history and default publisher exclusion',
            unqualified=['ordered schema journal admission','ordered schema publisher','public end-to-end activation'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.identity=json.loads(self.value('''SELECT json_build_object('system_identifier',origin_system_identifier::text,
            'database_oid',origin_database_oid::text,'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),
            'table_a',table_a_id,'table_b',table_b_id) FROM lake_capture.streams'''))
        self.destination('CREATE EXTENSION lake_replication_push')
        args=self.sql_args();args[args.index('source')]='destination';args[args.index('capture')]='analytics'
        old=Session(args)
        try:
            old.query('BEGIN ISOLATION LEVEL REPEATABLE READ;CREATE TEMP TABLE pg_extension(oid oid,extname name);SELECT count(*) FROM lake_replication.push_bindings')
            self.overlay_installed=True;self.bind_destination()
            denied=False
            try:old.query('DROP EXTENSION lake_replication_push CASCADE;COMMIT')
            except RuntimeError as error:denied='drop push subscriptions' in str(error)
            self.check('old destination snapshots and temporary catalogs cannot hide active push bindings',denied,True)
        finally:old.close()
        self.evidence['destination_foreign_contract']=json.loads(self.dest('''SELECT json_build_object(
            'options',f.ftoptions,'server',s.srvname,'fdw',w.fdwname) FROM pg_foreign_table f
            JOIN pg_foreign_server s ON s.oid=f.ftserver JOIN pg_foreign_data_wrapper w ON w.oid=s.srvfdw
            WHERE f.ftrelid='mirror.a'::regclass'''))
        definitions=json.loads(self.value("SELECT json_agg(json_build_object('table_id',table_id,'definition',definition_json) ORDER BY table_id) FROM lake_capture.schema_definitions"))
        self.dest('CREATE ROLE schema_untrusted LOGIN;CREATE ROLE schema_admin LOGIN IN ROLE lake_replication_admin')
        denied=self.initialize(definitions,False,'schema_untrusted')
        self.check('ordinary destination login cannot initialize protected schema history',denied.returncode!=0,True)
        invalid=json.loads(json.dumps(definitions));invalid[0]['definition']['columns'][1]['name']='mismatch'
        failed=self.initialize(invalid,False)
        self.check('conflicting initial definitions cannot initialize a binding',failed.returncode!=0,True)
        self.check('failed initialization leaves no partial history',self.state(),'false:0:0')
        duplicate=self.initialize([definitions[0],definitions[0]],False)
        self.check('duplicate identities roll back the complete initialization',duplicate.returncode!=0 and self.state()=='false:0:0',True)
        self.dest('ALTER TABLE mirror.a RENAME COLUMN note TO wrong')
        failed=self.initialize(definitions,False)
        self.check('destination descriptor drift prevents initialization',failed.returncode!=0 and 'destination schema differs' in failed.stderr,True)
        self.dest('ALTER TABLE mirror.a RENAME COLUMN wrong TO note')
        self.initialize(definitions,role='schema_admin')
        self.check('administrator initializes both retained table definitions atomically',self.state(),'true:2:2')
        self.check('imported and published schema heads begin at version one',self.dest(f'''SELECT bool_and(
            imported_version=1 AND published_version=1)::text FROM lake_replication.push_schema_tables
            WHERE subscription_id={self.subscription}'''),'true')
        failed=self.initialize(definitions,False)
        self.check('existing history cannot be reinitialized from later catalogs',failed.returncode!=0 and 'fresh push binding' in failed.stderr,True)
        for item in definitions:
            native=self.value(f"SELECT encode(definition_bytes,'hex') FROM lake_capture.schema_definitions WHERE table_id='{item['table_id']}'")
            self.check('destination retains canonical initial bytes for '+item['definition']['name'],self.dest(f"SELECT encode(definition_bytes,'hex') FROM lake_replication.push_schema_versions WHERE table_id='{item['table_id']}'"),native)
        changes=[dict(remote_relation_id=1,relation_schema_version=1,operation='insert',new_tuple=dict(id='1',note='guarded'))]
        failed=self.destination(f'''SELECT lake_replication.import_push_batch({self.subscription},'{self.stream}',
            {self.identity['system_identifier']},{self.identity['database_oid']},1,1,'11111111-1111-1111-1111-111111111111',
            repeat('a',64),77,'0/10','0/20','0/30',now(),{self.literal(changes)})''',required=False)
        self.check('frozen importer cannot bypass schema-enabled admission',failed.returncode!=0 and 'requires ordered admission' in failed.stderr,True)
        self.dest(f"SELECT lake_replication.try_stage_transaction({self.subscription},77,'0/10','0/20','0/30',now(),{self.literal(changes)})")
        def publish():return self.destination(f"SELECT * FROM lake_replication.publish_insert_batch({self.subscription},'schema_guard','0/30','0/30')",required=False)
        failed=publish()
        self.check('default publisher refuses a schema-enabled transaction without an apply marker',
            failed.returncode!=0 and 'requires ordered push publication' in failed.stderr,True)
        self.dest(f'''INSERT INTO lake_replication.push_schema_transactions(transaction_id,subscription_id)
            SELECT transaction_id,subscription_id FROM lake_replication.transactions WHERE subscription_id={self.subscription}''')
        failed=publish()
        self.check('default publisher refuses unapplied schema actions',failed.returncode!=0 and 'requires ordered push publication' in failed.stderr,True)
        self.check('publication refusal rolls back all preceding destination DML',self.dest('SELECT count(*) FROM mirror.a'),'0')
        self.check('publication refusal preserves the staged transaction and checkpoint',self.dest(f'''SELECT
            state::text||':'||(SELECT published_lsn FROM lake_replication.checkpoint_publication WHERE subscription_id={self.subscription})
            FROM lake_replication.transactions WHERE subscription_id={self.subscription}'''),'staged:0/0')
        failed=self.destination(f'''BEGIN;UPDATE lake_replication.push_schema_transactions SET applied=true;
            SELECT * FROM lake_replication.publish_insert_batch({self.subscription},'schema_guard','0/30','0/30');
            SELECT 1/0;COMMIT''',required=False)
        self.check('late failure rolls back apply marker, rows and publication together',
            failed.returncode!=0 and self.dest("SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT bool_or(applied) FROM lake_replication.push_schema_transactions)")=='0:false',True)


if __name__=='__main__':sys.exit(DestinationHistoryProbe().run())

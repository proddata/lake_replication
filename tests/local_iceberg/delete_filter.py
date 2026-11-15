#!/usr/bin/env python3
"""DELETE pushdown preserves typed identities, exact matches and trigger order."""
import json
import sys
from probe import LocalIcebergProbe,ROOT
from session import Session


def literal(value):
    return "E'"+str(value).replace('\\','\\\\').replace("'","''")+"'"


def apply(table,old):
    values='ARRAY['+','.join(literal(json.dumps(value,ensure_ascii=False))+'::jsonb' for value in old)+']'
    return f"SELECT lake_replication.apply_change_run('delete',{literal(table)}::regclass,{values},NULL::jsonb[])"


class DeleteFilterProbe(LocalIcebergProbe):
    def exercise(self):
        self.setup(mirror=False,seed=False)
        identity="'lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure"
        state=f"SELECT md5(prosrc)||':'||coalesce(proacl::text,'NULL') FROM pg_proc WHERE oid={identity}"
        before=self.value(state)
        migration=(ROOT/'migrations/0033_delete_key_filter.sql').read_text()
        self.sql('BEGIN;'+ '\n'.join(migration.splitlines()[1:-1])+'ROLLBACK;')
        self.check('migration rollback preserves body and privileges',self.value(state),before)
        self.sql(migration)
        self.check('migration preserves function privileges',self.value(f"SELECT coalesce(proacl::text,'NULL') FROM pg_proc WHERE oid={identity}"),before.split(':',1)[1])
        self.check('apply function remains an extension member',self.value(f"SELECT count(*) FROM pg_depend WHERE classid='pg_proc'::regclass AND objid={identity} AND refclassid='pg_extension'::regclass AND refobjid=(SELECT oid FROM pg_extension WHERE extname='lake_replication') AND deptype='e'"),'1')
        cases=[('boolean',['true','false']),('smallint',['32767','-32768']),
            ('integer',['2147483647','-2147483648']),('bigint',['9223372036854775807','-9223372036854775808']),
            ('text',["O'Reilly\\雪",'second']),('varchar(64)',["quoted'key",'second']),
            ('uuid',['00112233-4455-6677-8899-aabbccddeeff','10112233-4455-6677-8899-aabbccddeeff']),
            ('numeric(30,4)',['123456789012345678901.1234','-0.0001']),
            ('char(8)',['padded','other'])]
        session=Session(self.sql_args())
        try:
            for repetition in range(2):
                for index,(kind,values) in enumerate(cases if repetition==0 else list(reversed(cases))):
                    table=f'df_{repetition}_{index}';column="key'雪"
                    session.query(f'CREATE TABLE {table}("{column}" {kind} NOT NULL,note text) USING iceberg;'+
                        f'INSERT INTO {table} VALUES'+','.join(f"({literal(value)}::{kind},'original')" for value in values))
                    self.check(kind+f': typed deletion pass {repetition+1}',session.query(apply(table,[{column:v} for v in values])),['2'])
                    self.check(kind+': no rows remain',session.query(f'SELECT count(*) FROM {table}'),['0'])
        finally:session.close()
        self.sql("CREATE TABLE df_null(id bigint,note text) USING iceberg;INSERT INTO df_null VALUES(NULL,'null'),(1,'one')")
        self.check('nullable identity keeps null-safe fallback',self.value(apply('df_null',[{'id':None}])),'1')
        self.sql("CREATE TABLE df_compound(id bigint NOT NULL,part integer NOT NULL,document jsonb) USING iceberg;INSERT INTO df_compound VALUES(1,1,'{\"x\":1}'),(1,2,'null')")
        self.check('compound/full-row JSONB predicate keeps fallback',self.value(apply('df_compound',[{'id':'1','part':'1','document':'{"x":1}'}])),'1')
        self.check('compound sibling remains',self.value('SELECT part FROM df_compound'),'2')
        self.sql("CREATE TABLE df_exact(id bigint NOT NULL) USING iceberg;INSERT INTO df_exact VALUES(1),(2)")
        missing=self.sql(apply('df_exact',[{'id':'1'},{'id':'999'}]),required=False)
        self.check('missing identity still fails exactly-one-match check',missing.returncode!=0 and 'exactly one' in missing.stderr,True)
        self.check('failed run rolls back preceding deletion',self.value('SELECT string_agg(id::text,\',\' ORDER BY id) FROM df_exact'),'1,2')
        self.sql('INSERT INTO df_exact VALUES(1)')
        duplicate=self.sql(apply('df_exact',[{'id':'1'}]),required=False)
        self.check('duplicate target identity still fails',duplicate.returncode!=0 and 'exactly one' in duplicate.stderr,True)
        self.check('duplicate failure leaves all target rows',self.value('SELECT count(*) FROM df_exact'),'3')
        if int(self.value("SELECT count(*) FROM pg_collation WHERE collprovider='i'")):
            self.sql('''CREATE COLLATION df_case_insensitive(provider=icu,locale='und-u-ks-level2',deterministic=false);
                CREATE TABLE df_collation(id text COLLATE df_case_insensitive NOT NULL) USING iceberg;
                INSERT INTO df_collation VALUES('lowercase')''')
            self.check('nondeterministic collation keeps PostgreSQL equality fallback',self.value(apply('df_collation',[{'id':'LOWERCASE'}])),'1')
        else:self.evidence.setdefault('unqualified',[]).append('ICU nondeterministic collation unavailable')
        self.sql('''CREATE TABLE df_order(id bigint NOT NULL) USING iceberg;INSERT INTO df_order VALUES(1),(2);
            CREATE TABLE df_audit(sequence bigserial,event text);
            CREATE FUNCTION df_record() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
              INSERT INTO public.df_audit(event) VALUES(CASE WHEN TG_LEVEL='ROW' THEN OLD.id::text ELSE 'statement' END);
              RETURN OLD;END $$;
            CREATE TRIGGER df_row BEFORE DELETE ON df_order FOR EACH ROW EXECUTE FUNCTION df_record();
            CREATE TRIGGER df_statement AFTER DELETE ON df_order FOR EACH STATEMENT EXECUTE FUNCTION df_record();''')
        self.check('ordered DELETE run with triggers succeeds',self.value(apply('df_order',[{'id':'2'},{'id':'1'}])),'2')
        self.check('row and statement trigger order remains unchanged',self.value("SELECT string_agg(event,',' ORDER BY sequence) FROM df_audit"),'2,statement,1,statement')
        self.sql('CREATE TABLE df_plan(id bigint NOT NULL) USING iceberg;INSERT INTO df_plan VALUES(1),(2)')
        plan=self.value("EXPLAIN(VERBOSE,FORMAT JSON) DELETE FROM df_plan WHERE id IS NOT DISTINCT FROM (jsonb_populate_record(NULL::df_plan,'{\"id\":\"1\"}'::jsonb)).id AND id=ANY(ARRAY[1]::bigint[])")
        self.evidence['delete_filter_plan']=json.loads(plan)
        self.check('typed membership reaches vectorized engine SQL','WHERE' in plan and 'ANY' in plan,True)
        self.check('EXPLAIN leaves target rows unchanged',self.value('SELECT count(*) FROM df_plan'),'2')


if __name__=='__main__':sys.exit(DeleteFilterProbe().run())

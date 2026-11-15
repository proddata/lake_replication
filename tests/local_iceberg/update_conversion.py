#!/usr/bin/env python3
"""Wide UPDATE conversion: type fidelity, partial rows, triggers and failures."""
import json
import sys
from probe import LocalIcebergProbe,ROOT
from session import Session


def literal(value):
    return "E'"+str(value).replace('\\','\\\\').replace("'","''")+"'"


def ident(value):
    return '"'+value.replace('"','""')+'"'


def apply(table,old,new,control=False):
    array=lambda values:'ARRAY['+','.join(literal(json.dumps(v,ensure_ascii=False))+'::jsonb' for v in values)+']::jsonb[]'
    function='update_conversion_control' if control else 'apply_change_run'
    return f"SELECT lake_replication.{function}('update',{literal(table)}::regclass,{array(old)},{array(new)})"


class UpdateConversionProbe(LocalIcebergProbe):
    def exercise(self):
        self.setup(mirror=False,seed=False)
        original=(ROOT/'migrations/0033_delete_key_filter.sql').read_text().replace(
            'FUNCTION lake_replication.apply_change_run(','FUNCTION lake_replication.update_conversion_control(',1)
        self.sql(original)
        self.sql('REVOKE ALL ON FUNCTION lake_replication.update_conversion_control(text,regclass,jsonb[],jsonb[]) FROM PUBLIC')
        identity="'lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure"
        state=f"SELECT md5(prosrc)||':'||coalesce(proacl::text,'NULL') FROM pg_proc WHERE oid={identity}"
        before=self.value(state);migration=(ROOT/'migrations/0034_update_row_conversion.sql').read_text()
        self.sql('BEGIN;'+'\n'.join(migration.splitlines()[1:-1])+'ROLLBACK;')
        self.check('migration rollback preserves function and privileges',self.value(state),before)
        self.sql(migration)
        self.check('migration preserves function privileges',self.value(f'SELECT coalesce(proacl::text,\'NULL\') FROM pg_proc WHERE oid={identity}'),before.split(':',1)[1])
        self.check('UPDATE function remains an extension member',self.value(f"SELECT count(*) FROM pg_depend WHERE classid='pg_proc'::regclass AND objid={identity} AND refclassid='pg_extension'::regclass AND refobjid=(SELECT oid FROM pg_extension WHERE extname='lake_replication') AND deptype='e'"),'1')
        definitions=[('id','bigint NOT NULL'),('note','text'),('payload','text'),('document','jsonb'),
            ('amount','numeric(38,6)'),('small','smallint'),('whole','integer'),('precise','double precision'),
            ('label','varchar(32)'),('region','char(8)'),('active','boolean'),('day','date'),
            ('instant','timestamptz'),('local_time','timestamp(3)'),('token','uuid'),('bytes','bytea'),("quoted'雪",'text')]
        definitions += [(f'c{i}','text') for i in range(48-len(definitions))]
        self.sql('CREATE TABLE uc_wide('+','.join(ident(n)+' '+t for n,t in definitions)+') USING iceberg;'+
            "INSERT INTO uc_wide(id,note,payload) SELECT i,'before',repeat('large-value-',10000) FROM generate_series(1,32) i")
        old=[{'id':str(i)} for i in range(1,33)]
        documents=[None,'null','"string"','[]','{"n":123456789012345678901234567890123456789}']
        values=dict(note='updated',payload='replacement',amount='12345678901234567890123456789012.123456',
            small='-32768',whole='2147483647',precise='1.2345678901234567',label='雪',region='EU',active='true',
            day='2000-02-29',instant='2020-01-02 03:04:05.123456+05:30',local_time='2000-02-29 23:59:59.123',
            token='00112233-4455-6677-8899-aabbccddeeff',bytes='\\x0001ff275c',**{"quoted'雪":"O'Reilly\\雪"})
        values.update({n:'value-'+n for n,t in definitions if n.startswith('c')})
        new=[dict(values,id=str(i),document=documents[i%len(documents)]) for i in range(1,33)]
        session=Session(self.sql_args())
        def compare(label,olds,news,table='uc_wide'):
            outputs=[]
            for control in [True,False]:
                session.query('BEGIN')
                try:
                    self.check(label+': affected rows',session.query(apply(table,olds,news,control)),[str(len(news))])
                    outputs.append(session.query(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {table} t'))
                finally:session.query('ROLLBACK')
            self.check(label+': exact typed results equal control',outputs[1]==outputs[0],True)
        try:
            compare('mixed scalars and JSONB wire values',old,new)
            partial=[{k:v for k,v in item.items() if k!='payload'} for item in new]
            compare('omitted large field remains unchanged',old,partial)
            moved=[dict(item,id=str(int(item['id'])+1000)) for item in new]
            compare('bulk primary-key changes',old,moved)
            compare('wide threshold boundary',old[:16],new[:16])
            compare('smaller run falls back',old[:15],new[:15])
            narrow=[{k:v for k,v in item.items() if k in ['id','note']} for item in new]
            compare('narrow assignment set falls back',old,narrow)
            for missing in [False,True]:
                bad_old=old+[{'id':'9999'}] if missing else old
                bad_new=new+[dict(new[0],id='9999')] if missing else new
                if not missing:self.sql("INSERT INTO uc_wide(id,note) VALUES(1,'duplicate')")
                failed=self.sql(apply('uc_wide',bad_old,bad_new),required=False)
                self.check('missing match fails' if missing else 'duplicate match fails',failed.returncode!=0 and 'exactly one per change' in failed.stderr,True)
                self.check('failed bulk update rolls back all changed values',self.value("SELECT count(*) FROM uc_wide WHERE note='updated'"),'0')
                if not missing:self.sql("DELETE FROM uc_wide WHERE note='duplicate'")
            self.sql('''CREATE TABLE uc_audit(id bigint,event text);
                CREATE FUNCTION public.uc_record() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                  INSERT INTO public.uc_audit VALUES(CASE WHEN TG_LEVEL='ROW' THEN NEW.id ELSE NULL END,TG_LEVEL);
                  RETURN NEW;END $$;
                CREATE TRIGGER uc_row AFTER UPDATE ON uc_wide FOR EACH ROW EXECUTE FUNCTION public.uc_record();
                CREATE TRIGGER uc_statement AFTER UPDATE ON uc_wide FOR EACH STATEMENT EXECUTE FUNCTION public.uc_record();''')
            session.query('BEGIN')
            try:
                self.check('wide update invokes row/statement triggers',session.query(apply('uc_wide',old,new)),['32'])
                self.check('row triggers fire once for every affected row',session.query("SELECT count(*),count(DISTINCT id) FROM uc_audit WHERE event='ROW'"),['32|32'])
                self.check('statement trigger fires once',session.query("SELECT count(*) FROM uc_audit WHERE event='STATEMENT'"),['1'])
            finally:session.query('ROLLBACK')
            self.sql('''CREATE SEQUENCE uc_conversion_calls;
                CREATE FUNCTION public.uc_domain_check(text) RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN
                  PERFORM nextval('public.uc_conversion_calls');RETURN true;END $$;
                CREATE DOMAIN uc_domain AS text CHECK(public.uc_domain_check(VALUE));''')
            self.sql('CREATE TABLE uc_custom(id bigint NOT NULL,'+','.join(f'c{i} text' for i in range(31))+',custom uc_domain);'+
                'INSERT INTO uc_custom(id) SELECT i FROM generate_series(1,17) i')
            custom_new=[dict(id=str(i),custom='domain',**{f'c{j}':'new' for j in range(31)}) for i in range(1,18)]
            calls=[]
            for control in [True,False]:
                session.query("SELECT setval('uc_conversion_calls',1,false);BEGIN")
                try:
                    self.check('custom input fallback updates every row',session.query(apply('uc_custom',[{'id':str(i)} for i in range(1,18)],custom_new,control)),['17'])
                    calls.append(session.query("SELECT currval('uc_conversion_calls')"))
                finally:session.query('ROLLBACK')
            self.check('custom input fallback preserves conversion behavior',calls[0]==calls[1],True)
        finally:session.close()


if __name__=='__main__':sys.exit(UpdateConversionProbe().run())

#!/usr/bin/env python3
"""Qualify packaged key filtering across scalar types and extension refresh paths."""
import argparse
import hashlib
import json
import re
from pathlib import Path
import tarfile

from update_compare import Runner, historical_definition


def literal(value):
    return "E'"+str(value).replace('\\','\\\\').replace("'","''")+"'"


def identifier(value):
    return '"'+value.replace('"','""')+'"'


def array(values):
    if values is None:
        return 'NULL::jsonb[]'
    return 'ARRAY['+','.join(literal(json.dumps(v,ensure_ascii=False,allow_nan=False))+'::jsonb' for v in values)+']::jsonb[]'


def apply(table, old, new, operation='update'):
    return f'SELECT lake_replication.apply_change_run({literal(operation)},{literal(table)}::regclass,{array(old)},{array(new)})'


def body_hash(client):
    source=json.loads(client.query("SELECT to_json(prosrc) FROM pg_proc WHERE oid="
        "'lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure")[0])
    return hashlib.sha256(source.encode()).hexdigest()


def check_types(h):
    p,q=h.client,h.q
    cases=[
        ('boolean',True,False),('smallint',32766,-32767),('integer',2147483646,-2147483647),
        ('bigint',9007199254740993,-9007199254740993),
        ('real','1.234567','2.345678'),('double precision','1.2345678901234567','2.3456789012345678'),
        ('text',"O'Reilly\\line\nGrüße",'other'),('varchar(80)',"quoted'\\key",'other'),
        ('char(12)','padding','other'),('bytea',r'\x0001ff275c',r'\x000211'),
        ('uuid','00112233-4455-6677-8899-aabbccddeeff','10112233-4455-6677-8899-aabbccddeeff'),
        ('numeric(24,4)','1234567890123456789.1234','2234567890123456789.1234'),
        ('date','2020-02-03','2021-02-03'),
        ('timestamp(3)','2020-02-03T04:05:06.789','2021-02-03T04:05:06.789'),
        ('timestamptz','2020-02-03T04:05:06.789123+05:30','2021-02-03T04:05:06.789123+05:30'),
    ]
    # Reversing the order exercises cached PL/pgSQL record fields after their
    # array element type changes repeatedly in one persistent backend.
    for repetition in (1,2):
        for number,(kind,first,second) in enumerate(cases if repetition==1 else list(reversed(cases)),1):
            table=f'uf_type_{repetition}_{number}'
            column='Account "Key' if repetition==1 else 'customer key'
            quoted=identifier(column)
            p.query('RESET extra_float_digits; RESET DateStyle; RESET TimeZone')
            p.query(f'CREATE TABLE {table} ({quoted} {kind} NOT NULL,payload text) USING iceberg; '
                    f'INSERT INTO {table} VALUES({literal(first)}::{kind},\'first\'),({literal(second)}::{kind},\'second\')')
            if kind in ('boolean','smallint','integer','bigint','text','varchar(80)','uuid'):
                p.query("SET extra_float_digits=-3; SET DateStyle='SQL, DMY'; SET TimeZone='Asia/Kolkata'")
            q.check(f'{kind} typed arbitrary-name update pass {repetition}',p.query(apply(table,
                    [{column:first}],[{column:first,'payload':'updated'}])),['1'])
            p.query('RESET extra_float_digits; RESET DateStyle; RESET TimeZone')
            # Verify with PostgreSQL's native type semantics. A remote EXCEPT
            # can compare char padding differently from PostgreSQL bpchar.
            p.query(f'DROP TABLE IF EXISTS uf_actual; CREATE TEMP TABLE uf_actual ({quoted} {kind},payload text); '
                    f'INSERT INTO uf_actual SELECT {quoted},payload FROM {table}')
            expected=f"VALUES ({literal(first)}::{kind},'updated'::text),({literal(second)}::{kind},'second'::text)"
            q.check(f'{kind} exact keys and values pass {repetition}',p.query(
                f'WITH expected({quoted},payload) AS ({expected}), differences AS ('
                f'(SELECT {quoted},payload FROM uf_actual EXCEPT ALL SELECT * FROM expected) UNION ALL '
                f'(SELECT * FROM expected EXCEPT ALL SELECT {quoted},payload FROM uf_actual)) '
                'SELECT count(*) FROM differences'),['0'])
    p.query("RESET extra_float_digits; RESET DateStyle; RESET TimeZone")


def check_edges(h):
    p,q=h.client,h.q
    schema="key'filter";name='table; --';column='key"; --'
    table=identifier(schema)+'.'+identifier(name)
    p.query(f'CREATE SCHEMA {identifier(schema)}; CREATE TABLE {table} ({identifier(column)} bigint NOT NULL,payload text) USING iceberg; '
            f'INSERT INTO {table} VALUES(1,\'one\'),(2,\'two\'); CREATE TABLE uf_sentinel(id integer); INSERT INTO uf_sentinel VALUES(1)')
    dangerous="value'); DROP TABLE uf_sentinel; --\\雪"
    q.check('quoted schema/table/key names and payload remain data',p.query(apply(table,[{column:1}],[{column:1,'payload':dangerous}])),['1'])
    q.check('quoted payload preserved exactly',p.query(f'SELECT payload={literal(dangerous)} FROM {table} WHERE {identifier(column)}=1'),['t'])
    q.check('payload cannot execute SQL',p.query('SELECT count(*) FROM uf_sentinel'),['1'])
    p.query('BEGIN; '+apply(table,[{column:1}],[{column:10,'payload':'ten'}])+'; '+
            apply(table,[{column:10}],[{column:20,'payload':'twenty'}])+'; COMMIT')
    q.check('ordered key changes use current identity',p.query(f'SELECT payload FROM {table} WHERE {identifier(column)}=20'),['twenty'])
    p.query('BEGIN; '+apply(table,[{column:20}],[{column:20,'payload':'rollback'}])+'; ROLLBACK')
    q.check('update rollback preserves committed row',p.query(f'SELECT payload FROM {table} WHERE {identifier(column)}=20'),['twenty'])
    for label,old,new in [
        ('missing identity',[{column:99}],[{column:99,'payload':'bad'}]),
        ('repeated identity',[{column:2},{column:2}],[{column:2,'payload':'a'},{column:2,'payload':'b'}]),
    ]:
        result=q.sql_result('destination','analytics',apply(table,old,new),check_result=False)
        q.check(label+' is rejected without a partial commit',result.returncode!=0 and 'exactly one per change' in result.stderr,True)
        q.check(label+' leaves existing values unchanged',p.query(f'SELECT payload FROM {table} WHERE {identifier(column)}=2'),['two'])
    p.query('CREATE TABLE uf_duplicates(account bigint NOT NULL,payload text) USING iceberg; '
            "INSERT INTO uf_duplicates VALUES(1,'a'),(1,'b')")
    result=q.sql_result('destination','analytics',apply('uf_duplicates',[{'account':1}],[{'account':1,'payload':'bad'}]),check_result=False)
    q.check('duplicate target identities remain fail-closed',result.returncode!=0 and 'exactly one per change' in result.stderr,True)
    q.check('duplicate-target rejection rolls back all values',p.query("SELECT string_agg(payload,',' ORDER BY payload) FROM uf_duplicates"),['a,b'])
    p.query('CREATE TABLE uf_nullable(account uuid,payload text) USING iceberg; '
            "INSERT INTO uf_nullable VALUES(NULL,'before')")
    q.check('nullable UUID identity retains null-safe fallback',p.query(apply('uf_nullable',
            [{'account':None}],[{'account':None,'payload':'after'}])),['1'])
    p.query('CREATE TABLE uf_composite(account bigint NOT NULL,tenant text,payload text) USING iceberg; '
            "INSERT INTO uf_composite VALUES(1,NULL,'before'),(1,'other','untouched')")
    q.check('composite nullable identity retains complete predicate',p.query(apply('uf_composite',
            [{'account':1,'tenant':None}],[{'account':1,'tenant':None,'payload':'after'}])),['1'])
    q.check('composite matching preserves the other tenant',p.query("SELECT payload FROM uf_composite WHERE tenant='other'"),['untouched'])
    p.query("CREATE TYPE uf_enum AS ENUM('red','blue'); CREATE TABLE uf_other(account uf_enum NOT NULL,payload text); "
            "INSERT INTO uf_other VALUES('red','before')")
    q.check('non-whitelisted identity type retains baseline path',p.query(apply('uf_other',
            [{'account':'red'}],[{'account':'red','payload':'after'}])),['1'])
    q.check('inserts retain the original path',p.query(apply(table,None,[{column:30,'payload':'insert'}],'insert')),['1'])
    q.check('deletes retain the original path',p.query(apply(table,[{column:30}],None,'delete')),['1'])


def check_upgrade(h,patch):
    q=h.q
    expected=body_hash(h.client)
    for database,legacy in [('uf_upgrade','0.1.0-beta.4'),('uf_refresh',None)]:
        q.sql('destination','analytics',f'CREATE DATABASE {database}')
        client=h.c.Psql('destination',database)
        try:
            client.query('CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; '+
                         (f"CREATE EXTENSION lake_replication VERSION '{legacy}'" if legacy else 'CREATE EXTENSION lake_replication'))
            if not legacy:client.query(historical_definition())
            client.query('CREATE TABLE uf_existing(customer_uuid uuid NOT NULL,payload text) USING iceberg; '
                         "INSERT INTO uf_existing VALUES('00112233-4455-6677-8899-aabbccddeeff','before')")
            before=body_hash(client)
            acl=client.query("SELECT coalesce(proacl::text,'NULL') FROM pg_proc WHERE oid="
                            "'lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure")
            upgrade="ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.5'" if legacy else '\n'.join(patch.splitlines()[1:-1])
            client.query('BEGIN; '+upgrade)
            q.check(database+' transaction sees new function',body_hash(client),expected)
            client.query('ROLLBACK')
            q.check(database+' rollback restores old function',body_hash(client),before)
            if legacy:client.query(upgrade)
            else:client.query(patch)
            q.check(database+' installs exact fresh-install function',body_hash(client),expected)
            q.check(database+' preserves function privileges',client.query("SELECT coalesce(proacl::text,'NULL') FROM pg_proc WHERE oid="
                    "'lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure"),acl)
            q.check(database+' preserves pre-existing rows',client.query('SELECT payload FROM uf_existing'),['before'])
            key='00112233-4455-6677-8899-aabbccddeeff'
            q.check(database+' upgraded arbitrary-name UUID update works',client.query(apply('uf_existing',
                    [{'customer_uuid':key}],[{'customer_uuid':key,'payload':'after'}])),['1'])
            q.check(database+' function remains an extension member',client.query("SELECT count(*) FROM pg_depend WHERE "
                    "classid='pg_proc'::regclass AND objid='lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure "
                    "AND refclassid='pg_extension'::regclass AND refobjid=(SELECT oid FROM pg_extension WHERE extname='lake_replication') AND deptype='e'"),['1'])
        finally:
            client.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpuset',default='2-3')
    opts=parser.parse_args()
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',opts.cpuset):
        parser.error('invalid CPU affinity')
    args=argparse.Namespace(rows=1000,updates=500,file_rows=1000,payload_bytes=128,
        run_sizes=[500],variants=['baseline','key_filter'],patterns=['clustered'],repetitions=1,
        seed=20261111,cpuset=opts.cpuset,analyze=False,pipeline=False,installed_filter=True)
    h=Runner(args)
    h.report.update(scope=__doc__,mode='packaged generalized key filter correctness and upgrade')
    h.report['inputs']['tests/performance/update_filter_probe.py']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    target=h.output/'sources/tests/performance/update_filter_probe.py'
    target.write_text(Path(__file__).read_text())
    try:
        h.setup()
        archive=h.q.archive_path()
        with tarfile.open(archive) as bundle:
            matches=[m for m in bundle.getmembers() if m.name.endswith('/DEVELOPMENT_UPDATES/0030_update_key_filter.sql')]
            if len(matches)!=1:raise ValueError('archive is missing the development SQL refresh')
            patch=bundle.extractfile(matches[0]).read().decode()
        expected_patch=(Path(__file__).resolve().parents[2]/'migrations/0030_update_key_filter.sql').read_text()
        h.q.check('packaged refresh matches measured migration',patch,expected_patch)
        h.report['packaged_refresh_sha256']=hashlib.sha256(patch.encode()).hexdigest()
        check_types(h)
        check_edges(h)
        check_upgrade(h,patch)
        h.report['status']='passed'
    except Exception as error:
        h.report.update(status='error',error=str(error),error_type=type(error).__name__)
        print('ERROR:',error,flush=True)
    finally:
        h.report['checks']=h.q.EVIDENCE['checks']
        h.finalize()
    return int(h.report['status']!='passed')


if __name__=='__main__':raise SystemExit(main())

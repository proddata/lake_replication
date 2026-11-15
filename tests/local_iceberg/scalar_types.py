#!/usr/bin/env python3
"""Same-instance scalar, compound-key and unchanged-TOAST fidelity."""
import sys
import os
from probe import LocalIcebergProbe


class LocalTypeProbe(LocalIcebergProbe):
    def exercise(self):
        self.evidence.update(scope='local initial-copy and WAL fidelity for supported scalar types, compound keys and external TOAST',
            unqualified=['arrays, domains and enums','all floating-point edge cases','unbounded values'])
        self.setup(mirror=False,seed=False)
        self.sql('''ALTER TABLE a DROP CONSTRAINT a_pkey;
            ALTER TABLE a ADD COLUMN part smallint,ADD PRIMARY KEY(id,part),
            ADD COLUMN amount numeric(38,6),ADD COLUMN label varchar(32),ADD COLUMN region char(2),
            ADD COLUMN active boolean,ADD COLUMN small_real real,ADD COLUMN precise double precision,
            ADD COLUMN day date,ADD COLUMN local_time timestamp,ADD COLUMN instant timestamptz,
            ADD COLUMN token uuid,ADD COLUMN document jsonb,ADD COLUMN bytes bytea,
            ADD COLUMN "quoted'雪" text;
            ALTER TABLE b ALTER COLUMN note SET STORAGE EXTERNAL;
            INSERT INTO a VALUES(9223372036854775807,E'世界\\nO''Reilly',32767,
                12345678901234567890123456789012.123456,'雪','EU',true,1.25,-0.125,
                '2000-02-29','2000-02-29 23:59:59.123456','2020-01-02 03:04:05.123456+05:30',
                '12345678-1234-4234-8234-123456789abc','{"nested":[1,true,null,"雪"]}',decode('00017fff80','hex'),'quoted column');
            INSERT INTO a VALUES(-9223372036854775808,NULL,-32768,-0.000001,NULL,NULL,false,
                NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
            INSERT INTO b SELECT 1,string_agg(md5(i::text),'') FROM generate_series(1,4000) i;''')
        if os.environ.get('LOCAL_ICEBERG_FULL_IDENTITY')=='1':
            self.sql('ALTER TABLE a REPLICA IDENTITY FULL')
            self.evidence['replica_identity_a']='full'
        toast=self.value("SELECT reltoastrelid::regclass FROM pg_class WHERE oid='b'::regclass")
        self.check('large source text is stored in external TOAST chunks',int(self.value(f'SELECT count(*) FROM {toast}'))>0,True)
        self.start_subscription();self.wait_initial_copy()
        self.wait('SELECT count(*) FROM iceberg_mirror.a','2');self.wait('SELECT count(*) FROM iceberg_mirror.b','1');self.equal()
        self.check('initial copy preserves exact precision and binary/JSON/temporal values',self.value("SELECT jsonb_agg(to_jsonb(t) ORDER BY id,part) FROM iceberg_mirror.a t"),self.value("SELECT jsonb_agg(to_jsonb(t) ORDER BY id,part) FROM a t"))
        original=self.value('SELECT md5(note) FROM b WHERE id=1')
        self.sql("BEGIN;UPDATE b SET id=2 WHERE id=1;UPDATE a SET amount=-12345678901234567890123456789012.123456,bytes=decode('00ff0000','hex'),document='{\"changed\":true}' WHERE part=32767;COMMIT")
        self.wait('SELECT id FROM iceberg_mirror.b','2')
        self.wait("SELECT document->>'changed' FROM iceberg_mirror.a WHERE part=32767",'true');self.equal()
        self.check('primary-key update preserves unchanged external TOAST text',self.value('SELECT md5(note) FROM iceberg_mirror.b WHERE id=2'),original)
        self.sql("""INSERT INTO a(id,part,document) VALUES(10,1,'null'::jsonb),(11,1,'"text"'::jsonb),
            (12,1,'123456789012345678901234567890123456789'::jsonb),(13,1,'[]'::jsonb),(14,1,NULL)""")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','7');self.equal()
        self.check('WAL insertion distinguishes JSON null, string, number, array and SQL NULL',self.value("SELECT string_agg(id::text||':'||coalesce(jsonb_typeof(document),'SQL_NULL'),',' ORDER BY id) FROM iceberg_mirror.a WHERE id BETWEEN 10 AND 14"),'10:null,11:string,12:number,13:array,14:SQL_NULL')
        self.check('JSONB numeric text retains precision beyond a float mantissa',self.value('SELECT document::text FROM iceberg_mirror.a WHERE id=12'),'123456789012345678901234567890123456789')
        self.sql("""BEGIN;UPDATE a SET document='null'::jsonb WHERE id=11;
            UPDATE a SET document='"changed string"'::jsonb WHERE id=10;
            UPDATE a SET document='{}'::jsonb WHERE id=14;DELETE FROM a WHERE id=12;COMMIT""")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','6')
        self.wait('SELECT document::text FROM iceberg_mirror.a WHERE id=14','{}');self.equal()
        self.sql("BEGIN;UPDATE b SET note=repeat('replacement-雪',10000) WHERE id=2;UPDATE a SET part=123 WHERE part=-32768;COMMIT")
        self.wait('SELECT part FROM iceberg_mirror.a WHERE id=-9223372036854775808','123')
        self.wait('SELECT md5(note) FROM iceberg_mirror.b',self.value('SELECT md5(note) FROM b'));self.equal()
        self.sql('UPDATE b SET note=NULL WHERE id=2')
        self.wait('SELECT (note IS NULL)::text FROM iceberg_mirror.b','true');self.equal()
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('typed local mirror cleanup releases its slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(LocalTypeProbe().run())

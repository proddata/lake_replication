#!/usr/bin/env python3
"""Native canonical descriptors, stable column IDs and fresh post-lock metadata."""
from concurrent.futures import ThreadPoolExecutor
import json
import struct
import sys
from live import LiveProbe, ROOT
from session import Session


def encoded(namespace,table,columns):
    def text(value):
        value=value.encode();return struct.pack('!I',len(value))+value
    result=b'LKDSC1'+text(namespace)+text(table)+b'd'+struct.pack('!I',len(columns))
    for identity,name,kind,key,nullable in columns:
        result+=struct.pack('!I',identity)+text(name)+struct.pack('!IiBB',kind,-1,key,nullable)
    return result.hex()


class DescriptorProbe(LiveProbe):
    def as_role(self,role,statement,required=True):
        args=self.sql_args();args[args.index('postgres')]=role
        return self.command(args,input=statement,required=required)

    def exercise(self):
        self.evidence.update(scope='bounded native schema descriptor encoding and column identity',
            unqualified=['source DDL auditing','schema-event journal admission','ordered publisher integration'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE public.items(id bigint PRIMARY KEY,note text);
            CREATE SCHEMA "odd schema";
            CREATE TABLE "odd schema"."items 雪"("key id" bigint PRIMARY KEY,"note ' 世界" text,
                active boolean NOT NULL,qty integer);
            CREATE ROLE schema_reader LOGIN;CREATE ROLE schema_untrusted LOGIN;
            GRANT USAGE ON SCHEMA lake_capture,"odd schema" TO schema_reader,schema_untrusted;
            GRANT EXECUTE ON FUNCTION lake_capture.describe_relation(regclass) TO schema_reader,schema_untrusted;
            GRANT SELECT ON "odd schema"."items 雪" TO schema_reader''')
        self.check('native descriptor matches the Rust canonical golden format',self.value(
            "SELECT encode(lake_capture.describe_relation('public.items'),'hex')"),
            '4c4b44534331000000067075626c6963000000056974656d7364000000020000000100000002696400000014ffffffff010000000002000000046e6f746500000019ffffffff0001')
        relation='"odd schema"."items 雪"'
        query=f"SELECT encode(lake_capture.describe_relation('{relation}'),'hex')"
        columns=[(1,'key id',20,1,0),(2,"note ' 世界",25,0,1),(3,'active',16,0,0),(4,'qty',23,0,1)]
        self.check('quoted UTF8 names and scalar/key/nullability metadata are exact',self.as_role(
            'schema_reader',query).stdout.strip(),encoded('odd schema','items 雪',columns))
        self.sql('GRANT EXECUTE ON FUNCTION lake_capture.describe_relation_json(regclass) TO schema_reader,schema_untrusted')
        descriptor=json.loads(self.as_role('schema_reader',f"SELECT lake_capture.describe_relation_json('{relation}')").stdout)
        self.check('native JSON descriptors carry the same stable metadata',descriptor,dict(namespace='odd schema',name='items 雪',
            replica_identity='d',columns=[dict(id=i,name=n,type_oid=t,type_modifier=-1,key=bool(k),nullable=bool(v)) for i,n,t,k,v in columns]))
        self.destination((ROOT/'extensions/lake_replication_push/schema_codec.sql').read_text())
        def literal(value):return "'"+json.dumps(value,ensure_ascii=False).replace("'","''")+"'::jsonb"
        before_bytes=self.value(query)
        self.check('destination codec reproduces native source definition bytes',self.dest(
            f"SELECT encode(lake_replication.push_encode_schema_definition({literal(descriptor)}),'hex')"),before_bytes)
        denied=self.as_role('schema_untrusted',query,False)
        self.check('native metadata helper enforces explicit selected-table SELECT',
            denied.returncode!=0 and 'permission denied' in denied.stderr,True)
        denied=self.as_role('schema_untrusted',f"SELECT lake_capture.describe_relation_json('{relation}')",False)
        self.check('JSON metadata helper preserves the same SELECT fence',denied.returncode!=0,True)
        self.sql('''ALTER TABLE "odd schema"."items 雪" ADD COLUMN extra text;
            ALTER TABLE "odd schema"."items 雪" RENAME COLUMN "note ' 世界" TO "renamed""field"''')
        columns[1]=(2,'renamed"field',25,0,1);columns.append((5,'extra',25,0,1))
        self.check('addition and rename preserve existing column identities',self.value(query),encoded('odd schema','items 雪',columns))
        added=dict(descriptor,columns=descriptor['columns']+[dict(id=5,name='extra',type_oid=25,type_modifier=-1,key=False,nullable=True)])
        renamed=json.loads(json.dumps(added));renamed['columns'][1]['name']='renamed"field'
        self.check('SQL transition validator admits one nullable addition',self.dest(
            f"SELECT lake_replication.push_schema_operation({literal(descriptor)},{literal(added)})"),'add_nullable')
        self.check('SQL transition validator admits one stable-identity rename',self.dest(
            f"SELECT lake_replication.push_schema_operation({literal(added)},{literal(renamed)})"),'rename')
        expected=b'LKDDL1'+bytes([7])*16+struct.pack('!IIB',1,2,0)
        after_bytes=bytes.fromhex(encoded('odd schema','items 雪',columns[:1]+[(2,"note ' 世界",25,0,1)]+columns[2:]))
        old=bytes.fromhex(before_bytes);expected+=struct.pack('!I',len(old))+old+struct.pack('!I',len(after_bytes))+after_bytes
        self.check('destination change codec preserves canonical versioned before/after definitions',self.dest(
            f"SELECT encode(lake_replication.push_encode_schema_change('07070707-0707-0707-0707-070707070707',1,2,{literal(descriptor)},{literal(added)}),'hex')"),expected.hex())
        for label,invalid in [('type conversion',dict(descriptor,columns=[dict(c,type_oid=23) if c['id']==1 else c for c in descriptor['columns']])),
                              ('non-null addition',dict(added,columns=added['columns'][:-1]+[dict(added['columns'][-1],nullable=False)])),
                              ('combined addition and rename',renamed)]:
            failed=self.destination(f"SELECT lake_replication.push_schema_operation({literal(descriptor)},{literal(invalid)})",required=False)
            self.check('SQL transition validator rejects '+label,failed.returncode!=0,True)
        reader=Session(self.sql_args());writer=Session(self.sql_args());executor=ThreadPoolExecutor(max_workers=1)
        try:
            reader.query(query) # Warm the same backend's relation cache.
            writer.query('''BEGIN;ALTER TABLE "odd schema"."items 雪" ADD COLUMN "queued field" text''')
            pending=executor.submit(reader.query,query)
            self.check('descriptor waits for a concurrent schema transaction',self.source_wait('''SELECT count(*) FROM pg_stat_activity
                WHERE pid<>pg_backend_pid() AND query LIKE '%describe_relation%' AND wait_event_type='Lock' ''','1'),True)
            writer.query('COMMIT')
            columns.append((6,'queued field',25,0,1))
            self.check('post-wait descriptor uses committed metadata despite the older statement snapshot',
                pending.result(timeout=15)[0],encoded('odd schema','items 雪',columns))
        finally:
            writer.close();reader.close();executor.shutdown(wait=True,cancel_futures=True)
        self.sql('''CREATE TABLE with_default(id bigint PRIMARY KEY,note text DEFAULT 'x');
            CREATE TABLE with_numeric(id bigint PRIMARY KEY,note numeric);
            CREATE TABLE with_rls(id bigint PRIMARY KEY,note text);ALTER TABLE with_rls ENABLE ROW LEVEL SECURITY;
            CREATE TABLE without_key(id bigint,note text)''')
        for table in ['with_default','with_numeric','with_rls','without_key']:
            failed=self.sql(f"SELECT lake_capture.describe_relation('{table}')",required=False)
            self.check('unsupported descriptor fails closed: '+table,failed.returncode!=0,True)


if __name__=='__main__':sys.exit(DescriptorProbe().run())

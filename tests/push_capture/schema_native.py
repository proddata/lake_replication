#!/usr/bin/env python3
"""Native ordered schema controls with an explicit test audit heap, no DDL admission."""
import json
import struct
import sys
from live import LiveProbe, ROOT

STREAM='01010101-0101-0101-0101-010101010101'
A='07070707-0707-0707-0707-070707070707'
B='08080808-0808-0808-0808-080808080808'


def events(raw):
    raw=bytes.fromhex(raw)
    assert raw[:6]==b'LKCAP1'
    count,a,b=struct.unpack('!III',raw[42:54]);position=54;result=[]
    for ordinal in range(count):
        size=struct.unpack('!I',raw[position:position+4])[0];position+=4
        value=raw[position:position+size];position+=size
        assert struct.unpack('!I',value[:4])[0]==ordinal
        if value[4]:
            result.append(dict(ordinal=ordinal,table=value[5:21].hex(),version=struct.unpack('!I',value[21:25])[0],kind=value[25],body=value[26:].hex()))
        else:
            assert struct.unpack('!I',value[6:10])[0]==len(value)-10
            result.append(dict(ordinal=ordinal,table=None,version=None,kind=value[5],body=value[10:].hex()))
    assert position==len(raw)
    return dict(counts=[a,b],events=result)


class NativeSchemaProbe(LiveProbe):
    def payload(self,table,before_version,before,after):
        def literal(value):return "'"+json.dumps(value).replace("'","''")+"'::jsonb"
        return self.dest(f"SELECT encode(lake_replication.push_encode_schema_change('{table}',{before_version},{before_version+1},{literal(before)},{literal(after)}),'hex')")

    def audit(self,table,before,payload):
        return f"INSERT INTO test_schema_audit VALUES('{STREAM}',1,'{table}',{before},{before+1},decode('{payload}','hex'));"

    def read(self,slot='schema_native',a_version=1,b_version=1,consume=False,required=True):
        function='pg_logical_slot_get_binary_changes' if consume else 'pg_logical_slot_peek_binary_changes'
        return self.sql(f"SELECT coalesce(json_agg(encode(data,'hex')),'[]'::json)::text FROM {function}('{slot}',NULL,NULL,"
            f"'table-a-oid','{self.a_oid}','table-b-oid','{self.b_oid}','table-a-id','{A}','table-b-id','{B}',"
            f"'schema-table-oid','{self.audit_oid}','stream-id','{STREAM}','capture-generation','1',"
            f"'table-a-version','{a_version}','table-b-version','{b_version}')",required=required)

    def exercise(self):
        self.evidence.update(scope='native DML-DDL-DML and DDL-only versioned decoding with explicit test audit heap',
            unqualified=['production source DDL auditing','capture actor initial version selection','schema-event journal admission','ordered publication'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            CREATE TABLE test_schema_audit(stream_id uuid,capture_generation bigint,table_id uuid,
                before_version integer,after_version integer,payload bytea);''')
        self.sql('ALTER TABLE test_schema_audit ALTER COLUMN payload SET STORAGE EXTERNAL')
        self.sql('ALTER TABLE b '+','.join(f'ADD COLUMN "padding_{i:03d}_'+('x'*40)+'" text' for i in range(100)))
        self.destination((ROOT/'extensions/lake_replication_push/schema_codec.sql').read_text())
        self.a_oid=self.value("SELECT 'a'::regclass::oid")
        self.b_oid=self.value("SELECT 'b'::regclass::oid")
        self.audit_oid=self.value("SELECT 'test_schema_audit'::regclass::oid")
        a=json.loads(self.value("SELECT lake_capture.describe_relation_json('a')"))
        b=json.loads(self.value("SELECT lake_capture.describe_relation_json('b')"))
        added=dict(a,columns=a['columns']+[dict(id=3,name='extra',type_oid=16,type_modifier=-1,key=False,nullable=True)])
        renamed=json.loads(json.dumps(added));renamed['columns'][1]['name']='label'
        b_added=dict(b,columns=b['columns']+[dict(id=len(b['columns'])+1,name='qty',type_oid=23,type_modifier=-1,key=False,nullable=True)])
        b_renamed=json.loads(json.dumps(b_added));b_renamed['columns'][1]['name']='aborted_label'
        add=self.payload(A,1,a,added);rename=self.payload(A,2,added,renamed)
        add_b=self.payload(B,1,b,b_added);aborted_b=self.payload(B,2,b_added,b_renamed)
        self.sql("SELECT pg_create_logical_replication_slot('schema_native','lake_replication_capture')")
        self.sql("BEGIN;INSERT INTO a VALUES(1,'old');ALTER TABLE a ADD COLUMN extra boolean;"+self.audit(A,1,add)+
            "INSERT INTO a VALUES(2,'new',true);ALTER TABLE a RENAME COLUMN note TO label;"+self.audit(A,2,rename)+
            "UPDATE a SET label='renamed' WHERE id=2;COMMIT")
        self.sql('BEGIN;ALTER TABLE b ADD COLUMN qty integer;'+self.audit(B,1,add_b)+'COMMIT')
        self.sql('BEGIN;ALTER TABLE b RENAME COLUMN note TO aborted_label;'+self.audit(B,2,aborted_b)+'ROLLBACK')
        self.sql("BEGIN;INSERT INTO a VALUES(3,'later',false);INSERT INTO b(id,note,qty) VALUES(4,'later b',42);COMMIT")
        toast_relation=self.value("SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid='test_schema_audit'::regclass")
        self.check('large immutable audit payloads use real external TOAST chunks',
            int(self.value(f'SELECT count(*) FROM {toast_relation}'))>0,True)
        batches=[events(raw) for raw in json.loads(self.read().stdout)]
        self.evidence['decoded_batches']=batches
        self.check('native decoding retains three complete committed source boundaries',len(batches),3)
        self.check('DML-DDL-DML emits versions at the original event ordinals',
            [event['version'] for event in batches[0]['events']],[1,None,2,None,3])
        self.check('historic row descriptors retain widths before and after ADD COLUMN',
            [int(batches[0]['events'][i]['body'][:8],16) for i in [0,2]],[2,3])
        self.check('both schema payloads retain boundary-time before and after definitions',
            [event['body'] for event in batches[0]['events'] if event['kind']==8],[add,rename])
        self.check('schema controls do not inflate native row table counts',batches[0]['counts'],[3,0])
        self.check('DDL-only source commit emits one tableless Schema control',
            [(e['kind'],e['body']) for e in batches[1]['events']],[(8,add_b)])
        self.check('DDL-only source commit has no selected row counts',batches[1]['counts'],[0,0])
        self.check('aborted schema change cannot advance either later table version',
            [e['version'] for e in batches[2]['events']],[3,2])
        self.check('native peek replays the same immutable schema boundaries',
            [events(raw) for raw in json.loads(self.read().stdout)],batches)
        self.read(consume=True)
        self.sql("BEGIN;INSERT INTO a VALUES(5,'after confirmation',NULL);INSERT INTO b(id,note,qty) VALUES(6,'after confirmation',NULL);COMMIT")
        later=[events(raw) for raw in json.loads(self.read(a_version=3,b_version=2).stdout)]
        self.check('new decoder accepts explicit retained versions at its confirmed slot boundary',
            [e['version'] for e in later[0]['events']],[3,2])
        invalid=self.read(a_version=34,required=False)
        self.check('native initial schema versions are bounded before reading WAL',
            invalid.returncode!=0 and 'invalid initial capture schema version' in invalid.stderr,True)
        # A bad audit event must not confirm away the protected source history.
        self.read(a_version=3,b_version=2,consume=True)
        before=self.value("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='schema_native'")
        self.sql(self.audit(A,1,add))
        failed=self.read(a_version=3,b_version=2,consume=True,required=False)
        self.check('out-of-order schema audit fails native decoding',
            failed.returncode!=0 and 'does not follow retained history' in failed.stderr,True)
        self.check('failed schema decode cannot advance the confirmed slot boundary',self.value(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='schema_native'"),before)
        self.sql("SELECT pg_create_logical_replication_slot('schema_oversize','lake_replication_capture')")
        before=self.value("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='schema_oversize'")
        self.sql(f"INSERT INTO test_schema_audit VALUES('{STREAM}',1,'{A}',3,4,convert_to(repeat('x',2097217),'UTF8'))")
        failed=self.read(slot='schema_oversize',a_version=3,b_version=2,consume=True,required=False)
        self.check('oversized external audit fails before detoasting its payload',
            failed.returncode!=0 and 'schema audit payload byte budget exceeded' in failed.stderr,True)
        self.check('oversized external audit preserves its confirmed boundary',self.value(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='schema_oversize'"),before)


if __name__=='__main__':sys.exit(NativeSchemaProbe().run())

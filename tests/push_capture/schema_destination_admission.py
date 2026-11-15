#!/usr/bin/env python3
"""Ordered schema admission, original ordinals, and atomic full-payload quotas."""
import json
import sys
from live import ROOT
from schema_destination_history import DestinationHistoryProbe


class SchemaAdmissionProbe(DestinationHistoryProbe):
    def setup(self):
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.identity=json.loads(self.value('''SELECT json_build_object('system_identifier',origin_system_identifier::text,
            'database_oid',origin_database_oid::text,'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),
            'table_a',table_a_id,'table_b',table_b_id) FROM lake_capture.streams'''))
        self.destination('CREATE EXTENSION lake_replication_push');self.overlay_installed=True;self.bind_destination()
        definitions=json.loads(self.value("SELECT json_agg(json_build_object('table_id',table_id,'definition',definition_json)) FROM lake_capture.schema_definitions"))
        self.initialize(definitions)
        self.definitions={item['definition']['name']:item['definition'] for item in definitions}
        self.dest('CREATE ROLE schema_worker LOGIN IN ROLE lake_replication_worker')
        args=self.sql_args();args[args.index('source')]='destination';args[args.index('capture')]='analytics'
        self.command(args+['-v','worker_login=schema_worker','-v','destination_schema=mirror'],input=(ROOT/'deploy/install/worker-grants.sql').read_text())

    def schema(self,ordinal,table,version,before,after):
        table_id=self.identity['table_'+table]
        payload=self.dest(f"SELECT encode(lake_replication.push_encode_schema_change('{table_id}',{version},{version+1},{self.literal(before)},{self.literal(after)}),'hex')")
        return dict(operation='schema',source_ordinal=ordinal,table_id=table_id,before_version=version,
            after_version=version+1,before_definition=before,after_definition=after,payload_hex=payload)

    def row(self,ordinal,table,version,operation='insert',**tuples):
        return dict(operation=operation,source_ordinal=ordinal,table_id=self.identity['table_'+table],
            source_schema_version=version,**tuples)

    def mixed(self):
        before=self.definitions['a'];added=json.loads(json.dumps(before))
        added['columns'].append(dict(id=3,name='extra',type_oid=16,type_modifier=-1,key=False,nullable=True))
        renamed=json.loads(json.dumps(added));renamed['columns'][1]['name']='label'
        self.a_added=added;self.a_renamed=renamed
        return [self.row(0,'a',1,new_tuple=dict(id='1',note='old')),
            self.schema(1,'a',1,before,added),
            dict(operation='progress',source_ordinal=2,request_id='09090909-0909-0909-0909-090909090909'),
            self.row(3,'a',2,new_tuple=dict(id='2',note='new',extra='t')),
            self.schema(4,'a',2,added,renamed),
            self.row(5,'a',3,operation='update',old_tuple=dict(id='1'),new_tuple=dict(id='1',label='renamed',extra=None)),
            self.row(6,'b',1,new_tuple=dict(id='7',note='other'))]

    def admit(self,actions,sequence=1,digest=None,required=True,role='postgres'):
        begin,commit,end=[f'0/{sequence*32+offset:X}' for offset in [0,8,16]]
        digest=digest or str(sequence)*64
        return self.as_role(role,f'''SELECT lake_replication.import_push_schema_batch({self.subscription},'{self.stream}',
            {self.identity['system_identifier']},{self.identity['database_oid']},1,{sequence},
            '11111111-1111-1111-1111-{sequence:012d}','{digest}',{sequence+77},'{begin}','{commit}','{end}',
            TIMESTAMPTZ '2026-11-14 12:00:00+00',{self.literal(actions)})''',required)

    def exercise(self):
        self.evidence.update(scope='ordered schema journal admission and atomic late metadata quota rollback',
            unqualified=['ordered destination publication','native importer integration','public schema activation'])
        self.setup();actions=self.mixed()
        expected=json.loads(json.dumps(actions))
        for action in expected:
            if action['operation'] not in ['schema','progress']:
                action.update(remote_relation_id=1 if action['table_id']==self.identity['table_a'] else 2,
                    relation_schema_version=action['source_schema_version'])
        full_size=int(self.dest(f'SELECT octet_length({self.literal(expected)}::text)'))
        self.dest(f'''SELECT lake_replication.stage_transaction({self.subscription},70,'0/3','0/4','0/5',now(),
            '[{{"remote_relation_id":2,"relation_schema_version":1,"operation":"insert","new_tuple":{{"id":"99","note":"{'x'*1000}"}}}}]');
            INSERT INTO lake_replication.journal_limits(subscription_id,max_transactions,max_payload_bytes)
                VALUES({self.subscription},10,{full_size+500}) ON CONFLICT(subscription_id)
                DO UPDATE SET max_transactions=10,max_payload_bytes=excluded.max_payload_bytes''')
        self.check('late full-payload admission reports backpressure',self.admit(actions,role='schema_worker').stdout.strip(),'full')
        self.check('late quota failure rolls back all new core and retained schema versions',self.dest(f'''SELECT
            (SELECT count(*) FROM lake_replication.relations WHERE subscription_id={self.subscription})::text||':'||
            (SELECT count(*) FROM lake_replication.push_schema_versions WHERE subscription_id={self.subscription})'''),'2:2')
        self.check('late quota failure rolls back schema heads and push admission',self.dest(f'''SELECT
            imported_sequence::text||':'||(SELECT max(imported_version) FROM lake_replication.push_schema_tables)||':'||
            (SELECT count(*) FROM lake_replication.push_receipts)||':'||(SELECT count(*) FROM lake_replication.push_schema_transactions)
            FROM lake_replication.push_bindings WHERE subscription_id={self.subscription}'''),'0:1:0:0')
        self.check('late quota failure restores the original journal and ingress checkpoint',self.dest(f'''SELECT
            (SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={self.subscription})::text||':'||staged_lsn
            FROM lake_replication.checkpoint_ingress WHERE subscription_id={self.subscription}'''),'1:0/5')
        self.dest(f"DELETE FROM lake_replication.transactions WHERE subscription_id={self.subscription};UPDATE lake_replication.journal_limits SET max_payload_bytes=1048576 WHERE subscription_id={self.subscription}")
        self.check('repaired admission registers and stages the complete ordered transaction',self.admit(actions,role='schema_worker').stdout.strip(),'imported')
        self.check('durable journal preserves every source ordinal and barrier',self.dest(f'''SELECT string_agg(
            (a->>'source_ordinal')||':'||(a->>'operation'),',' ORDER BY n) FROM lake_replication.transactions t
            CROSS JOIN LATERAL jsonb_array_elements(t.payload) WITH ORDINALITY x(a,n) WHERE subscription_id={self.subscription}'''),
            '0:insert,1:schema,2:progress,3:insert,4:schema,5:update,6:insert')
        self.check('row journal keeps the per-event relation versions',self.dest(f'''SELECT string_agg(
            relation_schema_version::text,',' ORDER BY change_index) FROM lake_replication.changes c
            JOIN lake_replication.transactions t USING(transaction_id) WHERE subscription_id={self.subscription}'''),'1,2,3,1')
        self.check('all schema and progress bytes count toward durable journal admission',self.dest(f'''SELECT
            (journal_payload_bytes=octet_length(payload::text) AND journal_payload_bytes={full_size})::text
            FROM lake_replication.transactions WHERE subscription_id={self.subscription}'''),'true')
        self.check('import advances schema head without changing published schema',self.dest(f'''SELECT imported_version::text||':'||published_version
            FROM lake_replication.push_schema_tables WHERE subscription_id={self.subscription} AND table_id='{self.identity['table_a']}' '''),'3:1')
        self.check('admission performs no destination DDL or data publication',self.dest('''SELECT
            (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM pg_attribute
                WHERE attrelid='mirror.a'::regclass AND attnum>0 AND NOT attisdropped)'''),'0:2')
        self.check('identical ordered batch replay adds no metadata or journal rows',self.admit(actions,role='schema_worker').stdout.strip(),'replayed')
        conflict=self.admit(actions,digest='a'*64,required=False,role='schema_worker')
        self.check('conflicting ordered replay fails closed',conflict.returncode!=0 and 'conflicting schema push replay' in conflict.stderr,True)
        progress=[dict(operation='progress',source_ordinal=0,request_id='10101010-1010-1010-1010-101010101010')]
        self.check('progress-only transaction uses the same ordered journal boundary',self.admit(progress,2,role='schema_worker').stdout.strip(),'imported')
        before=self.definitions['b'];after=json.loads(json.dumps(before))
        after['columns'].append(dict(id=3,name='qty',type_oid=23,type_modifier=-1,key=False,nullable=True))
        self.b_added=after
        ddl=[self.schema(0,'b',1,before,after)]
        self.check('DDL-only transaction is admitted with no row changes',self.admit(ddl,3,role='schema_worker').stdout.strip(),'imported')
        self.check('DDL-only and progress-only commits retain two schema receipt versions each',self.dest(f'''SELECT
            count(*)::text||':'||(SELECT count(*) FROM lake_replication.changes) FROM lake_replication.push_schema_receipt_versions
            WHERE subscription_id={self.subscription}'''),'6:4')
        invalid=[self.row(0,'a',2,new_tuple=dict(id='3',note='stale',extra=None))]
        failed=self.admit(invalid,4,required=False,role='schema_worker')
        self.check('stale row version cannot enter the ordered journal',failed.returncode!=0 and 'out of source order' in failed.stderr,True)
        renamed=json.loads(json.dumps(after));renamed['columns'][1]['name']='renamed_b'
        invalid=[self.schema(0,'b',2,after,renamed)];invalid[0]['payload_hex']=ddl[0]['payload_hex']
        failed=self.admit(invalid,4,required=False,role='schema_worker')
        self.check('mismatched canonical schema payload rolls back admission',failed.returncode!=0 and 'differs from normalized definitions' in failed.stderr,True)
        self.check('rejected events preserve the admitted frontier and retained version count',self.dest(f'''SELECT
            imported_sequence::text||':'||(SELECT count(*) FROM lake_replication.push_schema_versions)
            FROM lake_replication.push_bindings WHERE subscription_id={self.subscription}'''),'3:5')


if __name__=='__main__':sys.exit(SchemaAdmissionProbe().run())

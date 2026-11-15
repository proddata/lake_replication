#!/usr/bin/env python3
"""Ordered DDL/data publication, rollback, replay and reader exclusion."""
from concurrent.futures import ThreadPoolExecutor
import json
import sys
from schema_destination_admission import SchemaAdmissionProbe
from session import Session


class SchemaPublisherProbe(SchemaAdmissionProbe):
    def publish_sql(self,key='ordered_schema',first='0/30',last='0/70'):
        return f"SELECT * FROM lake_replication.publish_push_schema_batch({self.subscription},'{key}','{first}','{last}')"

    def until(self,statement,expected,timeout=30):
        import time
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            if self.dest(statement)==expected:return True
            time.sleep(.1)
        raise RuntimeError('destination condition timed out: '+statement)

    def exercise(self):
        self.evidence.update(scope='ordered schema and DML publication in one catalog/data/checkpoint transaction',
            unqualified=['native schema importer integration','publication backend crash boundaries','public schema activation'])
        self.setup();actions=self.mixed()
        actions.append(self.row(7,'a',3,operation='delete',old_tuple=dict(id='900')))
        self.admit(actions)
        self.admit([dict(operation='progress',source_ordinal=0,request_id='10101010-1010-1010-1010-101010101010')],2)
        before=self.definitions['b'];after=json.loads(json.dumps(before))
        after['columns'].append(dict(id=3,name='qty',type_oid=23,type_modifier=-1,key=False,nullable=True))
        self.admit([self.schema(0,'b',1,before,after)],3)
        denied=self.as_role('schema_worker',self.publish_sql(),False)
        self.check('privileged ordered apply refuses an unfenced runtime login',
            denied.returncode!=0 and 'active managed worker' in denied.stderr,True)
        denied=self.as_role('schema_worker',"UPDATE lake_replication.push_schema_tables SET destination_relation='mirror.b'::regclass",False)
        self.check('runtime login cannot rewrite the immutable destination scope',
            denied.returncode!=0 and 'permission denied' in denied.stderr,True)
        failed=self.destination(self.publish_sql(),required=False)
        self.check('late mutation mismatch aborts after earlier schema and data changes',
            failed.returncode!=0 and 'exactly one' in failed.stderr,True)
        self.check('failed publication restores original destination descriptors',self.dest('''SELECT string_agg(attname,',' ORDER BY attnum)
            FROM pg_attribute WHERE attrelid='mirror.a'::regclass AND attnum>0 AND NOT attisdropped'''),'id,note')
        self.check('failed publication exposes no preceding row changes',self.dest("SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),'0:0')
        self.check('failed publication preserves old schema heads and checkpoint',self.dest(f'''SELECT
            (SELECT max(published_version) FROM lake_replication.push_schema_tables)::text||':'||published_lsn
            FROM lake_replication.checkpoint_publication WHERE subscription_id={self.subscription}'''),'1:0/0')
        self.check('failed publication leaves every source transaction staged',self.dest('''SELECT count(*)::text||':'||bool_and(state='staged')
            FROM lake_replication.transactions'''),'3:true')
        self.check('failed publication rolls back new relation destinations and apply markers',self.dest('''SELECT
            (SELECT count(*) FROM lake_replication.relations WHERE destination_relation IS NULL)::text||':'||
            (SELECT bool_or(applied) FROM lake_replication.push_schema_transactions)'''),'3:false')
        # Repair the required pre-existing row rather than changing the durable journal.
        self.dest("INSERT INTO mirror.a VALUES(900,'repaired prerequisite')")
        failed=self.destination('BEGIN;'+self.publish_sql()+';SELECT 1/0;COMMIT',required=False)
        self.check('failure after finish rolls back DDL-only publication and the whole batch',
            failed.returncode!=0 and 'division by zero' in failed.stderr,True)
        self.check('post-finish failure retains only the pre-existing repaired row',self.dest('''SELECT
            count(*)::text||':'||min(id)::text FROM mirror.a'''),'1:900')
        self.check('post-finish failure restores both table schemas and unapplied markers',self.dest('''SELECT
            (SELECT count(*) FROM pg_attribute WHERE attrelid IN ('mirror.a'::regclass,'mirror.b'::regclass)
                AND attnum>0 AND NOT attisdropped)::text||':'||(SELECT bool_or(applied) FROM lake_replication.push_schema_transactions)'''),'4:false')
        self.destination(self.publish_sql())
        self.check('mixed old/new version rows reproduce the final committed data',self.dest('''SELECT
            (count(*)=2 AND count(*) FILTER(WHERE id=1 AND label='renamed' AND extra IS NULL)=1
                AND count(*) FILTER(WHERE id=2 AND label='new' AND extra)=1)::text FROM mirror.a'''),'true')
        self.check('DDL-only second-table change preserves its earlier row',self.dest("SELECT (count(*)=1 AND bool_and(id=7 AND note='other' AND qty IS NULL))::text FROM mirror.b"),'true')
        self.check('both published schema heads advance only with the data checkpoint',self.dest(f'''SELECT
            (SELECT published_version FROM lake_replication.push_schema_tables WHERE table_id='{self.identity['table_a']}')::text||':'||
            (SELECT published_version FROM lake_replication.push_schema_tables WHERE table_id='{self.identity['table_b']}')||':'||published_sequence
            FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'3:2:3')
        self.check('all transactions and apply markers commit as published together',self.dest('''SELECT
            bool_and(state='published')::text||':'||(SELECT bool_and(applied) FROM lake_replication.push_schema_transactions)
            FROM lake_replication.transactions'''),'true:true')
        replay=self.dest(self.publish_sql())
        self.check('published batch replay applies no rows or DDL again',replay.split('|')[-1],'0')
        changed=json.loads(json.dumps(self.a_renamed));changed['columns'][1]['name']='details'
        self.admit([self.schema(0,'a',3,self.a_renamed,changed),
            self.row(1,'a',4,new_tuple=dict(id='3',details='after reader',extra='f'))],4)
        args=self.sql_args();args[args.index('source')]='destination';args[args.index('capture')]='analytics';reader=Session(args)
        try:
            reader.query('BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT count(*) FROM mirror.a')
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending=pool.submit(self.destination,self.publish_sql('reader_boundary','0/90','0/90'))
                self.until("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%publish_push_schema_batch%')::text",'true')
                self.check('reader sees the old schema and group while DDL publication waits',reader.query("SELECT count(*)::text||':'||count(label)::text FROM mirror.a")[-1],'2:2')
                self.check('waiting DDL cannot advance the publication frontier',self.dest(f"SELECT published_sequence FROM lake_replication.push_status WHERE subscription_id={self.subscription}"),'3')
                reader.query('COMMIT');pending.result(timeout=30)
        finally:reader.close()
        self.check('reader release publishes the new schema and following row atomically',self.dest("SELECT count(*)::text||':'||count(*) FILTER(WHERE id=3 AND details='after reader' AND NOT extra)::text FROM mirror.a"),'3:1')
        latest=json.loads(json.dumps(changed));latest['columns'][1]['name']='latest'
        self.admit([self.schema(0,'a',4,changed,latest)],5)
        self.dest('ALTER TABLE mirror.a RENAME COLUMN details TO tampered')
        failed=self.destination(self.publish_sql('drift','0/B0','0/B0'),required=False)
        self.check('out-of-band destination drift stops before applying retained DDL',
            failed.returncode!=0 and 'destination schema differs' in failed.stderr,True)
        self.check('descriptor drift cannot advance the published receipt frontier',self.dest(f"SELECT published_sequence FROM lake_replication.push_status WHERE subscription_id={self.subscription}"),'4')
        self.dest('ALTER TABLE mirror.a RENAME COLUMN tampered TO details')
        self.destination(self.publish_sql('drift','0/B0','0/B0'))
        self.check('repaired descriptor admits the original DDL-only journal transaction',self.dest("SELECT count(latest) FROM mirror.a"),'3')


if __name__=='__main__':sys.exit(SchemaPublisherProbe().run())

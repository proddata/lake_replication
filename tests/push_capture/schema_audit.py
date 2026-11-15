#!/usr/bin/env python3
"""Internal transactional DDL audit; public capture enablement remains fenced."""
import json
import subprocess
import sys
from live import LiveProbe, ROOT
from schema_native import events
from session import Session


class AuditProbe(LiveProbe):
    def app(self,statement,required=True):
        args=self.sql_args();args[args.index('postgres')]='application_owner'
        return self.command(args,input=statement,required=required)

    def peek(self):
        options=dict(zip(['table-a-oid','table-b-oid','table-a-id','table-b-id','stream-id','capture-generation','schema-table-oid'],
            [self.binding['a_oid'],self.binding['b_oid'],self.binding['a_id'],self.binding['b_id'],self.stream,1,self.binding['audit_oid']]))
        pairs=','.join("'"+str(value)+"'" for item in options.items() for value in item)
        raw=json.loads(self.value(f"SELECT coalesce(json_agg(encode(data,'hex')),'[]'::json)::text FROM pg_logical_slot_peek_binary_changes('{self.binding['slot']}',NULL,NULL,{pairs})"))
        return [batch for value in raw if (batch:=events(value))['events']]

    def capture_until(self,sequence):
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        worker=subprocess.Popen(self.capture_args(),text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams',str(sequence),timeout=60)
            self.source_wait("SELECT (r.confirmed_flush_lsn>=s.captured_end_lsn)::text FROM lake_capture.streams s JOIN pg_replication_slots r ON r.slot_name=s.slot_name",'true')
        finally:
            self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
            try:
                stdout,stderr=worker.communicate(timeout=20)
            except subprocess.TimeoutExpired:
                worker.kill();stdout,stderr=worker.communicate(timeout=10)
                raise RuntimeError('capture actor did not stop: '+stderr)
            self.evidence.setdefault('capture_runs',[]).append(dict(stdout=stdout,stderr=stderr,returncode=worker.returncode))

    def exercise(self):
        self.evidence.update(scope='internal same-transaction normalized source DDL audit with ordinary application ownership',
            unqualified=['public auditing enablement','schema import/publication'])
        self.sql('''CREATE EXTENSION lake_replication_capture;CREATE ROLE application_owner LOGIN;
            GRANT USAGE,CREATE ON SCHEMA public TO application_owner;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            ALTER TABLE a OWNER TO application_owner;ALTER TABLE b OWNER TO application_owner;''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*)::text FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.binding=json.loads(self.value('''SELECT json_build_object('a_oid',table_a::oid,'b_oid',table_b::oid,
            'a_id',table_a_id,'b_id',table_b_id,'slot',slot_name,'audit_oid','lake_capture.schema_events'::regclass::oid)
            FROM lake_capture.streams'''))
        # Only the disposable fixture administrator activates this internal gate.
        # The operator grants and normal driver do not expose auditing enablement.
        self.sql(f"SELECT lake_capture.verify_schema_history('{self.stream}');UPDATE lake_capture.streams SET schema_auditing=true")
        self.app("BEGIN;INSERT INTO a VALUES(1,'before');ALTER TABLE a ADD COLUMN extra boolean;"
            "INSERT INTO a VALUES(2,'after',true);ALTER TABLE a RENAME COLUMN note TO label;"
            "UPDATE a SET label='renamed' WHERE id=2;COMMIT")
        self.check('ordinary application DDL atomically records both schema events',self.value('''SELECT
            (SELECT count(*) FROM lake_capture.schema_events)::text||':'||(SELECT count(*) FROM lake_capture.schema_definitions)'''),'2:4')
        batch=self.peek()
        self.check('real event-trigger audit preserves native DML-DDL-DML versions',
            [event['version'] for event in batch[0]['events']],[1,None,2,None,3])
        ledger=json.loads(self.value("SELECT json_agg(encode(payload,'hex') ORDER BY after_version) FROM lake_capture.schema_events"))
        self.check('native schema controls carry exactly the committed event-trigger payloads',
            [event['body'] for event in batch[0]['events'] if event['kind']==8],ledger)
        self.destination((ROOT/'extensions/lake_replication_push/schema_codec.sql').read_text())
        rows=json.loads(self.value("SELECT json_agg(json_build_object('version',schema_version,'definition',definition_json) ORDER BY schema_version) FROM lake_capture.schema_definitions WHERE table_id='"+self.binding['a_id']+"'"))
        def literal(value):return "'"+json.dumps(value).replace("'","''")+"'::jsonb"
        for index in range(2):
            expected=self.dest(f"SELECT encode(lake_replication.push_encode_schema_change('{self.binding['a_id']}',{index+1},{index+2},{literal(rows[index]['definition'])},{literal(rows[index+1]['definition'])}),'hex')")
            self.check(f'audit revision {index+2} matches the independently qualified canonical codec',ledger[index],expected)
        self.app('BEGIN;ALTER TABLE a RENAME COLUMN label TO aborted;ROLLBACK')
        self.check('aborted DDL removes its definitions and audit event together',self.value('''SELECT
            (SELECT count(*) FROM lake_capture.schema_events)::text||':'||(SELECT count(*) FROM lake_capture.schema_definitions)'''),'2:4')
        for label,ddl in [
            ('default',"ALTER TABLE a ALTER COLUMN extra SET DEFAULT true"),
            ('non-null addition','ALTER TABLE a ADD COLUMN unsafe text NOT NULL'),
            ('type conversion','ALTER TABLE a ALTER COLUMN label TYPE varchar'),
            ('column drop','ALTER TABLE a DROP COLUMN extra'),
            ('combined additions','ALTER TABLE a ADD COLUMN x text,ADD COLUMN y text'),
            ('table rename','ALTER TABLE a RENAME TO unsafe'),
            ('row-level security','ALTER TABLE a ENABLE ROW LEVEL SECURITY')]:
            result=self.app(ddl,False)
            self.check('unsupported audited '+label+' is rejected',result.returncode!=0,True)
        self.check('rejected DDL leaves schema and history unchanged',self.value('''SELECT
            count(*)::text||':'||(SELECT count(*) FROM lake_capture.schema_events) FROM pg_attribute
            WHERE attrelid='a'::regclass AND attnum>0 AND NOT attisdropped'''),'3:2')
        args=self.sql_args();args[args.index('postgres')]='application_owner';reader=Session(args)
        try:
            reader.query('BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT count(*) FROM a')
            denied=False
            try:reader.query('ALTER TABLE a RENAME COLUMN label TO unsafe_repeatable;COMMIT')
            except RuntimeError as error:denied='schema is frozen' in str(error)
            self.check('audited DDL cannot bypass current history through an old snapshot',denied,True)
        finally:reader.close()
        result=self.sql(f"SELECT lake_capture.reset_stream('{self.stream}')",required=False)
        self.check('evolved schema requires controlled drop and registration for recopy',
            result.returncode!=0 and 'new registration' in result.stderr,True)
        self.capture_until(1)
        self.check('source actor captures mixed-version data and schema in one manifest',self.value("SELECT count(*)::text||':'||(SELECT count(*) FROM lake_capture_log.controls) FROM lake_capture_log.manifests"),'1:2')
        self.check('source receipt commits the post-transaction versions atomically',self.value("SELECT table_a_version::text||':'||table_b_version FROM lake_capture.receipts WHERE sequence=1"),'3:1')
        column='label'
        for version in range(4,34):
            renamed=f'revision_{version}'
            self.app(f'ALTER TABLE a RENAME COLUMN {column} TO {renamed}')
            column=renamed
        self.check('one bounded table history retains exactly 32 committed revisions',self.value('''SELECT
            count(*)::text||':'||max(after_version)::text FROM lake_capture.schema_events'''),'32:33')
        result=self.app(f'ALTER TABLE a RENAME COLUMN {column} TO too_many',False)
        self.check('revision exhaustion rejects DDL before losing representable history',
            result.returncode!=0 and 'at most 32 revisions' in result.stderr,True)
        self.sql('UPDATE lake_capture.streams SET transactions_per_read=8')
        self.capture_until(31)
        self.check('delayed capture initializes from confirmed receipt rather than latest DDL heads',self.value("SELECT table_a_version::text||':'||table_b_version FROM lake_capture.receipts WHERE sequence=31"),'33:1')
        self.check('every DDL-only commit retains its own receipt and ordered control',self.value("SELECT count(*)::text||':'||(SELECT count(*) FROM lake_capture_log.controls) FROM lake_capture.receipts"),'31:32')
        self.sql(f"SELECT lake_capture.drop_stream('{self.stream}')")
        # Manager is deliberately deregistered; restart it only for controlled cleanup.
        self.sql('SELECT lake_capture.start_manager()')
        self.source_wait('SELECT count(*)::text FROM lake_capture.streams','0')
        self.check('controlled drop removes definitions and immutable audit events',self.value('''SELECT
            (SELECT count(*) FROM lake_capture.schema_events)::text||':'||(SELECT count(*) FROM lake_capture.schema_definitions)'''),'0:0')


if __name__=='__main__':sys.exit(AuditProbe().run())

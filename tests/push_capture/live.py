#!/usr/bin/env python3
"""Optional native source extension -> Iceberg logs -> existing publisher."""
import importlib.util
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests/push_replication'))
spec = importlib.util.spec_from_file_location('synthetic_import_probe', ROOT/'tests/push_replication/probe.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class LiveProbe(module.ImportProbe):
    def __init__(self):
        super().__init__()
        self.evidence.update(scope='native source extension INSERT/abort vertical slice',
                             unqualified=['online snapshot','mutations','retention','scoped source roles',
                                          'production mode selection/packaging','automatic failover'])

    def provenance(self):
        super().provenance()
        for path in sorted((ROOT/'extensions/lake_replication_capture').rglob('*')):
            if path.is_file():
                self.evidence['files_sha256'][str(path.relative_to(ROOT))]=hashlib.sha256(path.read_bytes()).hexdigest()

    def source_wait(self, query, expected, timeout=30):
        deadline = time.monotonic()+timeout
        last = ''
        while time.monotonic()<deadline:
            last = self.value(query)
            if last==expected:
                return True
            state = self.value("SELECT coalesce(last_error,'') FROM lake_capture.streams WHERE desired_state='stopped'")
            if state:
                # The worker can commit the expected stopped diagnostic
                # between these two reads. Recheck before treating it as an
                # unrelated failure of the awaited condition.
                if self.value(query)==expected:
                    return True
                raise RuntimeError(state)
            time.sleep(.1)
        raise RuntimeError(f'source wait timed out: {last!r} != {expected!r}')

    def exercise(self):
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE public.a(id bigint PRIMARY KEY,note text);
            CREATE TABLE public.b(id bigint PRIMARY KEY,note text);''')
        self.sql("BEGIN; SELECT lake_capture.start_stream(lake_capture.register_stream('public.a','public.b')); ROLLBACK;")
        time.sleep(.5)
        self.check('rolled-back source registration cannot create a slot', self.value('SELECT count(*) FROM pg_replication_slots'), '0')
        self.check('rolled-back source registration leaves no stream', self.value('SELECT count(*) FROM lake_capture.streams'), '0')
        self.sql("CREATE PUBLICATION reference_pub FOR TABLE public.a,public.b; SELECT pg_create_logical_replication_slot('reference_slot','pgoutput');")
        stream = self.value("SELECT lake_capture.register_stream('public.a','public.b')")
        self.stream = stream
        self.sql(f"SELECT lake_capture.start_stream('{stream}')")
        self.check('empty activation creates a durable owned logical slot',self.source_wait(
            'SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true'),True)
        self.identity = json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.evidence['source_identity']=self.identity
        self.check('source capture slot uses the native output plugin',self.value(
            "SELECT plugin FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'lake_replication_capture')
        columns=self.bind_destination()
        self.check('source column OIDs use the journal numeric descriptor form',self.value("SELECT jsonb_typeof(columns->0->'type_oid') FROM lake_capture_log.registrations LIMIT 1"),'number')
        self.sql("BEGIN; INSERT INTO a VALUES(99,'abort'); INSERT INTO b VALUES(99,'abort'); ROLLBACK;")
        self.sql("BEGIN; INSERT INTO a VALUES(1,NULL),(2,'one'); SAVEPOINT s; INSERT INTO b VALUES(99,'subabort'); ROLLBACK TO s; INSERT INTO b VALUES(3,'two'); COMMIT;")
        self.check('committed two-table transaction becomes one source manifest',self.source_wait(
            'SELECT captured_sequence::text FROM lake_capture.streams','1'),True)
        self.check('aborted and subaborted rows never enter live logs',self.value(
            "SELECT (SELECT count(*) FROM lake_capture_log.events_a)::text||':'||(SELECT count(*) FROM lake_capture_log.events_b)"),'2:1')
        self.importer()
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('source extension materializes exact committed rows through existing publisher',self.dest(
            "SELECT (SELECT string_agg(id::text,',' ORDER BY id) FROM mirror.a)||':'||(SELECT string_agg(id::text,',' ORDER BY id) FROM mirror.b)"),'1,2:3')
        reference=int(self.dest(f'''SELECT lake_replication.create_subscription(
            {self.identity['system_identifier']},{self.identity['database_oid']},{self.identity['timeline']},
            'reference_pull','reference_slot',ARRAY['reference_pub'])'''))
        self.dest('CREATE SCHEMA reference')
        for name in 'ab':
            oid=self.value(f"SELECT 'public.{name}'::regclass::oid")
            version=int(self.dest(f"SELECT lake_replication.register_relation({reference},{oid},'public','{name}','d','{columns}'::jsonb)"))
            self.dest(f'''CREATE TABLE reference.{name}(id bigint,note text) USING iceberg;
                SELECT lake_replication.assign_destination({reference},{oid},{version},'reference.{name}'::regclass);''')
        self.command(self.worker_args('lake-replication-receiver',LAKE_SUBSCRIPTION_ID=str(reference),
            LAKE_SLOT='reference_slot',LAKE_PUBLICATION='reference_pub',LAKE_MAX_TRANSACTIONS='1'))
        self.command(self.worker_args('lake-replication-publisher',LAKE_SUBSCRIPTION_ID=str(reference)))
        self.check('native INSERT/abort output agrees with independent pgoutput reference',self.dest('''SELECT
            NOT EXISTS((TABLE mirror.a EXCEPT ALL TABLE reference.a) UNION ALL (TABLE reference.a EXCEPT ALL TABLE mirror.a))
            AND NOT EXISTS((TABLE mirror.b EXCEPT ALL TABLE reference.b) UNION ALL (TABLE reference.b EXCEPT ALL TABLE mirror.b))'''),'t')
        self.sql("SELECT pg_drop_replication_slot('reference_slot')")
        for statement in ['ALTER TABLE a ADD COLUMN bad integer','DROP TABLE b']:
            rejected=self.sql(statement,required=False)
            self.check('selected table DDL fails closed: '+statement,rejected.returncode!=0,True)
        self.sql(f"SELECT lake_capture.stop_stream('{stream}')")
        self.check('source stop preserves the persistent slot',self.source_wait(
            "SELECT active::text FROM pg_replication_slots WHERE plugin='lake_replication_capture'",'false'),True)
        self.sql("BEGIN; INSERT INTO a VALUES(4,'after-stop'); INSERT INTO b VALUES(5,'after-stop'); COMMIT;")
        self.check('stopped capture leaves its frontier unchanged',self.value('SELECT captured_sequence::text FROM lake_capture.streams'),'1')
        self.sql(f"SELECT lake_capture.start_stream('{stream}')")
        self.check('source restart replays protected WAL after stop',self.source_wait(
            'SELECT captured_sequence::text FROM lake_capture.streams','2'),True)
        self.importer()
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('restart adds each row once',self.dest(
            "SELECT (SELECT count(*) FROM mirror.a)::text||':'||(SELECT count(*) FROM mirror.b)"),'3:2')
        duplicate = self.command(self.capture_args(),required=False)
        self.check('duplicate source worker cannot acquire the capture lease',
                   duplicate.returncode!=0 and 'duplicate capture worker lease' in duplicate.stderr,True)
        self.command(self.compose+['exec','-T','source','pkill','-KILL','-f','(^|/)lake-replication-capture$'])
        self.sql("BEGIN; INSERT INTO a SELECT 6,string_agg(md5(i::text),'') FROM generate_series(1,32768)i; INSERT INTO b VALUES(7,'child-restart'); COMMIT;")
        self.check('capture child crash restarts from protected WAL',self.source_wait(
            'SELECT captured_sequence::text FROM lake_capture.streams','3'),True)
        self.check('capture progresses while importer is stopped',self.dest(
            'SELECT imported_sequence::text FROM lake_replication.push_bindings'),'2')
        self.importer()
        self.command(self.worker_args('lake-replication-publisher'))
        source_hash=self.value('SELECT md5(note) FROM a WHERE id=6')
        self.check('native logical decoding preserves external TOAST on INSERT',self.dest(
            'SELECT md5(note) FROM mirror.a WHERE id=6'),source_hash)
        self.sql(f"SELECT lake_capture.drop_stream('{stream}')")
        self.check('controlled source drop reaps worker and owned slot',self.source_wait(
            'SELECT count(*) FROM pg_replication_slots','0'),True)
        self.check('controlled drop removes durable source configuration',self.source_wait(
            'SELECT count(*) FROM lake_capture.streams','0'),True)
        self.sql('DROP EXTENSION lake_replication_capture CASCADE')
        self.check('source extension is independently removable',self.value(
            "SELECT count(*) FROM pg_extension WHERE extname='lake_replication_capture'"),'0')
        self.sql("SELECT pg_create_logical_replication_slot('after_push_removal','pgoutput')")
        after=int(self.dest(f"SELECT lake_replication.create_subscription({self.identity['system_identifier']},{self.identity['database_oid']},{self.identity['timeline']},'after_push_removal','after_push_removal',ARRAY['reference_pub'])"))
        self.dest('CREATE SCHEMA after_push_removal')
        for name in 'ab':
            oid=self.value(f"SELECT 'public.{name}'::regclass::oid")
            version=self.dest(f"SELECT lake_replication.register_relation({after},{oid},'public','{name}','d','{columns}'::jsonb)")
            self.dest(f"CREATE TABLE after_push_removal.{name}(id bigint,note text) USING iceberg;SELECT lake_replication.assign_destination({after},{oid},{version},'after_push_removal.{name}'::regclass)")
        self.sql("BEGIN;INSERT INTO a VALUES(100,'after-removal');INSERT INTO b VALUES(101,'after-removal');COMMIT")
        self.command(self.worker_args('lake-replication-receiver',LAKE_SUBSCRIPTION_ID=str(after),LAKE_SLOT='after_push_removal',LAKE_PUBLICATION='reference_pub',LAKE_MAX_TRANSACTIONS='1'))
        self.command(self.worker_args('lake-replication-publisher',LAKE_SUBSCRIPTION_ID=str(after)))
        self.check('logical pull operates after optional source extension removal',self.dest("SELECT (SELECT min(id) FROM after_push_removal.a)::text||':'||(SELECT min(id) FROM after_push_removal.b)"),'100:101')
        self.sql("SELECT pg_drop_replication_slot('after_push_removal')")


    def bind_destination(self,target_schema='mirror',capture_generation=1):
        stream=self.stream
        if not getattr(self,'overlay_installed',False):
            self.destination((ROOT/'extensions/lake_replication_push/schema.sql').read_text())
            self.overlay_installed=True
        self.subscription=int(self.dest(f'''SELECT lake_replication.create_subscription(
            {self.identity['system_identifier']},{self.identity['database_oid']},{self.identity['timeline']},
            'live_push_{capture_generation}','unused_destination_slot_{capture_generation}',ARRAY['unused_destination_publication'])'''))
        self.dest(f'''INSERT INTO lake_replication.push_bindings(subscription_id,stream_id,system_identifier,
            database_oid,capture_generation) VALUES({self.subscription},'{stream}',{self.identity['system_identifier']},
            {self.identity['database_oid']},{capture_generation}); CREATE SCHEMA {target_schema};''')
        literal=lambda value:"E'"+str(value).replace('\\','\\\\').replace("'","''")+"'"
        identifier=lambda value:'"'+value.replace('"','""')+'"'
        types={16:'boolean',20:'bigint',23:'integer',25:'text'}
        for index,name in enumerate('ab',1):
            registration=json.loads(self.value(f"SELECT to_jsonb(r) FROM lake_capture_log.registrations r WHERE table_id='{self.identity['table_'+name]}'"))
            columns=json.dumps(registration['columns'],ensure_ascii=False)
            version=int(self.dest(f"SELECT lake_replication.register_relation({self.subscription},{index},"
                f"{literal(registration['source_namespace'])},{literal(registration['source_relation'])},"
                f"{literal(registration['replica_identity'])},{literal(columns)}::jsonb)"))
            definition=','.join(identifier(column['name'])+' '+types[int(column['type_oid'])]+(' NOT NULL' if not column.get('nullable',int(column['flags'])!=1) else '') for column in registration['columns'])
            self.dest(f'''CREATE TABLE {target_schema}.{name}({definition}) USING iceberg;
                SELECT lake_replication.assign_destination({self.subscription},{index},{version},'{target_schema}.{name}'::regclass);
                INSERT INTO lake_replication.push_tables VALUES({self.subscription},'{self.identity['table_'+name]}',1,{index},{version});''')
        return columns

    def capture_args(self,**settings):
        generation=self.value('SELECT config_generation::text FROM lake_capture.streams')
        values=dict(LAKE_CAPTURE_STREAM=self.stream,LAKE_CONFIG_GENERATION=generation,
                    LAKE_SOURCE_USER='postgres',LAKE_SOURCE_DATABASE='capture',
                    LAKE_SOURCE_ADDRESS='127.0.0.1:5432')
        values.update(settings)
        args=self.compose+['exec','-T']
        for key,value in values.items():args+=['-e',f'{key}={value}']
        return args+['source','lake-replication-capture']


if __name__=='__main__':
    sys.exit(LiveProbe().run())

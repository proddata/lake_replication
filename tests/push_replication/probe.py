#!/usr/bin/env python3
"""Synthetic Iceberg logs -> optional importer -> existing journal/publisher."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests/push_capture'))
from probe import Probe
from model import STREAM, TABLES, batch, insert_sql


class ImportProbe(Probe):
    def __init__(self):
        super().__init__()
        self.compose += ['-f', str(ROOT / 'deploy/push/importer.compose.yml')]
        self.evidence.update(scope='synthetic source Iceberg logs -> existing journal/publisher',
                             unqualified=['source capture extension', 'bootstrap', 'mutations',
                                          'retention', 'managed source lifecycle'])

    def provenance(self):
        super().provenance()
        reference = os.environ['PUSH_DESTINATION_IMAGE']
        info = json.loads(self.command(['docker', 'image', 'inspect', reference]).stdout)[0]
        self.evidence['images']['PUSH_DESTINATION_IMAGE'] = dict(reference=reference, id=info['Id'])
        for directory in ['tests/push_replication', 'worker/src', 'extensions/lake_replication_push']:
            for path in sorted((ROOT / directory).rglob('*')):
                if path.is_file() and '__pycache__' not in path.parts:
                    self.evidence['files_sha256'][str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()

    def destination(self, statement, *, required=True):
        args = self.sql_args()
        args[args.index('source')] = 'destination'
        args[args.index('capture')] = 'analytics'
        return self.command(args, input=statement, required=required)

    def dest(self, statement):
        return self.destination(statement).stdout.strip()

    def worker_args(self, binary='lake-replication-log-importer', **settings):
        environment = dict(LAKE_SOURCE_ADDRESS='source:5432', LAKE_SOURCE_DATABASE='capture',
            LAKE_SOURCE_USER='postgres', LAKE_SOURCE_TLS_MODE='disable',
            LAKE_DESTINATION_ADDRESS='127.0.0.1:5432', LAKE_DESTINATION_DATABASE='analytics',
            LAKE_DESTINATION_USER='postgres', LAKE_DESTINATION_TLS_MODE='disable',
            LAKE_SUBSCRIPTION_ID=str(self.subscription), LAKE_CONFIG_GENERATION='0')
        environment.update(settings)
        args = self.compose + ['exec', '-T']
        for key, value in environment.items():
            args += ['-e', f'{key}={value}']
        return args + ['destination', binary]

    def importer(self, *, required=True, **settings):
        return self.command(self.worker_args(**settings), required=required)

    def progress(self):
        return self.dest(f'''SELECT imported_sequence::text || ':' ||
            (SELECT count(*) FROM lake_replication.transactions WHERE subscription_id={self.subscription}) || ':' ||
            (SELECT count(*) FROM lake_replication.push_receipts WHERE subscription_id={self.subscription})
            FROM lake_replication.push_bindings WHERE subscription_id={self.subscription}''')

    def add_batch(self, sequence, rows, **options):
        encoded = batch(self.identity, sequence, rows, **options)
        self.sql(insert_sql(sequence, encoded))
        end=sequence*256
        self.sql(f'''INSERT INTO lake_capture_log.receipt_state VALUES({sequence},'{encoded['header'][52:68].hex()}',
            '0/{end:X}','{encoded['digest'].hex()}')''')
        self.sql(f'UPDATE lake_capture_log.progress SET captured_sequence=greatest(captured_sequence,{sequence})')
        return encoded

    def remove_batch(self, sequence):
        self.sql(f'''BEGIN;
            DELETE FROM lake_capture_log.manifests WHERE sequence={sequence};
            DELETE FROM lake_capture_log.events_a WHERE sequence={sequence};
            DELETE FROM lake_capture_log.events_b WHERE sequence={sequence}; COMMIT;''')
        self.sql(f'DELETE FROM lake_capture_log.receipt_state WHERE sequence={sequence}')
        self.sql('UPDATE lake_capture_log.progress SET captured_sequence=(SELECT coalesce(max(sequence),0) FROM lake_capture_log.manifests)')

    def wait(self, query, expected):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.dest(query) == expected:
                return True
            time.sleep(.1)
        return False

    def kill_importer(self, **settings):
        child = subprocess.Popen(self.worker_args(**settings), text=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return child

    def kill_child(self, child):
        self.command(self.compose + ['exec', '-T', 'destination', 'pkill', '-KILL',
                                     '-f', '(^|/)lake-replication-log-importer$'])
        child.communicate(timeout=15)
        self.check('killed importer exits unsuccessfully', child.returncode != 0, True)

    def exercise(self):
        self.identity = json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',(SELECT system_identifier::text FROM pg_control_system()),
            'database_oid',(SELECT oid::text FROM pg_database WHERE datname=current_database()),
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()))'''))
        self.identity = {key: int(value) for key, value in self.identity.items()}
        self.evidence['source_identity'] = self.identity
        self.sql('''CREATE SCHEMA lake_capture_log; REVOKE ALL ON SCHEMA lake_capture_log FROM PUBLIC;
            CREATE TABLE lake_capture_log.manifests(sequence bigint,header_hex text,digest_hex text) USING iceberg;
            CREATE TABLE lake_capture_log.events_a(sequence bigint,ordinal integer,event_hex text) USING iceberg;
            CREATE TABLE lake_capture_log.events_b(sequence bigint,ordinal integer,event_hex text) USING iceberg;''')
        self.sql('CREATE TABLE lake_capture_log.controls(sequence bigint,ordinal integer,event_hex text) USING iceberg')
        self.sql('''CREATE TABLE lake_capture_log.registrations(table_id uuid,schema_version integer,
            source_namespace text,source_relation text,replica_identity "char",columns jsonb)''')
        self.sql(f'''CREATE TABLE lake_capture_log.progress(stream_id uuid,system_identifier numeric,database_oid oid,
            capture_generation bigint,captured_sequence bigint,pruned_through_sequence bigint);
            INSERT INTO lake_capture_log.progress VALUES('{STREAM.hex()}',{self.identity['system_identifier']},
            {self.identity['database_oid']},1,0,0);''')
        self.sql('CREATE TABLE lake_capture_log.receipt_state(sequence bigint,batch_id uuid,end_lsn pg_lsn,digest text)')
        self.destination((ROOT / 'extensions/lake_replication_push/schema.sql').read_text())
        self.subscription = int(self.dest(f'''SELECT lake_replication.create_subscription(
            {self.identity['system_identifier']},{self.identity['database_oid']},{self.identity['timeline']},
            'synthetic_push','unused_push_slot',ARRAY['unused_push_publication'])'''))
        self.dest(f'''INSERT INTO lake_replication.push_bindings(subscription_id,stream_id,
            system_identifier,database_oid,capture_generation) VALUES({self.subscription},'{STREAM.hex()}',
            {self.identity['system_identifier']},{self.identity['database_oid']},1);
            CREATE SCHEMA mirror;''')
        columns = json.dumps([dict(flags=1,name='id',type_oid=20,type_modifier=-1),
                              dict(flags=0,name='note',type_oid=25,type_modifier=-1)])
        for index, name in enumerate('ab', 1):
            self.sql(f'''INSERT INTO lake_capture_log.registrations VALUES('{TABLES[index-1].hex()}',1,
                'public','{name}','d','{columns}'::jsonb)''')
            version = int(self.dest(f"SELECT lake_replication.register_relation({self.subscription},"
                f"{index},'public','{name}','d','{columns}'::jsonb)"))
            self.dest(f'''CREATE TABLE mirror.{name}(id bigint,note text) USING iceberg;
                SELECT lake_replication.assign_destination({self.subscription},{index},{version},'mirror.{name}'::regclass);
                INSERT INTO lake_replication.push_tables VALUES({self.subscription},'{TABLES[index-1].hex()}',1,{index},{version});''')
        self.add_batch(1, [(0, [1, None]), (1, [2, "quote'\\世界"])])
        self.check('unobserved source frontier is unknown rather than caught up',self.dest(f'''
            SELECT (observed_captured_sequence IS NULL AND source_observed_at IS NULL
                AND observed_capture_to_import_batches IS NULL)::text FROM lake_replication.push_status
                WHERE subscription_id={self.subscription}'''),'true')
        self.importer()
        self.check('two-table manifest enters journal with atomic imported progress', self.progress(), '1:1:1')
        self.check('status distinguishes imported work from published work',self.dest(f"SELECT imported_sequence::text||':'||published_sequence FROM lake_replication.push_status WHERE subscription_id={self.subscription}"),'1:0')
        self.check('status records observed capture and unpublished journal independently',self.dest(f'''
            SELECT (observed_captured_sequence=1 AND observed_capture_to_import_batches=0
                AND imported_to_published_batches=1 AND source_observation_age_seconds>=0
                AND journal_pending_transactions=1 AND journal_pending_payload_bytes>0)::text
                FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'true')
        observe=f"SELECT lake_replication.observe_push_source({self.subscription},'{STREAM.hex()}',{self.identity['system_identifier']},{self.identity['database_oid']},"
        mismatched=self.destination(observe+'2,1)',required=False)
        self.check('source observation cannot change capture generation',
            mismatched.returncode!=0 and 'identity or generation mismatch' in mismatched.stderr,True)
        backwards=self.destination('BEGIN;'+observe+'1,2);'+observe+'1,1);COMMIT',required=False)
        self.check('observed but unimported source regression fails atomically',
            backwards.returncode!=0 and 'capture frontier moved backwards' in backwards.stderr
                and self.dest(f'SELECT observed_captured_sequence FROM lake_replication.push_bindings WHERE subscription_id={self.subscription}')=='1',True)
        self.check('import does not write final mirrors', self.dest('SELECT (SELECT count(*) FROM mirror.a)+(SELECT count(*) FROM mirror.b)'), '0')
        self.importer(LAKE_PUSH_REPLAY_FROM='1')
        self.check('identical importer replay has no duplicate journal transaction', self.progress(), '1:1:1')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('existing publisher materializes both logs and preserves NULL/text', self.dest(
            "SELECT jsonb_build_array((SELECT jsonb_agg(to_jsonb(a)) FROM mirror.a a),"
            "(SELECT jsonb_agg(to_jsonb(b)) FROM mirror.b b))::text"),
            json.dumps([[dict(id=1,note=None)],[dict(id=2,note="quote'\\世界")]],ensure_ascii=False))
        self.check('published and imported frontiers agree after apply', self.dest(f'''SELECT
            (c.published_lsn=b.imported_end_lsn)::text FROM lake_replication.checkpoints c
            JOIN lake_replication.push_bindings b USING(subscription_id) WHERE subscription_id={self.subscription}'''), 'true')
        self.check('publication drains journal backlog without changing observed capture',self.dest(f'''
            SELECT (observed_captured_sequence=1 AND imported_to_published_batches=0
                AND journal_pending_transactions=0 AND journal_pending_payload_bytes=0)::text
                FROM lake_replication.push_status WHERE subscription_id={self.subscription}'''),'true')
        known=self.value('SELECT digest FROM lake_capture_log.receipt_state WHERE sequence=1')
        self.sql("UPDATE lake_capture_log.receipt_state SET digest=repeat('0',64) WHERE sequence=1")
        divergent=self.importer(required=False)
        self.check('divergent source history fails even when the importer is caught up',
            divergent.returncode!=0 and 'continuity differs' in divergent.stderr,True)
        self.sql(f"UPDATE lake_capture_log.receipt_state SET digest='{known}' WHERE sequence=1")
        self.sql("UPDATE lake_capture_log.registrations SET columns=jsonb_set(columns,'{1,name}','\"tampered\"'::jsonb)")
        invalid_schema=self.importer(required=False,LAKE_PUSH_REPLAY_FROM='1')
        self.check('source schema contract mismatch fails before replay/admission',
                   invalid_schema.returncode!=0 and 'registration differs' in invalid_schema.stderr,True)
        self.sql(f"UPDATE lake_capture_log.registrations SET columns='{columns}'::jsonb")
        self.remove_batch(1)
        self.add_batch(1, [(0, [99, 'conflict']), (1, [2, "quote'\\世界"])])
        conflict = self.importer(required=False, LAKE_PUSH_REPLAY_FROM='1')
        self.check('same identity with altered valid payload fails closed',
                   conflict.returncode != 0 and 'conflicting push replay' in conflict.stderr, True)
        self.check('conflicting replay preserves imported progress', self.progress(), '1:1:1')
        self.remove_batch(1)
        self.add_batch(1,[(0,[1,None]),(1,[2,"quote'\\世界"])])
        self.add_batch(3, [(0, [3, 'gap'])])
        gap = self.importer(required=False)
        self.check('contiguous manifest sequence is enforced', gap.returncode != 0 and 'sequence gap' in gap.stderr, True)
        self.remove_batch(3)
        self.add_batch(2, [(0, [3, 'new-generation'])], generation=2)
        stale = self.importer(required=False)
        self.check('capture generation mismatch stops import', stale.returncode != 0 and 'generation mismatch' in stale.stderr, True)
        self.remove_batch(2)
        self.add_batch(2, [(0, [3, 'unsupported'])], operation=3, truncate_options=4)
        unsupported = self.importer(required=False)
        self.check('unknown truncate options cannot reach the journal',
                   unsupported.returncode != 0 and 'unknown truncate option' in unsupported.stderr, True)
        self.remove_batch(2)
        self.add_batch(2, [(0, [3, 'corrupt']), (1, [4, 'corrupt'])])
        self.sql("UPDATE lake_capture_log.manifests SET digest_hex=repeat('0',64) WHERE sequence=2")
        corrupt = self.importer(required=False)
        self.check('corrupt complete batch cannot advance progress',
                   corrupt.returncode != 0 and 'digest mismatch' in corrupt.stderr, True)
        self.check('all invalid batches leave progress unchanged', self.progress(), '1:1:1')
        self.remove_batch(2)
        self.add_batch(2, [(0, [3, 'resume']), (1, [4, 'resume'])])
        # Force a late error after journal insertion. The outer managed COMMIT
        # must roll back both the journal and the imported receipt/checkpoint.
        self.dest('''CREATE FUNCTION public.fail_push_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected receipt failure'; END $$;
            CREATE TRIGGER fail_push_receipt BEFORE INSERT ON lake_replication.push_receipts
            FOR EACH ROW EXECUTE FUNCTION public.fail_push_receipt();''')
        failed = self.importer(required=False)
        self.check('failure after journal admission aborts import', failed.returncode != 0 and 'injected receipt failure' in failed.stderr, True)
        self.check('late import failure rolls back journal and imported checkpoint', self.progress(), '1:1:1')
        self.dest('DROP TRIGGER fail_push_receipt ON lake_replication.push_receipts')
        self.importer()
        self.check('restart admits previously rolled-back batch once', self.progress(), '2:2:2')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('existing publisher catches up after importer recovery', self.dest(
            'SELECT (SELECT count(*) FROM mirror.a)::text||\':\'||(SELECT count(*) FROM mirror.b)'), '2:2')
        self.check('push importer never creates a source replication slot', self.value('SELECT count(*) FROM pg_replication_slots'), '0')
        self.add_batch(3, [(0, [5, 'precommit-kill']), (1, [6, 'precommit-kill'])])
        self.dest('''CREATE OR REPLACE FUNCTION public.fail_push_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_sleep(45); RETURN NEW; END $$;
            CREATE TRIGGER fail_push_receipt BEFORE INSERT ON lake_replication.push_receipts
            FOR EACH ROW EXECUTE FUNCTION public.fail_push_receipt();''')
        child = self.kill_importer()
        try:
            self.check('importer reaches precommit receipt barrier', self.wait(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_replication_worker/%' AND wait_event='PgSleep'", '1'), True)
            self.kill_child(child)
            self.check('disconnected import backend aborts', self.wait(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_replication_worker/%' AND wait_event='PgSleep'", '0'), True)
        finally:
            if child.poll() is None:
                self.kill_child(child)
        self.check('process death before commit preserves atomic progress', self.progress(), '2:2:2')
        self.dest('DROP TRIGGER fail_push_receipt ON lake_replication.push_receipts')
        child = self.kill_importer(LAKE_PUSH_TEST_STOP='after-admission')
        try:
            self.check('importer reaches durable post-admission barrier', self.wait(
                'SELECT imported_sequence::text FROM lake_replication.push_bindings', '3'), True)
            self.kill_child(child)
        finally:
            if child.poll() is None:
                self.kill_child(child)
        self.importer(LAKE_PUSH_REPLAY_FROM='3')
        self.check('process death after commit replays without duplicates', self.progress(), '3:3:3')
        self.add_batch(4, [(0, [7, 'capacity']), (1, [8, 'capacity'])])
        self.dest(f"INSERT INTO lake_replication.journal_limits(subscription_id,max_transactions,max_payload_bytes) VALUES({self.subscription},1,67108864) "
                  'ON CONFLICT(subscription_id) DO UPDATE SET max_transactions=1')
        child = self.kill_importer()
        try:
            self.check('full journal makes importer wait at admission', self.wait(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_replication_worker/%'", '1'), True)
            time.sleep(.5)
            self.check('backpressure leaves imported sequence unchanged', self.progress(), '3:3:3')
            self.check('backpressure exposes captured backlog without acknowledging import',self.dest(f'''
                SELECT (observed_captured_sequence=4 AND observed_capture_to_import_batches=1
                    AND imported_to_published_batches=1 AND journal_pending_transactions=1
                    AND journal_max_transactions=1)::text FROM lake_replication.push_status
                    WHERE subscription_id={self.subscription}'''),'true')
            self.command(self.worker_args('lake-replication-publisher'))
            child.communicate(timeout=20)
            self.check('publication releases waiting importer', child.returncode, 0)
        finally:
            if child.poll() is None:
                self.kill_child(child)
        self.check('waiting batch enters journal once capacity is available', self.progress(), '4:4:4')
        self.command(self.worker_args('lake-replication-publisher'))
        self.add_batch(5, [(0, [9, 'stale-worker']), (1, [10, 'stale-worker'])])
        stale = self.importer(required=False, LAKE_CONFIG_GENERATION='999')
        self.check('stale managed worker cannot admit a push batch', stale.returncode != 0, True)
        self.check('generation fence preserves imported progress', self.progress(), '4:4:4')
        self.check('stale worker cannot refresh the observed source frontier',self.dest(f'''
            SELECT observed_captured_sequence FROM lake_replication.push_status
                WHERE subscription_id={self.subscription}'''),'4')


if __name__ == '__main__':
    sys.exit(ImportProbe().run())

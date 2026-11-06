"""Explicit disposable-fixture maintenance policies; production defaults unchanged."""
import json
import sys
import time
from contextlib import contextmanager


POLICIES = {
    'snapshot-only': dict(snapshot_seconds=60, file_seconds=864000, interval_seconds=30),
    'bounded-test': dict(snapshot_seconds=60, file_seconds=120, interval_seconds=30),
}
TABLES = ('soak_items', 'soak_progress', 'soak_retention_guard')


class Maintenance:
    def __init__(self, runner, client_factory):
        self.runner = runner
        self.client_factory = client_factory
        self.name = getattr(runner.args, 'maintenance_profile', 'none')
        self.policy = POLICIES.get(self.name)
        self.enabled = self.policy is not None
        self.witness = None
        self.removal_observed = False
        self.samples = []
        runner.result['maintenance'] = dict(profile=self.name, policy=self.policy,
            scope='Explicit disposable profile; shortened file retention reduces its recovery horizon', passes=0,
            coordination='Publication-checkpoint row lock pauses this subscription during bounded vacuum; ingress remains independent',
            automatic_vacuum=False)

    @contextmanager
    def publication_pause(self):
        holders = []
        try:
            for _ in range(1):
                holder = self.client_factory('destination', 'analytics')
                holders.append(holder)
                rows = holder.query("SET lock_timeout='3s'; BEGIN; SELECT subscription_id FROM lake_replication.checkpoint_publication FOR UPDATE")
                if len(rows) != 1 or not rows[0].isdigit():
                    raise ValueError('maintenance requires the single private publication checkpoint')
            yield
        finally:
            primary_failure = sys.exc_info()[0] is not None
            errors = []
            for holder in reversed(holders):
                try:
                    holder.query('ROLLBACK')
                except Exception as error:
                    errors.append(str(error))
                try:
                    holder.close()
                except Exception as error:
                    errors.append(str(error))
            if errors:
                self.runner.result.setdefault('maintenance_gate_cleanup_errors', []).extend(errors)
                if not primary_failure:
                    raise RuntimeError('maintenance admission cleanup failed')

    def settings(self):
        return json.loads(self.runner.sql('destination', """SELECT jsonb_build_object(
 'snapshot_seconds',current_setting('pg_lake_iceberg.max_snapshot_age')::int,
 'file_seconds',extract(epoch FROM current_setting('pg_lake_engine.orphaned_file_retention_period')::interval)::bigint,
 'autovacuum_seconds',extract(epoch FROM current_setting('pg_lake_iceberg.autovacuum_naptime')::interval)::bigint)"""))

    def vacuum(self, table):
        if table not in TABLES:
            raise ValueError('unexpected maintenance target')
        statements = f"""SET lock_timeout='1s'; SET statement_timeout='15s';
SET pg_lake_engine.orphaned_file_retention_period='{self.policy['file_seconds']}s';
SET pg_lake_table.max_compactions_per_vacuum=2;
SET pg_lake_table.max_file_removals_per_vacuum=1000;
VACUUM (VERBOSE) {table}"""
        result = self.runner.sql('destination', statements, timeout=25, result=True)
        self.runner.append('maintenance-output.jsonl', dict(table=table, stdout=result.stdout, stderr=result.stderr))
        # pg_lake can downgrade failed vacuum phases to warnings. SQL success
        # alone therefore does not establish successful maintenance.
        if 'WARNING:' in result.stderr or 'ERROR:' in result.stderr:
            raise RuntimeError('vacuum reported a failed phase for ' + table)

    def witness_exists(self):
        uri = self.witness['path']
        if not uri.startswith('s3://lake-qualification/iceberg/') or '..' in uri:
            raise ValueError('retention witness is outside the private fixture')
        # Independent observation of the pinned, unversioned MinIO fixture.
        result = self.runner.compose('exec', '-T', 'storage', 'test', '-f',
            '/data/' + uri.removeprefix('s3://') + '/xl.meta', timeout=12, check_result=False)
        if result.returncode not in (0, 1):
            raise RuntimeError('could not inspect retention witness')
        return result.returncode == 0

    def configure(self):
        r = self.runner
        defaults = self.settings()
        r.result['maintenance']['defaults'] = defaults
        if defaults['file_seconds'] != 864000:
            raise ValueError('unexpected upstream deletion-retention default')
        for table in TABLES[:2]:
            r.sql('destination', f"ALTER FOREIGN TABLE {table} OPTIONS (ADD max_snapshot_age '60', ADD autovacuum_enabled 'false')")
        r.sql('destination', """CREATE TABLE soak_retention_guard(id integer) USING iceberg
 WITH(max_snapshot_age=0,autovacuum_enabled=false); INSERT INTO soak_retention_guard VALUES(1)""")
        uri = r.sql('destination', "SELECT metadata_location FROM lake_iceberg.tables_internal WHERE table_name='soak_retention_guard'::regclass")
        reader = self.client_factory('destination', 'analytics')
        try:
            original = reader.query('BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT jsonb_agg(id ORDER BY id) FROM soak_retention_guard')[-1]
            r.sql('destination', 'INSERT INTO soak_retention_guard VALUES(2)')
            r.sql('destination', 'INSERT INTO soak_retention_guard VALUES(3)')
            self.witness = json.loads(r.sql('destination', f"SELECT jsonb_build_object('path',path,'orphaned_at',orphaned_at) FROM lake_engine.deletion_queue WHERE path='{uri}'"))
            self.vacuum('soak_retention_guard')
            r.check('maintenance preserves a held repeatable-read result', reader.query('SELECT jsonb_agg(id ORDER BY id) FROM soak_retention_guard')[-1], original)
            r.check('vacuum retains a file inside the deletion-retention window', self.witness_exists(), True)
            r.check('vacuum retains the protected deletion-queue entry', r.sql('destination', f"SELECT count(*) FROM lake_engine.deletion_queue WHERE path='{uri}'"), '1')
            # A live reader's old metadata remains independently readable.
            r.sql('destination', f"SELECT lake_iceberg.metadata('{uri}') IS NOT NULL")
            reader.query('ROLLBACK')
        finally:
            reader.close()
        r.result['maintenance']['retention_witness'] = self.witness
        r.result['maintenance']['defaults_after_session_vacuum'] = self.settings()
        r.check('session-only cleanup leaves global retention unchanged', self.settings(), defaults)

    def stats(self):
        return json.loads(self.runner.sql('destination', """WITH metadata AS MATERIALIZED (
 SELECT table_name::text AS name, metadata_location, lake_iceberg.metadata(metadata_location)::jsonb AS doc
 FROM lake_iceberg.tables_internal WHERE table_name IN ('soak_items'::regclass,'soak_progress'::regclass)
), entries AS (
 SELECT name,metadata_location,octet_length(doc::text) AS document_bytes,
 jsonb_array_length(coalesce(doc->'snapshots','[]')) AS snapshots,
 jsonb_array_length(coalesce(doc->'metadata-log','[]')) AS metadata_log_entries,
 (SELECT extract(epoch FROM clock_timestamp())-min((snapshot->>'timestamp-ms')::bigint)/1000.0
  FROM jsonb_array_elements(coalesce(doc->'snapshots','[]')) snapshot) AS oldest_snapshot_seconds
 FROM metadata)
SELECT jsonb_build_object('tables',(SELECT jsonb_agg(to_jsonb(entries) ORDER BY name) FROM entries),
 'queue',(SELECT jsonb_build_object('records',count(*),'failed',count(*) FILTER(WHERE retry_count>0),
 'oldest_seconds',extract(epoch FROM clock_timestamp()-min(orphaned_at))) FROM lake_engine.deletion_queue))"""))

    def run(self):
        r = self.runner
        began = time.monotonic()
        before = self.stats()
        supervisor = r.sql('destination', 'SELECT supervisor_pid FROM lake_replication.service_status')
        with self.publication_pause():
            r.sample('maintenance')
            for table in TABLES:
                self.vacuum(table)
            after = self.stats()
        r.check('maintenance keeps the native supervisor alive', r.sql('destination', 'SELECT supervisor_pid FROM lake_replication.service_status'), supervisor)
        for table in after['tables']:
            if table['snapshots'] > 1 and table['oldest_snapshot_seconds'] > 80:
                raise ValueError('snapshot expiry did not enforce the declared window')
        if sum(t['document_bytes'] for t in after['tables']) > 1024**2:
            raise ValueError('active Iceberg metadata exceeds the profile budget')
        if after['queue']['failed']:
            raise ValueError('file deletion queue contains failed removals')
        uri = self.witness['path']
        queue = json.loads(r.sql('destination', f"SELECT jsonb_build_object('records',count(*),'age_seconds',extract(epoch FROM clock_timestamp()-min(orphaned_at))) FROM lake_engine.deletion_queue WHERE path='{uri}'"))
        exists = self.witness_exists()
        if queue['records'] and queue['age_seconds'] < self.policy['file_seconds']:
            r.check('maintenance does not delete its protected witness early', exists, True)
        if not queue['records']:
            r.check('eligible witness was actually removed from object storage', exists, False)
            self.removal_observed = True
        elif queue['age_seconds'] > self.policy['file_seconds'] + 60:
            raise ValueError('eligible retention witness was not reclaimed')
        if self.settings() != r.result['maintenance']['defaults']:
            raise ValueError('maintenance changed a global retention default')
        row = dict(elapsed_seconds=time.monotonic()-r.begin, duration_seconds=time.monotonic()-began,
                   before=before,after=after,witness=queue,witness_exists=exists)
        r.append('maintenance.jsonl',row)
        r.result['maintenance']['passes'] += 1
        r.result['maintenance']['last'] = row
        r.checkpoint('after maintenance')

    def final(self):
        if self.name == 'bounded-test':
            self.runner.check('bounded profile demonstrates actual object reclamation', self.removal_observed, True)
        elif self.name == 'snapshot-only':
            self.runner.check('default recovery window preserves its protected witness', self.witness_exists(), True)

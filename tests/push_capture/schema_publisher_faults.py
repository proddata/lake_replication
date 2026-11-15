#!/usr/bin/env python3
"""Real backend crashes during ordered schema/data publication and lost COMMIT."""
from datetime import datetime,timezone
import sys
import time
from schema_live import SchemaLiveProbe


class SchemaPublisherFaultProbe(SchemaLiveProbe):
    def ready_destination(self):
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            try:
                if self.dest('SELECT 1')=='1':return
            except RuntimeError:pass
            time.sleep(.2)
        raise RuntimeError('destination did not recover')

    def exercise(self):
        super().exercise()
        self.evidence.update(scope='real ordered publication crashes across two-table schema, data, markers and checkpoint',
            unqualified=['production sizing','failover promotion','REST catalogs'])
        a_column='appended';b_column='qty'
        for sequence,point in enumerate(['before_upload_completion','after_upload_completion','after_commit'],5):
            next_a=f'crash_a_{sequence}';next_b=f'crash_b_{sequence}'
            self.sql(f"BEGIN;UPDATE a SET {a_column}='{point}' WHERE id=4;ALTER TABLE a RENAME COLUMN {a_column} TO {next_a};"
                f"INSERT INTO a VALUES({sequence},'{point}',false,'new row');ALTER TABLE b RENAME COLUMN {b_column} TO {next_b};"
                f"UPDATE b SET {next_b}={sequence*10};COMMIT")
            self.capture_until(sequence);self.importer(LAKE_PUSH_MAX_BATCHES='64')
            started=datetime.now(timezone.utc).isoformat()
            failed=self.command(self.worker_args('lake-replication-push-publisher',LAKE_LAB_CRASH_POINT=point),required=False)
            self.check(point+': ordered publisher loses its success response',failed.returncode!=0,True)
            self.ready_destination()
            logs=self.command(self.compose+['logs','--no-color','--since',started,'destination'])
            diagnostic=logs.stdout+logs.stderr;(self.output/(point+'.log')).write_text(diagnostic)
            self.check(point+': actual backend SIGKILL causes destination recovery',
                'was terminated by signal 9' in diagnostic and 'all server processes terminated; reinitializing' in diagnostic,True)
            committed=point=='after_commit';expected=sequence if committed else sequence-1
            self.check(point+': schema publication checkpoint has one durable outcome',self.dest(
                f'SELECT published_sequence FROM lake_replication.push_status WHERE subscription_id={self.subscription}'),str(expected))
            expected_a=next_a if committed else a_column;expected_b=next_b if committed else b_column
            self.check(point+': both destination schemas share that outcome',self.dest(f'''SELECT
                (SELECT attname FROM pg_attribute WHERE attrelid='mirror.a'::regclass AND attnum=4)::text||':'||
                (SELECT attname FROM pg_attribute WHERE attrelid='mirror.b'::regclass AND attnum=3)'''),expected_a+':'+expected_b)
            self.check(point+': apply marker commits with the schema and checkpoint',self.dest(f'''SELECT s.applied::text
                FROM lake_replication.push_schema_transactions s JOIN lake_replication.transactions t USING(transaction_id)
                JOIN lake_replication.push_receipts r ON r.subscription_id=t.subscription_id AND r.end_lsn=t.end_lsn
                WHERE r.sequence={sequence}'''),'true' if committed else 'false')
            if committed:
                self.command(self.compose+['restart','destination'],timeout=90);self.ready_destination()
                self.check('full destination restart retains the committed ordered frontier',self.dest(
                    f'SELECT published_sequence FROM lake_replication.push_status WHERE subscription_id={self.subscription}'),str(sequence))
            self.command(self.worker_args('lake-replication-push-publisher'))
            self.importer(LAKE_PUSH_REPLAY_FROM=str(sequence),LAKE_PUSH_MAX_BATCHES='64')
            self.command(self.worker_args('lake-replication-push-publisher'))
            for table in ['a','b']:
                self.check(point+': recovered schema/data equals source '+table,self.dest(
                    f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM mirror.{table} t'),self.value(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {table} t'))
            self.check(point+': retry preserves one receipt and committed transaction per source batch',self.dest(f'''SELECT
                (SELECT count(*) FROM lake_replication.push_receipts)::text||':'||
                (SELECT count(*) FROM lake_replication.transactions WHERE state='published')'''),f'{sequence}:{sequence}')
            a_column=next_a;b_column=next_b


if __name__=='__main__':sys.exit(SchemaPublisherFaultProbe().run())

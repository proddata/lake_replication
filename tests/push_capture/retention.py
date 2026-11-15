#!/usr/bin/env python3
"""Publication-only multi-consumer pruning and durable source log budgets."""
import json
import sys
from live import LiveProbe


class RetentionProbe(LiveProbe):
    def __init__(self):
        super().__init__()
        self.evidence.update(scope='conservative publication acknowledgements, logical log pruning and bounded generation budgets',
            unqualified=['physical object-byte quota','automatic snapshot expiry/orphan cleanup',
                         'indefinite receipt garbage collection','scoped consumer credentials'])

    def acknowledge(self,consumer):
        # The acknowledgement authority is the real publication checkpoint,
        # never the imported/staged frontier or a running importer process.
        row=self.dest(f'''SELECT jsonb_build_object('sequence',r.sequence,'end',r.end_lsn::text,'digest',r.digest)
            FROM lake_replication.push_receipts r JOIN lake_replication.checkpoints c USING(subscription_id)
            WHERE r.subscription_id={self.subscription} AND r.end_lsn<=c.published_lsn ORDER BY r.sequence DESC LIMIT 1''')
        if not row:return False
        row=json.loads(row)
        self.value(f"SELECT lake_capture.acknowledge_publication('{self.stream}','{consumer}',1,{row['sequence']},'{row['end']}','{row['digest']}')")
        return True

    def prune(self):return self.value(f"SELECT lake_capture.prune_logs('{self.stream}')")

    def exercise(self):
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.bind_destination()
        first=self.value(f"SELECT lake_capture.register_consumer('{self.stream}')")
        second=self.value(f"SELECT lake_capture.register_consumer('{self.stream}')")
        self.sql("BEGIN;INSERT INTO a VALUES(1,'row');INSERT INTO b VALUES(1,'row');COMMIT;")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1')
        self.sql('UPDATE lake_capture.streams SET max_retained_log_bytes=retained_log_bytes+1')
        self.sql("BEGIN;INSERT INTO a VALUES(2,'row');INSERT INTO b VALUES(2,'row');COMMIT;")
        self.check('source stops at retained-log high water while destination is stalled',self.source_wait(
            "SELECT (desired_state='stopped' AND last_error LIKE '%log budget exhausted%')::text FROM lake_capture.streams",'true'),True)
        self.check('unadmitted source transaction cannot advance captured sequence',self.value(
            'SELECT captured_sequence::text FROM lake_capture.streams'),'1')
        self.check('unacknowledged consumer protects every live log batch',self.prune(),'0')
        self.importer()
        self.check('staged-only work sends no publication acknowledgement',self.acknowledge(first),False)
        self.check('imported journal state does not authorize source pruning',self.prune(),'0')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('actual publication supplies the acknowledgement frontier',self.acknowledge(first),True)
        self.check('one lagging registered consumer still protects its batch',self.prune(),'0')
        self.acknowledge(second)
        self.check('all publication acknowledgements permit conservative pruning',self.prune(),'1')
        self.check('logical pruning removes rows from both source logs and manifest',self.value('''SELECT
            (SELECT count(*) FROM lake_capture_log.events_a)::text||':'||
            (SELECT count(*) FROM lake_capture_log.events_b)||':'||(SELECT count(*) FROM lake_capture_log.manifests)'''),'0:0:0')
        self.check('pruning atomically releases retained payload budget',self.value(
            'SELECT retained_log_bytes::text FROM lake_capture.streams'),'0')
        self.check('durable replay tombstone survives logical pruning',self.value(
            'SELECT count(*) FROM lake_capture.receipts WHERE log_pruned'),'1')
        self.check('identical prune replay does not decrement counters twice',self.prune(),'0')
        gap=self.importer(required=False,LAKE_PUSH_REPLAY_FROM='1')
        self.check('a reader behind retention fails instead of silently idling',
                   gap.returncode!=0 and 'retention gap' in gap.stderr,True)
        newcomer=self.sql(f"SELECT lake_capture.register_consumer('{self.stream}')",required=False)
        self.check('late consumer cannot assume already-pruned history exists',
                   newcomer.returncode!=0 and 'new snapshot generation' in newcomer.stderr,True)
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.check('capture resumes protected WAL after budget is released',self.source_wait(
            'SELECT captured_sequence::text FROM lake_capture.streams','2'),True)
        self.importer()
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('continuity survives pruning and stopped-capture replay',self.dest(
            'SELECT (SELECT count(*) FROM mirror.a)::text||\':\'||(SELECT count(*) FROM mirror.b)'),'2:2')
        self.acknowledge(first);self.acknowledge(second);self.prune()
        self.check('cumulative generation budget is not reset by log pruning',self.value(
            'SELECT (retained_log_bytes=0 AND generation_log_bytes>0)::text FROM lake_capture.streams'),'true')
        self.sql('UPDATE lake_capture.streams SET max_generation_log_bytes=generation_log_bytes')
        self.sql("BEGIN;INSERT INTO a VALUES(3,'row');INSERT INTO b VALUES(3,'row');COMMIT;")
        self.check('generation upload budget is a durable stop boundary',self.source_wait(
            "SELECT (desired_state='stopped' AND last_error LIKE '%log budget exhausted%')::text FROM lake_capture.streams",'true'),True)
        self.check('generation budget exhaustion cannot skip the pending source batch',self.value(
            'SELECT captured_sequence::text FROM lake_capture.streams'),'2')


if __name__=='__main__':sys.exit(RetentionProbe().run())

#!/usr/bin/env python3
"""Initial source schema history is captured at registration, never inferred later."""
from concurrent.futures import ThreadPoolExecutor
import sys
from live import LiveProbe
from session import Session


class HistoryProbe(LiveProbe):
    def rejected(self,label,sql,diagnostic):
        result=self.sql(sql,required=False)
        self.check(label,result.returncode!=0 and diagnostic in result.stderr,True)

    def exercise(self):
        self.evidence.update(scope='registration-time immutable initial schema history',
            unqualified=['DDL auditing','ordered schema publication'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);''')
        self.rejected('registration refuses historical caller snapshots',
            "BEGIN ISOLATION LEVEL REPEATABLE READ;SELECT lake_capture.register_stream('a','b')",'READ COMMITTED')
        writer=Session(self.sql_args())
        try:
            writer.query('BEGIN;ALTER TABLE a ENABLE ROW LEVEL SECURITY')
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending=pool.submit(self.sql,"SELECT lake_capture.register_stream('a','b')",required=False)
                self.source_wait("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%register_stream%')::text",'true')
                writer.query('COMMIT')
                result=pending.result(timeout=20)
            self.check('registration uses post-lock metadata when DDL commits during its wait',
                result.returncode!=0 and 'row-level security' in result.stderr,True)
        finally:writer.close()
        self.check('rejected post-lock registration leaves no partial binding',self.value('SELECT count(*) FROM lake_capture.streams'),'0')
        self.sql('ALTER TABLE a DISABLE ROW LEVEL SECURITY')
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.check('registration stores both initial definitions before capture starts',self.value('''SELECT
            count(*)::text||':'||bool_and(schema_version=1 AND d.capture_generation=1 AND
                definition_bytes=lake_capture.describe_relation(CASE WHEN table_id=s.table_a_id THEN s.table_a ELSE s.table_b END)
                AND definition_json=lake_capture.describe_relation_json(CASE WHEN table_id=s.table_a_id THEN s.table_a ELSE s.table_b END))::text
            FROM lake_capture.schema_definitions d JOIN lake_capture.streams s USING(stream_id)'''),'2:true')
        self.rejected('incomplete baseline cannot verify schema history',
            f"SELECT lake_capture.verify_schema_history('{self.stream}')",'complete baseline')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.rejected('running actor prevents schema verification',
            f"SELECT lake_capture.verify_schema_history('{self.stream}')",'stopped stream')
        self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*)::text FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        lease=Session(self.sql_args())
        try:
            lease.query(f"SELECT pg_advisory_lock(hashtextextended('lake_capture/{self.stream}',0))")
            self.rejected('verification refuses a live manual capture lease',
                f"SELECT lake_capture.verify_schema_history('{self.stream}')",'capture actor lease')
        finally:lease.close()
        self.check('stopped completed baseline verifies idempotently',self.value(f"SELECT lake_capture.verify_schema_history('{self.stream}') AND lake_capture.verify_schema_history('{self.stream}')"),'t')
        self.rejected('registered nullable columns remain frozen','ALTER TABLE a ALTER COLUMN note SET NOT NULL','schema is frozen')
        self.rejected('registered default presence remains frozen',"ALTER TABLE a ALTER COLUMN note SET DEFAULT 'unsafe'",'schema is frozen')
        self.rejected('conflicting stored history is rejected atomically',f"BEGIN;UPDATE lake_capture.schema_definitions SET definition_bytes='\\x01';SELECT lake_capture.verify_schema_history('{self.stream}')",'conflicting initial')
        self.rejected('missing stored history is never reconstructed',f"BEGIN;DELETE FROM lake_capture.schema_definitions;SELECT lake_capture.verify_schema_history('{self.stream}')",'no eligible initial')
        self.sql(f"SELECT lake_capture.reset_stream('{self.stream}')")
        self.source_wait('SELECT capture_generation::text FROM lake_capture.streams','2')
        self.check('frozen reset carries the original definitions into the next generation',self.value('''SELECT
            count(*)::text||':'||min(capture_generation)::text FROM lake_capture.schema_definitions'''),'2:2')
        self.sql(f"SELECT lake_capture.drop_stream('{self.stream}')")
        self.source_wait('SELECT count(*)::text FROM lake_capture.streams','0')
        self.check('drop removes the bounded schema history',self.value('SELECT count(*) FROM lake_capture.schema_definitions'),'0')
        self.sql("ALTER TABLE a ALTER COLUMN note SET DEFAULT 'legacy'")
        self.stream=self.value("SELECT lake_capture.register_stream('a','b')")
        self.check('legacy frozen defaults remain supported without invented strict definitions',self.value('SELECT count(*) FROM lake_capture.schema_definitions'),'1')
        self.sql(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.sql(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*)::text FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        # Remove eligibility obstacle to prove verification still refuses absent historical bytes.
        self.sql('ALTER EVENT TRIGGER lake_capture_ddl_end DISABLE;ALTER TABLE a ALTER COLUMN note DROP DEFAULT;ALTER EVENT TRIGGER lake_capture_ddl_end ENABLE')
        self.rejected('later eligible catalogs cannot supply missing version one',
            f"SELECT lake_capture.verify_schema_history('{self.stream}')",'no eligible initial')


if __name__=='__main__':sys.exit(HistoryProbe().run())

#!/usr/bin/env python3
"""Source/destination object prefixes use distinct least-scope fixture users."""
import json
import sys
from live import LiveProbe

class StorageCredentialProbe(LiveProbe):
    def engine(self,service,query,required=True):
        return self.command(self.compose+['exec','-T',service,'psql','-XqAt','-h','/var/run/pgduck',
            '-p','5332','-U','postgres','-d','postgres','-c',query],required=required)

    def exercise(self):
        self.evidence.update(scope='distinct source/destination object-prefix IAM credentials',
            unqualified=['untrusted operators sharing one source pgduck','cloud-provider IAM qualification','credential rotation'],denials=[])
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
        self.sql("BEGIN;INSERT INTO a VALUES(1,'prefix');INSERT INTO b VALUES(1,'prefix');COMMIT")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1')
        self.importer();self.command(self.worker_args('lake-replication-publisher'))
        source=self.value("SELECT metadata_location FROM lake_iceberg.tables_internal WHERE table_name='lake_capture_log.events_a'::regclass")
        destination=self.dest("SELECT metadata_location FROM lake_iceberg.tables_internal WHERE table_name='mirror.a'::regclass")
        self.check('source metadata is within its private object prefix',source.startswith('s3://lake-push/iceberg/'),True)
        self.check('destination metadata is within its separate object prefix',destination.startswith('s3://lake-push/destination/'),True)
        for service,own,other,forbidden in [('pgduck',source,destination,'destination'),('destination-pgduck',destination,source,'iceberg')]:
            self.check(service+' can read its own metadata',self.engine(service,f"SELECT count(*) FROM read_text('{own}')").stdout.strip(),'1')
            denied=self.engine(service,f"SELECT count(*) FROM read_text('{other}')",False)
            self.evidence['denials'].append(dict(service=service,operation='read',returncode=denied.returncode,stdout=denied.stdout,stderr=denied.stderr))
            self.check(service+' cannot read the other engine prefix',(denied.returncode!=0 and any(word in (denied.stdout+denied.stderr).lower() for word in ['403','denied','forbidden'])) or (denied.returncode==0 and denied.stdout.strip()=='0'),True)
            denied=self.engine(service,f"COPY (SELECT 1 AS forbidden) TO 's3://lake-push/{forbidden}/forbidden-{service}.parquet' (FORMAT PARQUET)",False)
            self.evidence['denials'].append(dict(service=service,operation='write',returncode=denied.returncode,stdout=denied.stdout,stderr=denied.stderr))
            self.check(service+' cannot write the other engine prefix',denied.returncode!=0 and any(word in (denied.stdout+denied.stderr).lower() for word in ['403','denied','forbidden']),True)
            self.check(service+' has no MinIO administrative credential in its configured secret',
                'push-fixture-only' not in self.engine(service,'SELECT secret_string FROM duckdb_secrets()').stdout,True)
        self.check('read-only transport imports through the source catalog with independent destination credentials',self.dest('SELECT count(*) FROM mirror.a'),'1')

if __name__=='__main__':sys.exit(StorageCredentialProbe().run())

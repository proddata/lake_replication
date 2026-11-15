#!/usr/bin/env python3
"""Packaged wide UPDATE capture with unchanged TOAST and evolving row types."""
import sys
from probe import LocalIcebergProbe


class WideUpdateStreamProbe(LocalIcebergProbe):
    def exercise(self):
        self.setup(mirror=False,seed=False)
        self.check('packaged function includes convert-once UPDATE path',self.value("SELECT (position('CROSS JOIN LATERAL jsonb_populate_record' IN prosrc)>0)::text FROM pg_proc WHERE oid='lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure"),'true')
        self.sql('ALTER TABLE a ADD COLUMN document jsonb,ADD COLUMN payload text,'+
            ','.join(f'ADD COLUMN c{i} text' for i in range(44))+';ALTER TABLE a ALTER COLUMN payload SET STORAGE EXTERNAL')
        self.sql("INSERT INTO a(id,note,document,payload) SELECT i,'before','{\"start\":true}',(SELECT string_agg(md5(g::text),'') FROM generate_series(1,4000) g) FROM generate_series(1,32) i;INSERT INTO b SELECT i,'before' FROM generate_series(1,32) i")
        toast=self.value("SELECT reltoastrelid::regclass FROM pg_class WHERE oid='a'::regclass")
        self.check('wide source uses external TOAST storage',int(self.value(f'SELECT count(*) FROM {toast}'))>0,True)
        self.start_subscription();self.wait_initial_copy()
        self.wait('SELECT count(*) FROM iceberg_mirror.a','32');self.wait('SELECT count(*) FROM iceberg_mirror.b','32');self.equal()
        assignments="note='changed',document=jsonb_build_object('row',id,'number',123456789012345678901234567890123456789::numeric),"+','.join(f"c{i}='value-{i}'" for i in range(44))
        self.sql('BEGIN;UPDATE a SET '+assignments+";UPDATE b SET note='changed';COMMIT")
        self.wait("SELECT count(*) FROM iceberg_mirror.a WHERE note='changed'",'32');self.wait("SELECT count(*) FROM iceberg_mirror.b WHERE note='changed'",'32');self.equal()
        source_oid=self.value("SELECT 'a'::regclass::oid")
        self.check('wide WAL run omits unchanged external payload',self.value(f"SELECT count(*) FROM lake_replication.changes WHERE remote_relation_id={source_oid} AND operation='update' AND NOT (new_tuple?'payload')"),'32')
        self.check('wide WAL tuples qualify for conversion threshold',self.value(f"SELECT min((SELECT count(*) FROM jsonb_object_keys(c.new_tuple)))>=32 FROM lake_replication.changes c WHERE remote_relation_id={source_oid} AND operation='update'"),'t')
        self.check('JSONB large numeric value retains precision',self.value("SELECT document->>'number' FROM iceberg_mirror.a WHERE id=1"),'123456789012345678901234567890123456789')
        self.sql('ALTER TABLE a ADD COLUMN extra text');self.sql("UPDATE a SET extra='after schema change'")
        self.wait("SELECT count(*) FROM pg_attribute WHERE attrelid='iceberg_mirror.a'::regclass AND attname='extra' AND NOT attisdropped",'1')
        self.wait("SELECT count(*) FROM iceberg_mirror.a WHERE extra='after schema change'",'32');self.equal()
        self.sql('UPDATE a SET id=id+100')
        self.wait('SELECT min(id) FROM iceberg_mirror.a','101');self.equal()
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('wide UPDATE cleanup releases the local slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(WideUpdateStreamProbe().run())

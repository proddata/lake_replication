#!/usr/bin/env python3
"""A publisher crash restarts the same PostgreSQL hosting application and journal."""
from datetime import datetime,timezone
import json
import os
import sys
import time
from probe import LocalIcebergProbe


class LocalFaultProbe(LocalIcebergProbe):
    def worker(self,binary,**settings):
        environment=dict(LAKE_SOURCE_ADDRESS='127.0.0.1:5432',LAKE_DESTINATION_ADDRESS='127.0.0.1:5432',
            LAKE_SOURCE_DATABASE='capture',LAKE_DESTINATION_DATABASE='capture',LAKE_SOURCE_USER='postgres',
            LAKE_DESTINATION_USER='postgres',LAKE_SOURCE_TLS_MODE='disable',LAKE_DESTINATION_TLS_MODE='disable',
            LAKE_SLOT='local_fault_slot',LAKE_PUBLICATION='local_fault_pub',LAKE_SUBSCRIPTION_ID=str(self.subscription),
            LAKE_CONFIG_GENERATION='0',LAKE_MAX_TRANSACTIONS='1')
        environment.update(settings)
        args=self.compose+['exec','-T']
        for key,value in environment.items():args+=['-e',key+'='+value]
        return args+['postgres','lake-replication-'+binary]

    def exercise(self):
        self.evidence.update(scope='single-instance application/WAL/journal/Iceberg publisher crash recovery',
            unqualified=['managed runtime fault injection','initial-copy crash in this topology','production sizing'])
        wide=os.environ.get('LOCAL_ICEBERG_WIDE_UPDATES')=='1'
        extra=''.join(f',c{i} text' for i in range(46)) if wide else ''
        self.sql(f'''CREATE TABLE a(id bigint PRIMARY KEY,note text{extra});CREATE TABLE b(id bigint PRIMARY KEY,note text{extra});
            CREATE PUBLICATION local_fault_pub FOR TABLE a,b;
            SELECT pg_create_logical_replication_slot('local_fault_slot','pgoutput');CREATE SCHEMA iceberg_mirror''')
        identity=json.loads(self.value("SELECT json_build_array((pg_control_system()).system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()),(pg_control_checkpoint()).timeline_id)"))
        self.subscription=int(self.value(f"SELECT lake_replication.create_subscription({','.join(str(v) for v in identity)},'local_fault','local_fault_slot',ARRAY['local_fault_pub'])"))
        columns=json.dumps([dict(name='id',type_oid=20,type_modifier=-1,flags=1),dict(name='note',type_oid=25,type_modifier=-1,flags=0)]+
            ([dict(name=f'c{i}',type_oid=25,type_modifier=-1,flags=0) for i in range(46)] if wide else []))
        for table in 'ab':
            oid=self.value(f"SELECT '{table}'::regclass::oid")
            version=self.value(f"SELECT lake_replication.register_relation({self.subscription},{oid},'public','{table}','d','{columns}')")
            self.sql(f"CREATE TABLE iceberg_mirror.{table}(id bigint NOT NULL,note text{extra}) USING iceberg;SELECT lake_replication.assign_destination({self.subscription},{oid},{version},'iceberg_mirror.{table}'::regclass)")
        changes=["INSERT INTO a VALUES(1,'first');INSERT INTO b VALUES(1,'first')",
            "UPDATE a SET id=2,note='updated';DELETE FROM b;INSERT INTO b VALUES(2,'reinserted')",
            "TRUNCATE a,b;INSERT INTO a VALUES(3,'final');INSERT INTO b VALUES(3,'final')"]
        changes=[[statement] for statement in changes]
        if wide:
            self.evidence['wide_update_scope']='48 columns, 32 rows per target, optimized UPDATE at both post-upload and post-COMMIT crash boundaries'
            insert=''.join(f",'first-{i}'" for i in range(46))
            changes=[[';'.join(f"INSERT INTO {table} SELECT i,'first'{insert} FROM generate_series(1,32) i" for table in 'ab')]]
            for label in ['updated','final']:
                fields=','.join([f"note='{label}'"]+[f"c{i}='{label}-{i}'" for i in range(46)])
                changes.append([';'.join(f'UPDATE {table} SET {fields}' for table in 'ab')])
        grouped=os.environ.get('LOCAL_ICEBERG_GROUP_INSERTS')=='1'
        if grouped and wide:raise RuntimeError('select either grouped INSERT or wide UPDATE crash coverage')
        if grouped:
            self.sql('ALTER DATABASE capture SET lake_replication.group_independent_inserts=on')
            self.evidence['publication_scope']='three source transactions per batch, grouped INSERTs enabled'
            changes=[
                [f"INSERT INTO a VALUES({i},'first');INSERT INTO b VALUES({i},'first')" for i in range(1,4)],
                [f"UPDATE a SET id={i+10},note='updated' WHERE id={i};UPDATE b SET id={i+10},note='updated' WHERE id={i}" for i in range(1,4)],
                ["TRUNCATE a,b;INSERT INTO a VALUES(21,'final');INSERT INTO b VALUES(21,'final')",
                 "INSERT INTO a VALUES(22,'final');INSERT INTO b VALUES(22,'final')",
                 "INSERT INTO a VALUES(23,'final');INSERT INTO b VALUES(23,'final')"]]
        self.evidence['boundaries']={}
        for point,statements in zip(['before_upload_completion','after_upload_completion','after_commit'],changes):
            before=self.value(f'SELECT published_lsn FROM lake_replication.checkpoints WHERE subscription_id={self.subscription}')
            for statement in statements:self.sql('BEGIN;'+statement+';COMMIT')
            self.command(self.worker('receiver',LAKE_MAX_TRANSACTIONS=str(len(statements))))
            if wide and point!='before_upload_completion':
                self.check(point+': staged rows qualify for wide UPDATE conversion',self.value("SELECT count(*) FROM lake_replication.changes c JOIN lake_replication.transactions t USING(transaction_id) WHERE t.state='staged' AND c.operation='update' AND (SELECT count(*) FROM jsonb_object_keys(c.new_tuple))>=32"),'64')
            staged=self.value(f'SELECT staged_lsn FROM lake_replication.checkpoints WHERE subscription_id={self.subscription}')
            original={table:self.value(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {table} t') for table in 'ab'}
            started=datetime.now(timezone.utc).isoformat()
            failed=self.command(self.worker('publisher',LAKE_LAB_CRASH_POINT=point),required=False)
            self.check(point+': publisher loses its response',failed.returncode!=0,True)
            self.wait('SELECT 1','1')
            logs=self.command(self.compose+['logs','--no-color','--since',started,'postgres'])
            diagnostic=logs.stdout+logs.stderr;(self.output/(point+'.log')).write_text(diagnostic)
            self.check(point+': shared PostgreSQL actually crashes and recovers',
                'was terminated by signal 9' in diagnostic and 'all server processes terminated; reinitializing' in diagnostic,True)
            expected=staged if point=='after_commit' else before
            self.check(point+': publication checkpoint matches commit outcome',self.value(f'SELECT published_lsn FROM lake_replication.checkpoints WHERE subscription_id={self.subscription}'),expected)
            self.check(point+': local admission remains durable',self.value(f'SELECT staged_lsn FROM lake_replication.checkpoints WHERE subscription_id={self.subscription}'),staged)
            for table in 'ab':
                self.check(point+': committed application heap survives '+table,self.value(f'SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM {table} t'),original[table])
            self.command(self.worker('publisher'));self.command(self.worker('publisher'))
            self.equal()
            if grouped and point=='before_upload_completion':
                for table in 'ab':self.check('recovered grouped INSERTs use one current file '+table,self.value(f"SELECT count(*) FROM lake_table.files WHERE table_name='iceberg_mirror.{table}'::regclass AND content=0"),'1')
            self.check(point+': retry reaches admitted boundary exactly once',self.value(f'SELECT published_lsn FROM lake_replication.checkpoints WHERE subscription_id={self.subscription}'),staged)
            self.evidence['boundaries'][point]=dict(staged_lsn=staged,published_after_crash=expected)
        self.sql("SELECT pg_drop_replication_slot('local_fault_slot')")


if __name__=='__main__':sys.exit(LocalFaultProbe().run())

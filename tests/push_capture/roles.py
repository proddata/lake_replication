#!/usr/bin/env python3
"""Dedicated source LOGIN REPLICATION operator and read-only log consumer."""
import json
import sys
from live import LiveProbe, ROOT

class RoleProbe(LiveProbe):
    def as_role(self,role,statement,required=True):
        args=self.sql_args();args[args.index('postgres')]=role
        return self.command(args,input=statement,required=required)

    def exercise(self):
        self.evidence.update(scope='dedicated source replication LOGIN and read-only consumer',
            unqualified=['multiple untrusted capture operators','object-store credential isolation','production packaging'])
        self.sql('''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            CREATE TABLE unrelated(id bigint PRIMARY KEY,note text);
            INSERT INTO a VALUES(1,'initial');INSERT INTO b VALUES(2,'initial');
            CREATE ROLE capture_operator LOGIN REPLICATION;
            CREATE ROLE capture_reader LOGIN;
            CREATE ROLE untrusted_login LOGIN;''')
        args=self.sql_args()+['-v','capture_login=capture_operator']
        self.command(args,input=(ROOT/'deploy/push/source-grants.sql').read_text())
        self.sql('GRANT SELECT,MAINTAIN ON a,b TO capture_operator')
        denied=self.as_role('capture_operator',"SELECT lake_capture.register_stream('a','unrelated',NULL,'online_copy')",False)
        self.check('operator cannot register a table without explicit SELECT',denied.returncode!=0,True)
        denied=self.as_role('untrusted_login',"SELECT lake_capture.register_stream('a','b')",False)
        self.check('ungranted login cannot use the capture control plane',denied.returncode!=0,True)
        self.stream=self.as_role('capture_operator',"SELECT lake_capture.register_stream('a','b',NULL,'online_copy')").stdout.strip()
        denied=self.as_role('capture_operator',"INSERT INTO lake_capture.streams(schema_auditing) VALUES(true)",False)
        self.check('operator cannot opt into internal schema auditing through stream INSERT',
            denied.returncode!=0 and 'permission denied' in denied.stderr,True)
        self.check('operator cannot enable internal schema auditing on an existing stream',self.as_role('capture_operator',
            "UPDATE lake_capture.streams SET schema_auditing=true",False).returncode!=0,True)
        self.check('operator cannot change the privileged child identity',self.as_role('capture_operator',
            "UPDATE lake_capture.streams SET owner='postgres'::regrole",False).returncode!=0,True)
        for field,value in [('table_a',"'unrelated'::regclass"),('capture_generation','capture_generation+1'),('slot_name',"'foreign_slot'"),('log_locations',"'{}'::jsonb"),('object_prefix',"'s3://other'")]:
            self.check('operator cannot bypass controlled rebuild by rewriting '+field,self.as_role('capture_operator',
                'UPDATE lake_capture.streams SET '+field+'='+value,False).returncode!=0,True)
        self.as_role('capture_operator',f"SELECT lake_capture.start_stream('{self.stream}')")
        self.check('non-superuser source owner captures a consistent online baseline',self.source_wait(
            "SELECT snapshot_state FROM lake_capture.streams",'complete'),True)
        self.check('source status exposes live connection and retained WAL budgets',self.value(
            "SELECT (capture_connections=1 AND retained_wal_bytes>=0 AND retained_log_bytes>0)::text FROM lake_capture.status"),'true')
        self.check('source child authenticates as the registered owner',self.value("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%' AND usename='capture_operator'"),'1')
        self.identity=json.loads(self.value('''SELECT jsonb_build_object(
            'system_identifier',origin_system_identifier::text,'database_oid',origin_database_oid::text,
            'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'stream',stream_id::text,
            'table_a',table_a_id::text,'table_b',table_b_id::text) FROM lake_capture.streams'''))
        self.sql('''GRANT USAGE ON SCHEMA lake_capture_log TO capture_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA lake_capture_log TO capture_reader;
            GRANT lake_read TO capture_reader;''')
        self.bind_destination()
        self.importer(LAKE_SOURCE_USER='capture_reader')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('read-only source consumer imports without capture privileges',self.dest('SELECT count(*) FROM mirror.a'),'1')
        for label,query in [('mutate capture logs',"DELETE FROM lake_capture_log.events_a"),
                ('read unrelated application data','SELECT * FROM unrelated'),
                ('stop capture',f"SELECT lake_capture.stop_stream('{self.stream}')")]:
            self.check('log consumer cannot '+label,self.as_role('capture_reader',query,False).returncode!=0,True)
        self.check('operator cannot write selected application data',self.as_role('capture_operator',"INSERT INTO a VALUES(999,'forbidden')",False).returncode!=0,True)
        self.check('operator cannot create a superuser',self.as_role('capture_operator','CREATE ROLE forbidden SUPERUSER',False).returncode!=0,True)
        self.sql("BEGIN;UPDATE a SET note='mutable';INSERT INTO b VALUES(3,'live');COMMIT;")
        self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2')
        self.importer(LAKE_SOURCE_USER='capture_reader')
        self.command(self.worker_args('lake-replication-publisher'))
        self.check('scoped source capture continues through mutation publication',self.dest("SELECT note FROM mirror.a WHERE id=1"),'mutable')
        self.sql("SELECT extension_base.deregister_worker('lake_replication capture')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.command(self.capture_args(LAKE_SOURCE_USER='capture_operator',LAKE_CAPTURE_MAINTENANCE_ONLY='1'))
        self.check('dedicated source operator performs scoped log maintenance',self.value(
            'SELECT maintenance_attempt_count::text FROM lake_capture.streams'),'1')
        self.sql('SELECT lake_capture.start_manager()')
        self.as_role('capture_operator',f"SELECT lake_capture.drop_stream('{self.stream}')")
        self.source_wait('SELECT count(*) FROM lake_capture.streams','0')
        self.check('operator cleanup drops only the owned slot',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='lake_replication_capture'"),'0')

if __name__=='__main__':sys.exit(RoleProbe().run())

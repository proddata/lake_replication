#!/usr/bin/env python3
"""Source worker SCRAM credentials rotate through explicit fenced restart."""
import sys
import time
from live import LiveProbe, ROOT

class CredentialProbe(LiveProbe):
    def as_owner(self,statement):
        args=self.sql_args();args[args.index('postgres')]='capture_operator'
        return self.command(args,input=statement).stdout.strip()

    def write_credentials(self,path,password):
        code="from pathlib import Path;import os,pwd;p=Path("+repr(path)+");t=p.with_suffix('.new');t.write_text("+repr('LAKE_SOURCE_PASSWORD='+password+'\n')+");os.chmod(t,0o600);u=pwd.getpwnam('postgres');os.chown(t,u.pw_uid,u.pw_gid);os.replace(t,p)"
        self.command(self.compose+['exec','-T','source','python3','-c',code])

    def exercise(self):
        self.evidence.update(scope='source SCRAM secret-file rotation and authentication-failure retry fence',
            unqualified=['cloud-provider object-key rotation','remote source transport'])
        first='first-disposable-push-password';second='rotated-disposable-push-password'
        self.sql(f'''CREATE EXTENSION lake_replication_capture;
            CREATE TABLE a(id bigint PRIMARY KEY,note text);CREATE TABLE b(id bigint PRIMARY KEY,note text);
            CREATE ROLE capture_operator LOGIN REPLICATION PASSWORD '{first}';''')
        self.command(self.sql_args()+['-v','capture_login=capture_operator'],input=(ROOT/'deploy/push/source-grants.sql').read_text())
        self.sql('GRANT SELECT,MAINTAIN ON a,b TO capture_operator')
        hba="from pathlib import Path;p=Path('/var/lib/postgresql/18/data/pg_hba.conf');p.write_text('host capture capture_operator 127.0.0.1/32 scram-sha-256\\n'+p.read_text())"
        self.command(self.compose+['exec','-T','source','python3','-c',hba])
        self.sql('SELECT pg_reload_conf()')
        self.write_credentials('/tmp/capture.credentials',first)
        self.write_credentials('/tmp/capture-old.credentials',first)
        self.stream=self.as_owner("SELECT lake_capture.register_stream('a','b','/tmp/capture.credentials')")
        self.as_owner(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.source_wait('SELECT (baseline_lsn IS NOT NULL)::text FROM lake_capture.streams','true')
        self.sql("BEGIN;INSERT INTO a VALUES(1,'first-secret');INSERT INTO b VALUES(1,'first-secret');COMMIT")
        self.check('source child authenticates with an owner-only SCRAM credential file',self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','1'),True)
        self.as_owner(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.sql(f"ALTER ROLE capture_operator PASSWORD '{second}'")
        self.write_credentials('/tmp/capture.credentials',second)
        self.sql("INSERT INTO a VALUES(2,'protected-during-rotation')")
        self.as_owner(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.check('explicit generation restart reloads rotated credentials and protected WAL',self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','2'),True)
        self.as_owner(f"SELECT lake_capture.stop_stream('{self.stream}')")
        self.source_wait("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'",'0')
        self.as_owner("UPDATE lake_capture.streams SET credential_file='/tmp/capture-old.credentials'")
        self.as_owner(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.sql("INSERT INTO b VALUES(3,'pending-auth-repair')")
        time.sleep(3)
        self.check('obsolete authentication cannot advance capture or begin an upload',self.value("SELECT captured_sequence::text||':'||upload_attempt_count FROM lake_capture.streams"),'2:2')
        self.check('obsolete authentication leaves no connected source child',self.value("SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'lake_capture/%'"),'0')
        self.as_owner("UPDATE lake_capture.streams SET credential_file='/tmp/capture.credentials'")
        self.as_owner(f"SELECT lake_capture.start_stream('{self.stream}')")
        self.check('repaired authentication resumes each pending transaction once',self.source_wait('SELECT captured_sequence::text FROM lake_capture.streams','3'),True)
        args=self.command(self.compose+['exec','-T','source','ps','-eo','args']).stdout
        self.check('credential values stay out of process arguments',first not in args and second not in args,True)
        logs=self.command(self.compose+['logs','--no-color','source'])
        self.check('credential values stay out of source failure diagnostics',all(secret not in logs.stdout+logs.stderr for secret in [first,second]),True)

if __name__=='__main__':sys.exit(CredentialProbe().run())

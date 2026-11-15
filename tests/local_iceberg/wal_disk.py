#!/usr/bin/env python3
"""Actual private WAL ENOSPC, shared-instance recovery and continued mirroring."""
import sys
from probe import LocalIcebergProbe,ROOT


class LocalWalDiskProbe(LocalIcebergProbe):
    def __init__(self,scope='wal'):
        super().__init__()
        self.disk_scope=scope
        self.directory='/wal-budget' if scope=='wal' else '/var/lib/postgresql/18/data'
        self.compose+=['-f',str(ROOT/f'deploy/local/{scope}-disk.compose.yml')]

    def exec_postgres(self,args,**kwargs):
        return self.command(self.compose+['exec','-T','--user','postgres','postgres']+args,**kwargs)

    def exercise(self):
        self.evidence.update(scope=f'physical ENOSPC on bounded private {self.disk_scope} filesystem with shared-instance recovery',
            unqualified=['power loss or container restart of volatile data','production disk quotas','all ambiguous application commit outcomes'])
        self.setup()
        self.sql("BEGIN;INSERT INTO a VALUES(3,'durable before disk failure');INSERT INTO b VALUES(3,'durable before disk failure');COMMIT")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','2');self.wait('SELECT count(*) FROM iceberg_mirror.b','2')
        self.equal()
        before=self.value('SELECT pg_postmaster_start_time()')
        if self.disk_scope=='wal':
            self.check('fixture puts WAL on its private bounded filesystem',self.exec_postgres(['python3','-c',"import os;print(os.readlink('/var/lib/postgresql/18/data/pg_wal'))"]).stdout.strip(),'/wal-budget')
        else:
            self.check('fixture mounts the bounded shared PGDATA filesystem',self.exec_postgres(['python3','-c',"import os;print(os.path.ismount('/var/lib/postgresql/18/data'))"]).stdout.strip(),'True')
        fill="""import os,json,sys
from pathlib import Path
p=Path(sys.argv[1]);v=os.statvfs(p);free=v.f_bavail*v.f_frsize
assert v.f_blocks*v.f_frsize<=int(sys.argv[2])
with p.joinpath('qualification-filler').open('wb',buffering=0) as f:
 for unused in range(max(0,(free-1048576)//1048576)):f.write(b'x'*1048576)
v=os.statvfs(p);print(json.dumps(dict(total_bytes=v.f_blocks*v.f_frsize,free_bytes=v.f_bavail*v.f_frsize)))
"""
        import json
        capacity=134217728 if self.disk_scope=='wal' else 536870912
        self.evidence['filesystem_filled']=json.loads(self.exec_postgres(['python3','-c',fill,self.directory,str(capacity)]).stdout)
        try:
            # Each failed attempt is bounded; ENOSPC arrives at segment creation.
            query="""SET statement_timeout='20s';DO $$ BEGIN FOR counter IN 1..48 LOOP
                PERFORM pg_logical_emit_message(false,'local_disk_full',repeat('x',1048576),true);
                PERFORM pg_switch_wal();
                END LOOP;END $$""" if self.disk_scope=='wal' else """SET statement_timeout='20s';
                INSERT INTO a SELECT i,repeat(md5(i::text),32) FROM generate_series(10000,20000) i"""
            failed=self.sql(query,required=False)
            self.check(self.disk_scope+': allocation fails while the filesystem is full',failed.returncode!=0,True)
            logs=self.command(self.compose+['logs','--no-color','postgres']).stdout
            paths=('pg_wal','/wal-budget') if self.disk_scope=='wal' else ('base/','pg_wal')
            self.check(self.disk_scope+': PostgreSQL reports actual physical ENOSPC','No space left on device' in logs and any(path in logs for path in paths),True)
            self.evidence['allocation_failure']=dict(returncode=failed.returncode,stderr=failed.stderr[-3000:])
        finally:
            self.exec_postgres(['python3','-c',"from pathlib import Path;import sys;Path(sys.argv[1],'qualification-filler').unlink(missing_ok=True)",self.directory])
        # PANIC can leave the postmaster recovering or exit it. The wrapper keeps
        # this container/tmpfs alive; restart only the database if necessary.
        status=self.exec_postgres(['pg_ctl','-D','/var/lib/postgresql/18/data','status'],required=False)
        if status.returncode:
            restart="""import shlex,subprocess
from pathlib import Path
data='/var/lib/postgresql/18/data'
options=shlex.join(shlex.split(Path(data+'/postmaster.opts').read_text())[1:])
raise SystemExit(subprocess.run(['pg_ctl','-D',data,'-l','/tmp/wal-repair.log','-w','-o',options,'start']).returncode)
"""
            result=self.exec_postgres(['python3','-c',restart],required=False,timeout=90)
            log=self.exec_postgres(['python3','-c',"from pathlib import Path;p=Path('/tmp/wal-repair.log');print(p.read_text() if p.exists() else '')"],required=False)
            (self.output/'wal-repair.log').write_text(log.stdout+log.stderr)
            if result.returncode:raise RuntimeError('WAL repair restart failed: '+result.stderr+'\n'+log.stdout[-4000:])
        self.wait('SELECT 1','1',timeout=90)
        self.evidence['postmaster_start_after_repair']=self.value('SELECT pg_postmaster_start_time()')
        self.evidence['postmaster_start_before_failure']=before
        self.check('committed application heaps survive physical '+self.disk_scope+' failure',self.value("SELECT (SELECT count(*) FROM a)::text||':'||(SELECT count(*) FROM b)"),'2:2')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'healthy',timeout=120)
        self.equal()
        self.sql("SET statement_timeout='10s';BEGIN;INSERT INTO a VALUES(4,'after disk repair');INSERT INTO b VALUES(4,'after disk repair');COMMIT")
        self.wait('SELECT count(*) FROM iceberg_mirror.a','3',timeout=90);self.wait('SELECT count(*) FROM iceberg_mirror.b','3',timeout=90)
        self.equal()
        self.check('recovery retains one persistent local slot',self.value("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'"),'1')
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('repaired local runtime cleans up its slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(LocalWalDiskProbe().run())

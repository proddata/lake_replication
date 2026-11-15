#!/usr/bin/env python3
"""Diagnostic timings only: SQL wrappers and split worker transaction phases."""
import json
import re
import sys
from collections import defaultdict
from pull_compare import ComparisonProbe
from probe import ROOT


class PublicationProfile(ComparisonProbe):
    def __init__(self,workload='mixed'):
        super().__init__(False,workload,120,4,.25)
        self.evidence.update(scope=__doc__,diagnostic_only=True)

    def setup(self,mirror=True,seed=False):
        super().setup(mirror=False,seed=False)
        code="""from pathlib import Path
p=Path('/usr/local/bin/lake-replication-supervisor');p.rename(str(p)+'-profile-original')
p.write_text('#!/bin/sh\\nexport LAKE_TRACE_TRANSACTIONS=1\\nexport LAKE_TRACE_TRANSACTION_PHASES=1\\nexec /usr/local/bin/lake-replication-supervisor-profile-original "$@"\\n');p.chmod(0o755)
"""
        self.command(self.compose+['exec','-T','--user','root','postgres','python3','-c',code])
        functions=[
            ('apply_change_run','text,regclass,jsonb[],jsonb[]','bigint',"'apply/'||$1"),
            ('prepare_batch','bigint,text,pg_lsn,pg_lsn','bigint',"'prepare'"),
            ('finish_batch','bigint','pg_lsn',"'finish'"),
            ('try_stage_transaction','bigint,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb','boolean',"'stage'"),
        ]
        statements=[]
        for name,arguments,returns,label in functions:
            params=','.join('$'+str(i+1) for i in range(len(arguments.split(','))))
            statements.append(f'''ALTER FUNCTION lake_replication.{name}({arguments}) RENAME TO {name}_profile_original;
                CREATE FUNCTION lake_replication.{name}({arguments}) RETURNS {returns}
                LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
                DECLARE started timestamptz:=clock_timestamp(); result {returns};
                BEGIN result:=lake_replication.{name}_profile_original({params});
                  RAISE LOG 'lake-apply-profile|%|%',{label},extract(epoch FROM clock_timestamp()-started)*1000;
                  RETURN result;END $$;
                REVOKE ALL ON FUNCTION lake_replication.{name}({arguments}) FROM PUBLIC;
                GRANT EXECUTE ON FUNCTION lake_replication.{name}({arguments}) TO lake_replication_admin,lake_replication_worker;''')
        sql='BEGIN;'+''.join(statements)+'COMMIT;'
        (self.output/'profile-wrappers.sql').write_text(sql)
        self.sql(sql)

    def exercise(self):
        super().exercise()
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        code="""from pathlib import Path
p=Path('/var/lib/postgresql/18/data/lake_replication_logs')
for f in sorted(p.glob('*')):
 if f.is_file():print(f.read_text())
"""
        worker_log=self.command(self.compose+['exec','-T','--user','postgres','postgres','python3','-c',code]).stdout
        (self.output/'worker-profile.jsonl').write_text(worker_log)
        server_log=self.command(self.compose+['logs','--no-color','postgres']).stdout
        sql=defaultdict(list);worker=defaultdict(list)
        for label,milliseconds in re.findall(r'lake-apply-profile\|([^|\s]+)\|([0-9.]+)',server_log):sql[label].append(float(milliseconds))
        for line in worker_log.splitlines():
            try:event=json.loads(line)
            except json.JSONDecodeError:continue
            if event.get('event')=='query_timing':
                worker[event['operation']+'/'+event['phase']].append(int(event['elapsed_us'])/1000)
        def summarize(groups):
            return {label:dict(count=len(values),total_ms=sum(values),mean_ms=sum(values)/len(values),maximum_ms=max(values)) for label,values in sorted(groups.items())}
        self.evidence['phase_profile']=dict(sql=summarize(sql),worker=summarize(worker),
            scope='Elapsed wall times, not CPU. SQL wrappers are nested within publish_batch/statement; do not sum overlapping layers. Split phases add round trips and log overhead, so this is diagnosis, not a performance comparison. Initial-copy/setup events may be present.')
        self.check('SQL profile covers every operation',all(label in sql for label in ['stage','prepare','finish','apply/insert','apply/update','apply/delete']),True)
        self.check('worker profile captures publication commit',bool(worker.get('publish_batch/commit')),True)


if __name__=='__main__':sys.exit(PublicationProfile().run())

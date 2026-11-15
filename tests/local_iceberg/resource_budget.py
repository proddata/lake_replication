#!/usr/bin/env python3
"""Execute local mirroring under explicit container CPU/RAM qualification caps."""
import json
import sys
from probe import LocalIcebergProbe,ROOT
from session import Session


class LocalBudgetProbe(LocalIcebergProbe):
    def __init__(self):
        super().__init__();self.compose+=['-f',str(ROOT/'deploy/local/limits.compose.yml')]
        self.evidence.update(scope='single-instance local mirroring under a bounded private CPU/RAM fixture',
            unqualified=['production sizing','no-OOM guarantee for arbitrary transactions','host-wide resource isolation'])

    def resources(self,service):
        code="from pathlib import Path;import json;p=Path('/sys/fs/cgroup');print(json.dumps(dict(memory_max=p.joinpath('memory.max').read_text().strip(),memory_peak=p.joinpath('memory.peak').read_text().strip(),cpu_max=p.joinpath('cpu.max').read_text().strip(),memory_events=dict(x.split() for x in p.joinpath('memory.events').read_text().splitlines()))))"
        return json.loads(self.command(self.compose+['exec','-T',service,'python3','-c',code]).stdout)

    def exercise(self):
        self.setup(mirror=False,seed=False)
        self.sql('ALTER DATABASE capture SET lake_replication.publish_interval_ms=1000;ALTER DATABASE capture SET pg_lake_table.target_row_group_size_mb=16')
        self.evidence['writer_configuration']=dict(query_memory_limit='768MB',query_threads=1,target_row_group_size_mb=16,journal_max_transactions=1)
        self.start_subscription();self.wait_initial_copy()
        self.sql('UPDATE lake_replication.journal_limits SET max_transactions=1')
        before={s:self.resources(s) for s in ['postgres','pgduck']}
        for service,values in before.items():
            expected=536870912 if service=='postgres' else 1073741824
            self.check(service+': memory cgroup enforces the fixture budget',int(values['memory_max']),expected)
            quota,period=values['cpu_max'].split()
            self.check(service+': CPU cgroup enforces one core',int(quota),int(period))
        writer=Session(self.sql_args())
        try:
            for transaction in range(100):
                low=transaction*10+1;high=low+9
                writer.query(f"BEGIN;INSERT INTO a SELECT i,repeat('x',256) FROM generate_series({low},{high}) i;INSERT INTO b SELECT i,repeat('y',256) FROM generate_series({low},{high}) i;COMMIT")
        finally:writer.close()
        self.check('bounded local flow publishes every source transaction',self.wait("SELECT count(*) FROM lake_replication.transactions WHERE state='published'",'100',timeout=120),'100')
        self.check('bounded local runtime returns to healthy',self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'healthy'),'healthy')
        # Observe publication through PostgreSQL while writers are active. A
        # concurrent Iceberg scan needs its own query-memory budget.
        self.sql('ALTER SUBSCRIPTION local_iceberg DISABLE')
        self.wait("SELECT health_state FROM lake_replication.subscription_status WHERE subscription_name='local_iceberg'",'stopped')
        self.wait('SELECT count(*) FROM iceberg_mirror.a','1000');self.wait('SELECT count(*) FROM iceberg_mirror.b','1000');self.equal()
        after={s:self.resources(s) for s in before}
        for service,values in after.items():
            self.check(service+': fixture completes without an OOM kill',int(values['memory_events'].get('oom_kill',0))-int(before[service]['memory_events'].get('oom_kill',0)),0)
            self.check(service+': recorded peak stays within its memory cap',int(values['memory_peak'])<=int(values['memory_max']),True)
        self.evidence['resource_cgroups']=dict(before=before,after=after)
        self.sql('DROP SUBSCRIPTION local_iceberg')
        self.check('resource-bounded cleanup releases the local slot',self.wait("SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'",'0'),'0')


if __name__=='__main__':sys.exit(LocalBudgetProbe().run())

"""Deterministic isolation and frontier checks for the disposable split overlay."""


def run(q, c):
    database='checkpoint_split_test'
    q.sql('destination','analytics',f'CREATE DATABASE {database}')
    q.sql('destination',database,'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    def sql(statement):return q.sql('destination',database,statement)
    sid=int(sql("SELECT lake_replication.create_subscription(42,1,1,'split_probe','split_probe_slot',ARRAY['pub'])"))
    sql(f'INSERT INTO lake_replication.journal_limits(subscription_id,max_transactions) VALUES({sid},2)')
    def stage(number,xid=None):
        return f"SELECT lake_replication.try_stage_transaction({sid},{xid or number},'0/0'::pg_lsn+{number},'0/0'::pg_lsn+{number},'0/0'::pg_lsn+{number},'2026-01-01'::timestamptz,'[]'::jsonb)::text"
    def checkpoint():
        return sql(f"SELECT staged_lsn||':'||published_lsn||':'||acknowledged_lsn FROM lake_replication.checkpoints WHERE subscription_id={sid}")
    def bounded(statement):
        return q.sql_result('destination',database,"SET lock_timeout='250ms'; "+statement,check_result=False)
    owner=c.Psql('destination',database)
    try:
        q.check('split stages first transaction',sql(stage(1)),'true')
        owner.query('BEGIN')
        batch=owner.query(f"SELECT lake_replication.prepare_batch({sid},'split-1','0/1','0/1')")[0]
        staged=bounded(stage(2))
        q.check('split stages during held publication',staged.returncode==0 and staged.stdout.strip().endswith('true'),True)
        q.check('split acknowledges during held publication',bounded(f"SELECT lake_replication.mark_acknowledged({sid},'0/1')").returncode,0)
        q.check('split respects capacity during uncommitted publication',sql(stage(3)),'false')
        second=bounded(f"SELECT lake_replication.prepare_batch({sid},'split-2','0/2','0/2')")
        q.check('split serializes competing publishers',second.returncode!=0 and 'lock timeout' in second.stderr,True)
        q.check('split hides uncommitted publication frontier',checkpoint(),'0/2:0/0:0/1')
        owner.query('ROLLBACK')
        q.check('split rollback preserves staged and acknowledged progress',checkpoint(),'0/2:0/0:0/1')
        q.check('split rollback restores publishing journal state',sql("SELECT count(*) FROM lake_replication.transactions WHERE state='staged'"),'2')
        owner.query('BEGIN')
        batch=owner.query(f"SELECT lake_replication.prepare_batch({sid},'split-1','0/1','0/1')")[0]
        owner.query(f'SELECT lake_replication.finish_batch({batch})')
        q.check('split does not release journal capacity before publication commit',sql(stage(3)),'false')
        q.check('split keeps completed batch and frontier invisible before commit',checkpoint(),'0/2:0/0:0/1')
        owner.query('COMMIT')
        q.check('split commits publication frontier with journal state',checkpoint(),'0/2:0/1:0/1')
        q.check('split releases capacity after publication commit',sql(stage(3)),'true')
        q.check('split replay remains accepted at capacity',sql(stage(3)),'true')
        rejected=bounded(stage(3,999))
        q.check('split rejects conflicting replay at capacity',rejected.returncode!=0 and 'conflicting replay' in rejected.stderr,True)
        sql(f"SELECT lake_replication.mark_acknowledged({sid},'0/3')")
        q.check('split preserves independent monotonic frontiers',checkpoint(),'0/3:0/1:0/3')
        rejected=bounded(f"SELECT lake_replication.prepare_batch({sid},'future','0/4','0/4')")
        q.check('split rejects publication beyond durable staged frontier',rejected.returncode!=0 and 'exceeds staged LSN' in rejected.stderr,True)
        rejected=bounded(f"SELECT lake_replication.mark_acknowledged({sid},'0/4')")
        q.check('split rejects acknowledgement beyond durable staging',rejected.returncode!=0 and 'exceeds staged LSN' in rejected.stderr,True)
    finally:
        owner.close()
        q.sql('destination','analytics',f'DROP DATABASE {database} WITH (FORCE)')

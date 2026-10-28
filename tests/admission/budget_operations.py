"""Live diagnostic-budget invariants; runs before and resets the measured state."""
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

T="'2026-01-01 00:00:00+00'::timestamptz"


def run(budget,c):
    q=budget.q
    def sql(text,database='analytics'):
        return q.sql('destination',database,text)
    def reset():
        sql('TRUNCATE staging_budget.credit')
    def reserve(amount,when=T):
        return f'SELECT staging_budget.reserve_at({amount},{when})'
    try:
        sql('UPDATE staging_budget.settings SET rate_bytes_per_second=1024,burst_bytes=1024')
        reset()
        q.check('shared budget permits one initial burst',float(sql(reserve(1024))),0.0)
        q.check('shared budget accounts first queued debt',float(sql(reserve(512))),0.5)
        q.check('shared budget accumulates global debt',float(sql(reserve(512))),1.0)
        sql('CREATE DATABASE budget_other')
        sql('CREATE EXTENSION dblink','budget_other')
        remote="SELECT * FROM dblink('dbname=analytics user=postgres', $$"+reserve(512)+"$$) AS t(delay double precision)"
        q.check('another database consumes the same budget',float(sql(remote,'budget_other')),1.5)
        before=sql('SELECT reserved_bytes||\':\'||requests FROM staging_budget.credit')
        for label,amount in [('oversized request',1025),('excess reservation horizon',1024),('zero request',0)]:
            failed=q.sql_result('destination','analytics',reserve(amount),check_result=False)
            q.check(label+' fails closed',failed.returncode!=0,True)
            q.check(label+' leaves accounting unchanged',sql('SELECT reserved_bytes||\':\'||requests FROM staging_budget.credit'),before)
        sql('BEGIN; '+reserve(256)+'; ROLLBACK')
        q.check('budget transaction rollback restores credit',sql('SELECT reserved_bytes||\':\'||requests FROM staging_budget.credit'),before)
        owner=c.Psql('destination','analytics')
        try:
            owner.query('BEGIN; SELECT 1 FROM staging_budget.credit FOR UPDATE')
            failed=q.sql_result('destination','analytics',"SET lock_timeout='100ms'; "+reserve(1),check_result=False)
            q.check('budget contention supports lock timeout',failed.returncode!=0 and 'lock timeout' in failed.stderr,True)
            owner.query('ROLLBACK')
        finally:
            owner.close()
        q.check('abandoned credit refills without explicit release',float(sql(reserve(1024,T+"+interval '3 seconds'"))),0.0)
        reset()
        sql(reserve(1024))
        q.check('backward clock grants no extra credit',float(sql(reserve(512,T+"-interval '1 second'"))),0.5)
        q.check('forward clock restores at most one burst',float(sql(reserve(1024,T+"+interval '1 day'"))),0.0)
        q.check('forward clock cannot create unlimited credit',float(sql(reserve(1024,T+"+interval '1 day'"))),1.0)
        reset()
        clients=[c.Psql('destination','analytics') for _ in range(4)]
        try:
            with ThreadPoolExecutor(max_workers=4) as pool:
                delays=list(pool.map(lambda client:float(client.query(reserve(512))[0]),clients))
            q.check('concurrent requests share one aggregate rate',sorted(delays),[0.0,0.0,0.5,1.0])
            q.check('concurrent accounting loses no bytes',sql("SELECT reserved_bytes||':'||requests FROM staging_budget.credit"),'2048:4')
        finally:
            for client in clients:client.close()
        sql('DROP DATABASE budget_other')
        cancellation(budget,c)
    finally:
        sql(f'UPDATE staging_budget.settings SET rate_bytes_per_second={budget.rate},burst_bytes={budget.burst}')
        reset()


def cancellation(budget,c):
    """Cancel a native initial-sync worker after reservation, during Rust sleep."""
    q=budget.q
    def sql(text,db='analytics'):
        return q.sql('destination',db,text)
    # Keep the ordinary native stop/retry path; the source dataset is tiny.
    q.sql('source','application',"CREATE TABLE budget_cancel_src(id bigint PRIMARY KEY); INSERT INTO budget_cancel_src SELECT generate_series(1,10); CREATE PUBLICATION budget_cancel_pub FOR TABLE budget_cancel_src; GRANT SELECT ON budget_cancel_src TO lake_source")
    sql('CREATE DATABASE budget_cancel')
    sql('CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication','budget_cancel')
    sql('UPDATE staging_budget.settings SET rate_bytes_per_second=1048576,burst_bytes=262144; TRUNCATE staging_budget.credit')
    sql('SELECT staging_budget.reserve(1)')
    owner=c.Psql('destination','analytics')
    try:
        owner.query('BEGIN; SELECT 1 FROM staging_budget.credit FOR UPDATE')
        connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
        sql(f"CREATE SUBSCRIPTION budget_cancel_sub CONNECTION '{connection}' PUBLICATION budget_cancel_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')",'budget_cancel')
        query="SELECT count(*) FROM pg_stat_activity WHERE application_name='diagnostic_staging_budget' AND wait_event_type='Lock'"
        q.check('native copy reaches the shared budget',q.wait_for('destination','analytics',query,'1',timeout=30),'1')
        owner.query("UPDATE staging_budget.credit SET available_bytes=-1310720,refilled_at=clock_timestamp(); COMMIT")
        q.check('native copy reserves before staging',q.wait_for('destination','analytics','SELECT requests FROM staging_budget.credit','2',timeout=3),'2')
        sql('ALTER SUBSCRIPTION budget_cancel_sub DISABLE','budget_cancel')
        q.check('disable interrupts budget sleep',q.wait_for('destination','budget_cancel',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'",'0',timeout=5),'0')
        q.check('disabled budget waiter stages no rows',sql('SELECT count(*) FROM lake_replication.initial_copy_rows','budget_cancel'),'0')
        q.check('disabled budget worker releases coordinator connection',q.wait_for('destination','analytics',"SELECT count(*) FROM pg_stat_activity WHERE application_name='diagnostic_staging_budget'",'0',timeout=5),'0')
        sql(f'UPDATE staging_budget.settings SET rate_bytes_per_second={budget.rate},burst_bytes={budget.burst}; TRUNCATE staging_budget.credit')
        sql('ALTER SUBSCRIPTION budget_cancel_sub ENABLE','budget_cancel')
        q.check('budgeted native copy resumes',q.wait_for('destination','budget_cancel',"SELECT state FROM lake_replication.subscriptions WHERE subscription_name='budget_cancel_sub'",'streaming',timeout=30),'streaming')
        q.check('budgeted retry preserves exact data',sql('SELECT count(*)||\':\'||sum(id) FROM budget_cancel_src','budget_cancel'),'10:55')
        sql('DROP SUBSCRIPTION budget_cancel_sub','budget_cancel')
        q.check('budgeted drop removes source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='budget_cancel_sub'"),'0')
        sql('DROP EXTENSION lake_replication CASCADE','budget_cancel')
        sql('DROP DATABASE budget_cancel WITH (FORCE)')
        q.sql('source','application','DROP PUBLICATION budget_cancel_pub; DROP TABLE budget_cancel_src')
    finally:
        owner.close()

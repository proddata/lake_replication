"""Live permit cleanup and native initial-copy lifecycle checks."""
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote


def run(gate,c):
    q=gate.q
    def sql(text,database='analytics'):return q.sql('destination',database,text)
    def count(state):return sql("SELECT count(*) FROM staging_gate.activity WHERE state='"+state+"'")
    def wait(state,number):return q.wait_for('destination','analytics',"SELECT count(*) FROM staging_gate.activity WHERE state='"+state+"'",str(number),timeout=10)
    q.sql('destination','analytics','CREATE DATABASE gate_other')
    gate.install('gate_other',q.sql)
    owner=waiter=bulk=stream=None
    try:
        owner=c.Psql('destination','analytics')
        owner.query('BEGIN; SAVEPOINT before_gate; SELECT staging_gate.admit(); SELECT staging_gate.admit()')
        q.check('repeated staging admission uses one permit',count('admitted'),'1')
        owner.query('ROLLBACK TO before_gate')
        q.check('savepoint rollback releases newly acquired staging permit',count('admitted'),'0')
        owner.query('SELECT staging_gate.admit(); SAVEPOINT nested; SELECT staging_gate.admit(); ROLLBACK TO nested')
        q.check('nested rollback preserves outer staging permit',count('admitted'),'1')
        denied=q.sql_result('destination','gate_other',"SET lock_timeout='100ms'; SELECT staging_gate.admit()",check_result=False)
        q.check('cross-database staging lock timeout propagates',denied.returncode!=0 and 'lock timeout' in denied.stderr,True)
        q.check('timeout clears only the staging waiter',count('waiting'),'0')
        waiter=c.Psql('destination','gate_other');waiter.query('BEGIN');pid=waiter.query('SELECT pg_backend_pid()')[0]
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(waiter.query,'SELECT staging_gate.admit()');wait('waiting',1)
            q.check('staging permit spans destination databases',sql("SELECT count(DISTINCT database_name) FROM staging_gate.activity"),'2')
            sql(f'SELECT pg_cancel_backend({pid})')
            try:pending.result(timeout=10)
            except RuntimeError:
                waiter.error_reader.join(timeout=2)
                q.check('staging waiter cancellation propagates','canceling statement' in ''.join(waiter.errors),True)
            else:raise AssertionError('staging waiter survived cancellation')
        waiter.close();waiter=None
        q.check('cancellation leaves the original holder',count('admitted'),'1')
        q.check('cancelled staging waiter leaves no lock',count('waiting'),'0')
        bulk=c.Psql('destination','analytics');bulk.query('BEGIN; SELECT lake_replication.admit_bulk()')
        stream=c.Psql('destination','analytics');stream.query('BEGIN; SELECT lake_replication.admit_stream()')
        q.check('staging is independent from bulk and streaming permits',
            sql("SELECT (SELECT count(*) FROM staging_gate.activity WHERE state='admitted')||':'||(SELECT count(*) FROM lake_replication.bulk_admission WHERE admission_state='admitted')||':'||(SELECT count(*) FROM lake_replication.stream_admission WHERE admission_state='admitted')"),'1:1:1')
        bulk.close();bulk=None;stream.close();stream=None
        owner.query('ROLLBACK')
        q.check('transaction rollback releases staging permit',count('admitted'),'0')
        waiter=c.Psql('destination','gate_other');waiter.query('BEGIN; SELECT staging_gate.admit()')
        pid=waiter.query('SELECT pg_backend_pid()')[0];sql(f'SELECT pg_terminate_backend({pid})')
        q.check('backend termination releases staging permit',wait('admitted',0),'0')
        waiter.close();waiter=None
    finally:
        for connection in [owner,waiter,bulk,stream]:
            if connection:connection.close()
    sql('DROP DATABASE gate_other WITH (FORCE)')
    native(gate,c)


def native(gate,c):
    q=gate.q
    def src(text):return q.sql('source','application',text)
    def dst(database,text):return q.sql('destination',database,text)
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    src('CREATE TABLE gate_src(id bigint PRIMARY KEY); INSERT INTO gate_src SELECT generate_series(1,10); CREATE PUBLICATION gate_pub FOR TABLE gate_src; GRANT SELECT ON gate_src TO lake_source')
    def setup(database):
        dst('analytics','CREATE DATABASE '+database)
        dst(database,'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    def create(database,sub):
        dst(database,f"CREATE SUBSCRIPTION {sub} CONNECTION '{connection}' PUBLICATION gate_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    def drain(database,sub):
        dst(database,'ALTER SUBSCRIPTION '+sub+' DISABLE')
        return q.wait_for('destination',database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'",'0',timeout=5)
    def complete(database):
        q.check(database+' resumes initial copy',q.wait_for('destination',database,"SELECT state FROM lake_replication.subscriptions",'streaming',timeout=45),'streaming')
        q.check(database+' preserves exact initial data',dst(database,"SELECT count(*)||':'||sum(id) FROM gate_src"),'10:55')
    def drop(database,sub):
        dst(database,'DROP SUBSCRIPTION '+sub)
        q.check(database+' removes source slot',src("SELECT count(*) FROM pg_replication_slots WHERE slot_name='"+sub+"'"),'0')
        dst(database,'DROP EXTENSION lake_replication CASCADE')
        dst('analytics','DROP DATABASE '+database+' WITH (FORCE)')
    owner=None
    try:
        setup('gate_copy')
        owner=c.Psql('destination','analytics');owner.query('BEGIN; SELECT staging_gate.admit()')
        create('gate_copy','gate_copy_sub')
        wait="SELECT count(*) FROM staging_gate.activity WHERE state='waiting' AND database_name='gate_copy'"
        q.check('native initial copy waits on staging permit',q.wait_for('destination','analytics',wait,'1',timeout=30),'1')
        q.check('waiting native copy stages no rows',dst('gate_copy','SELECT count(*) FROM lake_replication.initial_copy_rows'),'0')
        q.check('disable reaps native staging waiter',drain('gate_copy','gate_copy_sub'),'0')
        q.check('disable removes native staging wait',q.wait_for('destination','analytics',wait,'0',timeout=5),'0')
        q.check('disable preserves unrelated permit owner',dst('analytics',"SELECT count(*) FROM staging_gate.activity WHERE state='admitted'"),'1')
        owner.query('ROLLBACK');owner.close();owner=None
        dst('gate_copy','ALTER SUBSCRIPTION gate_copy_sub ENABLE');complete('gate_copy')
        q.check('only successful append commits its gate accounting',dst('gate_copy','SELECT chunks FROM staging_gate.counters'),'1')
        drop('gate_copy','gate_copy_sub')

        setup('gate_active')
        dst('gate_active',"CREATE FUNCTION staging_gate.hold_row() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(1280460112,72); RETURN NEW; END$$; CREATE TRIGGER gate_pause BEFORE INSERT ON lake_replication.initial_copy_rows FOR EACH ROW EXECUTE FUNCTION staging_gate.hold_row()")
        owner=c.Psql('destination','gate_active');owner.query('BEGIN; SELECT pg_advisory_xact_lock(1280460112,72)')
        create('gate_active','gate_active_sub')
        holder="SELECT pid FROM staging_gate.activity WHERE state='admitted' AND database_name='gate_active'"
        ready="SELECT EXISTS(SELECT 1 FROM staging_gate.activity WHERE state='admitted' AND database_name='gate_active' AND wait_event_type='Lock')::text"
        q.check('native append holds permit while insert is blocked',q.wait_for('destination','analytics',ready,'true',timeout=30),'true')
        pid=dst('analytics',holder)
        q.check('uncommitted gate accounting is invisible',dst('gate_active','SELECT chunks FROM staging_gate.counters'),'0')
        q.check('disable reaps native permit holder',drain('gate_active','gate_active_sub'),'0')
        q.check('disconnected append releases its staging permit',q.wait_for('destination','analytics',"SELECT count(*) FROM staging_gate.activity",'0',timeout=5),'0')
        q.check('disconnected append backend exits',q.wait_for('destination','analytics',f'SELECT count(*) FROM pg_stat_activity WHERE pid={pid}','0',timeout=5),'0')
        q.check('disconnected append rolls back accounting',dst('gate_active','SELECT chunks FROM staging_gate.counters'),'0')
        q.check('disconnected append rolls back staged rows',dst('gate_active','SELECT count(*) FROM lake_replication.initial_copy_rows'),'0')
        owner.query('ROLLBACK');owner.close();owner=None
        dst('gate_active','DROP TRIGGER gate_pause ON lake_replication.initial_copy_rows')
        dst('gate_active','ALTER SUBSCRIPTION gate_active_sub ENABLE');complete('gate_active')
        q.check('retried append commits one counter update',dst('gate_active','SELECT chunks FROM staging_gate.counters'),'1')
        drop('gate_active','gate_active_sub')
        src('DROP PUBLICATION gate_pub; DROP TABLE gate_src')
    finally:
        if owner:owner.close()

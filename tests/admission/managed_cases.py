"""Artifact-only managed staging, journal admission, and stop/generation races."""
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote


def run(q,c):
    def sql(text,db='analytics'):return q.sql('destination',db,text)
    q.check('admin can admit and inspect staging',sql("SELECT has_function_privilege('lake_replication_admin','lake_replication.admit_staging()','EXECUTE') AND has_table_privilege('lake_replication_admin','lake_replication.staging_admission','SELECT')"),'t')
    denied=q.sql_result('destination','analytics','SET ROLE pg_read_all_data; SELECT lake_replication.admit_staging()',check_result=False)
    q.check('read-only role cannot acquire staging permit',denied.returncode!=0,True)
    q.sql('source','application','CREATE TABLE managed_src(id bigint PRIMARY KEY); CREATE PUBLICATION managed_pub FOR TABLE managed_src; GRANT SELECT ON managed_src TO lake_source')
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    sql('CREATE DATABASE managed_test');sql('CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication','managed_test')
    sql(f"CREATE SUBSCRIPTION managed_sub CONNECTION '{connection}' PUBLICATION managed_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')",'managed_test')
    q.check('managed fixture starts streaming',q.wait_for('destination','managed_test',"SELECT state FROM lake_replication.subscriptions",'streaming'),'streaming')
    q.check('managed defaults install count and byte policy',sql("SELECT max_transactions||':'||max_payload_bytes FROM lake_replication.journal_limits",'managed_test'),'8:67108864')
    sql('ALTER SUBSCRIPTION managed_sub DISABLE','managed_test')
    q.wait_for('destination','managed_test',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'",'0')
    sid=int(sql('SELECT subscription_id FROM lake_replication.subscriptions','managed_test'))
    base=sql('SELECT staged_lsn FROM lake_replication.checkpoints','managed_test')
    def stage(n,try_api=True,xid=None):
        function='try_stage_transaction' if try_api else 'stage_transaction'
        return f"SELECT lake_replication.{function}({sid},{xid or 900000+n},'{base}'::pg_lsn+{n},'{base}'::pg_lsn+{n},'{base}'::pg_lsn+{n},'2026-01-01'::timestamptz,'[]'::jsonb)::text"
    sql('UPDATE lake_replication.journal_limits SET max_transactions=2,max_payload_bytes=1000','managed_test')
    q.check('first journal reservation succeeds',sql(stage(1),'managed_test'),'true')
    q.check('second journal reservation succeeds',sql(stage(2),'managed_test'),'true')
    frontier=sql('SELECT staged_lsn FROM lake_replication.checkpoints','managed_test')
    q.check('full journal returns retry without consuming commit',sql(stage(3),'managed_test'),'false')
    q.check('full journal leaves checkpoint unchanged',sql('SELECT staged_lsn FROM lake_replication.checkpoints','managed_test'),frontier)
    rejected=q.sql_result('destination','managed_test',stage(3,False),check_result=False)
    q.check('legacy staging API cannot bypass managed bound',rejected.returncode!=0 and 'journal admission full' in rejected.stderr,True)
    a=c.Psql('destination','managed_test');b=c.Psql('destination','managed_test')
    try:
        a.query('BEGIN');a.query(stage(3))
        q.check('retry releases checkpoint lock before caller sleeps',b.query("SET lock_timeout='100ms'; UPDATE lake_replication.checkpoints SET updated_at=clock_timestamp(); SELECT 'ok'")[-1],'ok')
        a.query('ROLLBACK')
        sql("UPDATE lake_replication.transactions SET state='published',published_at=clock_timestamp() WHERE xid=900001",'managed_test')
        a.query('BEGIN');q.check('one newly free slot is reserved',a.query(stage(3))[0],'true')
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(b.query,stage(4))
            a.query('COMMIT')
            q.check('concurrent producer sees committed capacity',pending.result(timeout=10)[0],'false')
        q.check('full-journal replay remains idempotent',sql(stage(3),'managed_test'),'true')
        rejected=q.sql_result('destination','managed_test',stage(3,xid=1234567),check_result=False)
        q.check('conflicting replay remains an error at capacity',rejected.returncode!=0 and 'conflicting replay' in rejected.stderr,True)
        sql("UPDATE lake_replication.transactions SET state='published',published_at=clock_timestamp(); UPDATE lake_replication.journal_limits SET max_transactions=10,max_payload_bytes=3",'managed_test')
        q.check('first two-byte payload fits byte allowance',sql(stage(5),'managed_test'),'true')
        q.check('byte bound applies independently from count',sql(stage(6),'managed_test'),'false')
        sql('UPDATE lake_replication.journal_limits SET max_payload_bytes=1','managed_test')
        rejected=q.sql_result('destination','managed_test',stage(7),check_result=False)
        q.check('oversized transaction fails without endless admission retry',rejected.returncode!=0 and 'exceeds journal byte limit' in rejected.stderr,True)
        q.check('journal view reports normalized payload bytes',sql("SELECT pending_transactions||':'||pending_payload_bytes FROM lake_replication.journal_admission",'managed_test'),'1:2')
    finally:a.close();b.close()
    sql("UPDATE lake_replication.transactions SET state='published'; UPDATE lake_replication.journal_limits SET max_transactions=1,max_payload_bytes=1024",'managed_test')
    sql(stage(8),'managed_test')
    sql(f"SELECT lake_replication.begin_stream({sid},900009,'{base}'::pg_lsn+9)",'managed_test')
    relation=sql("SELECT remote_relation_id||':'||schema_version FROM lake_replication.relations LIMIT 1",'managed_test').split(':')
    change='[{"remote_relation_id":'+relation[0]+',"relation_schema_version":'+relation[1]+',"operation":"insert","new_tuple":{"id":"9"}}]'
    sql(f"SELECT lake_replication.append_stream_segment({sid},900009,'{change}'::jsonb)",'managed_test')
    commit=f"SELECT lake_replication.try_commit_stream({sid},900009,'{base}'::pg_lsn+9,'{base}'::pg_lsn+9,'2026-01-01'::timestamptz)::text"
    q.check('streamed commit returns NULL at capacity',sql(commit,'managed_test'),'')
    q.check('streamed admission retry preserves durable segments',sql('SELECT count(*) FROM lake_replication.streamed_changes','managed_test'),'1')
    sql("UPDATE lake_replication.transactions SET state='published'",'managed_test')
    q.check('streamed commit succeeds when capacity returns',sql(commit,'managed_test'),'1')
    q.check('accepted streamed commit clears segment state',sql('SELECT count(*) FROM lake_replication.streamed_transactions','managed_test'),'0')
    sql('DROP SUBSCRIPTION managed_sub','managed_test');sql('DROP EXTENSION lake_replication CASCADE','managed_test');sql('DROP DATABASE managed_test WITH (FORCE)')
    native(q,c,connection)
    staging(q,c,connection)
    q.sql('source','application','DROP PUBLICATION managed_pub; DROP TABLE managed_src')


def native(q,c,connection):
    def sql(text,db='managed_native'):return q.sql('destination',db,text)
    q.sql('destination','analytics','CREATE DATABASE managed_native')
    sql('CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    sql(f"CREATE SUBSCRIPTION managed_native_sub CONNECTION '{connection}' PUBLICATION managed_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    q.wait_for('destination','managed_native',"SELECT state FROM lake_replication.subscriptions",'streaming')
    sid=int(sql('SELECT subscription_id FROM lake_replication.subscriptions'))
    generation=int(sql('SELECT config_generation FROM lake_replication.worker_launch_config'))
    control=c.Psql('destination','managed_native')
    try:
        control.query('BEGIN; UPDATE lake_replication.service_status SET heartbeat_at=clock_timestamp()')
        admitted=q.sql_result('destination','managed_native',f'SELECT lake_replication.assert_worker({sid},{generation})',check_result=False)
        q.check('worker fence coexists with an uncommitted heartbeat',admitted.returncode,0)
        control.query('ROLLBACK; BEGIN; SELECT 1 FROM lake_replication.service_status FOR UPDATE')
        rejected=q.sql_result('destination','managed_native',f'SELECT lake_replication.assert_worker({sid},{generation})',check_result=False)
        q.check('stop control lock refuses new worker admission',rejected.returncode!=0 and 'could not obtain lock' in rejected.stderr,True)
        control.query('ROLLBACK; BEGIN; UPDATE lake_replication.worker_launch_config SET config_generation=config_generation+1')
        rejected=q.sql_result('destination','managed_native',f'SELECT lake_replication.assert_worker({sid},{generation})',check_result=False)
        q.check('reconfiguration lock refuses new worker admission',rejected.returncode!=0 and 'could not obtain lock' in rejected.stderr,True)
        control.query('ROLLBACK')
    finally:control.close()
    holders=[]
    try:
        for _ in range(4):
            holder=c.Psql('destination','analytics');holders.append(holder);holder.query('BEGIN; SELECT lake_replication.admit_stream()')
        q.sql('source','application','INSERT INTO managed_src VALUES(1)')
        q.check('publisher waits behind engine slots with managed identity',q.wait_for('destination','analytics',"SELECT EXISTS(SELECT 1 FROM lake_replication.stream_admission WHERE database_name='managed_native' AND admission_state='waiting_for_stream')::text",'true',timeout=20),'true')
        for number in range(2,13):q.sql('source','application',f'INSERT INTO managed_src VALUES({number})')
        q.check('native receiver stops at eight unpublished transactions',q.wait_for('destination','managed_native',"SELECT pending_transactions FROM lake_replication.journal_admission",'8',timeout=15),'8')
        frontier=sql('SELECT staged_lsn FROM lake_replication.checkpoints')
        q.check('native admission wait preserves durable source feedback',q.wait_for('source','application',"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='managed_native_sub'",frontier,timeout=5),frontier)
        sql('ALTER SUBSCRIPTION managed_native_sub DISABLE')
        q.check('stop cancels publisher before waiting on its fence',q.wait_for('destination','analytics',"SELECT count(*) FROM lake_replication.stream_admission WHERE database_name='managed_native'",'0',timeout=5),'0')
        q.check('native supervisor drains with publisher blocked',q.wait_for('destination','managed_native',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'",'0',timeout=5),'0')
        stale=f"SET lake_replication.worker_generation='{generation}'; SET lake_replication.worker_subscription='{sid}'; UPDATE lake_replication.initial_copies SET rows_copied=rows_copied+1"
        rejected=q.sql_result('destination','managed_native',stale,check_result=False)
        q.check('stopped worker cannot mutate copy state',rejected.returncode!=0 and 'stopped or obsolete' in rejected.stderr,True)
    finally:
        for holder in holders:holder.close()
    sql('ALTER SUBSCRIPTION managed_native_sub ENABLE')
    q.check('enable advances worker generation',int(sql('SELECT config_generation FROM lake_replication.worker_launch_config'))>generation,True)
    q.check('resume publishes retained committed source data',q.wait_for('destination','managed_native','SELECT count(*) FROM managed_src','12',timeout=30),'12')
    rejected=q.sql_result('destination','managed_native',stale,check_result=False)
    q.check('old generation remains fenced after re-enable',rejected.returncode!=0 and 'stopped or obsolete' in rejected.stderr,True)
    q.sql('source','application','INSERT INTO managed_src VALUES(13)')
    q.check('replacement receiver continues exact streaming',q.wait_for('destination','managed_native',"SELECT count(*)||':'||sum(id) FROM managed_src",'13:91',timeout=30),'13:91')
    q.check('long-lived receiver reports live cycle counters',q.wait_for('destination','managed_native',"SELECT (receiver_cycles>0 AND publisher_cycles>0)::text FROM lake_replication.service_status",'true',timeout=5),'true')
    q.check('managed byte accounting tracks each payload',sql('SELECT bool_and(journal_payload_bytes=octet_length(payload::text)) FROM lake_replication.transactions'),'t')
    sql('DROP SUBSCRIPTION managed_native_sub')
    q.check('managed drop removes source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='managed_native_sub'"),'0')
    sql('DROP EXTENSION lake_replication CASCADE');q.sql('destination','analytics','DROP DATABASE managed_native WITH (FORCE)')


def staging(q,c,connection):
    """Real initial-sync backends release global admission on worker stop."""
    q.sql('source','application','TRUNCATE managed_src; INSERT INTO managed_src SELECT generate_series(1,10)')
    staging_rows=getattr(q,'STAGING_ROWS','initial_copy_rows')
    for held,crash in [(False,False),(True,False),(False,True),(True,True)]:
        database=('managed_copy_crash_' if crash else 'managed_copy_')+('held' if held else 'wait')
        sub=database+'_sub'
        q.sql('destination','analytics','CREATE DATABASE '+database)
        def sql(text):return q.sql('destination',database,text)
        sql('CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
        owner=c.Psql('destination',database if held else 'analytics')
        paused=None
        try:
            if held:
                sql("CREATE FUNCTION public.hold_copy() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(1280460112,72); RETURN NEW; END$$; CREATE TRIGGER hold_copy BEFORE INSERT ON lake_replication."+staging_rows+" FOR EACH ROW EXECUTE FUNCTION public.hold_copy()")
                owner.query('BEGIN; SELECT pg_advisory_xact_lock(1280460112,72)')
            else:owner.query('BEGIN; SELECT lake_replication.admit_staging()')
            sql(f"CREATE SUBSCRIPTION {sub} CONNECTION '{connection}' PUBLICATION managed_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
            condition=("admission_state='admitted' AND wait_event_type='Lock'" if held else "admission_state='waiting_for_staging'")
            query="SELECT EXISTS(SELECT 1 FROM lake_replication.staging_admission WHERE database_name='"+database+"' AND "+condition+")::text"
            q.check(database+' reaches real staging permit',q.wait_for('destination','analytics',query,'true',timeout=30),'true')
            if crash:
                # Freeze supervision so a replacement cannot hide socket cleanup.
                backend=sql("SELECT pid FROM lake_replication.staging_admission WHERE database_name=current_database()")
                paused=sql('SELECT supervisor_pid FROM lake_replication.service_status')
                assert backend.isdigit() and paused.isdigit()
                q.compose('exec','-T','destination','kill','-STOP',paused)
                child=q.compose('exec','-T','destination','pgrep','-f','^/usr/local/bin/lake-replication-initial-sync$').stdout.strip()
                q.check(database+' locates native initial-sync child',child.isdigit(),True)
                q.compose('exec','-T','destination','kill','-KILL',child)
                q.check(database+' detects abrupt peer disconnect',q.wait_for('destination','analytics',f'SELECT count(*) FROM pg_stat_activity WHERE pid={backend}','0',timeout=5),'0')
            else:sql('ALTER SUBSCRIPTION '+sub+' DISABLE')
            q.check(database+' releases disconnected staging backend',q.wait_for('destination','analytics',"SELECT count(*) FROM lake_replication.staging_admission WHERE database_name='"+database+"'",'0',timeout=5),'0')
            q.check(database+' leaves no partial copy rows',sql('SELECT count(*) FROM lake_replication.initial_copy_rows'),'0')
            if not crash:q.wait_for('destination',database,"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'",'0',timeout=5)
            owner.query('ROLLBACK');owner.close();owner=None
            if held:sql('DROP TRIGGER hold_copy ON lake_replication.'+staging_rows)
            if crash:
                q.compose('exec','-T','destination','kill','-CONT',paused);paused=None
            else:sql('ALTER SUBSCRIPTION '+sub+' ENABLE')
            q.check(database+' resumes initial copy',q.wait_for('destination',database,"SELECT state FROM lake_replication.subscriptions",'streaming',timeout=30),'streaming')
            q.check(database+' copies exact data',sql("SELECT count(*)||':'||sum(id) FROM managed_src"),'10:55')
            sql('DROP SUBSCRIPTION '+sub)
            q.check(database+' removes source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='"+sub+"'"),'0')
            sql('DROP EXTENSION lake_replication CASCADE');q.sql('destination','analytics','DROP DATABASE '+database+' WITH (FORCE)')
        finally:
            if paused:q.compose('exec','-T','destination','kill','-CONT',paused,check_result=False)
            if owner:owner.close()

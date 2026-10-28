"""Live receiver diagnostic lifecycle checks; leave no subscriptions behind."""
import json
from urllib.parse import quote


def run(receiver,c):
    q=receiver.q
    def src(text):return q.sql('source','application',text)
    def dst(text):return q.sql('destination','receiver_ops',text)
    latest="(SELECT s FROM receiver_progress.snapshots() s WHERE s->>'database'='receiver_ops' ORDER BY (s->>'started_epoch')::numeric DESC LIMIT 1)"
    def observe():return json.loads(q.sql('destination','analytics','SELECT '+latest))
    def wait(condition,timeout=15):
        return q.wait_for('destination','analytics','SELECT coalesce(('+condition+'),false)::text','true',timeout=timeout)
    src('CREATE TABLE receiver_ops_src(id bigint PRIMARY KEY); CREATE PUBLICATION receiver_ops_pub FOR TABLE receiver_ops_src; GRANT SELECT ON receiver_ops_src TO lake_source')
    q.sql('destination','analytics','CREATE DATABASE receiver_ops')
    dst('CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    owner=source=None
    try:
        dst(f"CREATE SUBSCRIPTION receiver_ops_sub CONNECTION '{connection}' PUBLICATION receiver_ops_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        q.check('telemetry fixture reaches streaming',q.wait_for('destination','receiver_ops',"SELECT state FROM lake_replication.subscriptions",'streaming'),'streaming')
        wait(latest+' IS NOT NULL')
        before=observe()
        ticks=before['read_timeouts']+5
        q.check('quiet receiver records read timeouts',wait(f"({latest}->>'read_timeouts')::bigint>={ticks}"),'true')
        after=observe()
        q.check('quiet receiver retains durable frontier',after['durably_staged_lsn'],before['durably_staged_lsn'])
        q.check('timeout does not pretend to be a source message',after['messages']>before['messages'] or
            (after['last_message_age_at_snapshot_seconds'] is None and before['last_message_age_at_snapshot_seconds'] is None) or
            after['last_message_age_at_snapshot_seconds']>=before['last_message_age_at_snapshot_seconds'],True)
        src('CREATE TABLE receiver_unpublished(id bigint); INSERT INTO receiver_unpublished VALUES(1)')
        q.check('unpublished WAL creates no fixture rows',dst('SELECT count(*) FROM receiver_ops_src'),'0')
        source=c.Psql('source','application');source.query('BEGIN; INSERT INTO receiver_ops_src VALUES(999)')
        ticks=observe()['read_timeouts']+5
        wait(f"({latest}->>'read_timeouts')::bigint>={ticks}")
        q.check('open source transaction is not published',dst('SELECT count(*) FROM receiver_ops_src'),'0')
        source.query('ROLLBACK');source.close();source=None
        frontier=dst('SELECT staged_lsn FROM lake_replication.checkpoints')
        owner=c.Psql('destination','receiver_ops');owner.query('BEGIN; LOCK TABLE lake_replication.transactions IN ACCESS EXCLUSIVE MODE')
        ack=src("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='receiver_ops_sub'")
        src('INSERT INTO receiver_ops_src VALUES(1)')
        locked="SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname='receiver_ops' AND wait_event_type='Lock')::text"
        q.check('receiver can be observed blocked on staging',q.wait_for('destination','analytics',locked,'true',timeout=15),'true')
        q.check('blocked receiver snapshot becomes stale',wait(f"extract(epoch FROM clock_timestamp())-({latest}->>'snapshot_epoch')::numeric>1.5"),'true')
        blocked=observe()
        q.check('wire progress never advances diagnostic durable LSN',blocked['durably_staged_lsn'],frontier)
        q.check('observed source commit remains separate from durable frontier',blocked.get('observed_commit') is not None and blocked['observed_commit']['end_lsn']!=frontier,True)
        q.check('blocked receiver exposes pre-staging phase',blocked['phase'],'before_staging')
        q.check('blocked staging does not acknowledge new WAL',src("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='receiver_ops_sub'"),ack)
        filename='/worker-logs/receiver-progress-'+b'receiver_ops'.hex()+'-'+str(blocked['subscription_id'])+'.json'
        q.compose('exec','-T','destination','mv',filename,filename+'.hidden')
        try:
            q.check('missing telemetry produces no invented healthy record',q.sql('destination','analytics',"SELECT count(*) FROM receiver_progress.snapshots() s WHERE s->>'database'='receiver_ops'"),'0')
        finally:
            q.compose('exec','-T','destination','mv',filename+'.hidden',filename)
        # The control hook should interrupt the locked receiver before returning.
        dst('ALTER SUBSCRIPTION receiver_ops_sub DISABLE')
        q.check('disable reaps blocked telemetry receiver',q.wait_for('destination','receiver_ops',"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor'",'0',timeout=5),'0')
        old=observe()
        owner.query('ROLLBACK');owner.close();owner=None
        dst('ALTER SUBSCRIPTION receiver_ops_sub ENABLE')
        q.check('restart exposes a new receiver generation',wait(f"({latest}->>'started_epoch')::numeric>{old['started_epoch']}"),'true')
        q.check('receiver generations replace one subscription record',q.sql('destination','analytics',"SELECT count(*) FROM receiver_progress.snapshots() s WHERE s->>'database'='receiver_ops'"),'1')
        q.check('restart publishes exact committed source data',q.wait_for('destination','receiver_ops',"SELECT count(*)||':'||sum(id) FROM receiver_ops_src",'1:1',timeout=30),'1:1')
        dst('DROP SUBSCRIPTION receiver_ops_sub')
        q.check('telemetry fixture drop removes slot',src("SELECT count(*) FROM pg_replication_slots WHERE slot_name='receiver_ops_sub'"),'0')
        # Records persist intentionally; age/generation must be checked by readers.
        q.check('stopped receiver record remains explicit stale evidence',wait(f"extract(epoch FROM clock_timestamp())-({latest}->>'snapshot_epoch')::numeric>1.5"),'true')
        dst('DROP EXTENSION lake_replication CASCADE')
        q.sql('destination','analytics','DROP DATABASE receiver_ops WITH (FORCE)')
        src('DROP PUBLICATION receiver_ops_pub; DROP TABLE receiver_ops_src; DROP TABLE receiver_unpublished')
    finally:
        for connection in [owner,source]:
            if connection:connection.close()

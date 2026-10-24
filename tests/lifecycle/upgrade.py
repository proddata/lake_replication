"""Stopped-server beta.1 handoff assertions used by the packaged lifecycle probe.

The caller supplies the qualification helper as q and manages images and cleanup.
"""
import time
from urllib.parse import quote

q = None
TARGET_VERSION = '0.1.0-beta.3'
LAUNCHER_COUNT_SQL = "SELECT count(*) FROM extension_base.list_base_workers() w JOIN pg_extension e ON e.oid=w.extension_id WHERE e.extname='lake_replication' AND w.database_id=(SELECT oid FROM pg_database WHERE datname=current_database()) AND w.pid>0"


def launcher_count():
    return q.sql('destination', 'analytics', LAUNCHER_COUNT_SQL)


def contents(expected):
    statement = "SELECT string_agg(note,',' ORDER BY id) FROM public.upgrade_items"
    q.check(f'upgraded replication reaches {expected}', q.wait_for('destination', 'analytics', statement, expected), expected)


def upgrade_case(new_image):
    q.sql('source', 'application', "CREATE TABLE public.upgrade_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO public.upgrade_items VALUES(1,'one'); CREATE PUBLICATION prototype_upgrade_pub FOR TABLE public.upgrade_items; GRANT SELECT ON public.upgrade_items TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    q.sql('destination', 'analytics', f"CREATE SUBSCRIPTION prototype_upgrade_sub CONNECTION '{connection}' PUBLICATION prototype_upgrade_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    contents('one')
    q.check('packaged release starts through its static launcher', q.sql('destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication launcher'"), '1')
    q.check('packaged release has no replication base registration', q.sql('destination', 'analytics', "SELECT count(*) FROM extension_base.workers WHERE extension_name='lake_replication'"), '0')
    old_start = q.sql('destination', 'analytics', 'SELECT pg_postmaster_start_time()')
    supervisor = q.sql('destination', 'analytics', 'SELECT supervisor_pid FROM lake_replication.service_status')
    executable = q.compose('exec', '-T', '--user', 'postgres', 'destination', 'readlink', f'/proc/{supervisor}/exe').stdout.strip()
    q.check('preflight can inspect the real old supervisor executable', executable, '/usr/local/bin/lake-replication-supervisor')
    q.sql('destination', 'analytics', 'ALTER SUBSCRIPTION prototype_upgrade_sub DISABLE')
    q.check('old subscription stops before package replacement', q.wait_for('destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'", '0'), '0')
    q.check('old source slot is inactive before replacement', q.wait_for('source', 'application', "SELECT active::text FROM pg_replication_slots WHERE slot_name='prototype_upgrade_sub'", 'false'), 'false')
    deadline = time.monotonic() + 10
    while True:
        child_count = q.compose('exec', '-T', '--user', 'postgres', 'destination', 'sh', '-ec',
                                "for p in /proc/[0-9]*/exe; do readlink \"$p\" 2>/dev/null || true; done").stdout
        if '/usr/local/bin/lake-replication-' not in child_count or time.monotonic() >= deadline:
            break
        time.sleep(.25)
    q.check('old external worker executables are gone before replacement',
            '/usr/local/bin/lake-replication-' in child_count, False)
    q.check('old subscription drain does not restart PostgreSQL', q.sql('destination', 'analytics', 'SELECT pg_postmaster_start_time()'), old_start)
    checkpoint = q.sql('destination', 'analytics', 'SELECT published_lsn FROM lake_replication.checkpoints')
    q.EVIDENCE['checkpoint_before_upgrade'] = checkpoint
    before = q.sql('destination', 'analytics', 'SELECT pg_postmaster_start_time()')
    q.sql('source', 'application', "INSERT INTO public.upgrade_items VALUES(2,'two')")

    # Both servers using the destination binaries stop before image replacement.
    # Source/object storage and all private data volumes remain intact.
    q.compose('stop', 'destination', 'pgduck', timeout=120)
    q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = new_image
    q.compose('up', '-d', '--no-deps', 'pgduck', timeout=180)
    q.compose('up', '-d', '--no-deps', 'destination', timeout=180)
    q.check('new package starts the existing database', q.wait_for('destination', 'analytics', 'SELECT 1', '1', timeout=90), '1')
    q.check('package replacement used a server restart', q.sql('destination', 'analytics', 'SELECT pg_postmaster_start_time()') != before, True)
    q.check('old explicit preload setting is retained', q.sql('destination', 'analytics', 'SHOW shared_preload_libraries'), 'pg_extension_base,lake_replication')
    q.check('old single-database setting is retained', q.sql('destination', 'analytics', 'SHOW lake_replication.database_name'), 'analytics')
    q.check('old session preload setting is retained', q.sql('destination', 'analytics', 'SHOW session_preload_libraries'), 'lake_replication')
    q.check('new binary does not register the old static owner', q.sql('destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication launcher'"), '0')
    q.check('metadata stays on the old version until SQL upgrade', q.sql('destination', 'analytics', "SELECT extversion FROM pg_extension WHERE extname='lake_replication'"), '0.1.0-beta.1')
    q.check('subscription remains durably stopped during handoff', q.sql('destination', 'analytics', 'SELECT desired_state FROM lake_replication.worker_launch_config'), 'stopped')
    q.check('no base owner starts before registration', launcher_count(), '0')

    q.sql('destination', 'analytics', f"BEGIN; ALTER EXTENSION lake_replication UPDATE TO '{TARGET_VERSION}'; ROLLBACK")
    q.check('rolled-back upgrade preserves the old SQL version', q.sql('destination', 'analytics', "SELECT extversion FROM pg_extension WHERE extname='lake_replication'"), '0.1.0-beta.1')
    q.check('rolled-back upgrade leaves no durable base registration', q.sql('destination', 'analytics', "SELECT count(*) FROM extension_base.workers WHERE extension_name='lake_replication'"), '0')
    q.check('rolled-back upgrade leaves no base owner', q.wait_for('destination', 'analytics', LAUNCHER_COUNT_SQL, '0'), '0')
    q.check('rolled-back upgrade leaves no worker dispatch', q.wait_for('destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'", '0'), '0')
    contents('one')
    q.sql('source', 'application', "INSERT INTO public.upgrade_items VALUES(3,'three')")
    q.sql('destination', 'analytics', f"ALTER EXTENSION lake_replication UPDATE TO '{TARGET_VERSION}'")
    q.check('SQL upgrade commits the prototype version', q.sql('destination', 'analytics', "SELECT extversion FROM pg_extension WHERE extname='lake_replication'"), TARGET_VERSION)
    q.check('SQL upgrade records exactly one registration', q.sql('destination', 'analytics', "SELECT count(*) FROM extension_base.workers WHERE extension_name='lake_replication'"), '1')
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline and launcher_count() != '1':
        time.sleep(.25)
    q.check('committed upgrade starts exactly one base owner', launcher_count(), '1')
    q.check('committed upgrade keeps dispatch stopped until explicit enable', q.sql('destination', 'analytics', 'SELECT desired_state FROM lake_replication.worker_launch_config'), 'stopped')
    q.check('committed upgrade has no subscription wrapper before enable', q.sql('destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'"), '0')
    q.check('base dependency is explicit after real upgrade', q.sql('destination', 'analytics', "SELECT count(*) FROM pg_depend d JOIN pg_extension e ON e.oid=d.objid JOIN pg_extension r ON r.oid=d.refobjid WHERE d.classid='pg_extension'::regclass AND d.refclassid='pg_extension'::regclass AND e.extname='lake_replication' AND r.extname='pg_extension_base' AND d.deptype='n'"), '1')
    q.check('SQL upgrade preserves the replication checkpoint', q.sql('destination', 'analytics', 'SELECT published_lsn FROM lake_replication.checkpoints'), checkpoint)
    q.check('SQL upgrade preserves the completed copy', q.sql('destination', 'analytics', 'SELECT count(*) FROM lake_replication.initial_copies'), '1')
    q.sql('destination', 'analytics', 'ALTER SUBSCRIPTION prototype_upgrade_sub ENABLE')
    contents('one,two,three')
    q.sql('source', 'application', "INSERT INTO public.upgrade_items VALUES(4,'four')")
    contents('one,two,three,four')
    q.check('handoff retains exactly one source slot', q.sql('source', 'application', "SELECT count(*) FROM pg_replication_slots WHERE slot_name='prototype_upgrade_sub'"), '1')
    q.check('handoff does not repeat the initial copy', q.sql('destination', 'analytics', 'SELECT count(*) FROM lake_replication.initial_copies'), '1')
    q.compose('restart', 'destination', timeout=180)
    q.check('upgraded database restarts', q.wait_for('destination', 'analytics', 'SELECT 1', '1', timeout=90), '1')
    q.sql('source', 'application', "INSERT INTO public.upgrade_items VALUES(5,'five')")
    contents('one,two,three,four,five')
    q.check('restart keeps exactly one base owner', launcher_count(), '1')
    q.check('restart keeps the legacy owner absent', q.sql('destination', 'analytics', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication launcher'"), '0')
    q.check('post-upgrade rows are unique', q.sql('destination', 'analytics', 'SELECT count(*)::text||\':\'||count(DISTINCT id)::text FROM public.upgrade_items'), '5:5')
    q.sql('destination', 'analytics', 'DROP SUBSCRIPTION prototype_upgrade_sub')
    q.check('post-upgrade drop releases the retained source slot', q.sql('source', 'application', "SELECT count(*) FROM pg_replication_slots WHERE slot_name='prototype_upgrade_sub'"), '0')

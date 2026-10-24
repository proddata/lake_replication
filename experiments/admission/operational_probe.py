#!/usr/bin/env python3
"""Qualify bulk-only cancellation, schema contention and rebuild rollback."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import tempfile
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('reserved_probe', Path(__file__).with_name('reserved_probe.py'))
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
q, c, m = p.q, p.c, p.m
CLIENTS = []
ADMIT = 'lake_replication.experiment_admit_engine(true)'


def sql(statement):
    return q.sql('destination', 'analytics', statement)


def client():
    connection = c.Psql('destination', 'analytics')
    CLIENTS.append(connection)
    return connection


def wait(test, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = test()
        if result:
            return result
        time.sleep(.03)
    raise TimeoutError('expected operational state was not observed')


def column(name, kind):
    return dict(name=name, type_oid=20 if kind=='bigint' else 25,
                source_type=kind, not_null=False, generated=False)


def relation(subscription, columns, logical):
    descriptor = json.dumps([dict(name=x['name'], type_oid=x['type_oid']) for x in columns])
    version = sql(f"SELECT lake_replication.register_relation({subscription},100,'public','{logical}','d','{descriptor}'::jsonb)")
    contract = json.dumps(columns)
    sql(f"SELECT lake_replication.register_type_contract({subscription},100,{version},1,'{contract}'::jsonb)")
    return int(version)


def fixture(name, target=None, rows=2, logical=None):
    """SQL-contract fixture with real Iceberg data; no physical source-slot claim."""
    target = target or name
    logical = logical or target
    subscription = int(sql(f"SELECT lake_replication.create_subscription(42,1,1,'{name}','{name}',ARRAY['fixture_pub'])"))
    columns = [column('id','bigint'), column('payload','text')]
    version = relation(subscription, columns, logical)
    sql(f"SELECT lake_replication.ensure_iceberg_destination({subscription},100,{version},'public','{target}',true)")
    sql(f"SELECT lake_replication.begin_initial_copy({subscription},100,{version},'{target}'::regclass,'0/100','fixture_snapshot',false)")
    sql(f"""INSERT INTO lake_replication.initial_copy_rows
        SELECT {subscription},100,{version},n-1,jsonb_build_object('id',n,'payload','row-'||n)
        FROM generate_series(1,{rows}) n;
        UPDATE lake_replication.initial_copies SET rows_copied={rows} WHERE subscription_id={subscription};
        SELECT lake_replication.finish_initial_sync_group({subscription},1)""")
    return subscription, columns, logical


def pending_schema(name='schema_items'):
    subscription, columns, logical = fixture(name)
    version = relation(subscription, columns + [column('extra','text')], logical)
    return subscription, version


def schema_state(subscription):
    return sql(f"SELECT state||':'||(last_error IS NULL)::text FROM lake_replication.subscriptions WHERE subscription_id={subscription}")


def schema_case(result):
    subscription, version = pending_schema()
    owner = client()
    owner.query(f'BEGIN; SELECT {ADMIT}')
    attempted = q.sql_result('destination', 'analytics', f"SET lock_timeout='250ms'; SELECT lake_replication.reconcile_relation_schema({subscription},100,{version})", check_result=False)
    result['timeout_observation'] = dict(returncode=attempted.returncode,
        state=schema_state(subscription), response=attempted.stdout.strip(), error=attempted.stderr.strip())
    owner.query('ROLLBACK')
    q.check('queued schema timeout propagates as a retryable SQL error', attempted.returncode!=0 and 'lock timeout' in attempted.stderr, True)
    q.check('resource timeout does not durably pause the subscription', schema_state(subscription), 'streaming:true')
    q.check('timed-out DDL adds no column', sql("SELECT count(*) FROM pg_attribute WHERE attrelid='schema_items'::regclass AND attname='extra' AND NOT attisdropped"), '0')
    q.check('schema reconciliation succeeds after permit release', sql(f'SELECT lake_replication.reconcile_relation_schema({subscription},100,{version})'), 'ready')

    subscription, version = pending_schema('ddl_lock_items')
    owner.query('BEGIN; SELECT count(*) FROM ddl_lock_items')
    blocked = q.sql_result('destination', 'analytics', f"SET lock_timeout='250ms'; SELECT lake_replication.reconcile_relation_schema({subscription},100,{version})", check_result=False)
    owner.query('ROLLBACK')
    q.check('actual DDL relation-lock timeout propagates', blocked.returncode!=0 and 'lock timeout' in blocked.stderr, True)
    q.check('actual DDL contention does not durably pause metadata', schema_state(subscription), 'streaming:true')
    q.check('failed DDL releases the admission permit', sql(f'SELECT count(*) FROM {p.VIEW}'), '0')
    q.check('DDL retry succeeds after conflicting reader leaves', sql(f'SELECT lake_replication.reconcile_relation_schema({subscription},100,{version})'), 'ready')

    # Deliberately invert a control-row/permit order to test abort semantics.
    # The normal schema fast path must remain unqueued; a cycle must not turn
    # transient contention into a committed schema pause.
    subscription, version = pending_schema('deadlock_items')
    schema = client()
    schema.query("\\set VERBOSITY verbose\nSET deadlock_timeout='100ms'")
    schema_pid = schema.query('SELECT pg_backend_pid()')[0]
    owner.query(f"SET deadlock_timeout='10s'; BEGIN; SELECT {ADMIT}")
    monitor = client()
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(schema.query, f'SELECT lake_replication.reconcile_relation_schema({subscription},100,{version})')
        wait(lambda: monitor.query(f"SELECT count(*) FROM {p.VIEW} WHERE pid={schema_pid} AND admission_state='waiting_for_writer'")[0]=='1')
        control = pool.submit(owner.query, f'SELECT 1 FROM lake_replication.relations WHERE subscription_id={subscription} AND schema_version={version} FOR UPDATE')
        try:
            response = pending.result(timeout=15)
            result['deadlock_response'] = response
            aborted = False
        except RuntimeError as error:
            result['deadlock_error'] = str(error)
            aborted = '40P01' in str(error)
        control.result(timeout=15)
    owner.query('ROLLBACK')
    q.check('schema lock-order victim propagates SQLSTATE 40P01', aborted, True)
    q.check('deadlock abort preserves streaming metadata', schema_state(subscription), 'streaming:true')
    q.check('deadlock retry can perform the additive DDL', sql(f'SELECT lake_replication.reconcile_relation_schema({subscription},100,{version})'), 'ready')
    q.check('successful schema retry preserves exact original rows', sql("SELECT string_agg(id||':'||payload||':'||coalesce(extra,'NULL'),',' ORDER BY id) FROM deadlock_items"), '1:row-1:NULL,2:row-2:NULL')
    invalid = relation(subscription, [column('id','bigint'), column('payload','bigint'), column('extra','text')], 'deadlock_items')
    rejected = sql(f'SELECT lake_replication.reconcile_relation_schema({subscription},100,{invalid})')
    q.check('genuine incompatible source schema still pauses replication', rejected.startswith('paused:') and schema_state(subscription)=='paused:false', True)


def rebuild_case(result):
    old, _, _ = fixture('old_sub', target='rb_items', logical='rb_items')
    new, _, _ = fixture('new_sub', target='rb_items__rebuild', logical='rb_items', rows=3)
    sql(f"UPDATE lake_replication.subscriptions SET state='paused' WHERE subscription_id={old}")
    promote = f"SELECT lake_replication.promote_subscription_rebuild({old},{new},'public','__rebuild','__backup')"
    schema_sub, version = pending_schema()
    schema = client()
    rebuild = client()
    rebuild_pid = rebuild.query('SELECT pg_backend_pid()')[0]
    owner = client()
    owner.query(f'BEGIN; SELECT {ADMIT}')
    monitor = client()
    with ThreadPoolExecutor(max_workers=1) as pool:
        queued = pool.submit(rebuild.query, promote)
        wait(lambda: monitor.query(f"SELECT count(*) FROM {p.VIEW} WHERE pid={rebuild_pid} AND admission_state='waiting_for_writer'")[0]=='1')
        sql(f'SELECT pg_cancel_backend({rebuild_pid})')
        try:
            queued.result(timeout=10)
            canceled = False
        except RuntimeError as error:
            canceled = 'canceling statement' in str(error)
    owner.query('ROLLBACK')
    q.check('queued rebuild cancellation propagates', canceled, True)
    q.check('canceled rebuild preserves old subscription state', sql(f'SELECT state FROM lake_replication.subscriptions WHERE subscription_id={old}'), 'paused')
    q.check('canceled rebuild records no promotion', sql('SELECT count(*) FROM lake_replication.rebuild_promotions'), '0')
    rebuild = client()
    owner.query(f'BEGIN; SELECT {ADMIT}')
    with ThreadPoolExecutor(max_workers=2) as pool:
        schema_job = pool.submit(schema.query, f'SELECT lake_replication.reconcile_relation_schema({schema_sub},100,{version})')
        # Observe the schema waiter before submitting an explicit transaction
        # which will deliberately retain its permit for rollback inspection.
        wait(lambda: monitor.query(f"SELECT count(*) FROM {p.VIEW} WHERE admission_state='waiting_for_writer'")[0]=='1')
        rebuild_job = pool.submit(rebuild.query, 'BEGIN; '+promote)
        wait(lambda: monitor.query(f"SELECT count(*) FROM {p.VIEW} WHERE admission_state='waiting_for_writer'")[0]=='2')
        owner.query('COMMIT')
        q.check('concurrent independent schema operation completes', schema_job.result(timeout=20), ['ready'])
        q.check('concurrent rebuild performs one real rename group', rebuild_job.result(timeout=20), ['1'])
    q.check('rebuilt canonical rows are visible in owning transaction', rebuild.query('SELECT count(*) FROM rb_items'), ['3'])
    rebuild.query('ROLLBACK')
    q.check('rebuild rollback restores original table contents', sql('SELECT count(*) FROM rb_items'), '2')
    q.check('rebuild rollback preserves shadow table contents', sql('SELECT count(*) FROM rb_items__rebuild'), '3')
    q.check('rebuild rollback removes backup rename', sql("SELECT to_regclass('rb_items__backup') IS NULL"), 't')
    q.check('rebuild rollback removes promotion metadata', sql('SELECT count(*) FROM lake_replication.rebuild_promotions'), '0')
    q.check('rebuild retry commits exactly one promotion', sql(promote), '1')
    q.check('promotion commits canonical and retained backup data', sql('SELECT (SELECT count(*) FROM rb_items)||\':\'||(SELECT count(*) FROM rb_items__backup)'), '3:2')
    q.check('promotion disables the old subscription only', sql(f"SELECT o.state||':'||n.state FROM lake_replication.subscriptions o, lake_replication.subscriptions n WHERE o.subscription_id={old} AND n.subscription_id={new}"), 'disabled:streaming')
    q.check('independent schema changes survive rebuild rollback', sql(f'SELECT state FROM lake_replication.subscriptions WHERE subscription_id={schema_sub}'), 'streaming')


def cancel_case(result, rows):
    q.sql('source','application', f"CREATE TABLE cancel_items(id bigint PRIMARY KEY,payload text); INSERT INTO cancel_items SELECT n,repeat('x',128) FROM generate_series(1,{rows}) n; CREATE PUBLICATION cancel_pub FOR TABLE cancel_items; GRANT SELECT ON cancel_items TO lake_source")
    connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    original_start = sql('SELECT pg_postmaster_start_time()')
    sql(f"CREATE SUBSCRIPTION cancel_sub CONNECTION '{connection}' PUBLICATION cancel_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
    monitor = client()
    def executing_write():
        records = json.loads(monitor.query(f"SELECT coalesce(json_agg(t),'[]') FROM (SELECT a.pid,extract(epoch FROM a.query_start) AS began,a.wait_event_type,a.wait_event FROM pg_stat_activity a JOIN {p.VIEW} v USING(pid) WHERE a.query LIKE 'SELECT lake_replication.finish_initial_sync_group(%' AND a.state='active' AND a.wait_event_type='Extension' AND v.admission_state='admitted')t")[0])
        if not records:
            return None
        logs = q.compose('logs','--no-color','--timestamps','pgduck')
        for line in reversed(logs.stdout.splitlines()):
            operation = re.search(r'(?:sent query|executing command on DuckDB):.*?\b(COPY|INSERT)\b', line, re.I)
            timestamp = re.search(r'\d{4}-\d\d-\d\dT[0-9:.]+(?:Z|[+-]\d\d:\d\d)', line)
            if operation and timestamp:
                epoch = datetime.fromisoformat(timestamp[0].replace('Z','+00:00')).timestamp()
                if epoch >= float(records[0]['began']):
                    result['engine_write_observation'] = dict(operation=operation[1].upper(), logged_epoch=epoch, line_sha256=hashlib.sha256(line.encode()).hexdigest(), **records[0])
                    return records[0]['pid']
        return None
    victim = wait(executing_write, timeout=120)
    q.check('copy cancellation observes a real engine write request', bool(victim), True)
    canceled = sql(f"SELECT pg_cancel_backend({victim}); UPDATE lake_replication.worker_launch_config SET desired_state='stopped' WHERE subscription_id=(SELECT subscription_id FROM lake_replication.subscriptions WHERE subscription_name='cancel_sub')")
    q.check('active copy backend accepts cancellation', 't' in canceled.splitlines(), True)
    q.check('canceled subscription wrapper drains', q.wait_for('destination','analytics',p.s.WRAPPERS,'0',timeout=30), '0')
    q.check('cancellation releases its bulk permit', sql(f"SELECT count(*) FROM {p.VIEW}"), '0')
    q.check('canceled copy preserves all zero checkpoints', sql("SELECT (received_lsn='0/0' AND staged_lsn='0/0' AND published_lsn='0/0' AND acknowledged_lsn='0/0')::text FROM lake_replication.checkpoints"), 'true')
    q.check('canceled copy publishes no partial Iceberg rows', sql('SELECT count(*) FROM cancel_items'), '0')
    q.check('canceled native source slot remains inactive', q.wait_for('source','application',"SELECT active::text FROM pg_replication_slots WHERE slot_name='cancel_sub'",'false'), 'false')
    sql('ALTER SUBSCRIPTION cancel_sub ENABLE')
    q.check('native restart completes a fresh initial copy', q.wait_for('destination','analytics',"SELECT state FROM lake_replication.subscriptions WHERE subscription_name='cancel_sub'",'streaming',timeout=180), 'streaming')
    q.check('native copy recovery restores exact IDs and payloads', sql("SELECT count(*)||':'||count(DISTINCT id)||':'||min(id)||':'||max(id)||':'||bool_and(payload=repeat('x',128))::text FROM cancel_items"), f'{rows}:{rows}:1:{rows}:true')
    q.sql('source','application', f"INSERT INTO cancel_items VALUES({rows+1},repeat('x',128))")
    q.check('recovered copy continues live replication', q.wait_for('destination','analytics','SELECT count(*) FROM cancel_items',str(rows+1)), str(rows+1))
    q.check('copy cancellation preserves the PostgreSQL postmaster', sql('SELECT pg_postmaster_start_time()'), original_start)
    sql('DROP SUBSCRIPTION cancel_sub')
    q.check('recovered subscription drops its source slot', q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='cancel_sub'"), '0')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cases', nargs='+', choices=('schema','rebuild','cancel'), default=['schema','rebuild','cancel'])
    parser.add_argument('--rows', type=int, default=2000000)
    args = parser.parse_args()
    if q.VERSION != '0.1.0-beta.2':
        parser.error('historical beta.2 overlay: run with VERSION=0.1.0-beta.2; use tests/admission/probe.py for production packages')
    if args.rows < 500000:
        parser.error('use at least 500000 rows to observe real engine publication')
    q.RESULTS = ROOT/'.qualification'/(q.RUN+'_bulk_operations')
    root = q.RESULTS
    root.mkdir(parents=True)
    q.PROJECT += '-bulk-operations'
    q.SOURCE_IMAGE += '-bulk-operations'
    q.DESTINATION_IMAGE += '-bulk-operations'
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE, QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    result = dict(status='running',scope=__doc__,parameters=vars(args),cases=[],production_changed=False)
    inputs = [*Path(__file__).parent.glob('*'),ROOT/'tests/qualification/probe.py',ROOT/'extensions/lake_replication/lake_replication.c',*sorted((ROOT/'migrations').glob('*.sql'))]
    result['inputs'] = {str(x.relative_to(ROOT)): hashlib.sha256(x.read_bytes()).hexdigest() for x in inputs if x.is_file()}
    for name in result['inputs']:
        target=root/'sources'/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(ROOT/name,target)
    try:
        archive=q.archive_path();q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-bulk-operations-') as directory:
            q.build_images(archive,Path(directory))
        image='lake-replication-bulk-operations:'+q.RUN
        build=q.command(['docker','build','--build-arg',f'QUAL_IMAGE={q.DESTINATION_IMAGE}','--build-arg','RESERVED=1','--build-arg','BULK_ONLY=1','-t',image,'-f',str(Path(__file__).with_name('Dockerfile.reserved')),str(ROOT)],timeout=600)
        (root/'build.log').write_text(build.stdout+build.stderr)
        result['image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip()
        q.ENV['QUALIFICATION_DESTINATION_IMAGE']=image
        text=m.compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n','').replace('exec pgduck_server ','exec pgduck_server --debug ')
        for case in args.cases:
            directory=root/case;directory.mkdir();q.COMPOSE_FILE=directory/'compose.yml';q.COMPOSE_FILE.write_text(text)
            observed=dict(case=case,status='running')
            print('CASE',case,flush=True)
            try:
                q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
                if case=='cancel':cancel_case(observed,args.rows)
                elif case=='schema':schema_case(observed)
                else:rebuild_case(observed)
                observed['status']='passed'
            except AssertionError as error:
                observed.update(status='finding',error=str(error))
            except Exception as error:
                observed.update(status='error',error=str(error),error_type=type(error).__name__)
            finally:
                for connection in CLIENTS:connection.close()
                CLIENTS.clear()
                logs=q.compose('logs','--no-color',check_result=False)
                (directory/'compose.log').write_text(logs.stdout+logs.stderr)
                workers=q.compose('exec','-T','destination','sh','-ec',"find /worker-logs -type f -name '*.log' -exec cat {} +",check_result=False)
                (directory/'workers.jsonl').write_text(workers.stdout)
                cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
                observed['cleanup_returncode']=cleanup.returncode
                if cleanup.returncode:observed['status']='error'
                result['cases'].append(observed)
                result['checks']=q.EVIDENCE['checks']
                s=p.s;s.write_json(directory/'results.json',observed);s.write_json(root/'results.json',result)
                print('RESULT',case,observed['status'],observed.get('error',''),flush=True)
        result['status']='passed' if all(x['status']=='passed' for x in result['cases']) else 'completed_with_findings'
    except Exception as error:
        result.update(status='error',error=str(error));print('ERROR:',error,flush=True)
    finally:
        result['archive_provenance']=q.EVIDENCE
        p.s.write_json(root/'results.json',result)
        print('Evidence:',root,flush=True)
    return 0 if result['status']=='passed' else 1


if __name__=='__main__':
    raise SystemExit(main())

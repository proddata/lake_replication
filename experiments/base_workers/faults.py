#!/usr/bin/env python3
"""Crash ownership tests on fresh, private, archive-backed prototype fixtures."""
import argparse
import hashlib
import importlib.util
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('qualification', ROOT / 'tests/qualification/probe.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
CASES = ('supervisor-kill', 'supervisor-paused-kill', 'launcher-kill',
         'launcher-paused-children-kill', 'wrapper-kill', 'wrapper-paused-children-kill', 'slot-contention-timeout')
PROCESS_SCRIPT = '''
import json, os
from pathlib import Path
rows=[]
for p in Path('/proc').glob('[0-9]*'):
    try:
        s=(p/'stat').read_text().rsplit(')',1)[1].split()
        try: exe=os.readlink(p/'exe')
        except OSError: exe=''
        rows.append(dict(pid=int(p.name),state=s[0],ppid=int(s[1]),pgrp=int(s[2]),start=s[19],exe=exe))
    except (OSError,IndexError): pass
print(json.dumps(rows))
'''
LAUNCHER = "SELECT coalesce(max(w.pid),0) FROM extension_base.list_base_workers() w JOIN pg_extension e ON e.oid=w.extension_id WHERE e.extname='lake_replication' AND w.database_id=(SELECT oid FROM pg_database WHERE datname=current_database())"
WRAPPERS = "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'"


def processes():
    return json.loads(q.compose('exec', '-T', '--user', 'postgres', 'destination', 'python3', '-c', PROCESS_SCRIPT).stdout)


def signal(row, signum, group=False):
    # Refuse stale PIDs; group signaling is limited to the verified supervisor's group.
    script = '''import os,sys
pid,start,sig,group=sys.argv[1:]
s=open('/proc/'+pid+'/stat').read().rsplit(')',1)[1].split()
assert s[19]==start, 'PID identity changed'
if group=='1':
    assert int(s[2])==int(pid), 'not private group leader'
    os.killpg(int(pid),int(sig))
else: os.kill(int(pid),int(sig))
'''
    q.compose('exec', '-T', '--user', 'postgres', 'destination', 'python3', '-c', script,
              str(row['pid']), row['start'], str(signum), str(int(group)))


def logs():
    result = q.compose('logs', '--no-color', 'destination', check_result=False)
    return result.stdout + result.stderr


def sql(statement):
    return q.sql('destination', 'analytics', statement)


def ready():
    return q.wait_for('destination', 'analytics', 'SELECT 1', '1', timeout=30) == '1'



def persistent_contention(check, record):
    sql('ALTER SUBSCRIPTION fault_sub DISABLE')
    q.check('contention fixture drains its original wrapper', q.wait_for('destination', 'analytics', WRAPPERS, '0'), '0')
    q.check('contention fixture releases its source slot', q.wait_for('source', 'application', "SELECT active::text FROM pg_replication_slots WHERE slot_name='fault_sub'", 'false'), 'false')
    args = ['docker', 'compose', '-p', q.PROJECT, '-f', str(q.COMPOSE_FILE), 'exec', '-T', '--user', 'postgres', 'source',
            'pg_recvlogical', '-U', 'postgres', '-d', 'application', '--slot=fault_sub', '--start', '--file=/dev/null',
            '--no-loop', '--status-interval=0', '--fsync-interval=0', '-o', 'proto_version=1', '-o', 'publication_names=fault_pub']
    with (q.RESULTS / 'slot-holder.log').open('w') as output:
        holder = subprocess.Popen(args, stdout=output, stderr=output, env=q.ENV)
        try:
            q.check('independent holder owns source slot before enable', q.wait_for('source', 'application', "SELECT active::text FROM pg_replication_slots WHERE slot_name='fault_sub'", 'true', timeout=10), 'true')
            pid = q.sql('source', 'application', "SELECT active_pid FROM pg_replication_slots WHERE slot_name='fault_sub'")
            started = time.monotonic()
            sql('ALTER SUBSCRIPTION fault_sub ENABLE')
            state = q.wait_for('destination', 'analytics', 'SELECT state FROM lake_replication.subscriptions', 'error', timeout=15)
            elapsed = time.monotonic() - started
            record['contention_seconds_until_error'] = elapsed
            check('persistent contention reaches durable error', state, 'error')
            check('contention budget expires within 15 seconds', elapsed < 15, True)
            message = sql('SELECT last_source_error FROM lake_replication.service_status')
            record['contention_error'] = message
            check('error identifies expired contention budget', 'source slot contention' in message and '5000ms budget' in message, True)
            check('independent holder still owns the same active slot', q.sql('source', 'application', "SELECT active_pid FROM pg_replication_slots WHERE slot_name='fault_sub' AND active"), pid)
            check('refused contention leaves destination contents unchanged', sql("SELECT count(*)::text||':'||string_agg(note,',' ORDER BY id) FROM public.fault_items"), '1:one')
            check('refused contention does not repeat initial copy', sql('SELECT count(*) FROM lake_replication.initial_copies'), '1')
            sql('ALTER SUBSCRIPTION fault_sub DISABLE')
            check('disable retires the paused replacement', q.wait_for('destination', 'analytics', WRAPPERS, '0'), '0')
        finally:
            q.sql('source', 'application', "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='fault_sub' AND active_pid IS NOT NULL", check_result=False)
            try:
                holder.wait(timeout=10)
            except subprocess.TimeoutExpired:
                holder.terminate()
                holder.wait(timeout=5)


def case(name):
    record = {'case': name, 'checks': [], 'expected_postgresql_crash_recovery': name.startswith(('launcher', 'wrapper'))}
    q.EVIDENCE['cases'].append(record)
    def check(label, actual, expected):
        record['checks'].append({'name': label, 'actual': actual, 'expected': expected, 'passed': actual == expected})
        print(f"{'PASS' if actual == expected else 'FINDING'} {name}: {label}", flush=True)
    try:
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        q.sql('source', 'application', "CREATE TABLE public.fault_items(id bigint PRIMARY KEY,note text NOT NULL); INSERT INTO public.fault_items VALUES(1,'one'); CREATE PUBLICATION fault_pub FOR TABLE public.fault_items; GRANT SELECT ON public.fault_items TO lake_source")
        connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
        sql(f"CREATE SUBSCRIPTION fault_sub CONNECTION '{connection}' PUBLICATION fault_pub WITH (lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        q.check(name + ' completes initial copy', q.wait_for('destination', 'analytics', 'SELECT count(*) FROM public.fault_items', '1'), '1')
        if name == 'slot-contention-timeout':
            persistent_contention(check, record)
            record['passed'] = all(c['passed'] for c in record['checks'])
            return
        launcher = int(sql(LAUNCHER))
        wrapper = int(sql("SELECT pid FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'"))
        supervisor = int(sql('SELECT supervisor_pid FROM lake_replication.service_status'))
        before = processes()
        by_pid = {row['pid']: row for row in before}
        raw = by_pid[supervisor]
        assert raw['exe'] == '/usr/local/bin/lake-replication-supervisor'
        assert raw['ppid'] == wrapper and raw['pgrp'] == supervisor
        # Capture a real receiver in flight, then pause the whole group atomically
        # for paused cases. This preserves a descendant when its owner is killed.
        if 'paused' in name:
            deadline = time.monotonic() + 10
            while True:
                signal(raw, 19, group=True)
                owned = [row for row in processes() if row['pgrp'] == supervisor]
                if any(row['exe'].endswith('/lake-replication-receiver') and row['state'] == 'T' for row in owned):
                    break
                signal(raw, 18, group=True)
                if time.monotonic() > deadline:
                    raise AssertionError('could not pause a live receiver with its supervisor')
                time.sleep(.02)
        else:
            owned = [row for row in processes() if row['pgrp'] == supervisor]
        if 'paused' in name:
            q.check(name + ' really stops the external process group', all(row['state'] == 'T' for row in owned), True)
            q.check(name + ' includes a receiver descendant', any(row['exe'].endswith('/lake-replication-receiver') for row in owned), True)
        record.update(launcher=by_pid[launcher], wrapper=by_pid[wrapper], owned_before=owned)
        log_before = logs()
        target = raw if name.startswith('supervisor') else by_pid[launcher if name.startswith('launcher') else wrapper]
        record['target'] = target
        signal(target, 9)
        # Observe first, without manually resuming or cleaning up the killed owner's children.
        time.sleep(5)
        after = processes()
        old = {(row['pid'], row['start']) for row in owned}
        retained = [row for row in after if (row['pid'], row['start']) in old]
        record['old_children_after_5_seconds'] = retained
        check('all old external processes are reaped', retained, [])
        is_ready = ready()
        check('database accepts connections after crash', is_ready, True)
        if is_ready:
            q.sql('source', 'application', "INSERT INTO public.fault_items VALUES(2,'two')")
            actual = q.wait_for('destination', 'analytics', "SELECT count(*)::text||':'||count(DISTINCT id)::text||':'||coalesce(string_agg(note,',' ORDER BY id),'') FROM public.fault_items", '2:2:one,two', timeout=30)
            check('replication recovers with unique correct rows', actual, '2:2:one,two')
            check('exactly one wrapper after recovery', sql(WRAPPERS), '1')
            check('exactly one live launcher after recovery', sql("SELECT count(*) FROM pg_stat_activity WHERE pid=(" + LAUNCHER + ")"), '1')
            check('initial copy is not repeated', sql('SELECT count(*) FROM lake_replication.initial_copies'), '1')
            check('source slot becomes active after recovery', q.wait_for('source', 'application', "SELECT count(*) FROM pg_replication_slots WHERE slot_name='fault_sub' AND active", '1', timeout=5), '1')
        delta = logs().removeprefix(log_before)
        record['crash_recovery_events'] = {phrase: delta.count(phrase) for phrase in
                                          ('terminating any other active server processes', 'reinitializing', 'database system is ready to accept connections', 'untracked child process')}
        crash_count = record['crash_recovery_events']['terminating any other active server processes']
        check('PostgreSQL crash recovery matches worker type', crash_count, 0 if name.startswith('supervisor') else 1)
        check('no unexpected untracked children reach postmaster', record['crash_recovery_events']['untracked child process'], 0)
        final = processes()
        record['external_processes_after_recovery'] = [row for row in final if '/lake-replication-' in row['exe']]
        check('exactly one external supervisor after recovery', sum(row['exe'].endswith('/lake-replication-supervisor') for row in final), 1)
        if is_ready:
            record['subscription_status'] = sql('SELECT row_to_json(s) FROM lake_replication.subscription_status s')
            record['source_slot_status'] = q.sql('source', 'application', "SELECT row_to_json(s) FROM pg_replication_slots s WHERE slot_name='fault_sub'")
        if is_ready and actual != '2:2:one,two':
            sql('ALTER SUBSCRIPTION fault_sub DISABLE')
            q.wait_for('destination', 'analytics', WRAPPERS, '0', timeout=10)
            sql('ALTER SUBSCRIPTION fault_sub ENABLE')
            retried = q.wait_for('destination', 'analytics', "SELECT count(*)::text||':'||count(DISTINCT id)::text||':'||coalesce(string_agg(note,',' ORDER BY id),'') FROM public.fault_items", '2:2:one,two', timeout=30)
            check('explicit disable/enable recovers correct rows after automatic failure', retried, '2:2:one,two')
        # Shutdown also must reap anything created after recovery.
        if is_ready:
            sql('ALTER SUBSCRIPTION fault_sub DISABLE')
            q.wait_for('destination', 'analytics', WRAPPERS, '0', timeout=10)
            time.sleep(1)
            residue = [row for row in processes() if '/lake-replication-' in row['exe'] or (row['pid'], row['start']) in old]
            record['remaining_after_disable'] = residue
            check('disable removes all external workers including old generation', residue, [])
    except Exception as error:
        record['error'] = str(error)
        print(f'ERROR {name}: {error}', flush=True)
    finally:
        (q.RESULTS / (name + '.log')).write_text(logs())
        worker = q.compose('exec', '-T', 'destination', 'sh', '-ec', "find /worker-logs -maxdepth 1 -type f -name '*.log' -exec cat {} +", check_result=False)
        (q.RESULTS / (name + '.jsonl')).write_text(worker.stdout)
        record['worker_failure_events'] = []
        for line in worker.stdout.splitlines():
            try:
                event = json.loads(line)
                if event.get('level') in ('ERROR', 'WARN', 'WARNING'):
                    record['worker_failure_events'].append(event)
            except json.JSONDecodeError:
                pass
        source_logs = q.compose('logs', '--no-color', 'source', check_result=False)
        (q.RESULTS / (name + '-source.log')).write_text(source_logs.stdout + source_logs.stderr)
        # A fresh fixture per case prevents one failure from contaminating the next.
        cleanup = q.compose('down', '-v', '--remove-orphans', check_result=False, timeout=120)
        if cleanup.returncode:
            raise RuntimeError('fixture cleanup failed: ' + cleanup.stderr)
    record['passed'] = not record.get('error') and all(c['passed'] for c in record['checks'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cases', nargs='+', choices=CASES, default=CASES)
    parser.add_argument('--with-init', action='store_true', help='Use Docker init to isolate orphan reaping from PostgreSQL PID 1.')
    parser.add_argument('--recovery', action='store_true', help='Overlay the bounded Rust contention-retry experiment.')
    parser.add_argument('--binding', action='store_true', help='Overlay origin-tuple dispatch and cleanup checks; implies --recovery.')
    args = parser.parse_args()
    if args.binding:
        args.recovery = True
    q.RESULTS = ROOT / '.qualification' / (q.RUN + '_faults')
    q.RESULTS.mkdir(parents=True, exist_ok=False)
    q.EVIDENCE.update(experiment='forced crash process ownership', production_changed=False, cases=[], docker_init=args.with_init, recovery_adapter=args.recovery, binding_adapter=args.binding,
                      limits=['Linux arm64 private fixtures; not a full physical clone/takeover qualification.',
                              '5-second orphan and recovery endpoint observations are not continuous ownership proofs.'])
    names = ('faults.py', 'adapt.py', 'runtime.c.inc', 'Dockerfile', 'register.sql', 'destination-init.sql', 'recovery.py', 'recovery.rs.inc', 'Dockerfile.recovery', 'binding.py', 'binding.sql', 'Dockerfile.binding')
    q.EVIDENCE['source_fingerprints'] = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() for name in names}
    sources = q.RESULTS / 'sources'
    sources.mkdir()
    for name in names:
        shutil.copy2(Path(__file__).with_name(name), sources / name)
    q.EVIDENCE['worker_source_fingerprints'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((ROOT / 'worker').rglob('*.rs'))}
    q.EVIDENCE['cargo_lock_sha256'] = hashlib.sha256((ROOT / 'Cargo.lock').read_bytes()).hexdigest()
    q.EVIDENCE['qualification_helper_sha256'] = hashlib.sha256(Path(q.__file__).read_bytes()).hexdigest()
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        q.command(['git', 'diff', '--exit-code', q.EVIDENCE['source_revision'], '--', 'worker', 'migrations', 'extensions/lake_replication'])
        with tempfile.TemporaryDirectory(prefix='lake-faults-') as temporary:
            q.build_images(archive, Path(temporary))
        image = f'lake-replication-fault-prototype:{q.RUN}'
        build = q.command(['docker', 'build', '--build-arg', f'QUAL_IMAGE={q.DESTINATION_IMAGE}', '--build-arg', 'PROTOTYPE_HARDENED=1', '--tag', image, '--file', str(Path(__file__).with_name('Dockerfile')), str(ROOT)], timeout=600)
        (q.RESULTS / 'build.log').write_text(build.stdout + build.stderr)
        if args.recovery:
            original_image = image
            image = f'lake-replication-recovery-prototype:{q.RUN}'
            build = q.command(['docker', 'build', '--build-arg', f'QUAL_IMAGE={original_image}', '--tag', image, '--file', str(Path(__file__).with_name('Dockerfile.recovery')), str(ROOT)], timeout=900)
            (q.RESULTS / 'recovery-build.log').write_text(build.stdout + build.stderr)
        if args.binding:
            parent = image
            image = f'lake-replication-binding-crash-prototype:{q.RUN}'
            build = q.command(['docker','build','--build-arg',f'QUAL_IMAGE={parent}','--tag',image,'--file',str(Path(__file__).with_name('Dockerfile.binding')),str(ROOT)],timeout=600)
            (q.RESULTS / 'binding-build.log').write_text(build.stdout + build.stderr)
        q.EVIDENCE['image_id'] = q.command(['docker', 'image', 'inspect', '--format', '{{.Id}}', image]).stdout.strip()
        q.ENV['QUALIFICATION_DESTINATION_IMAGE'] = image
        text = q.COMPOSE_FILE.read_text().replace('shared_preload_libraries=pg_extension_base,lake_replication', 'shared_preload_libraries=pg_extension_base').replace('      - -c\n      - lake_replication.database_name=analytics\n', '')
        if args.with_init:
            text = text.replace('  destination:\n', '  destination:\n    init: true\n')
        q.COMPOSE_FILE = q.RESULTS / 'compose.yml'
        q.COMPOSE_FILE.write_text(text)
        for name in args.cases:
            case(name)
        q.EVIDENCE.update(passed=all(c['passed'] for c in q.EVIDENCE['cases']), status='completed')
    except Exception as error:
        q.EVIDENCE.update(passed=False, status='error', error=str(error))
        print(f'ERROR: {error}', flush=True)
    finally:
        cleanup = q.compose('down', '-v', '--remove-orphans', check_result=False, timeout=120)
        if cleanup.returncode:
            q.EVIDENCE.update(passed=False, status='cleanup_error', cleanup_error=cleanup.stderr)
        (q.RESULTS / 'results.json').write_text(json.dumps(q.EVIDENCE, indent=2, sort_keys=True) + '\n')
        print(f'Evidence: {q.RESULTS}', flush=True)
    return 0 if q.EVIDENCE.get('passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())

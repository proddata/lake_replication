#!/usr/bin/env python3
"""Exercise an unchanged CI archive and the supported pg_lake runtime over time."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import threading
import time
from urllib.parse import quote

from model import Progress, qualifies_soak, schedule, validate_events

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('soak_shared', ROOT / 'experiments/admission/compare.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
q = c.q

SNAPSHOT = """SELECT jsonb_build_object('seq',(SELECT seq FROM soak_progress WHERE id=1),
 'items',(SELECT coalesce(jsonb_agg(jsonb_build_array(id,revision,payload) ORDER BY id),'[]') FROM soak_items))"""


def stop_process(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


class Client(c.Psql):
    def query(self, statement, timeout=10):
        return super().query(statement, timeout=timeout)


class Producer:
    def __init__(self, interval):
        self.interval = interval
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.progress = Progress()
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        client = None
        try:
            client = Client('source', 'application')
            client.query("SET statement_timeout='10s'")
            self.ready.set()
            while not self.stop.is_set():
                began = time.monotonic()
                with self.lock:
                    n = self.progress.produced + 1
                    base = n * 16 % 1024
                    rows = client.query(f"""BEGIN;
INSERT INTO soak_items SELECT ({base}+g)%1024,{n},'insert-{n}-'||g||repeat('x',64)
 FROM generate_series(0,15) g WHERE true ON CONFLICT(id) DO UPDATE SET revision=EXCLUDED.revision,payload=EXCLUDED.payload;
UPDATE soak_items SET payload='update-{n}-'||id WHERE id IN (SELECT ({base}+g)%1024 FROM generate_series(0,7) g);
DELETE FROM soak_items WHERE id=({base}+15)%1024;
UPDATE soak_progress SET seq={n} WHERE id=1; COMMIT; SELECT seq FROM soak_progress WHERE id=1""")
                    if rows != [str(n)]:
                        raise ValueError('source did not confirm the expected committed sequence')
                    self.progress.commit(n, time.monotonic())
                self.stop.wait(max(0, self.interval - (time.monotonic() - began)))
        except Exception as error:
            self.error = str(error)
        finally:
            self.ready.set()
            if client:
                try:
                    client.close()
                except Exception as error:
                    self.error = self.error or str(error)


class Runner:
    def __init__(self, args):
        self.args = args
        self.producer = Producer(args.interval_seconds)
        self.begin = None
        self.wall = None
        self.postmaster = None
        self.storage_up = True
        self.events = self.awake = None
        self.expected_deaths = dict(source=0, destination=0, pgduck=0, storage=0)
        self.result = q.EVIDENCE
        self.result.update(status='preparing', scope=__doc__, parameters=vars(args), faults=[], checkpoints=[],
                           qualifies_24_hour_soak=False, resource_samples=0, max_memory_bytes={},
                           archive_overlay=False, timing_scope='monotonic; visibility upper bounds include observer polling and faults')

    def persist(self):
        if self.begin is not None:
            self.result['elapsed_seconds'] = time.monotonic() - self.begin
        self.result['source_transactions'] = self.producer.progress.produced
        path = q.RESULTS / 'results.json'
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(self.result, indent=2) + '\n')
        temporary.replace(path)

    def append(self, name, data):
        with (q.RESULTS / name).open('a') as output:
            output.write(json.dumps(data, sort_keys=True) + '\n')

    def sql(self, service, statement, timeout=12):
        database = 'application' if service == 'source' else 'analytics'
        return q.compose('exec', '-T', service, 'psql', '-XAtq', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres',
                         '-d', database, input="SET statement_timeout='8s'; " + statement, timeout=timeout).stdout.strip()

    def wait(self, service, statement, expected, timeout=60):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            if self.producer.error:
                raise RuntimeError('source producer: ' + self.producer.error)
            try:
                last = self.sql(service, statement)
                if time.monotonic() > deadline:
                    break
                if last == expected:
                    return
            except (RuntimeError, subprocess.TimeoutExpired):
                pass
            time.sleep(.25)
        raise TimeoutError('recovery did not reach expected state: ' + str(last))

    def checkpoint(self, name):
        began = time.monotonic()
        with self.producer.lock:
            if self.producer.error:
                raise RuntimeError('source producer: ' + self.producer.error)
            expected = json.loads(self.sql('source', SNAPSHOT))
            self.wait('destination', 'SELECT seq FROM soak_progress WHERE id=1', str(expected['seq']), self.args.catchup_seconds)
            actual = json.loads(self.sql('destination', SNAPSHOT))
            if actual != expected:
                (q.RESULTS / 'mismatch-source.json').write_text(json.dumps(expected))
                (q.RESULTS / 'mismatch-destination.json').write_text(json.dumps(actual))
            q.check(name + ' exact rows and committed sequence', actual == expected, True)
            self.producer.progress.observe(actual['seq'], time.monotonic())
            wrappers = self.sql('destination', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'")
            q.check(name + ' exactly one supervisor wrapper', wrappers, '1')
            self.result['checkpoints'].append(dict(name=name, sequence=actual['seq'], rows=len(actual['items']),
                data_sha256=hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest(),
                elapsed_seconds=time.monotonic() - self.begin, catchup_seconds=time.monotonic() - began))
        self.persist()

    def sample(self, fault=None):
        now = time.monotonic()
        if abs((time.time() - self.wall) - (now - self.begin)) > 1:
            raise RuntimeError('wall clock differs from monotonic soak duration')
        if self.producer.error:
            raise RuntimeError('source producer: ' + self.producer.error)
        if self.events.poll() is not None:
            raise RuntimeError('Docker event recorder exited during the soak')
        row = dict(elapsed_seconds=now - self.begin, fault=fault, services={})
        if not fault:
            metrics = json.loads(self.sql('destination', """SELECT jsonb_build_object(
 'sequence',(SELECT seq FROM soak_progress WHERE id=1), 'service',to_jsonb(s), 'journal',to_jsonb(j),
 'postmaster',pg_postmaster_start_time(), 'database_bytes',pg_database_size(current_database()),
 'checkpoint_order_valid',p.published_lsn<=p.staged_lsn AND p.acknowledged_lsn<=p.staged_lsn AND p.staged_lsn<=p.received_lsn)
 FROM lake_replication.service_status s JOIN lake_replication.journal_admission j USING(subscription_id)
 JOIN lake_replication.checkpoints p USING(subscription_id)"""))
            if not metrics['checkpoint_order_valid']:
                raise ValueError('replication checkpoint order violated')
            if metrics['postmaster'] != self.postmaster:
                raise ValueError('unexpected PostgreSQL restart')
            journal = metrics['journal']
            if journal['pending_transactions'] > journal['max_transactions'] or journal['pending_payload_bytes'] > journal['max_payload_bytes']:
                raise ValueError('journal admission bounds exceeded')
            with self.producer.lock:
                progress = self.producer.progress.observe(metrics['sequence'], time.monotonic())
            if progress['oldest_pending_seconds'] > self.args.catchup_seconds:
                raise ValueError('steady replication backlog exceeded catch-up budget')
            row.update(metrics=metrics, progress=progress)
        for service in ('destination', 'pgduck'):
            resources = json.loads(q.compose('exec', '-T', '--user', 'postgres', service, 'python3',
                '-c', (ROOT / 'experiments/admission/sample.py').read_text(), '', timeout=15).stdout)
            self.result['max_memory_bytes'][service] = max(self.result['max_memory_bytes'].get(service, 0), resources['memory_current_bytes'])
            if any(int(line.split()[1]) for line in resources['cgroup']['memory.events'].splitlines() if line.split()[0] in ('oom', 'oom_kill')):
                raise ValueError(service + ' cgroup reported OOM')
            if min(value['available_bytes'] for value in resources['filesystems'].values()) < 1024**3:
                raise ValueError('less than 1 GiB available on fixture filesystem')
            row['services'][service] = resources
        if self.storage_up:
            stored = int(q.compose('exec', '-T', 'storage', 'du', '-sk', '/data', timeout=12).stdout.split()[0]) * 1024
            row['object_storage_bytes'] = stored
            if stored > self.args.max_storage_mib * 1024**2:
                raise ValueError('fixture object-storage budget exceeded')
        self.result['resource_samples'] += 1
        self.append('resources.jsonl', row)
        self.persist()
        print('SAMPLE', round(now - self.begin), 'seconds;', self.producer.progress.produced, 'source transactions', flush=True)

    def hold(self, seconds, fault):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.sample(fault)
            time.sleep(min(self.args.sample_seconds, max(0, deadline - time.monotonic())))

    def fault(self, name, planned):
        before = self.producer.progress.produced
        event = dict(name=name, planned_seconds=planned, started_seconds=time.monotonic() - self.begin)
        self.result['faults'].append(event)
        print('FAULT', name, flush=True)
        self.persist()
        if name == 'worker_crash':
            pid = self.sql('destination', 'SELECT supervisor_pid FROM lake_replication.service_status')
            if not pid.isdigit():
                raise ValueError('missing native supervisor PID')
            q.compose('exec', '-T', 'destination', 'kill', '-KILL', pid, timeout=12)
            self.wait('destination', f"SELECT (supervisor_pid IS NOT NULL AND supervisor_pid<>{pid})::text FROM lake_replication.service_status", 'true')
            event['victim_pid'] = int(pid)
        elif name == 'storage_outage':
            q.compose('stop', '-t', '3', 'storage', timeout=20)
            self.storage_up = False
            self.expected_deaths['storage'] += 1
            self.hold(10, name)
            q.compose('start', 'storage', timeout=20)
            self.storage_up = True
        elif name == 'destination_restart':
            q.compose('restart', 'destination', timeout=60)
            self.expected_deaths['destination'] += 1
            self.wait('destination', 'SELECT 1', '1')
            self.postmaster = self.sql('destination', 'SELECT to_json(pg_postmaster_start_time())').strip('"')
        elif name == 'backlog':
            self.sql('destination', 'ALTER SUBSCRIPTION soak_sub DISABLE')
            self.wait('source', "SELECT active::text FROM pg_replication_slots WHERE slot_name='soak_sub'", 'false', 10)
            before_progress = self.sql('destination', 'SELECT seq FROM soak_progress WHERE id=1')
            self.hold(10, name)
            q.check('disabled subscription retains its publication frontier', self.sql('destination', 'SELECT seq FROM soak_progress WHERE id=1'), before_progress)
            self.sql('destination', 'ALTER SUBSCRIPTION soak_sub ENABLE')
        if name in ('storage_outage', 'backlog'):
            q.check(name + ' source writes continue', self.producer.progress.produced > before, True)
        self.checkpoint('after ' + name)
        q.check(name + ' expected postmaster identity', self.sql('destination', 'SELECT to_json(pg_postmaster_start_time())').strip('"'), self.postmaster)
        event.update(completed_seconds=time.monotonic() - self.begin, source_transactions_during_event=self.producer.progress.produced - before)
        self.persist()

    def execute(self):
        self.sql('source', """CREATE TABLE soak_items(id integer PRIMARY KEY,revision bigint NOT NULL,payload text NOT NULL);
INSERT INTO soak_items SELECT n,0,'seed-'||n FROM generate_series(0,127)n;
CREATE TABLE soak_progress(id integer PRIMARY KEY,seq bigint NOT NULL); INSERT INTO soak_progress VALUES(1,0);
CREATE PUBLICATION soak_pub FOR TABLE soak_items,soak_progress; GRANT SELECT ON soak_items,soak_progress TO lake_source""")
        connection = f'postgresql://lake_source:{quote(q.SOURCE_PASSWORD, safe="")}@source:5432/application'
        self.sql('destination', f"CREATE SUBSCRIPTION soak_sub CONNECTION '{connection}' PUBLICATION soak_pub WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
        self.wait('destination', 'SELECT state FROM lake_replication.subscriptions', 'streaming')
        self.postmaster = self.sql('destination', 'SELECT to_json(pg_postmaster_start_time())').strip('"')
        self.begin, self.wall = time.monotonic(), time.time()
        self.checkpoint('initial snapshot')
        self.producer.thread.start()
        if not self.producer.ready.wait(15) or self.producer.error:
            raise RuntimeError('source producer startup failed: ' + str(self.producer.error))
        self.result['status'] = 'running'
        plan = schedule(self.args.duration_seconds)
        self.result['fault_plan'] = list(plan)
        next_sample = next_checkpoint = 0
        while time.monotonic() - self.begin < self.args.duration_seconds:
            elapsed = time.monotonic() - self.begin
            if plan and elapsed >= plan[0][0]:
                when, name = plan.pop(0)
                self.fault(name, when)
            if elapsed >= next_sample:
                self.sample()
                next_sample = time.monotonic() - self.begin + self.args.sample_seconds
            if elapsed >= next_checkpoint:
                self.checkpoint('periodic')
                next_checkpoint = time.monotonic() - self.begin + 60
            time.sleep(.1)
        self.producer.stop.set()
        self.producer.thread.join(timeout=20)
        q.check('source producer stops cleanly', not self.producer.thread.is_alive() and not self.producer.error, True)
        self.checkpoint('final data')
        q.check('every scheduled fault completes', not plan and all('completed_seconds' in f for f in self.result['faults']), True)
        q.check('at least one transaction per second offered on average', self.producer.progress.produced >= self.args.duration_seconds, True)
        self.sample()
        self.sql('destination', 'DROP SUBSCRIPTION soak_sub')
        self.wait('source', "SELECT count(*) FROM pg_replication_slots WHERE slot_name='soak_sub'", '0', 10)
        self.wait('destination', "SELECT count(*) FROM pg_stat_activity WHERE backend_type='lake_replication supervisor'", '0', 10)
        deadline = time.monotonic() + 5
        gone = False
        while time.monotonic() < deadline:
            result = q.compose('exec', '-T', 'destination', 'pgrep', '-f', '^/usr/local/bin/lake-replication-', check_result=False, timeout=5)
            if result.returncode == 1:
                gone = True
                break
            if result.returncode != 0:
                raise RuntimeError('could not inspect native child cleanup')
            time.sleep(.1)
        q.check('drop removes native Rust children', gone, True)
        self.result['visibility_observations'] = dict(count=self.producer.progress.completed,
            max_seconds=self.producer.progress.max_visibility_seconds, buckets=self.producer.progress.buckets)
        self.result['status'] = 'passed'

    def finish(self):
        errors = self.result.setdefault('cleanup_errors', [])
        def collect(operation, callback):
            try:
                return callback()
            except Exception as error:
                errors.append(dict(operation=operation, error=str(error)))

        self.producer.stop.set()
        if self.producer.thread.ident:
            collect('stop producer', lambda: self.producer.thread.join(timeout=20))
            if self.producer.thread.is_alive():
                errors.append(dict(operation='stop producer', error='producer did not stop'))
        collect('stop Docker event recorder', lambda: stop_process(self.events))
        try:
            if self.result['status'] == 'passed':
                events = [json.loads(line) for line in (q.RESULTS / 'docker-events.jsonl').read_text().splitlines()]
                self.result['container_deaths'] = validate_events(events, self.expected_deaths)
        except Exception as error:
            self.result.update(status='error', error=str(error))
        for name, args in [('workers.jsonl', ('exec', '-T', 'destination', 'sh', '-ec', "find /worker-logs -type f -name '*.log' -exec cat {} +")),
                           ('compose.log', ('logs', '--no-color'))]:
            try:
                with (q.RESULTS / name).open('w') as output:
                    q.compose(*args, timeout=30, capture_output=False, stdout=output, stderr=subprocess.STDOUT)
            except Exception as error:
                errors.append(dict(operation=name, error=str(error)))
        try:
            cleanup = q.compose('down', '-v', '--remove-orphans', timeout=60, check_result=False)
            self.result['cleanup_returncode'] = cleanup.returncode
            if cleanup.returncode:
                errors.append(dict(operation='down', error=cleanup.stderr))
        except Exception as error:
            errors.append(dict(operation='down', error=str(error)))
        if errors:
            self.result['status'] = 'error'
        if self.awake:
            collect('release idle-sleep assertion', lambda: stop_process(self.awake))
        if errors:
            self.result['status'] = 'error'
        self.result['qualifies_24_hour_soak'] = qualifies_soak(self.result['status'], self.args.duration_seconds,
            time.monotonic() - self.begin if self.begin is not None else 0)
        self.persist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duration-seconds', type=int, default=600)
    parser.add_argument('--interval-seconds', type=float, default=.5)
    parser.add_argument('--sample-seconds', type=float, default=5)
    parser.add_argument('--catchup-seconds', type=int, default=60)
    parser.add_argument('--max-storage-mib', type=int, default=4096)
    parser.add_argument('--cpuset', default='0-1')
    args = parser.parse_args()
    if not 180 <= args.duration_seconds <= 86400 or not 0 < args.interval_seconds <= 1 or not 1 <= args.sample_seconds <= 30 or not 1 <= args.catchup_seconds <= 300 or args.max_storage_mib < 1:
        parser.error('invalid duration, rate, sampling or resource budget')
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*', args.cpuset):
        parser.error('invalid CPU affinity')
    q.RESULTS = q.RESULTS.with_name(q.RUN + '_soak')
    q.RESULTS.mkdir(parents=True)
    q.PROJECT += '-soak'
    runner = Runner(args)
    inputs = [Path(__file__), Path(__file__).with_name('model.py'), ROOT / 'experiments/admission/compare.py', ROOT / 'experiments/admission/sample.py', ROOT / 'tests/qualification/probe.py']
    runner.result['inputs'] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}
    for path in inputs:
        destination = q.RESULTS / 'sources' / path.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
    try:
        if platform.system() == 'Darwin' and shutil.which('caffeinate'):
            runner.awake = subprocess.Popen(['caffeinate', '-i', '-w', str(os.getpid())], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        q.verify_archive(q.archive_path())
        with tempfile.TemporaryDirectory() as context:
            q.build_images(q.archive_path(), Path(context))
        text = (ROOT / 'deploy/qualification/compose.yml').read_text()
        for service, memory in [('source', '1g'), ('destination', '2g'), ('pgduck', '2g'), ('storage', '512m')]:
            text, count = re.subn(r'^  ' + service + r':$', f'  {service}:\n    cpuset: "{args.cpuset}"\n    mem_limit: {memory}', text, flags=re.MULTILINE)
            if count != 1:
                raise ValueError('fixture service definition changed: ' + service)
        q.COMPOSE_FILE = q.RESULTS / 'compose.yml'
        q.COMPOSE_FILE.write_text(text)
        q.compose('config', '--quiet', timeout=15)
        with (q.RESULTS / 'docker-events.jsonl').open('w') as output, (q.RESULTS / 'docker-events.stderr').open('w') as errors:
            runner.events = subprocess.Popen(['docker', 'events', '--filter', 'label=com.docker.compose.project=' + q.PROJECT,
                                              '--format', '{{json .}}'], stdout=output, stderr=errors, env=q.ENV)
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        runner.execute()
    except Exception as error:
        runner.result.update(status='error', error=str(error))
        print('ERROR', error, flush=True)
    finally:
        runner.finish()
        print('Evidence:', q.RESULTS, flush=True)
    return int(runner.result['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())

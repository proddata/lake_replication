#!/usr/bin/env python3
"""Compare update-run size and explicit key pruning in disposable Iceberg tables.

Destination-only trials exclude WAL receipt, staging and worker startup; the
optional native pipeline mode includes them. New archives use their installed
filter against a historical SQL control; old archives use a fixture-only
prototype. All changes stay in disposable fixtures, outside release qualification.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import re
import statistics
import shutil
import tempfile
import threading
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]


def keys(rows, updates, pattern, seed):
    if not 0 < updates <= rows:
        raise ValueError('updates must be between one and the table row count')
    if pattern == 'clustered':
        return list(range(1, updates + 1))
    if pattern != 'random':
        raise ValueError('unsupported key pattern')
    return random.Random(seed).sample(range(1, rows + 1), updates)


def filtered_definition(original):
    replacements = {
        'FUNCTION lake_replication.apply_change_run(': 'FUNCTION update_probe.filtered_run(',
        'DECLARE\n': 'DECLARE\n    v_probe_keys bigint[];\n',
        "    IF p_operation = 'update' THEN\n": """    IF p_operation = 'update' THEN
        -- This diagnostic accelerates a non-null bigint identity named id only.
        -- Preserve the installed implementation for every other identity shape.
        IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid=p_destination
                       AND attname='id' AND atttypid=20 AND attnotnull AND NOT attisdropped)
           OR EXISTS (SELECT 1 FROM unnest(p_old) AS input(value)
                      WHERE value - 'id' <> '{}'::jsonb
                         OR value->'id' IS NULL OR value->'id' = 'null'::jsonb) THEN
            RETURN lake_replication.apply_change_run(p_operation,p_destination,p_old,p_new);
        END IF;
        SELECT array_agg((value->>'id')::bigint) INTO v_probe_keys
          FROM unnest(p_old) AS input(value);
""",
        '                  WHERE %s\n': '                  WHERE (%s) AND destination.id = ANY ($3::bigint[])\n',
        'INTO v_count, v_distinct USING p_old, p_new;':
            'INTO v_count, v_distinct USING p_old, p_new, v_probe_keys;',
    }
    for before, after in replacements.items():
        if original.count(before) != 1:
            raise ValueError('installed update function differs at ' + repr(before))
        original = original.replace(before, after, 1)
    return original


def historical_definition():
    source=(ROOT/'migrations/0024_bulk_admission.sql').read_text()
    start=source.index('CREATE OR REPLACE FUNCTION lake_replication.apply_change_run(')
    end=source.index('\n$$;',start)+4
    return source[start:end]


def statement(old, new, selected, filtered):
    predicate = 'destination.id = (jsonb_populate_record(NULL::update_target, source.old_tuple)).id'
    if filtered:
        predicate += f' AND destination.id = ANY ({selected})'
    return f"""WITH source AS (
 SELECT old_tuple,new_tuple,ordinality FROM unnest({old},{new})
 WITH ORDINALITY AS input(old_tuple,new_tuple,ordinality)
), applied AS (
 UPDATE update_target AS destination SET
 id=(jsonb_populate_record(NULL::update_target,source.new_tuple)).id,
 payload=(jsonb_populate_record(NULL::update_target,source.new_tuple)).payload
 FROM source WHERE {predicate} RETURNING source.ordinality
) SELECT count(*),count(DISTINCT ordinality) FROM applied"""


def plan_summary(document):
    nodes = []
    def walk(value):
        if isinstance(value, dict):
            if 'Node Type' in value:
                nodes.append({k: v for k, v in value.items()
                              if k in ('Node Type', 'Join Type', 'Relation Name', 'Filter',
                                       'Data Files Scanned', 'Data Files Skipped', 'Plan Rows',
                                       'Actual Rows', 'Actual Loops', 'Actual Total Time',
                                       'Sort Method', 'Sort Space Used', 'Sort Space Type',
                                       'Temp Read Blocks', 'Temp Written Blocks')})
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)
    walk(document)
    return nodes


def server_commits(log):
    return {int(marker): float(milliseconds) / 1000
            for milliseconds, marker in re.findall(
                r'duration:\s*([\d.]+) ms\s+statement:\s*COMMIT /\* update_probe:(\d+) \*/', log)}


class Runner:
    def __init__(self, args):
        spec = importlib.util.spec_from_file_location('update_shared', ROOT / 'tests/support/compare.py')
        self.c = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.c)
        self.q = q = self.c.q
        q.PROJECT += '-updates'
        q.SOURCE_IMAGE += '-updates'
        q.DESTINATION_IMAGE += '-updates'
        q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE,
                     QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
        q.RESULTS = ROOT / '.qualification' / (q.RUN + '_update_compare')
        q.RESULTS.mkdir(parents=True)
        self.output, self.args = q.RESULTS, args
        self.report = dict(status='running', scope=__doc__, parameters=vars(args), trials=[],
                           release_qualified=False, production_sql_changed=False)
        self.client = None
        self.current_trial = None
        inputs = [Path(__file__), ROOT/'tests/support/compare.py', ROOT/'tests/support/sample.py',
                  *sorted((ROOT/'tests/qualification').glob('*.py')),
                  *sorted((ROOT/'deploy/qualification').glob('*')),
                  *sorted((ROOT/'deploy/pg_lake').glob('*'))]
        self.report['inputs'] = {}
        for source in inputs:
            if not source.is_file():
                continue
            name = str(source.relative_to(ROOT))
            self.report['inputs'][name] = hashlib.sha256(source.read_bytes()).hexdigest()
            target = self.output/'sources'/name
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(source,target)

    def save(self):
        (self.output / 'results.json').write_text(json.dumps(self.report, indent=2, sort_keys=True) + '\n')

    def resources(self):
        script = (ROOT/'tests/support/sample.py').read_text()
        return {name: json.loads(self.q.compose('exec','-T','--user','postgres',name,
                                              'python3','-c',script,'',timeout=15).stdout)
                for name in ('destination','pgduck')}

    def setup(self):
        q = self.q
        archive = q.archive_path()
        q.verify_archive(archive)
        self.report['archive'] = dict(q.EVIDENCE)
        with tempfile.TemporaryDirectory(prefix='lake-update-compare-') as context:
            q.build_images(archive, Path(context))
        self.report['prerequisite'] = q.EVIDENCE['prerequisite']
        self.report['storage_fixtures'] = q.EVIDENCE['storage_fixtures']
        text = q.COMPOSE_FILE.read_text()
        for service, memory in [('source', '512m'), ('destination', '1536m'),
                                ('pgduck', '2g'), ('storage', '768m')]:
            text = re.sub(rf'^  {service}:$',
                          f'  {service}:\n    cpuset: "{self.args.cpuset}"\n    mem_limit: {memory}',
                          text, flags=re.MULTILINE)
        q.COMPOSE_FILE = self.output / 'compose.yml'
        q.COMPOSE_FILE.write_text(text)
        q.compose('up', '-d', '--wait', '--wait-timeout', '300', timeout=600)
        self.client = p = self.c.Psql('destination', 'analytics')
        p.query("SET statement_timeout='180s'; SET application_name='update_probe'; "
                "SET log_min_duration_statement=0; CREATE SCHEMA update_probe")
        original = '\n'.join(p.query("SELECT pg_get_functiondef('lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure)"))
        self.original = original
        (self.output / 'installed.sql').write_text(original)
        has_installed_filter='v_key_query' in original
        if getattr(self.args,'installed_filter',False) and not has_installed_filter:
            raise ValueError('selected archive does not contain the generalized key filter')
        self.args.installed_filter=has_installed_filter
        if has_installed_filter:
            baseline=historical_definition()
            (self.output/'baseline.sql').write_text(baseline)
            p.query(baseline.replace('FUNCTION lake_replication.apply_change_run(',
                                    'FUNCTION update_probe.baseline_run(',1))
            candidate=original.replace('FUNCTION lake_replication.apply_change_run(',
                                       'FUNCTION update_probe.filtered_run(',1)
            self.report['installed_filter']=True
            self.baseline=baseline
        else:
            (self.output/'baseline.sql').write_text(original)
            candidate = filtered_definition(original)
            self.baseline=original
        (self.output / 'filtered.sql').write_text(candidate)
        p.query(candidate)
        baseline_call='update_probe.baseline_run' if getattr(self.args,'installed_filter',False) else 'lake_replication.apply_change_run'
        candidate_call='lake_replication.apply_change_run' if getattr(self.args,'installed_filter',False) else 'update_probe.filtered_run'
        measure_sql="""CREATE FUNCTION update_probe.measure(kind text, batch_number integer)
 RETURNS jsonb LANGUAGE plpgsql AS $$
 DECLARE old_rows jsonb[]; new_rows jsonb[]; began timestamptz; affected bigint;
 BEGIN
  SELECT old_values,new_values INTO STRICT old_rows,new_rows FROM pg_temp.update_input WHERE batch=batch_number;
  began:=clock_timestamp();
  IF kind='baseline' THEN
   affected:=BASELINE_CALL('update','update_target',old_rows,new_rows);
  ELSIF kind='key_filter' THEN
   affected:=CANDIDATE_CALL('update','update_target',old_rows,new_rows);
  ELSE RAISE EXCEPTION 'unsupported variant'; END IF;
  RETURN jsonb_build_object('rows',affected,'server_statement_seconds',extract(epoch FROM clock_timestamp()-began));
 END $$"""
        p.query(measure_sql.replace('BASELINE_CALL',baseline_call).replace('CANDIDATE_CALL',candidate_call))
        p.query(f"CREATE TABLE update_seed AS SELECT g::bigint AS id, "
                f"left(repeat(md5(g::text),{(self.args.payload_bytes + 31)//32}),{self.args.payload_bytes}) AS payload "
                f"FROM generate_series(1,{self.args.rows}) g; ALTER TABLE update_seed ADD PRIMARY KEY(id)", timeout=180)
        self.report['server'] = p.query('SELECT version(); SELECT pg_postmaster_start_time()')
        self.report['initial_resources'] = self.resources()
        names = q.command(['docker','ps','--format','{{.Names}}']).stdout.splitlines()
        self.report['background_containers'] = [name for name in names if not name.startswith(q.PROJECT)]

    def correctness(self):
        p, q = self.client, self.q
        p.query("CREATE TABLE update_correctness(id bigint NOT NULL,payload text) USING iceberg; "
                "INSERT INTO update_correctness VALUES(1,'one'),(2,'two')")
        p.query("BEGIN; SELECT update_probe.filtered_run('update','update_correctness',"
                "ARRAY['{\"id\":1}'::jsonb],ARRAY['{\"id\":10,\"payload\":\"changed\"}'::jsonb]); "
                "SELECT update_probe.filtered_run('update','update_correctness',"
                "ARRAY['{\"id\":10}'::jsonb],ARRAY['{\"id\":20,\"payload\":\"again\"}'::jsonb]); COMMIT")
        q.check('filtered updates preserve ordered key changes',
                p.query("SELECT string_agg(id||':'||payload,',' ORDER BY id) FROM update_correctness"),['2:two,20:again'])
        p.query("BEGIN; SELECT update_probe.filtered_run('update','update_correctness',"
                "ARRAY['{\"id\":2}'::jsonb],ARRAY['{\"id\":2,\"payload\":\"rollback\"}'::jsonb]); ROLLBACK")
        q.check('filtered update rollback preserves data',p.query('SELECT payload FROM update_correctness WHERE id=2'),['two'])
        for label, old, new in [('missing identity','[{"id":99}]','[{"id":99,"payload":"bad"}]'),
                                ('repeated identity','[{"id":2},{"id":2}]','[{"id":2,"payload":"a"},{"id":2,"payload":"b"}]')]:
            result = q.sql_result('destination','analytics',f"SELECT update_probe.filtered_run('update','update_correctness',"
                                  f"ARRAY(SELECT jsonb_array_elements('{old}'::jsonb)),"
                                  f"ARRAY(SELECT jsonb_array_elements('{new}'::jsonb)))",check_result=False)
            q.check('filtered update rejects '+label,result.returncode!=0 and 'exactly one per change' in result.stderr,True)
        q.check('rejected updates leave data intact',p.query('SELECT payload FROM update_correctness WHERE id=2'),['two'])
        p.query("CREATE TABLE update_nullable(id bigint,payload text) USING iceberg; INSERT INTO update_nullable VALUES(NULL,'old'); "
                "SELECT update_probe.filtered_run('update','update_nullable',ARRAY['{\"id\":null}'::jsonb],"
                "ARRAY['{\"id\":null,\"payload\":\"new\"}'::jsonb])")
        q.check('nullable identities retain baseline null-safe matching',p.query('SELECT payload FROM update_nullable WHERE id IS NULL'),['new'])
        p.query("CREATE TABLE update_composite(id bigint NOT NULL,tenant text,payload text) USING iceberg; "
                "INSERT INTO update_composite VALUES(1,'a','old'),(1,'b','untouched'); "
                "SELECT update_probe.filtered_run('update','update_composite',ARRAY['{\"id\":1,\"tenant\":\"a\"}'::jsonb],"
                "ARRAY['{\"id\":1,\"tenant\":\"a\",\"payload\":\"new\"}'::jsonb])")
        q.check('composite identities retain the full baseline predicate',
                p.query("SELECT string_agg(tenant||':'||payload,',' ORDER BY tenant) FROM update_composite"),['a:new,b:untouched'])
        self.report['correctness_checks'] = list(q.EVIDENCE['checks'])

    def trial(self, number, pattern, variant, run_size, repetition):
        p, args = self.client, self.args
        folder = self.output / f'{number:03d}_{pattern}_{variant}_{run_size}'
        folder.mkdir()
        observed = dict(number=number, pattern=pattern, variant=variant, run_size=run_size,
                        repetition=repetition, resources=[], statements=[])
        self.current_trial = observed
        if shutil.disk_usage(self.output).free < 4*1024**3:
            raise RuntimeError('less than 4 GiB host free space remains')
        selected = keys(args.rows, args.updates, pattern, args.seed)
        key_sql = 'ARRAY[' + ','.join(map(str, selected)) + ']::bigint[]'
        p.query("DROP TABLE IF EXISTS update_target; CREATE TABLE update_target "
                "(id bigint NOT NULL,payload text) USING iceberg WITH (autovacuum_enabled=false)")
        for first in range(1, args.rows + 1, args.file_rows):
            p.query(f'INSERT INTO update_target SELECT * FROM update_seed WHERE id BETWEEN '
                    f'{first} AND {min(args.rows,first+args.file_rows-1)} ORDER BY id', timeout=180)
        p.query('DROP TABLE IF EXISTS update_keys,update_input; '
                f'CREATE TEMP TABLE update_keys AS SELECT id,ordinality FROM unnest({key_sql}) WITH ORDINALITY AS t(id,ordinality); '
                f'CREATE TEMP TABLE update_input AS SELECT ((ordinality-1)/{run_size})::integer AS batch, '
                "array_agg(jsonb_build_object('id',id) ORDER BY ordinality) AS old_values, "
                f"array_agg(jsonb_build_object('id',id,'payload',repeat('u',{args.payload_bytes})) ORDER BY ordinality) AS new_values, "
                'array_agg(id ORDER BY ordinality) AS selected FROM update_keys GROUP BY 1; ANALYZE update_keys')
        files = p.query("SELECT row_to_json(t) FROM (SELECT content,count(*) AS files,sum(record_count) AS records,"
                        "sum(file_size_in_bytes) AS bytes FROM lake_iceberg.tables_internal,"
                        "LATERAL lake_iceberg.files(metadata_location) WHERE table_name='update_target'::regclass GROUP BY content)t")
        literals = p.query("SELECT quote_literal(old_values::text)||'::jsonb[]',quote_literal(new_values::text)||'::jsonb[]',"
                           "quote_literal(selected::text)||'::bigint[]' FROM update_input WHERE batch=0")[0].split('|')
        query = statement(*literals, variant == 'key_filter')
        (folder / 'explain.sql').write_text(query)
        plan = json.loads('\n'.join(p.query('EXPLAIN (VERBOSE,FORMAT JSON) ' + query)))
        (folder / 'plan.json').write_text(json.dumps(plan, indent=2) + '\n')
        observed.update(initial_files=[json.loads(row) for row in files],plan=plan_summary(plan))
        if args.analyze:
            # Profiling gets a separate run: executing EXPLAIN warms caches.
            p.query('BEGIN')
            try:
                analyzed = json.loads('\n'.join(p.query('EXPLAIN (ANALYZE,VERBOSE,BUFFERS,FORMAT JSON) ' + query,timeout=180)))
            finally:
                p.query('ROLLBACK')
            if p.query(f"SELECT count(*) FROM update_target WHERE payload=repeat('u',{args.payload_bytes})") != ['0']:
                raise AssertionError('rolled-back execution profile changed the destination')
            (folder/'analyzed-plan.json').write_text(json.dumps(analyzed,indent=2)+'\n')
            observed['analyzed_plan'] = plan_summary(analyzed)
        stop = threading.Event()
        errors = []
        def sample():
            while not stop.is_set():
                try:
                    observed['resources'].append(self.resources())
                except Exception as error:
                    errors.append(str(error)); return
                stop.wait(1)
        sampler = threading.Thread(target=sample, daemon=True)
        observed['resources'].append(self.resources())
        sampler.start()
        p.query('BEGIN')
        began = time.monotonic()
        try:
            for batch in range((args.updates + run_size - 1)//run_size):
                result = json.loads(p.query(f"SELECT update_probe.measure('{variant}',{batch})")[0])
                if result['rows'] != min(run_size, args.updates - batch * run_size):
                    raise AssertionError('update run did not match every identity exactly once')
                observed['statements'].append(result)
            commit_started = time.monotonic()
            p.query(f'COMMIT /* update_probe:{number} */', timeout=180)
            finished = time.monotonic()
        finally:
            stop.set();sampler.join(timeout=35)
        if sampler.is_alive() or errors:
            raise RuntimeError('resource sampling failed: ' + str(errors))
        observed['resources'].append(self.resources())
        observed.update(statement_seconds=sum(x['server_statement_seconds'] for x in observed['statements']),
                        client_commit_seconds=finished-commit_started, client_total_seconds=finished-began,
                        rows_per_second=args.updates/(finished-began))
        exact = p.query(f"SELECT count(*),count(DISTINCT t.id),bool_and(s.id IS NOT NULL AND t.payload = CASE WHEN k.id IS NULL "
                        f"THEN s.payload ELSE repeat('u',{args.payload_bytes}) END) FROM update_target t "
                        'LEFT JOIN update_keys k USING(id) LEFT JOIN update_seed s USING(id)', timeout=180)
        if exact != [f'{args.rows}|{args.rows}|t']:
            raise AssertionError('exact destination comparison failed: ' + repr(exact))
        for row in observed['resources']:
            for value in row.values():
                if any(int(line.split()[1]) for line in value['cgroup']['memory.events'].splitlines()
                       if line.split()[0] in ('oom','oom_kill')):
                    raise AssertionError('fixture reported an OOM')
        if p.query('SELECT pg_postmaster_start_time()') != self.report['server'][-1:]:
            raise AssertionError('unexpected destination restart')
        observed['exact_data_verified'] = True
        self.report['trials'].append(observed)
        self.current_trial = None
        self.save()
        print(number,pattern,variant,run_size,f"{observed['rows_per_second']:.0f} rows/s",
              f"statement={observed['statement_seconds']:.3f}s commit={observed['client_commit_seconds']:.3f}s",flush=True)

    def pipeline(self):
        """Exercise small native source transactions in baseline/filter/baseline order."""
        p, q, args = self.client, self.q, self.args
        source = self.c.Psql('source','application')
        selected = keys(args.rows,args.updates,'random',args.seed)
        key_sql = 'ARRAY['+','.join(map(str,selected))+']::bigint[]'
        self.report.update(mode='native CDC baseline/filter/baseline with candidate crash recovery',
                           fixture_sql_overlay=True,pipeline=[])
        try:
            source.query("SET statement_timeout='180s'; CREATE TABLE update_stream_items(id bigint PRIMARY KEY,payload text NOT NULL)")
            source.query(f"INSERT INTO update_stream_items SELECT g,left(repeat(md5(g::text),"
                         f"{(args.payload_bytes+31)//32}),{args.payload_bytes}) FROM generate_series(1,{args.rows}) g",timeout=180)
            source.query(f"INSERT INTO update_stream_items VALUES({args.rows+1},'initial'); "
                         'CREATE PUBLICATION update_stream_pub FOR TABLE update_stream_items; '
                         'GRANT SELECT ON update_stream_items TO lake_source')
            p.query(f'CREATE TEMP TABLE pipeline_keys AS SELECT unnest({key_sql}) AS id')
            backup = self.baseline.replace('FUNCTION lake_replication.apply_change_run(',
                                           'FUNCTION update_probe.original_run(',1)
            p.query(backup)
            candidate = (self.original if getattr(args,'installed_filter',False) else
                         filtered_definition(self.original).replace('FUNCTION update_probe.filtered_run(',
                            'FUNCTION lake_replication.apply_change_run(',1).replace(
                            'RETURN lake_replication.apply_change_run(', 'RETURN update_probe.original_run(',1))
            (self.output/'pipeline-overlay.sql').write_text(candidate)
            for number, variant in enumerate(('baseline','key_filter','baseline'),1):
                p.query(candidate if variant=='key_filter' else self.baseline)
                if variant=='key_filter':
                    q.check('installed diagnostic retains nullable fallback',p.query(
                            "SELECT lake_replication.apply_change_run('update','update_nullable',"
                            "ARRAY['{\"id\":null}'::jsonb],ARRAY['{\"id\":null,\"payload\":\"fallback\"}'::jsonb])"),['1'])
                source.query(f"UPDATE update_stream_items SET payload=left(repeat(md5(id::text),"
                             f"{(args.payload_bytes+31)//32}),{args.payload_bytes}) WHERE id=ANY({key_sql}); "
                             f"UPDATE update_stream_items SET payload='initial' WHERE id={args.rows+1}")
                connection = f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application?sslmode=disable"
                # Native DDL carries a disposable credential; omit it from this
                # session's duration logging while retaining timed UPDATE logs.
                p.query('SET log_min_duration_statement=-1')
                p.query(f"CREATE SUBSCRIPTION update_perf_sub CONNECTION '{connection}' PUBLICATION update_stream_pub "
                        "WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
                p.query('SET log_min_duration_statement=0')
                q.check(f'{variant} native initial copy completes',q.wait_for('destination','analytics',
                        "SELECT state FROM lake_replication.subscriptions WHERE subscription_name='update_perf_sub'",
                        'streaming',timeout=300),'streaming')
                observed=dict(number=number,variant=variant,source_transactions=args.updates+1,
                              updated_data_rows=args.updates,status='running')
                self.report['pipeline'].append(observed)
                if variant=='key_filter':
                    q.check('native bigint identity enables the filter',p.query("SELECT attnotnull::text FROM pg_attribute "
                            "WHERE attrelid='update_stream_items'::regclass AND attname='id'"),['true'])
                observed['resources_before']=self.resources()
                began=time.monotonic()
                for key in selected:
                    changed=source.query(f"UPDATE update_stream_items SET payload=repeat('u',{args.payload_bytes}) WHERE id={key} RETURNING id")
                    if changed!=[str(key)]:raise AssertionError('source update did not affect exactly one key')
                marker=f'complete-{number}'
                source.query(f"UPDATE update_stream_items SET payload='{marker}' WHERE id={args.rows+1}")
                written=time.monotonic()
                deadline=written+180
                while p.query(f'SELECT payload FROM update_stream_items WHERE id={args.rows+1}')!=[marker]:
                    if time.monotonic()>deadline:raise TimeoutError('native update backlog did not drain')
                    time.sleep(.1)
                finished=time.monotonic()
                observed.update(source_write_seconds=written-began,end_to_end_seconds=finished-began,
                                catchup_after_source_seconds=finished-written,
                                data_updates_per_second=args.updates/(finished-began))
                def verify(recovered=False):
                    expected=(f"CASE WHEN t.id={args.rows+1} THEN '{marker}' "
                              +(f"WHEN t.id={selected[0]} THEN repeat('r',{args.payload_bytes}) " if recovered else '')
                              +f"WHEN k.id IS NOT NULL THEN repeat('u',{args.payload_bytes}) ELSE s.payload END")
                    actual=p.query("SELECT count(*),count(DISTINCT t.id),bool_and(t.id BETWEEN 1 AND "
                                   f"{args.rows+1} AND t.payload=({expected})) FROM update_stream_items t "
                                   'LEFT JOIN pipeline_keys k USING(id) LEFT JOIN update_seed s USING(id)',timeout=180)
                    q.check(f'{variant} native exact rows and payloads'+(' after crash' if recovered else ''),
                            actual,[f'{args.rows+1}|{args.rows+1}|t'])
                verify()
                if variant=='key_filter':
                    pid=p.query('SELECT supervisor_pid FROM lake_replication.service_status')[0]
                    if not pid.isdigit():raise ValueError('missing native supervisor PID')
                    source.query(f"UPDATE update_stream_items SET payload=repeat('c',{args.payload_bytes}) WHERE id={selected[0]}")
                    crashed=time.monotonic()
                    q.compose('exec','-T','destination','kill','-KILL',pid,timeout=12)
                    q.check('filtered candidate replaces the killed supervisor',q.wait_for('destination','analytics',
                            f"SELECT (supervisor_pid IS NOT NULL AND supervisor_pid<>{pid})::text FROM lake_replication.service_status",
                            'true',timeout=60),'true')
                    source.query(f"UPDATE update_stream_items SET payload=repeat('r',{args.payload_bytes}) WHERE id={selected[0]}")
                    q.check('filtered candidate recovers committed source update',q.wait_for('destination','analytics',
                            f'SELECT payload FROM update_stream_items WHERE id={selected[0]}','r'*args.payload_bytes,timeout=90),
                            'r'*args.payload_bytes)
                    verify(recovered=True)
                    observed['supervisor_crash_recovery_verified']=True
                    observed['crash_and_resumed_write_seconds']=time.monotonic()-crashed
                observed['resources_after']=self.resources()
                observed['subscription_status']=json.loads(p.query(
                    "SELECT row_to_json(s) FROM lake_replication.subscription_status s "
                    "WHERE subscription_name='update_perf_sub'")[0])
                for resources in (observed['resources_before'],observed['resources_after']):
                    for value in resources.values():
                        if any(int(line.split()[1]) for line in value['cgroup']['memory.events'].splitlines()
                               if line.split()[0] in ('oom','oom_kill')):raise AssertionError('pipeline fixture OOM')
                q.check(f'{variant} native updates preserve the destination postmaster',
                        p.query('SELECT pg_postmaster_start_time()'),self.report['server'][-1:])
                # Keep teardown out of the timed comparison and wait for the
                # receiver to release its source slot before removing it.
                p.query('ALTER SUBSCRIPTION update_perf_sub DISABLE')
                q.check(f'{variant} native subscription stops',q.wait_for('destination','analytics',
                        "SELECT health_state FROM lake_replication.subscription_status "
                        "WHERE subscription_name='update_perf_sub'",'stopped'),'stopped')
                q.check(f'{variant} native source slot is inactive',q.wait_for('source','application',
                        "SELECT active FROM pg_replication_slots WHERE slot_name='update_perf_sub'",'f'),'f')
                p.query('DROP SUBSCRIPTION update_perf_sub')
                q.check(f'{variant} native source slot is removed',q.wait_for('source','application',
                        "SELECT count(*) FROM pg_replication_slots WHERE slot_name='update_perf_sub'",'0'),'0')
                p.query('DROP TABLE update_stream_items')
                observed['status']='passed'
                self.save()
                print('PIPELINE',number,variant,f"{observed['data_updates_per_second']:.1f} updates/s",
                      f"end-to-end={observed['end_to_end_seconds']:.3f}s",flush=True)
            p.query(self.original)
            self.report['pipeline_checks']=list(q.EVIDENCE['checks'])
        finally:
            source.close()

    def run(self):
        try:
            self.setup()
            self.correctness()
            if self.args.pipeline:
                self.pipeline()
            else:
                number = 0
                configurations = [(variant, size) for variant in self.args.variants for size in self.args.run_sizes]
                for repetition in range(1, self.args.repetitions + 1):
                    for pattern in self.args.patterns:
                        order = configurations if repetition % 2 else list(reversed(configurations))
                        for variant, size in order:
                            number += 1
                            self.trial(number, pattern, variant, size, repetition)
            current = '\n'.join(self.client.query("SELECT pg_get_functiondef('lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[])'::regprocedure)"))
            if current != self.original:
                raise AssertionError('installed baseline function changed')
            self.report['status'] = 'passed'
        except (Exception, KeyboardInterrupt) as error:
            message=str(error)
            for secret in (self.q.SOURCE_PASSWORD,self.q.ADMIN_PASSWORD,quote(self.q.SOURCE_PASSWORD,safe='')):
                message=message.replace(secret,'[fixture credential]')
            self.report.update(status='error',error=message,error_type=type(error).__name__)
            if self.current_trial is not None:
                self.report['failed_trial'] = self.current_trial
            print('ERROR:',message,flush=True)
        finally:
            self.finalize()
        return int(self.report['status']!='passed')

    def finalize(self):
        errors = []
        if self.client:
            try:
                self.client.close()
            except Exception as error:
                errors.append('client close: '+str(error))
        try:
            logs = self.q.compose('logs','--no-color',check_result=False,timeout=30)
            (self.output / 'compose.log').write_text(logs.stdout + logs.stderr)
            commits = server_commits(logs.stdout)
            for trial in self.report['trials']:
                trial['server_commit_seconds'] = commits.get(trial['number'])
                if trial['server_commit_seconds'] is None:
                    errors.append('missing server commit duration for trial '+str(trial['number']))
            self.report['summary'] = []
            for pattern in self.args.patterns:
                for variant in self.args.variants:
                    for size in self.args.run_sizes:
                        trials = [t for t in self.report['trials'] if (t['pattern'],t['variant'],t['run_size'])==(pattern,variant,size)]
                        if trials:
                            self.report['summary'].append(dict(pattern=pattern,variant=variant,run_size=size,
                                trials=len(trials),median_rows_per_second=statistics.median(t['rows_per_second'] for t in trials),
                                median_statement_seconds=statistics.median(t['statement_seconds'] for t in trials),
                                median_client_commit_seconds=statistics.median(t['client_commit_seconds'] for t in trials)))
        except Exception as error:
            errors.append('evidence capture: '+str(error))
        finally:
            try:
                cleanup = self.q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
                self.report['cleanup_returncode'] = cleanup.returncode
                if cleanup.returncode:
                    errors.append('fixture teardown returned '+str(cleanup.returncode))
            except Exception as error:
                errors.append('fixture teardown: '+str(error))
        if errors:
            self.report['cleanup_errors'] = errors
            self.report['status'] = 'error'
        self.save()
        print('Evidence:',self.output,flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows',type=int,default=1000000)
    parser.add_argument('--updates',type=int,default=10000)
    parser.add_argument('--file-rows',type=int,default=100000)
    parser.add_argument('--payload-bytes',type=int,default=128)
    parser.add_argument('--run-sizes',nargs='+',type=int,default=[500,2000,5000])
    parser.add_argument('--variants',nargs='+',choices=['baseline','key_filter'],default=['baseline','key_filter'])
    parser.add_argument('--patterns',nargs='+',choices=['clustered','random'],default=['clustered','random'])
    parser.add_argument('--repetitions',type=int,default=3)
    parser.add_argument('--seed',type=int,default=20261111)
    parser.add_argument('--cpuset',default='2-3')
    parser.add_argument('--analyze',action='store_true',help='separate warmed diagnostic with executed/rolled-back EXPLAIN')
    parser.add_argument('--pipeline',action='store_true',help='run native small-transaction CDC and crash checks instead of run-size trials')
    parser.add_argument('--installed-filter',action='store_true',help='test the packaged generalized filter against the preserved historical SQL control')
    args = parser.parse_args()
    if not 0 < args.updates <= args.rows or min(args.file_rows,args.payload_bytes,args.repetitions,*args.run_sizes)<=0:
        parser.error('row, payload, run and repetition sizes must be positive and updates cannot exceed rows')
    if args.payload_bytes>512 or max(args.run_sizes)>5000 or args.repetitions>5 or args.rows>10000000:
        parser.error('diagnostic resource limits exceeded')
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',args.cpuset):
        parser.error('invalid CPU affinity')
    for values in (args.run_sizes,args.variants,args.patterns):
        if len(set(values))!=len(values):parser.error('duplicate configuration')
    if args.pipeline and (args.updates>1000 or args.analyze):
        parser.error('native pipeline diagnostic requires at most 1000 updates and no executed EXPLAIN')
    return Runner(args).run()


if __name__=='__main__':
    raise SystemExit(main())

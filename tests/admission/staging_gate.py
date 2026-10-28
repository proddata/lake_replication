"""Instance-wide append-transaction gate in a disposable initial-sync overlay."""
import hashlib
import json
import types


class Prototype:
    def __init__(self,q,root,mode="serial"):
        self.q,self.root=q,root
        self.mode=mode;self.binary_sha256=None
        self.inputs={name:(root/'tests/admission'/name).read_bytes() for name in
            ['staging_gate.py','staging_gate.c','staging_gate.sql','staging_gate_operations.py','staging_gate_results.py','budget_results.py']}
        self.analysis=self.frozen('staging_gate_results.py')
        self.analysis.b=self.frozen('budget_results.py')
        self.operation_module=self.frozen('staging_gate_operations.py')
        self.installed=set();self.captured=False;self.final={};self.sources={}

    def frozen(self,name):
        module=types.ModuleType(name[:-3]);module.__file__=str(self.root/'tests/admission'/name)
        exec(compile(self.inputs[name],module.__file__,'exec'),module.__dict__)
        return module

    def operations(self,c):
        self.operation_module.run(self,c)

    def audit(self,case):
        result=self.analysis.audit(case)
        result['analysis_source_sha256']=hashlib.sha256(self.inputs['staging_gate_results.py']).hexdigest()
        return result

    def prepare(self,context):
        path=context/'worker/src/initial_sync.rs';source=path.read_text()
        old='"SELECT lake_replication.append_initial_copy_rows({}, {}, {}, {}, {}::jsonb)::text",'
        new='"BEGIN; SET LOCAL client_connection_check_interval=\'100ms\'; SET LOCAL lock_timeout=\'10s\'; SELECT staging_gate.admit(); UPDATE staging_gate.counters SET chunks=chunks+1, bytes=bytes+{5} WHERE singleton; SELECT lake_replication.append_initial_copy_rows({0}, {1}, {2}, {3}, {4}::jsonb)::text; COMMIT",'
        if source.count(old)!=1:raise RuntimeError('initial-sync append hook is not unique')
        anchor='        sql_literal(&json_rows),\n'
        if source.count(anchor)!=1:raise RuntimeError('initial-sync byte argument hook is not unique')
        source=source.replace(old,new).replace(anchor,anchor+'        json_rows.len(),\n')
        path.write_text(source)
        for name,data in [('worker/src/initial_sync.rs',path.read_bytes()),('staging_gate.c',self.inputs['staging_gate.c'])]:
            dest=self.q.RESULTS/'staging-gate-sources'/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
            self.sources[name]=hashlib.sha256(data).hexdigest()
        (context/'staging_gate.c').write_bytes(self.inputs['staging_gate.c'])
        return path

    def overlay(self,builder):
        return ('COPY --from='+builder+' /src/target/release/lake-replication-initial-sync /usr/local/bin/lake-replication-initial-sync\n'
            'COPY staging_gate.c /qualification/staging_gate.c\n'
            'RUN cc -shared -fPIC -O2 -Wall -Wextra -Werror -Wno-unused-parameter '
            '-DSTAGING_GATE_ENABLED='+('1' if self.mode=='serial' else '0')+' -I"$(pg_config --includedir-server)" -I"$(pg_config --includedir)" '
            '/qualification/staging_gate.c -o /qualification/staging_gate.so\n')

    def install(self,database,sql):
        if database not in self.installed:
            sql('destination',database,self.inputs['staging_gate.sql'].decode())
            self.installed.add(database)
            (self.q.RESULTS/'staging-gate-installed.sql').write_bytes(self.inputs['staging_gate.sql'])

    def capture(self):
        self.captured=True;q=self.q
        try:
            result=q.compose('exec','-T','destination','psql','-XqAt','-U','postgres','-d','analytics','-c',
                'SELECT jsonb_agg(datname) FROM pg_database WHERE datallowconn',timeout=10)
            existing=set(json.loads(result.stdout))
            for database in sorted(self.installed & existing):
                result=q.compose('exec','-T','destination','psql','-XqAt','-U','postgres','-d',database,'-c',
                    'SELECT to_jsonb(c) FROM staging_gate.counters c',timeout=10)
                self.final[database]=json.loads(result.stdout)
        except Exception as error:self.final['error']=str(error)
        (q.RESULTS/'staging-gate-final.json').write_text(json.dumps(self.final,indent=2)+'\n')

    def metadata(self):
        return {'mode':self.mode,'binary_sha256':self.binary_sha256,'client_connection_check_interval_ms':100,'lock_timeout_ms':10000,'scope':('Initial-sync/accounting control with no lock' if self.mode=='accounting' else 'Initial-sync and diagnostic C helper overlay; transaction-scoped instance-wide staging permit, not a byte or whole-memory bound'),
            'sources':self.sources,'inputs':{n:hashlib.sha256(b).hexdigest() for n,b in self.inputs.items()},
            'installed_databases':sorted(self.installed),'final':self.final}

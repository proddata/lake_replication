"""Disposable shared-byte-budget implementation; never edits production sources."""
import hashlib
import json
from pathlib import Path
import shutil
import tempfile


class Prototype:
    def __init__(self,q,root,rate_mib,burst_kib):
        self.q,self.root=q,root
        self.rate,self.burst=rate_mib*1024*1024,burst_kib*1024
        self.installed=False
        self.captured=False
        self.stats={}
        self.sources={}
        self.inputs={name:(root/'tests/admission'/name).read_bytes() for name in
            ('staging_budget.py','staging_budget.rs','staging_budget.sql','Dockerfile.staging-budget','budget_operations.py')}

    def build(self):
        q,root=self.q,self.root
        with tempfile.TemporaryDirectory(prefix='lake-shared-budget-') as temp:
            context=Path(temp)
            for name in ('Cargo.toml','Cargo.lock'):
                shutil.copy2(root/name,context/name)
            shutil.copytree(root/'worker',context/'worker',ignore=shutil.ignore_patterns('target','__pycache__','.env','.DS_Store'))
            (context/'worker/src/staging_budget.rs').write_bytes(self.inputs['staging_budget.rs'])
            path=context/'worker/src/initial_sync.rs'
            source=path.read_text()
            if 'mod staging_budget;' in source or (root/'worker/src/staging_budget.rs').exists():
                raise RuntimeError('shared budget already present in production; refuse diagnostic overlay')
            anchor='    let json_rows = json_array(rows);\n'
            if source.count(anchor)!=1:
                raise RuntimeError('initial-sync hook must have exactly one injection point')
            path.write_text('mod staging_budget;\n'+source.replace(anchor,
                anchor+'    staging_budget::admit(json_rows.len(), config)?;\n'))
            self.sources={str(p.relative_to(context)):p.read_bytes() for p in
                [context/'worker/src/initial_sync.rs',context/'worker/src/staging_budget.rs']}
            for relative,data in self.sources.items():
                dest=q.RESULTS/'budget-sources'/relative;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
            (context/'Dockerfile').write_bytes(self.inputs['Dockerfile.staging-budget'])
            builder='lake-replication-staging-budget-builder:'+q.RUN
            q.command(['docker','build','-t',builder,str(context)],timeout=1200)
            (context/'initial-sync').write_text('#!/bin/sh\nexec env LAKE_DIAGNOSTIC_STAGING_DATABASE=analytics /usr/local/bin/lake-replication-initial-sync-budgeted "$@"\n')
            (context/'Dockerfile.overlay').write_text('FROM '+q.DESTINATION_IMAGE+'\nUSER root\n'
                'COPY --from='+builder+' /src/target/release/lake-replication-initial-sync /usr/local/bin/lake-replication-initial-sync-budgeted\n'
                'COPY --chmod=0755 initial-sync /usr/local/bin/lake-replication-initial-sync\n'
                'LABEL io.lake-replication.diagnostic="shared-staging-byte-budget"\n')
            q.command(['docker','build','-f',str(context/'Dockerfile.overlay'),'-t',q.DESTINATION_IMAGE,str(context)],timeout=300)

    def install(self):
        sql=self.inputs['staging_budget.sql'].decode().replace('__RATE_BYTES__',str(self.rate)).replace('__BURST_BYTES__',str(self.burst))
        (self.q.RESULTS/'budget-installed.sql').write_text(sql)
        self.q.sql('destination','analytics',sql)
        self.installed=True

    def operations(self,c):
        import importlib.util
        path=self.root/'tests/admission/budget_operations.py'
        spec=importlib.util.spec_from_file_location('budget_operations',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        module.run(self,c)

    def capture(self):
        self.captured=True
        try:
            result=self.q.compose('exec','-T','destination','psql','-XqAt','-U','postgres',
                '-d','analytics','-c',"SET statement_timeout='5s'; SELECT jsonb_build_object('settings',(SELECT to_jsonb(s) FROM staging_budget.settings s),'credit',(SELECT to_jsonb(c) FROM staging_budget.credit c))",timeout=10)
            self.stats=json.loads(result.stdout)
        except Exception as error:
            self.stats={'error':str(error)}
        try:
            logs=self.q.compose('exec','-T','destination','sh','-ec',
                "find /worker-logs -type f -name '*.log' -exec cat {} +",timeout=20)
            data=(logs.stdout+logs.stderr).encode()
            (self.q.RESULTS/'budget-workers.jsonl').write_bytes(data)
            self.stats['worker_log_sha256']=hashlib.sha256(data).hexdigest()
        except Exception as error:
            self.stats['worker_log_error']=str(error)
        (self.q.RESULTS/'staging-budget.json').write_text(json.dumps(self.stats,indent=2)+'\n')

    def metadata(self):
        return {'rate_bytes_per_second':self.rate,'burst_bytes':self.burst,
            'coordinator_database':'analytics','stats':self.stats,
            'inputs':{name:hashlib.sha256(data).hexdigest() for name,data in self.inputs.items()},
            'overlaid_sources':{path:hashlib.sha256(data).hexdigest() for path,data in self.sources.items()}}

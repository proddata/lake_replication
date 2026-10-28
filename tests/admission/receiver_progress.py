"""Build a disposable receiver-only telemetry overlay, preserving durable logic."""
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

SQL = "SELECT coalesce(jsonb_agg(s),'[]'::jsonb) FROM receiver_progress.snapshots() s"


class Prototype:
    def __init__(self,q,root,cycle_timing=False,receiver_batch=1,staging_gate=None):
        if receiver_batch not in (1,64) or (receiver_batch>1 and not cycle_timing):
            raise ValueError('bounded receiver mode requires cycle timing and a supported transaction limit')
        self.q,self.root=q,root
        self.cycle_timing=cycle_timing
        self.receiver_batch=receiver_batch
        self.staging_gate=staging_gate
        self.installed=self.captured=False
        self.inputs={name:(root/'tests/admission'/name).read_bytes() for name in
            ['receiver_progress.py','receiver_progress.rs','receiver_progress.sql','Dockerfile.receiver-progress']}
        if cycle_timing:self.inputs['cycle_timing.rs']=(root/'tests/admission/cycle_timing.rs').read_bytes()
        self.sources={}

    def build(self):
        q,root=self.q,self.root
        with tempfile.TemporaryDirectory(prefix='lake-receiver-progress-') as temp:
            context=Path(temp)
            for name in ['Cargo.toml','Cargo.lock']:shutil.copy2(root/name,context/name)
            shutil.copytree(root/'worker',context/'worker',ignore=shutil.ignore_patterns('target','__pycache__','.env','.DS_Store'))
            if (root/'worker/src/receiver_progress.rs').exists():raise RuntimeError('refuse to overlay production telemetry')
            path=context/'worker/src/main.rs';source=path.read_text()
            if 'mod receiver_progress;' in source:raise RuntimeError('receiver telemetry already present')
            replacements=[
                ('    let mut feedback_lsn = start_lsn;\n', '    let mut progress = receiver_progress::Progress::new(&config.destination_database, config.subscription_id, start_lsn)?;\n',1),
                ('                    // restart decoding. MessageReader retains partial frames.\n', '                    progress.timeout()?;\n',1),
                ('        match copy_data.first().copied() {\n', '        progress.observe(&copy_data)?;\n',1),
                ('                        let transaction = assembler.commit(commit)?;\n', '                        progress.committing(transaction.end_lsn, transaction.commit_time_micros)?;\n',1),
                ('                        let transaction = stream_assembler.commit(stream_commit.commit)?;\n', '                        progress.committing(transaction.end_lsn, transaction.commit_time_micros)?;\n',1),
                ('                        feedback_lsn = transaction.end_lsn;\n', '                        progress.staged(transaction.end_lsn)?;\n',2)]
            for anchor,extra,count in replacements:
                if source.count(anchor)!=count:raise RuntimeError('unexpected receiver hook count: '+anchor.strip())
                # The hook before decode must run before the match, others after.
                source=source.replace(anchor,extra+anchor if 'match copy_data' in anchor else anchor+extra)
            if self.receiver_batch>1:
                anchor='                        progress.committing(transaction.end_lsn, transaction.commit_time_micros)?;\n'
                if source.count(anchor)!=2:raise RuntimeError('journal guard must cover both commit formats')
                source=source.replace(anchor,anchor+'                        progress.wait_for_journal(&mut destination, 8)?;\n')
            path.write_text('mod receiver_progress;\n'+source)
            (context/'worker/src/receiver_progress.rs').write_bytes(self.inputs['receiver_progress.rs'])
            sources=[path,context/'worker/src/receiver_progress.rs']
            if self.cycle_timing:
                supervisor=context/'worker/src/supervisor.rs'
                source=supervisor.read_text()
                begin=source.index('fn run_cycle(');end=source.index('// All return/error paths release',begin)
                body=source[begin:end]
                replacements=[
                    ('    let source_address = source_preflight(destination, config, busy)?;',
                     '    let timing = cycle_timing::Cycle::new(&config.destination_database, config.subscription_id);\n    let source_address = source_preflight(destination, config, busy)?;\n    timing.mark("preflight_finished");'),
                    ('    let mut published = 0;', '    timing.mark("receiver_spawned");\n    let mut published = 0;'),
                    ('        // DROP holds', '        if status.is_some() { timing.mark("receiver_exit_detected"); }\n        // DROP holds'),
                    ('            let output = Command::new(publisher)', '            timing.mark("publisher_started");\n            let output = Command::new(publisher)'),
                    ('            ensure_success("publisher", &output).map_err(CycleError::Retry)?;',
                     '            timing.mark("publisher_finished");\n            ensure_success("publisher", &output).map_err(CycleError::Retry)?;')]
                for old,new in replacements:
                    if body.count(old)!=1:raise RuntimeError('unexpected supervisor timing hook: '+old)
                    body=body.replace(old,new)
                if self.receiver_batch>1:
                    anchor='.env("LAKE_MAX_TRANSACTIONS", "1")'
                    if body.count(anchor)!=1:raise RuntimeError('receiver transaction limit hook not unique')
                    body=body.replace(anchor,'.env("LAKE_MAX_TRANSACTIONS", "'+str(self.receiver_batch)+'")')
                supervisor.write_text('mod cycle_timing;\n'+source[:begin]+body+source[end:])
                module=context/'worker/src/cycle_timing.rs';module.write_bytes(self.inputs['cycle_timing.rs'])
                sources.extend([supervisor,module])
            if self.staging_gate:
                sources.append(self.staging_gate.prepare(context))
            for p in sources:
                relative=str(p.relative_to(context));self.sources[relative]=hashlib.sha256(p.read_bytes()).hexdigest()
                dest=q.RESULTS/'receiver-sources'/relative;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(p.read_bytes())
            (context/'Dockerfile').write_bytes(self.inputs['Dockerfile.receiver-progress'])
            builder='lake-replication-receiver-progress-builder:'+q.RUN
            built=q.command(['docker','build','-t',builder,str(context)],timeout=1200)
            (q.RESULTS/'receiver-build.log').write_text(built.stdout+built.stderr)
            extra=('COPY --from='+builder+' /src/target/release/lake-replication-supervisor /usr/local/bin/lake-replication-supervisor\n') if self.cycle_timing else ''
            if self.staging_gate:extra+=self.staging_gate.overlay(builder)
            (context/'Dockerfile.overlay').write_text('FROM '+q.DESTINATION_IMAGE+'\nUSER root\nCOPY --from='+builder+' /src/target/release/lake-replication-receiver /usr/local/bin/lake-replication-receiver\n'+extra+'LABEL io.lake-replication.diagnostic="receiver-progress"\n')
            q.command(['docker','build','-f',str(context/'Dockerfile.overlay'),'-t',q.DESTINATION_IMAGE,str(context)],timeout=300)
            if self.staging_gate:
                result=q.command(['docker','run','--rm','--network','none','--entrypoint','sha256sum',q.DESTINATION_IMAGE,'/qualification/staging_gate.so'],timeout=60)
                self.staging_gate.binary_sha256=result.stdout.split()[0]
                (q.RESULTS/'staging-gate-binary.sha256').write_text(result.stdout)

    def install(self):
        self.q.sql('destination','analytics',self.inputs['receiver_progress.sql'].decode())
        (self.q.RESULTS/'receiver-installed.sql').write_bytes(self.inputs['receiver_progress.sql'])
        self.installed=True

    def capture(self):
        self.captured=True
        try:
            result=self.q.compose('exec','-T','destination','psql','-XqAt','-U','postgres','-d','analytics','-c',SQL,timeout=10)
            self.final=json.loads(result.stdout)
            (self.q.RESULTS/'receiver-final.json').write_text(json.dumps(self.final,indent=2)+'\n')
        except Exception as error:self.final={'error':str(error)}
        if self.cycle_timing:
            try:self.capture_cycles()
            except Exception as error:
                (self.q.RESULTS/'cycle-capture-error.txt').write_text(str(error)+'\n')

    def capture_cycles(self):
        q=self.q
        timings=[]
        for database in ['analytics','fair_stream_1','fair_stream_2']:
            result=q.compose('exec','-T','destination','psql','-XqAt','-U','postgres','-d',database,'-c',
                "SELECT jsonb_build_object('database',current_database(),'database_oid',(SELECT oid FROM pg_database WHERE datname=current_database()),'transactions',(SELECT coalesce(jsonb_agg(jsonb_build_object('xid',xid,'end_lsn',end_lsn,'source_commit_epoch',extract(epoch FROM commit_time),'journal_insert_epoch',extract(epoch FROM staged_at),'publication_commit_epoch',extract(epoch FROM pg_xact_commit_timestamp(xmin)))),'[]'::jsonb) FROM lake_replication.transactions WHERE state='published'))",timeout=15)
            timings.append(json.loads(result.stdout))
        (q.RESULTS/'cycle-transactions.json').write_text(json.dumps(timings,indent=2)+'\n')
        result=q.compose('exec','-T','destination','psql','-XqAt','-U','postgres','-d','analytics','-c',
            "SELECT coalesce(jsonb_object_agg(name,pg_read_file('/worker-logs/'||name)),'{}'::jsonb) FROM pg_ls_dir('/worker-logs') f(name) WHERE name ~ '^lake_replication_db[0-9]+_[0-9]+_supervisor[.]log$'",timeout=20)
        (q.RESULTS/'cycle-worker-logs.json').write_text(result.stdout+'\n')

    def metadata(self):
        return {'scope':('Isolated receiver lifetime/journal backpressure experiment; durable feedback ordering retained' if self.receiver_batch>1 else 'Receiver telemetry overlay; durable feedback ordering retained'),
                'cycle_timing':self.cycle_timing,'receiver_transaction_limit':self.receiver_batch,'journal_pending_limit':8 if self.receiver_batch>1 else None,'inputs':{n:hashlib.sha256(b).hexdigest() for n,b in self.inputs.items()},
                'overlaid_sources':self.sources,'final':getattr(self,'final',None)}

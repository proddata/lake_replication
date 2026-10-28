"""Build a disposable receiver-only telemetry overlay, preserving durable logic."""
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

SQL = "SELECT coalesce(jsonb_agg(s),'[]'::jsonb) FROM receiver_progress.snapshots() s"


class Prototype:
    def __init__(self,q,root):
        self.q,self.root=q,root
        self.installed=self.captured=False
        self.inputs={name:(root/'tests/admission'/name).read_bytes() for name in
            ['receiver_progress.py','receiver_progress.rs','receiver_progress.sql','Dockerfile.receiver-progress']}
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
            path.write_text('mod receiver_progress;\n'+source)
            (context/'worker/src/receiver_progress.rs').write_bytes(self.inputs['receiver_progress.rs'])
            for p in [path,context/'worker/src/receiver_progress.rs']:
                relative=str(p.relative_to(context));self.sources[relative]=hashlib.sha256(p.read_bytes()).hexdigest()
                dest=q.RESULTS/'receiver-sources'/relative;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(p.read_bytes())
            (context/'Dockerfile').write_bytes(self.inputs['Dockerfile.receiver-progress'])
            builder='lake-replication-receiver-progress-builder:'+q.RUN
            built=q.command(['docker','build','-t',builder,str(context)],timeout=1200)
            (q.RESULTS/'receiver-build.log').write_text(built.stdout+built.stderr)
            (context/'Dockerfile.overlay').write_text('FROM '+q.DESTINATION_IMAGE+'\nUSER root\nCOPY --from='+builder+' /src/target/release/lake-replication-receiver /usr/local/bin/lake-replication-receiver\nLABEL io.lake-replication.diagnostic="receiver-progress"\n')
            q.command(['docker','build','-f',str(context/'Dockerfile.overlay'),'-t',q.DESTINATION_IMAGE,str(context)],timeout=300)

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

    def metadata(self):
        return {'scope':'Receiver-only file telemetry overlay; no checkpoint/acknowledgement semantics changed',
                'inputs':{n:hashlib.sha256(b).hexdigest() for n,b in self.inputs.items()},
                'overlaid_sources':self.sources,'final':getattr(self,'final',None)}

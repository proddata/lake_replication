"""Aggregate fixture MinIO S3 traces without retaining headers, paths or bodies."""
from collections import Counter
import json
import subprocess
import tempfile
import threading
import time
from urllib.parse import parse_qs


class ObjectTrace:
    def __init__(self,probe,label):
        self.probe=probe;self.name=probe.project+'-trace-'+label
        self.ready=threading.Event();self.ended=threading.Event()
        self.counts={};self.error=None;self.process=None;self.reader=None
        self.shapes=[]
        self.stderr=tempfile.TemporaryFile(mode='w+')
        self.marker='/lake-push/benchmark-observer/'+label+'/'

    def start(self):
        script='mc alias set observed http://storage:9000 lake-push push-fixture-only >/dev/null; exec mc admin trace --json --call s3 observed'
        args=self.probe.compose+['run','--rm','--no-deps','--name',self.name,'--entrypoint','/bin/sh','storage-init','-ec',script]
        self.process=subprocess.Popen(args,text=True,stdout=subprocess.PIPE,stderr=self.stderr)
        self.reader=threading.Thread(target=self.read);self.reader.start()
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            if self.error:raise RuntimeError(self.error)
            if self.process.poll() is not None:raise RuntimeError('object trace exited before its readiness witness')
            self.witness('ready')
            if self.ready.wait(.2):return
        raise RuntimeError('object trace did not observe its readiness witness; record shapes='+repr(self.shapes))

    def witness(self,kind):
        # A deliberately missing object generates a harmless, excluded trace
        # witness. This root credential belongs only to the private fixture.
        self.probe.command(['docker','exec',self.name,'mc','stat','observed'+self.marker+kind],required=False)

    def read(self):
        buffer='';decoder=json.JSONDecoder();total=0
        try:
            for line in self.process.stdout:
                buffer+=line
                if len(buffer)>1048576:raise RuntimeError('object trace record exceeds byte budget')
                while buffer.strip():
                    buffer=buffer.lstrip()
                    try:record,end=decoder.raw_decode(buffer)
                    except json.JSONDecodeError:break
                    buffer=buffer[end:]
                    if len(self.shapes)<3:self.shapes.append(dict(keys=sorted(record),path_type=type(record.get('path')).__name__,witness_path='benchmark-observer' in str(record.get('path',''))))
                    if record.get('status')=='error':raise RuntimeError('object trace reported an error')
                    if 'api' not in record or 'path' not in record:continue
                    path='/'+record['path'].lstrip('/')
                    query=parse_qs(record.get('query',''))
                    prefix=query.get('prefix',[''])[0]
                    witness=path if path.startswith(self.marker) else '/lake-push/'+prefix.lstrip('/')
                    if witness.startswith(self.marker):
                        if witness.endswith('/ready'):self.ready.set()
                        if witness.endswith('/end'):self.ended.set()
                        continue
                    total+=1
                    if total>1000000:raise RuntimeError('object trace request budget exceeded')
                    category='source_capture' if path.startswith('/lake-push/iceberg/') or prefix.startswith('iceberg/') else 'destination_publish' if path.startswith('/lake-push/destination/') or prefix.startswith('destination/') else 'bucket_or_other'
                    counters=self.counts.setdefault(category,Counter())
                    counters['requests']+=1;counters['api:'+record['api']]+=1
                    counters['status:'+str(record.get('statusCode',0))]+=1
                    stats=record.get('callStats') or {}
                    counters['request_bytes']+=int(stats.get('rx',0));counters['response_bytes']+=int(stats.get('tx',0))
                    kind='data_parquet' if path.endswith('.parquet') else 'metadata_json' if path.endswith('.json') else 'metadata_avro' if path.endswith('.avro') else 'discovery_or_other'
                    counters['kind:'+kind+':requests']+=1
                    counters['kind:'+kind+':request_bytes']+=int(stats.get('rx',0))
                    counters['kind:'+kind+':response_bytes']+=int(stats.get('tx',0))
                    counters['request_duration_ns_sum']+=int(record.get('duration',0))
            if buffer.strip():raise RuntimeError('object trace ended with a partial record')
        except Exception as error:self.error=str(error)

    def finish(self):
        self.witness('end')
        if not self.ended.wait(10):raise RuntimeError('object trace did not observe its completion witness')
        self.close()
        if self.error:raise RuntimeError(self.error)
        return dict(scope='observed fixture S3 requests by object prefix; bucket-wide requests remain separate',
            readiness_witness=True,completion_witness=True,categories={key:dict(value) for key,value in sorted(self.counts.items())})

    def close(self):
        if self.process is not None:
            self.probe.command(['docker','stop','-t','2',self.name],required=False)
            try:self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:self.process.kill();self.process.wait(timeout=10)
            if self.reader:self.reader.join(timeout=10)
            self.process.stdout.close();self.process=None
        self.stderr.close()

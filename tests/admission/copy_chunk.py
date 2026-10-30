"""Disposable archived-worker configuration overlay; no rebuilt worker binary."""
import hashlib

BYTE_BUDGET=8*1024*1024


class Overlay:
    def __init__(self,q,rows,pause_ms=0):
        if rows not in (1000,2000,4000):raise ValueError('unsupported diagnostic row cap')
        if pause_ms not in (0,40):raise ValueError('unsupported diagnostic post-commit pause')
        self.q,self.rows,self.pause_ms=q,rows,pause_ms
        self.metadata=dict(chunk_rows=rows,chunk_row_text_bytes=BYTE_BUDGET,
            pause_after_chunk_ms=pause_ms,pause_placement='after_managed_transaction_commit',
            scope=__doc__,single_oversized_row_behavior='unchanged')

    def build(self,context):
        q=self.q
        executable='/usr/local/bin/lake-replication-initial-sync'
        original=executable+'-archived'
        self.metadata['base_destination_image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',q.DESTINATION_IMAGE]).stdout.strip()
        before=q.command(['docker','run','--rm','--entrypoint','sha256sum',q.DESTINATION_IMAGE,executable]).stdout.split()[0]
        wrapper=('#!/bin/bash\n'
            f'export LAKE_INITIAL_COPY_CHUNK_ROWS={self.rows}\n'
            f'export LAKE_INITIAL_COPY_CHUNK_BYTES={BYTE_BUDGET}\n'
            f'export LAKE_INITIAL_COPY_CHUNK_PAUSE_MS={self.pause_ms}\n'
            f'exec -a {executable} {original} "$@"\n')
        (context/'copy-chunk-wrapper').write_text(wrapper)
        recipe=f'FROM {q.DESTINATION_IMAGE}\nUSER root\nRUN mv {executable} {original}\nCOPY --chmod=0755 copy-chunk-wrapper {executable}\n'
        (context/'Dockerfile.copy-chunk').write_text(recipe)
        q.command(['docker','build','-t',q.DESTINATION_IMAGE,'-f',str(context/'Dockerfile.copy-chunk'),str(context)],timeout=300)
        after=q.command(['docker','run','--rm','--entrypoint','sha256sum',q.DESTINATION_IMAGE,original]).stdout.split()[0]
        if before!=after:raise ValueError('archived initial-sync binary changed')
        self.metadata.update(worker_sha256=after,wrapper_sha256=hashlib.sha256(wrapper.encode()).hexdigest())
        (q.RESULTS/'copy-chunk-wrapper').write_text(wrapper)
        (q.RESULTS/'Dockerfile.copy-chunk').write_text(recipe)

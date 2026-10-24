# Streaming admission: reproduced engine OOM

Historical bulk-only candidate evidence. The four-slot streaming policy is now
adopted and locally package-qualified in the [production beta.3 candidate](stream-pool-migration-results.md).

The production beta.3 bulk-only candidate does not close the resource gate.
Its sixteen-subscription test reproduced a fatal query-engine OOM at the
configured 2 GB limit. A separate four-slot streaming prototype passes three
complete repetitions of that workload. It remains experimental; production
streaming is still unrestricted.

## Finding

The final arm64 archive has SHA256
`ea10f47b5cedf7084fa34dd682c2df14d23f21d5c9d5e3bf2a312411e4a7ec37`.
It includes the production bulk permit and worker package version beta.3. In the
four-database/sixteen-subscription fixture, every initial copy reached streaming.
A live-update check then timed out with the first target still holding its
initial row. A separate repeat passed live updates but failed after destination
PostgreSQL restart and another update burst.

The repeated failure retained the engine's explicit error: it could not allocate
76.5 MiB with approximately 1.7 GiB of its 1.8 GiB limit used. The container exited
139 with `OOMKilled=false`; the log records unrecoverable DuckDB OOM termination.
This distinguishes an internal query-engine failure from a Docker memory kill.
The supervisor logs show publishers backing off after the engine became
unavailable. The restart workload changes PostgreSQL; it does not deliberately
restart the engine.

The first failed run lost its compose log because the outer harness captured
logs again after the shared fixture had already removed its containers. Its
engine exit cause cannot be independently established. The harness now preserves
existing logs, and the shared scale fixture captures worker logs and container
states before cleanup. The repeated failure supplies the retained OOM evidence.
Earlier passing diagnostics and paired single-stream throughput measurements did
not establish that sixteen streaming writers always fit in 2 GB.

## Narrower prototype

The separate overlay keeps the production bulk permit and adds a pool of four
transaction-scoped streaming permits to batch publication and change-run apply.
It uses four instance-wide PostgreSQL advisory lock tags (objects 3–6 in the
existing admission namespace). A transaction reuses its held slot, otherwise
tries any available slot, then waits in one slot's native queue when all are
occupied. This bounds admitted managed streaming transactions; it does not
provide one global FIFO or reserve engine memory.

Four lock assertions verify four reentrant holders, a waiting fifth transaction,
interruptible cancellation and rollback cleanup. Three complete repetitions of
the four-database/sixteen-subscription fixture pass initial copy, exact live and
restarted data, worker ownership, disable/drop and source-slot cleanup. The
four-copy/three-stream coexistence diagnostic also passes: eight million copied
rows in 35.80 seconds, streaming p95 342–355 ms, exact final data and valid timing
audits. These remain single diagnostics, not paired performance estimates.

This overlay recompiles a diagnostic image and appends experimental SQL. It is
not a release package or a production streaming migration. Its passing repeats
do not establish performance equivalence, fairness under sustained overload,
arbitrary transaction lock ordering, a total memory bound or a capacity ceiling.
Before adoption, qualify paired steady/catch-up and copy overhead, copy/stream
coexistence, cross-database cancellation/rollback and busy-pool progress. Then
promote the chosen policy and repeat native package qualification and the soak.

[Retained diagnostic evidence](evidence/2026-10-23-stream-pool-results.json)
includes 216 checks across archive/lock handling and three scale repetitions,
plus twelve checks in the four-copy run. All are passing. Raw result, log and
source hashes are retained; full private fixture logs are not committed. The
failed prototype startup caused by an OID arithmetic expression is retained and
excluded from accepted measurements. All containers and volumes were removed.
The [production migration evidence](evidence/2026-10-23-bulk-admission-migration-results.json)
retains the failed candidate and the reproduced engine exit state separately.

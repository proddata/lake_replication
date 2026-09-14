# pg_lake_replication

An early feasibility project for replicating PostgreSQL heap tables into
`pg_lake` Iceberg tables. The requirements are in [project_brief.md](project_brief.md).
The current prototype includes initial snapshot synchronization, a logical
replication receiver, a micro-batch publisher, and a supervising service loop;
it is not a production service or PostgreSQL extension yet.

Initial lab result: PostgreSQL 18 rejects native Iceberg subscription targets
with SQLSTATE `42809`. Direct Iceberg SQL, basic crash recovery, and the heap
subscription control passed. Ten separate inserts produced ten snapshots/files
versus one for a batch. Precise local-catalog crash probes also passed and expose
the expected ambiguous-success window after commit. See the feasibility report
for evidence, scope, and remaining gates.

## Milestone 0 laboratory

The lab builds pinned upstream `pg_lake` against PostgreSQL 18 and runs one
cluster with separate `application` and `analytics` databases, `pgduck_server`,
and MinIO. PostgreSQL is published only on loopback port `55434` for the Rust
receiver integration probe. Commands run through Docker Compose.
The credentials and trust authentication are exclusively for this isolated lab.

Prerequisites: Docker Compose, Python 3, Make, internet access, and disk space
for a C/C++ development image. The initial build compiles DuckDB and Azure SDK
dependencies and can take a substantial amount of time. Compilation defaults
to two jobs to limit memory use.

```sh
make lab-build
make lab-up
make lab-test
make lab-crash-test
make journal-test
make transport-test
make publisher-test
make mutations-test
make streaming-test
make toast-test
make initial-sync-test
make multi-table-test
make auto-schema-test
make schema-evolution-test
make rebuild-test
make supervisor-test
make source-continuity-test
make lab-logs
make lab-stop
```

`lab-test` creates uniquely named test schemas and preserves tables and evidence
under `.lab/probe_*/`. It stops and removes its own subscriptions and source
slots after probing. Exit status 0 means the implemented basic probes passed;
2 means native replication was blocked; 1 means a test or environment failed.
None of these results alone establishes replication correctness.

The probe covers direct inserts, updates, deletes, large values, rollback,
transaction visibility to another PostgreSQL session, backend termination
before commit, PostgreSQL SIGKILL/restart after a catalog-plus-checkpoint commit,
and truncate. It compares ten separate commits with one batch, verifies a
heap-to-heap replication control, then attempts both native initial copy and
native apply into Iceberg without initial copy. Metadata JSON and file
inventories are captured between direct operations. Native mutation checks run
only if the first row reaches the destination.

`lab-crash-test` uses development-image-only instrumentation to terminate a
backend immediately before upload completion, immediately after upload
completion but before PostgreSQL commit, and from the post-commit callback.
Each crash makes the postmaster terminate and recover all server child processes.
Do not use the image or these probes with data you need to keep.

Milestone 1 has started with a destination journal migration and a
dependency-free Rust pgoutput decoder. `journal-test` exercises durable staging,
idempotent and conflicting replays, LSN acknowledgement bounds, atomic local
Iceberg publication, and post-commit reconciliation. See
[the Milestone 1 notes](docs/milestone-1.md). On a new lab volume the journal
migration is installed automatically; `make journal-install` is available for
an existing volume that predates it.

Milestone 2 adds a dependency-free PostgreSQL wire client and one-shot receiver.
`transport-test` creates a real pgoutput publication and slot, stages source
transactions, confirms the slot advances only to the durable journal position,
restarts the receiver, and cleans up its source objects. See
[the Milestone 2 notes](docs/milestone-2.md).

Milestone 3 closes the insert-only path from source heap tables through the
durable journal into Iceberg. `publisher-test` proves deterministic
micro-batching, source transaction boundaries, snapshot reduction, and recovery
from an ambiguous post-commit connection loss. See
[the Milestone 3 notes](docs/milestone-3.md).

Milestone 4 extends the live path to UPDATE, DELETE, primary-key changes, and
TRUNCATE. Mutation application requires an identity that matches exactly one
destination row and otherwise rolls the batch back. See
[the Milestone 4 notes](docs/milestone-4.md).

Milestone 5 adds protocol-v4 streamed transactions with durable per-segment
spill, commit/abort handling, and mid-stream receiver restart recovery. The lab
uses PostgreSQL's immediate-streaming developer setting to make segment coverage
deterministic. See [the Milestone 5 notes](docs/milestone-5.md).

Milestone 6 validates true external TOAST values and unchanged-TOAST update
semantics through Iceberg, including external, inline, and NULL transitions. See
[the Milestone 6 notes](docs/milestone-6.md).

Milestone 7 adds a slot-exported snapshot copy with durable table progress,
atomic Iceberg replacement and checkpoint initialization, explicit failed-copy
rebuilds, and WAL catch-up for writes committed during the copy. See
[the Milestone 7 notes](docs/milestone-7.md).

Milestone 8 replaces in-memory snapshot materialization with PostgreSQL COPY
streaming and row/byte-bounded durable chunks, including robust framing for
escaped and multibyte text. See [the Milestone 8 notes](docs/milestone-8.md).

Milestone 9 discovers effective publication tables, column lists, and row
filters, then atomically cuts over every table at one shared snapshot boundary.
See [the Milestone 9 notes](docs/milestone-9.md).

Milestone 10 adds versioned source type contracts, automatic creation of missing
Iceberg destinations, and fail-fast validation of pre-created tables. See
[the Milestone 10 notes](docs/milestone-10.md).

Milestone 11 reconciles live pgoutput relation versions, automatically appends
safe nullable columns, and pauses incompatible changes before their WAL is
staged or acknowledged. See [the Milestone 11 notes](docs/milestone-11.md).

Milestone 12 adds an operator-controlled replacement subscription: a new slot
and shadow Iceberg snapshot catch up before an atomic table promotion, while the
paused table and slot remain intact until verified cleanup. See
[the Milestone 12 notes](docs/milestone-12.md).

Milestone 13 adds a long-running supervisor around bounded receiver and publisher
cycles, with exponential retry, duplicate-process fencing, durable health and
queue reporting, subscription-aware pausing, and SQL-controlled graceful stop.
See [the Milestone 13 notes](docs/milestone-13.md).

Milestone 14 keeps an established supervisor alive across destination control
connection loss, safely reacquires subscription ownership, records reconnection
telemetry, and resumes replication. See
[the Milestone 14 notes](docs/milestone-14.md).

Milestone 15 validates the source system/database identity, logical slot safety,
durable checkpoint boundary, and timeline ancestry before every receiver cycle,
failing closed without consuming WAL when continuity cannot be proven. See
[the Milestone 15 notes](docs/milestone-15.md).

The lab retains PostgreSQL and object data in named volumes. `lab-stop` preserves
them. Rebuilding the image does not reinitialize existing databases. Avoid using
this Compose project for unrelated data: failure experiments target it.

See [the feasibility report](docs/feasibility.md) for evidence, limitations, and
remaining experiments, and [the dependency inventory](docs/dependencies.md) for
source and licensing notes. `make check` runs offline probe-classification tests
and validates Python syntax and Compose configuration; it does not run integration
tests.

# Open-Source PostgreSQL-to-Iceberg Replication with `pg_lake`

## Purpose

Build an open-source system that continuously replicates ordinary PostgreSQL heap tables into writable Apache Iceberg tables managed by [`pg_lake`](https://github.com/Snowflake-Labs/pg_lake).

The intended feature set is based on the behavior publicly documented for [Crunchy Data Warehouse Postgres-to-Iceberg Data Sync](https://docs.crunchybridge.com/warehouse/replication), but the implementation must be independently developed from public specifications and permissively licensed dependencies. Do not copy non-public or proprietary Crunchy Data code.

The system should feel native to PostgreSQL:

```sql
-- Source database
CREATE PUBLICATION app_publication
FOR TABLE public.users, public.orders;

-- Destination database or cluster
CREATE SUBSCRIPTION app_to_iceberg
CONNECTION 'postgres://...'
PUBLICATION app_publication
WITH (
    create_tables_using = 'iceberg'
);
```

The exact SQL extension syntax may differ during early milestones, but native `CREATE PUBLICATION` and `CREATE SUBSCRIPTION` compatibility is the desired user experience.

## Project principles

1. Use PostgreSQL logical replication and the standard `pgoutput` protocol.
2. Use open-source `pg_lake` for writable Iceberg tables and the Iceberg catalog.
3. Preserve committed source transaction order and boundaries in every visible destination state.
4. Buffer changes and publish them to Iceberg in micro-batches instead of creating an Iceberg commit for every source transaction.
5. Keep the PostgreSQL source lightly loaded. It should perform standard logical decoding, initial-copy reads, and replication-slot retention only.
6. Make replay idempotent. Crashes may cause retries but must not cause lost changes or duplicate live rows.
7. Treat PostgreSQL failover, object-storage failures, schema incompatibilities, and long outages as normal operating conditions.
8. Keep all code required to build and operate the data plane available under an OSI-approved open-source license.

## Required deployment topologies

### Separate PostgreSQL clusters

This is the recommended production topology.

```text
OLTP Patroni cluster
  ordinary heap tables
          │
          │ PostgreSQL logical replication
          ▼
Analytics Patroni cluster
  staging + pg_lake + Iceberg catalog
          │
          ▼
S3-compatible object storage
```

### Same PostgreSQL cluster, separate databases

This topology is required for development and smaller installations.

```text
One PostgreSQL/Patroni cluster
├── database: application
│   └── public.orders (heap publisher)
└── database: analytics
    └── public.orders (Iceberg subscriber)
```

PostgreSQL logical replication maps relations by schema-qualified name. Consequently, same-database heap-to-Iceberg replication is out of scope for the initial release; the source and destination may share a PostgreSQL instance but must use different databases.

When publisher and subscriber share a PostgreSQL cluster, the logical slot must be created manually in the source database and the subscription must use `create_slot = false` and the explicit `slot_name`. Automate and document this workflow.

## Functional scope

### Initial synchronization

- Create missing destination tables as `USING iceberg` when requested.
- Permit users to pre-create destination Iceberg tables.
- Validate source and destination schemas before starting.
- Copy existing table data without blocking normal source writes for the duration of the copy.
- Buffer and later apply changes generated during the initial copy.
- Expose per-table snapshot progress.
- Transition each table atomically from initial copy to continuous replication.
- Support rebuilding a table while its previous Iceberg state remains readable until replacement is ready.
- Support an explicit overwrite/rebuild option for non-empty destination tables.

### Continuous replication

- Replicate `INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` where permitted by publication settings and table identity.
- Preserve source transaction ordering.
- Never expose half of a source transaction.
- Support PostgreSQL streaming replication protocol v4 behavior for large in-progress transactions.
- Support binary transfer when source and destination types are compatible.
- Correctly handle unchanged TOAST markers and retrieve or preserve complete row values as required.
- Support publication row filters where PostgreSQL supports them.
- Support publication column lists where PostgreSQL supports them.
- Support a mixture of Iceberg and ordinary heap destination tables when explicitly configured.

### Table identity

Implement and document these policies:

| Primary key | `REPLICA IDENTITY FULL` | Required behavior |
|---|---|---|
| Yes | Yes | Full insert, update, delete, and truncate support; preferred configuration |
| Yes | No | Full replication supported, with potentially higher destination-side state or lookup cost |
| No | Yes | Initially allow insert/truncate-only publications; expand update/delete support only when correctness is demonstrated |
| No | No | Inserts may be supported; source PostgreSQL will reject update/delete where replica identity is insufficient |

Do not silently approximate updates or deletes. Reject unsupported configurations before replication starts where possible.

### Schema handling

- Detect source schema changes before applying incompatible row changes.
- Automatically handle additive columns when safe.
- Pause a relation with a clear diagnostic for incompatible type changes, generated columns, unsupported types, or destructive DDL.
- Do not allow arbitrary writes or schema changes to a destination Iceberg table while it is actively managed by replication.
- Keep relation identity independent of names so renames and PostgreSQL OID changes can be reasoned about explicitly.
- Version all source-to-Iceberg type mappings and expose them through inspection commands or views.

Generated-column replication may initially be unsupported, matching the documented limitation of the reference behavior. It must fail visibly rather than diverge silently.

## Apply and storage architecture

Do not apply each source transaction directly as a separate Iceberg snapshot. Introduce a durable staging layer on the destination side.

```text
pgoutput stream
      │
      ▼
transaction decoder
      │
      ▼
durable staging journal
      │ complete committed transactions
      ▼
micro-batch planner
      │
      ├── inserted/new row versions
      ├── equality or position deletes
      └── truncation/table replacement operations
      ▼
pg_lake Iceberg transaction
      │
      ▼
durable source-LSN checkpoint
```

The staging layer may initially use ordinary PostgreSQL heap tables in the destination database. It must record at least:

- Source PostgreSQL system identifier.
- Source database OID.
- Source timeline where available.
- Publication/subscription identity.
- Transaction ID.
- Begin and commit LSN.
- Transaction sequence/order.
- Relation identity and schema version.
- Operation type.
- Replica key or old-row identity.
- New row data.
- Commit timestamp when available.
- Apply and flush state.

The journal must distinguish received, locally durable, Iceberg-published, and source-acknowledged positions.

### Micro-batching

- Default to a configurable flush interval of approximately 30 seconds, targeting typical end-to-end lag below 60 seconds.
- Also flush when configurable row-count or byte-size thresholds are reached.
- Preserve source transaction boundaries when forming batches.
- If one source transaction exceeds a batch threshold, process it as one logical unit.
- Coalesce repeated mutations of the same key inside a batch when this does not change externally visible transaction semantics.
- Write inserts and update post-images as data rows.
- Represent deletes and update pre-images using the most interoperable Iceberg delete mechanism supported by the chosen `pg_lake` version.
- Track files affected by deletes so compaction can be selective.

### Atomicity and crash recovery

For every flush, establish a recoverable protocol covering:

1. Durable staged source transactions.
2. Object files written to temporary or final locations.
3. Iceberg metadata/catalog commit through `pg_lake`.
4. Durable advancement of the published source LSN.
5. Replication feedback to PostgreSQL.

Required invariants:

- Never acknowledge source WAL that exists only in volatile memory.
- Never advance the published checkpoint before the corresponding Iceberg catalog commit is durable.
- Retrying a completed or ambiguous flush must not duplicate live rows.
- Files orphaned by a pre-commit crash must be discoverable and safely garbage-collected.
- A catalog-committed snapshot must never reference missing data or delete files.
- Restart must resume automatically from the last provably safe position.

Store source coordinates in Iceberg snapshot metadata when `pg_lake` provides a suitable mechanism. Maintain an authoritative PostgreSQL-side checkpoint as well.

## Compaction and maintenance

- Compact insertion and deletion files automatically.
- Prioritize files that have accumulated significant changes rather than scanning the entire table.
- Keep compaction independent from replication correctness; replication must continue if compaction is paused.
- Prevent compaction from invalidating snapshots required by active readers or recovery.
- Provide configurable snapshot retention and orphan-file cleanup.
- Expose compaction queue depth, last completion time, affected bytes, and failures.
- Rate-limit maintenance to protect PostgreSQL and object-storage workloads.

## PostgreSQL and Patroni high availability

Support PostgreSQL 17 and newer first.

- Create logical slots with failover support where available.
- Document and validate `sync_replication_slots` on eligible Patroni standbys.
- Document and validate `synchronized_standby_slots` on the primary when strict failover safety is requested.
- Verify that a synchronized, persistent, non-invalidated slot exists before declaring a standby failover-ready.
- Connect through a leader-aware endpoint or rediscover the Patroni leader after disconnect.
- Store and validate the PostgreSQL system identifier. Refuse to attach silently to a different cluster.
- Detect promotion and reconnect to a valid descendant history.
- Expect at-least-once delivery around reconnects and deduplicate safely.
- Detect slot loss or invalidation and require a controlled rebuild rather than skipping ahead.
- Bound retained WAL with monitoring and explicit operator policy.

For same-cluster, separate-database deployment, ensure that subscription workers do not create a feedback loop. Only source heap tables may belong to the source publication.

## User-facing SQL and observability

Aim to provide SQL-native setup and monitoring. If PostgreSQL core syntax cannot be extended safely, expose equivalent extension functions while retaining familiar terminology.

Desired setup experience:

```sql
SELECT lake_replication.create_subscription(
    subscription_name => 'app_to_iceberg',
    connection_string => 'postgres://...',
    publication_names => ARRAY['app_publication'],
    create_tables_using => 'iceberg',
    overwrite_iceberg_data => false,
    streaming => true,
    binary => true
);
```

Desired subscription monitoring:

```text
subscription_name
state
source_system_identifier
received_lsn
staged_lsn
published_lsn
acknowledged_lsn
last_receive_time
last_flush_time
last_flush_duration
unflushed_inserts
unflushed_deletes
last_error
```

Desired per-table monitoring:

```text
subscription_name
source_relation
destination_relation
state
copy_started
copy_finished
copy_rows
copy_bytes
unflushed_inserts
unflushed_deletes
rows_inserted
rows_deleted
last_flush_lsn
last_flush_time
last_error
```

Provide these through PostgreSQL views plus Prometheus metrics. Include structured logs with subscription, relation, transaction and LSN context.

## Security

- Require a dedicated source role with `LOGIN`, `REPLICATION`, and the minimum required `SELECT` privileges.
- Require TLS for non-local source connections by default.
- Do not log connection passwords, object-storage secrets, row contents, or signed URLs.
- Support secret injection without persisting plaintext credentials in project configuration.
- Use least-privilege object-storage and Iceberg-catalog credentials.
- Treat replicated row data, WAL messages, catalog metadata, and object-store responses as untrusted input.
- Validate destination ownership and prevent one subscription from writing another subscription's tables.

## Open-source and licensing requirements

- Prefer Apache-2.0 or PostgreSQL licensing for the new project.
- `pg_lake` is expected to be used according to its open-source license; verify licenses for every bundled component and transitive native dependency.
- Do not copy implementation code from AGPL projects such as WalShadow unless the project deliberately adopts compatible AGPL licensing.
- Public documentation, PostgreSQL behavior, PostgreSQL source code under its license, and clean-room black-box compatibility tests may inform the implementation.
- Keep a dependency license inventory and automated license checks in CI.
- Clearly distinguish upstream `pg_lake` behavior from functionality implemented by this project.

## Suggested implementation shape

Use a destination-side PostgreSQL extension plus an external worker unless early experiments demonstrate that one component is sufficient.

```text
repository/
├── extension/                 PostgreSQL SQL API, catalogs and monitoring views
├── worker/                    pgoutput consumer, staging and batch orchestration
├── migrations/                Extension schema migrations
├── tests/
│   ├── sql/                   Extension behavior
│   ├── replication/           Snapshot and continuous apply
│   ├── failover/              Patroni and slot synchronization
│   ├── crash/                 Failpoint-based recovery
│   └── interoperability/      Spark, DuckDB and other Iceberg readers
├── deploy/
│   ├── docker-compose/        Local PostgreSQL, MinIO and test environment
│   └── kubernetes/            Patroni examples
└── docs/
```

Candidate implementation languages:

- Rust for the worker and any PostgreSQL extension logic where `pgrx` is appropriate.
- SQL/C only for small PostgreSQL hooks that cannot be implemented safely through existing APIs.
- Avoid introducing a JVM runtime unless required for interoperability testing.

The system should use PostgreSQL's logical decoding rather than implement physical WAL decoding.

## Delivery milestones

### Milestone 0: feasibility laboratory

- Build and run open-source `pg_lake` against PostgreSQL 17 or 18.
- Demonstrate inserts, updates, deletes, and transactions on a `USING iceberg` table.
- Test whether the stock PostgreSQL logical apply and table-sync workers can target a pre-created `pg_lake` table.
- Measure Iceberg files and snapshots created per source transaction.
- Inject crashes before and after object writes and catalog commits.
- Document all differences between stock PostgreSQL behavior and the required behavior.

Exit criterion: a written decision on whether to extend stock logical apply or implement an external `pgoutput` consumer.

### Milestone 1: insert-only replication

- One source database and one destination database.
- Manual destination table creation.
- Consistent initial copy.
- Continuous inserts through a durable staging journal.
- Micro-batched Iceberg publication.
- Persistent received/staged/published LSNs.
- Restart without loss or duplication.

### Milestone 2: mutable tables

- Primary-key updates and deletes.
- `REPLICA IDENTITY FULL` support.
- Correct unchanged-TOAST handling.
- Truncation.
- Batch coalescing.
- Selective compaction.

### Milestone 3: production lifecycle

- Automatic Iceberg target creation.
- Table addition and removal.
- Rebuild/overwrite while retaining the old readable version.
- Relation-level pause and resume.
- Additive schema evolution.
- Complete SQL monitoring views and Prometheus metrics.

### Milestone 4: high availability

- PostgreSQL 17+ failover slots.
- Patroni leader discovery and reconnection.
- Planned switchover tests.
- Unplanned failover tests.
- Slot invalidation and WAL-loss handling.
- Same-cluster, separate-database deployment automation.

### Milestone 5: performance and interoperability

- High transaction-rate tests.
- Large streaming transactions.
- Row filters and column lists.
- Binary mode.
- Automatic maintenance tuning.
- Validate tables with at least Spark, DuckDB, PyIceberg or `iceberg-go`, and one REST-catalog implementation.

## Acceptance criteria

The first stable release must demonstrate:

1. Initial copy plus continuous replication under concurrent source writes.
2. Correct insert, update, delete, and truncate behavior for supported table identities.
3. No partial visibility of source transactions.
4. No lost or duplicated live rows after process, PostgreSQL, network, catalog, or object-store failures.
5. Automatic continuation after a safe Patroni promotion.
6. Explicit failure rather than silent divergence after slot loss, incompatible schema changes, or unsupported types.
7. Configurable micro-batch publication with typical lag below 60 seconds under the documented reference workload.
8. Automatic compaction that does not block ingestion correctness.
9. Query compatibility with independent Iceberg readers.
10. Reproducible builds, dependency/license inventory, operator documentation, and a complete local integration environment.

## Required test strategy

- Differentially compare destination state with repeatable source snapshots at known LSN barriers.
- Generate randomized transactional workloads with inserts, updates, deletes, aborts, savepoints, truncates, and long transactions.
- Run crash failpoints at every state transition in the flush protocol.
- Restart the source, destination, worker, object store, and catalog independently.
- Test network partitions and ambiguous commit responses.
- Test table copy while mutations continue.
- Test TOAST transitions between inline, external, null, and unchanged values.
- Test primary-key changes.
- Test concurrent destination readers during flush, compaction, rebuild, and failover.
- Test PostgreSQL and Patroni upgrades separately from ordinary failover.
- Verify that retained WAL and local staging storage remain bounded by configured policies.

Every correctness bug should result in a permanent regression test.

## Initial Codex task

When beginning implementation, do not attempt the complete system immediately. Start with Milestone 0:

1. Inspect the current open-source `pg_lake` repository and supported PostgreSQL versions.
2. Create a reproducible local environment with two PostgreSQL databases, `pg_lake`, and S3-compatible object storage.
3. Pre-create a writable Iceberg destination table.
4. Attempt native logical replication from a heap source table into that destination.
5. Exercise initial copy, insert, update, delete, truncate, multi-statement transactions, TOAST values, and restart.
6. Inspect the resulting Iceberg snapshots, data files, and delete files.
7. Add failure injection around destination commits.
8. Produce a feasibility report and recommend either:
   - extending PostgreSQL's native apply path, or
   - implementing an external `pgoutput` consumer with a durable staging layer.

Do not infer success from a simple replicated insert. The feasibility decision must be based on transaction atomicity, update/delete correctness, file amplification, checkpoint safety, and crash recovery.

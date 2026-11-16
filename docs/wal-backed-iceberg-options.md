# WAL-backed replication to Iceberg: journal options

Status: discussion draft, 15 November 2026. This records the options from the
design discussion; it does not select a replacement for the default pipeline.

## Goal

An application PostgreSQL instance should be able to replicate its heap tables
into Iceberg without requiring a separate destination PostgreSQL instance.
Reduce repeated storage, WAL generation and conversion work while retaining
transaction boundaries, replay correctness and controlled schema evolution.

The question is whether the original WAL can replace our bulk PostgreSQL
change journal, or whether that journal should instead live in Iceberg/S3.

Two decisions are independent:

1. Where do decoded changes become durable enough to acknowledge source WAL?
2. Where do changes get applied to the final analytical tables?

One PostgreSQL instance can still use background workers and the pgduck engine.
Moving apply elsewhere introduces analytical compute, but need not introduce
another PostgreSQL. Removing a journal does not remove final-table apply work.

## Baseline discussed: PostgreSQL JSONB journal

```text
Application heap + WAL
        ↓ logical decoding / pgoutput
Rust receiver
        ↓ JSON change envelope
PostgreSQL JSONB journal + staging checkpoint
        ↓ acknowledge durable staging
Publisher + pgduck
        ↓
Final Iceberg tables + publication checkpoint
```

The journal holds a full transaction payload and individual change records.
Those heap writes also generate WAL. Publication and control-state updates
generate further PostgreSQL writes/WAL. These are logical writes, not necessarily
separate physical disk flushes per row: PostgreSQL buffers pages and groups WAL
flushes.

Non-null tuple cells normally remain PostgreSQL text values inside the JSON
envelope, including numeric and JSONB column values. Publication reconstructs
typed rows; JSONB document columns need explicit parsing of their enclosed text.

The journal provides a durable replay point after source acknowledgement,
decouples capture from publication, and supports SQL-based validation and retry.
It is a useful implementation choice, not a fundamental replication requirement.

## Option A: retain the PostgreSQL journal

Keep the baseline and optimize its representation and write path.

- Durable handoff: PostgreSQL journal commit.
- Source acknowledgement: after durable staging, before final publication.
- Final apply: local pgduck/publisher or a separate destination deployment.
- Benefit: reuse the existing recovery, admission and publication machinery.
- Cost: bulk journal heap writes, their WAL, duplicated representations and
  conversion work remain. A more compact encoding could reduce some costs but
  would not remove the extra durability stage.

This remains the control for evaluating the alternatives.

## Option B: retain WAL until direct final-table publication

```text
Application heap + WAL protected by a logical slot
        ↓ decode committed changes
Bounded memory / disposable local spill
        ↓ publish
Final Iceberg tables + durable publication checkpoint
        ↓ acknowledge source WAL
```

Do not persist every decoded row into PostgreSQL journal tables. Keep the source
WAL and required decoding history until the sink publication commits. On failure,
decode and replay from the durable publication boundary.

Small durable PostgreSQL metadata may remain: source identity/generation,
relation descriptors, schema versions, subscription ownership and checkpoints.
This option removes the bulk row-data journal, not every replication table.

Benefits:

- Fits the single-PostgreSQL goal and continues to produce final Iceberg mirrors.
- Avoids the additional PostgreSQL row-data journal and its WAL.
- Can initially retain the current in-memory change representation and publisher
  semantics, isolating the durability change from a later encoding optimization.

Trade-offs:

- Object-store outages retain more source WAL; the journal no longer provides an
  independently acknowledged staging boundary.
- Crashes can require decoding again. Temporary buffers/spill do not authorize
  acknowledgement and must be reconstructible from retained history.
- Large and streamed transactions require bounded buffering/spill and correct
  commit/abort handling. An open transaction can require history earlier than
  the most recently acknowledged commit.
- Final-table UPDATE/DELETE and Iceberg metadata work still run locally.

Persisting spill files and acknowledging against them would create another
durable journal design. That is a separate choice, with its own fsync, recovery
and cleanup protocol; it must not happen accidentally under this option.

## Option C: capture into Iceberg/S3 change logs

```text
Application heap + retained WAL
        ↓ decode / temporary batch files
Iceberg per-table change logs + ordered metadata log
        ↓ durable capture commit and receipt
Acknowledge source WAL
        ↓ apply independently
Final analytical tables
```

Replace the PostgreSQL bulk journal with an append-oriented change log in object
storage. The log records operations, transaction coordinates and schema versions;
it is distinct from the final mirrored table contents. Publish batches rather
than one object-store request per application row.

The handoff must atomically identify a complete captured batch and its source
boundary. A successful file upload alone is insufficient. Required objects and
catalog/manifest state must be durable and usable for replay before acknowledgement.

Two placements for apply are possible:

| Apply placement | Fit and trade-off |
| --- | --- |
| Local publisher/pgduck on the same PostgreSQL instance | Preserves the deployment goal and releases source WAL after capture. Adds Iceberg log writes/reads while keeping final-table apply cost on the application side. |
| External analytical engine or apply service | Moves final-table work away from the application instance and allows independent batching/scaling. Requires that compute and a defined log-consumer protocol; a destination PostgreSQL is one possible consumer, not a requirement. |

Benefits include replay from retained captured batches and separating capture
progress from final-table application. Costs include extra object writes,
metadata, retention/garbage collection and a second logical data stage. This is
not automatically cheaper than direct final-table publication.

This follows the capture/apply separation described in the
[Snowflake engineering article](https://www.snowflake.com/en/blog/engineering/postgres-to-snowflake-replication-mirroring/).
It does not imply zero PostgreSQL bookkeeping/WAL or copying Snowflake's complete
implementation. Its final apply runs in Snowflake; a local implementation must
explicitly choose its own apply placement.

Catalog availability is part of this option. Our qualified local PostgreSQL
catalog can coordinate PostgreSQL/Iceberg transactions, but object files alone
do not provide an independent external discovery/commit service. A consumer may
still need source-catalog access. Fully independent object-store consumption
needs a qualified catalog/manifest publication and recovery protocol.

The existing [source-push protocol draft](push-protocol.md) is relevant prior
work, not proof that a journal-free or independently discoverable variant exists.
Importing its logs back into a PostgreSQL row journal would retain that journal's
write amplification at the importer.

## Reference alternative: external physical-WAL decoding

A WalShadow-inspired design ships physical WAL to external decoding processes,
using a PostgreSQL catalog/recovery shadow to interpret historical tuples, then
writes to its destination. It moves decoding work off the application instance.
See [WalShadow's architecture](https://github.com/ClickHouse/walshadow/blob/main/architecture/README.md).

For this project, an Iceberg sink, its commit/checkpoint protocol and schema
handling would still be needed. The additional shadow PostgreSQL instance also
conflicts with a strict requirement for only one PostgreSQL instance.
This is a larger architectural alternative, rather than a prerequisite for
either WAL-backed direct publication or Iceberg change-log capture.

## Comparison

| Design | Bulk row-data journal | Acknowledge source after | Replay authority after acknowledgement | Final apply |
| --- | --- | --- | --- | --- |
| A: current-style journal | PostgreSQL JSONB tables | Journal commit | PostgreSQL journal/checkpoints | Local or separate destination |
| B: WAL-backed direct publication | None beyond retained source WAL and disposable buffers | Final-table publication + checkpoint | Final sink/checkpoint; unacknowledged work still relies on WAL | Local for the single-instance variant |
| C: Iceberg change logs, local apply | Iceberg/S3 logs | Durable capture commit/receipt | Captured logs + metadata/catalog state | Local |
| C: Iceberg change logs, external apply | Iceberg/S3 logs | Durable capture commit/receipt | Captured logs + metadata/catalog state | External analytical compute |

For every option, loss of required replay history needs an explicit failure or
rebuild protocol. Acknowledged history cannot be discarded merely because the
capture process has moved on; downstream progress and retention policy matter.

## Schema changes are a separate requirement

Physical WAL contains the logged effects of persistent schema/catalog changes;
it is not a convenient history of original DDL statements. Standard `pgoutput`
provides relation descriptions and row events, not general DDL replication.
See PostgreSQL's [logical replication restrictions](https://www.postgresql.org/docs/18/logical-replication-restrictions.html)
and [message formats](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).

A journal-free design still needs:

- Historical descriptors sufficient to interpret each decoded row, with stable
  relation identity and schema version. Querying only today's catalogs is not
  a substitute for historical interpretation.
- A defined ordering between schema effects and data in the same transaction.
- An explicit supported DDL subset and failure behavior for unsupported changes.
- Snapshot/stream alignment and a generation boundary for rebuilds.

Possible mechanisms include versioned relation metadata, transactionally captured
schema events/logical messages, or native source hooks. Relation-message changes
alone do not describe every default, constraint, rename/drop or schema-only
transaction. Storing row changes as JSONB does not solve this gap either.

Small schema/control records can remain durable even when bulk row journaling
is removed. PostgreSQL logical slots protect required WAL and catalog history;
that protection has resource and operational costs, not unlimited retention.
See [logical decoding concepts](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html).

## Recovery rules to preserve

Use explicit progress boundaries: decoded (possibly volatile), durably captured
if there is a capture log, and durably published/applied. Never acknowledge a
source boundary beyond the durable handoff selected by the design.

| Failure point | Required behavior |
| --- | --- |
| Before the durable handoff commits | Do not advance source acknowledgement; discard/rebuild provisional buffers and retry from protected history. |
| Objects uploaded, commit outcome unknown | Reconcile against the authoritative batch receipt/checkpoint; file existence alone does not prove publication. |
| Handoff committed, acknowledgement lost | Recognize the committed boundary and avoid applying replayed changes twice. |
| Object storage unavailable | Backpressure within explicit memory/spill/WAL budgets; report lag and disk pressure. |
| Required WAL/catalog/log history lost | Fail closed and use an explicit fresh-snapshot/rebuild protocol. Automatic rebuild is an undecided policy. |
| Source restored, cloned or promoted | Verify identity, generation and lineage before resuming; an LSN alone is insufficient. |

For option B, data and its publication checkpoint should commit together through
the qualified PostgreSQL/pg_lake catalog path. For option C, the captured change
batch and its completeness/order record need an equivalent authoritative commit.
Neither assumes arbitrary multi-object S3 uploads are atomic.

## Proposed first experiment, if pursued

Prototype option B with a frozen schema and existing publisher semantics. Keep
only durable control/schema/checkpoint metadata in PostgreSQL, use bounded
decode buffers/spill, and acknowledge only after final publication commits.
Initially keep the existing decoded-value representation to isolate the journal
change from JSON/typed-format changes.

Compare against option A with identical runtime, source transactions, batching,
storage and observers. Measure PostgreSQL/engine CPU, source and total WAL,
database writes/growth, retained WAL, buffer/spill usage, object operations,
application latency and publication lag. Include INSERT and mixed workloads.

Recovery qualification must cover worker/server crashes, lost COMMIT responses,
partial uploads, stop/restart, object-store outage, oversized/streamed and aborted
transactions, snapshot interruption, schema-boundary cases and WAL invalidation.
The existing journal-based qualification does not establish these guarantees
for a journal-free path.

Option C can then be evaluated separately if durable object-store replay or
independent external apply is desired. Its comparison must include both capture
and final apply, not compare capture-only cost against a complete mirror pipeline.

Open choices are the desired durable handoff, apply placement, supported schema
changes, WAL/spill retention budgets and rebuild policy. No performance gain or
implementation selection is assumed by this draft.

## Experimental implementation

An isolated [frozen-schema prototype](../experiments/wal_direct/README.md) now
implements the first option-B slice: bounded committed transactions feed the
existing row-apply functions, with a direct publication checkpoint and no bulk
PostgreSQL journal admission. It is outside the default installation and does
not replace the qualified journal pipeline. Its bootstrap, recovery evidence,
resource bounds and remaining gaps are tracked with the experiment.

The [first bounded performance comparison](wal-direct-performance.md) finds
lower WAL generation relative to matched one-transaction journal publications,
but little total CPU improvement. The existing coalesced journal reference is
considerably cheaper than the unbatched direct prototype. The [coalesced follow-up](wal-direct-coalescing.md) now qualifies bounded
direct coalescing and records lower CPU/generated WAL under matched policy
settings, with more publication groups/requests and higher mixed-workload WAL
retention. Nonempty snapshot, streaming/spill, managed lifecycle and outage
qualification remain open.


The [bounded nonempty snapshot slice](wal-direct-snapshot.md) now copies existing tables directly to Iceberg under an exported logical-slot snapshot, with one atomic activation/checkpoint commit and concurrent-write catch-up. Native interruption, response-loss, budget/target/type guards and publication-crash tests pass. This does not provide a large resumable-copy pipeline or complete production validation.

The optional direct experiment also supports [streamed large transactions and outage recovery](wal-direct-streaming.md): private disposable spill, bounded chunks in one final transaction, a durable WAL replay floor for interleaved commits, and explicit snapshot recovery after WAL-history loss. Streamed mode publishes each source transaction separately. Production memory admission, sustained sizing and managed direct-worker lifecycle remain open.

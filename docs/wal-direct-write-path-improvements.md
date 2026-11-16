# Potential improvements to the direct Iceberg write path

Status: proposal, 16 November 2026. These are experiments to evaluate, not implemented interfaces or measured performance gains. This proposal is separate from the active native maintenance lock-order experiment.

## Goal and current flow

Reduce conversion, statement-execution and temporary-file work between logical decoding and final Iceberg publication. Preserve native Iceberg semantics, exact replay and the existing acknowledgement boundary.

The two current replication designs differ before final-table application:

```text
Direct:
Application heap → WAL → direct worker → SQL against final Iceberg tables
    → pg_lake native write machinery → final Parquet files + Iceberg metadata

Journaled:
Application heap → WAL → receiver → durable PostgreSQL change journal
    → publisher → SQL against final Iceberg tables
    → pg_lake native write machinery → final Parquet files + Iceberg metadata
```

The application heap is the source table. Neither flow requires another application-data heap as part of pg_lake's final writer. The journaled design deliberately adds durable change storage; its source acknowledgement follows durable staging. Direct publication acknowledges source WAL after final publication and the replication checkpoint commit.

The direct worker currently constructs JSON change envelopes and SQL calls. The apply functions reconstruct PostgreSQL values and execute INSERT/UPDATE/DELETE against the final Iceberg-backed foreign tables. For PostgreSQL-executed row writes, the pinned pg_lake implementation can then take this path:

```text
Decoded changes → JSON/SQL → PostgreSQL executor tuples
    → temporary CSV → pgduck conversion → final Parquet
    → native Iceberg metadata publication + replication checkpoint
```

This is a conceptual flow: mutation algorithms and query pushdown can take different branches. CSV is a native intermediate format, not the final Iceberg data format or our durable replication journal.

Source anchors in the pinned checkout:

- [The data-file receiver](../vendor/pg_lake/pg_lake_table/src/fdw/multi_data_file_dest.c) creates a temporary CSV receiver, then calls `PrepareCSVInsertion` when it flushes.
- [The native writer](../vendor/pg_lake/pg_lake_table/src/fdw/writable_table.c) converts that CSV into the table format through `ConvertCSVFileTo`. It also has `AddQueryResultToTable` / `PrepareToAddQueryResultToTable` paths for writing engine query results.
- [The position-delete receiver](../vendor/pg_lake/pg_lake_table/src/fdw/position_delete_dest.c) also uses intermediate CSV. Its existence does not mean every DELETE in our workload uses that branch; the previous [publication profile](wal-direct-publication-profile.md) found a candidate in that path had no useful gain for the tested workload.

“Direct” therefore describes removal of durable bulk change journaling. It does not imply a single conversion, zero-copy transport, or the absence of temporary files.

## Options

| Option | Proposed change | Potential benefit | Main work and risk |
| --- | --- | --- | --- |
| A. Improve the existing SQL/native path | Reduce repeated metadata reads, coordinate maintenance, and combine operations only where dependencies permit | Lower cost while retaining the current apply and commit mechanisms | Unsafe regrouping can change mutation order or affected-row checks; native caches must see transaction-local changes |
| B. Replace the native CSV handoff | Feed PostgreSQL executor tuples into a bounded typed batch or binary/columnar handoff consumed by the engine | Avoid CSV formatting, escaping, reparsing and some temporary-file I/O; may benefit journaled publication too | Define an actual supported producer/consumer API, type conversions, backpressure and resource ownership; such an interface is not established by this proposal |
| C. Add a native change-batch API | Let the direct worker submit bounded typed INSERT/UPDATE/DELETE batches to a PostgreSQL/native entry point | Potentially avoid JSON reconstruction and many per-operation SQL calls as well as CSV | Requires relation/type validation, mutation semantics, dependency handling and integration with transaction-scoped native publication |
| D. Write Parquet in the worker and register files natively | Worker generates candidate files; a native transaction validates and publishes them with the checkpoint | Could remove most PostgreSQL row-by-row write conversion for INSERT-only ingestion | File schema/field IDs, partitioning, statistics, credentials, orphan cleanup and native registration compatibility become substantial responsibilities; general UPDATE/DELETE remains harder |

These options can be combined, but should be evaluated independently first. A typed handoff could use a binary row protocol, a columnar batch, or an Arrow-compatible representation if the chosen integration supports it. That is a design choice to prototype, not a claim that pg_lake currently exposes such an API. Logical decoding and PostgreSQL types may still require parsing/conversion even after CSV is removed.

Option B has a broader scope than direct replication: both replication designs use native final-table writing. Savings in that common path must not be presented as savings attributable solely to journal removal. Option C could also expose a shared entry point for the journal publisher later.

## INSERTs versus mutations

INSERT-only batches are the best first experiment. Once values are correctly typed, they can become new Parquet files with the required Iceberg field IDs, partition values and statistics.

UPDATE and DELETE require identifying existing rows and choosing the native rewrite/delete strategy. Removing CSV alone does not remove file discovery, metadata scans, row matching, copy-on-write, deletion tracking or compaction. A typed change API should initially reuse these native mechanisms.

Do not sort all changes by operation or key merely to obtain larger batches. Repeated changes to a key, key-changing updates, delete/reinsert, TRUNCATE, cross-table ordering and transaction dependencies need explicit treatment. Any restricted grouping policy should have a documented eligibility check and a fallback to ordered application.

## Correctness requirements

Every candidate must preserve these properties:

- Final data, native catalog changes and the replication checkpoint retain the current same-instance transaction/recovery contract. A raw object upload does not authorize advancing the checkpoint or acknowledging WAL. Do not assume an arbitrary external catalog offers the same atomicity.
- Retried publications do not duplicate changes. Confirmed transaction aborts and unknown COMMIT outcomes remain distinct; a lost response requires durable-state reconciliation.
- Large streamed transactions remain atomic. Bounded batches/chunks and disposable spill do not become independent acknowledgement points. Replay rewinds completely, and safe feedback still protects other unfinished source transactions.
- Type fidelity remains exact: SQL NULL versus JSON null, large numeric and JSONB values, bytea, text escaping, Unicode, timestamp/date behavior, supported ranges and schema/field mapping all need validation. Preserve the qualified type subset until broader coverage passes.
- Missing/duplicate mutation matches, unsupported schemas, type failures, cancellation, storage errors and quota exhaustion fail without committing a partial mirror or advancing its receipt.
- Temporary buffers/files and uploaded-but-uncommitted objects have bounded resource use and a cleanup/recovery story. Process memory, temporary disk, object storage, retained WAL and backlog must all be accounted for.

## Test plan

### 1. Measure where conversion time goes

Use the existing native SQL path as the control. Add opt-in diagnostic instrumentation around the actual hot branches:

- Worker decoding, JSON serialization and request preparation.
- PostgreSQL JSON-to-row reconstruction and statement execution.
- Native CSV serialization: rows, output bytes, flush count and elapsed time.
- Engine CSV reading/conversion and Parquet encoding/upload.
- File-catalog reads/pruning, native lock waits, metadata publication and COMMIT.

Use CPU profiling or separate process/cgroup counters where needed to distinguish CPU from waits. Nested wall timers overlap: do not add CSV conversion, apply and COMMIT timings as if they were independent CPU costs. Separate temporary bytes written from physical disk I/O; page cache can make a temporary-file path appear inexpensive in short runs.

Retain an uninstrumented control to quantify instrumentation overhead. No result should claim CSV is the bottleneck merely because the source contains a CSV intermediate. Existing [metadata/maintenance profiling](wal-direct-publication-profile.md) and [sustained-load results](wal-direct-sustained-maintenance.md) already identify other material costs.

### 2. Prototype the smallest replacement

Start with Option B for the existing supported scalar INSERT subset, or a restricted INSERT-only slice of Option C if that is the practical integration boundary. Keep the original path selectable. Use the native writer/publication APIs for metadata and commit; do not introduce a custom Iceberg commit implementation in the first prototype.

First check that the replacement really bypasses CSV on the intended branch, using counters or tracing. A typed interface that eventually serializes back to the same CSV path has not demonstrated the intended removal. Also verify that it does not replace bounded temporary files with unbounded memory buffering.

### 3. Run matched performance fixtures

For each experiment compare a fresh baseline and candidate with identical runtime/native sources except for the candidate, storage images, source data, publication limits and observer settings. Change one mechanism at a time. Keep maintenance policy constant; do not compare a CSV candidate with coordinated maintenance against a baseline without it and attribute the combined gain to CSV removal.

Cover these shapes:

| Shape | Purpose |
| --- | --- |
| Small INSERT-only transactions with matched coalescing | Measure fixed costs and final-file/request counts |
| One transaction with 1,000, 5,000 and 20,000 rows per table | Match the existing bulk sizing study and expose buffering/conversion costs |
| Narrow and wide payloads, including escaping-heavy text, bytea and precise numeric/JSONB values | Exercise representation cost and exact type fidelity |
| Mixed INSERT/UPDATE/DELETE, repeated keys and TRUNCATE | Measure the supported mutation path and verify any grouping fallback |
| Large streamed transactions with several chunks and subtransaction aborts | Confirm bounded handoff and replay behavior |

Use at least three fresh paired repetitions for short performance cases, reversing baseline/candidate order. Report individual results and ranges, not only the best run. Predeclare which metric the candidate aims to improve and its acceptable memory, latency and object-request tradeoffs before examining results.

Record total PostgreSQL-plus-engine CPU-seconds, worker CPU/RSS, application commit time, sampled publication-lag bounds, final drain, realized batch distribution, generated/retained WAL, temporary bytes and I/O, cgroup memory events, file counts, object requests/bytes and exact final row hashes. Include source writes and maintenance in total-cost measurements, with the same exclusions as the existing studies. A failed run has no completed-workload CPU total.

Where the native handoff changes, run both journaled and direct controls to distinguish common writer savings from direct-worker-specific savings. This also checks that improving the direct prototype does not regress the supported journal path.

### 4. Qualify failures before sustained load

Run transaction rollback and native crash cases before upload completion, after upload but before publication commit, and after commit before feedback. Add lost responses, restart/replay, storage outage, cancellation, insufficient temporary disk/memory, and retry exhaustion as applicable to the changed handoff. Verify exact rows, unchanged checkpoints/feedback on abort, safe replay floors, and eventual orphan cleanup.

If Option D is attempted, explicitly test worker-generated files uploaded before a crash, crash during native registration, mismatched schema/field IDs and duplicate registration. Successful upload or readable Parquet alone is insufficient qualification.

### 5. Repeat sustained maintenance tests

Only after functional/recovery checks and short matched measurements justify the candidate, repeat the same sequential 15-minute workloads at 6 and 8 transactions/second, with fresh volumes, the existing CPU/memory/no-swap limits, and maintenance enabled. Retain failed envelopes and compare backlog trajectories as well as final drain. Follow with longer soaks and broader contention before making production capacity claims.

## Decision

First measure conversion costs alongside metadata and maintenance, then select the smallest change supported by those measurements. Prefer a shared native typed handoff if CSV accounts for a material cost and the replacement preserves bounded resources and existing native commit semantics. Consider a broader change-batch API only if JSON/per-operation execution remains expensive enough to justify its additional correctness surface. Worker-owned Parquet generation is a later option with a substantially larger integration burden.

No implementation is selected here. Existing performance evidence does not yet quantify the fraction attributable to CSV, or establish a speedup from any proposed typed interface.

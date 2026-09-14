# Milestone 0 feasibility investigation

Status: Milestone 0's local-catalog feasibility decision is complete. Native
subscriptions reject Iceberg targets, while direct SQL, crash recovery, and
orphan discovery/cleanup pass the implemented PostgreSQL 18 probes. REST-catalog
failure windows and independent-reader checks remain later production gates.

## Observed lab results

Run: `probe_20260914_045949`, evidence in `.lab/probe_20260914_045949/`.
The [assertion results](evidence/2026-09-14-pg18-results.json) are also retained
in the repository; full logs and inventories remain local under `.lab/`.
Tested PostgreSQL 18.6 on Linux arm64, pg_lake extension version 3.6 at the
revision below, and DuckDB v1.5.5. The instrumented image identity is
`sha256:28742e0c68561d0f6bad27346d27db687105ddacae3d77b05bfc3824bbf622a6`.

All 21 executed assertions passed. The probe returned 2 (`native-blocked`),
the expected characterization result rather than a successful replication system.

| Probe | Observation |
|---|---|
| Direct SQL | INSERT, 80,000-character UPDATE, DELETE, ROLLBACK, and TRUNCATE passed |
| Two-table transaction | PostgreSQL reader saw no partial transaction; both tables contained committed rows |
| Backend termination before commit | Neither table exposed the aborted row |
| PostgreSQL SIGKILL after commit | Both Iceberg rows and the heap checkpoint survived; aborted rows remained absent |
| Heap subscription control | Initial copy, UPDATE/INSERT, DELETE, and TRUNCATE passed |
| Iceberg subscription with initial copy | CREATE SUBSCRIPTION rejected with SQLSTATE 42809 |
| Iceberg subscription without initial copy | CREATE SUBSCRIPTION rejected with SQLSTATE 42809 |
| Cleanup | Zero subscriptions and zero replication slots remained |

Both Iceberg cases failed in `CheckSubscriptionRelkind` with
`This operation is not supported for foreign tables.` Neither reached an
initial-copy or apply worker, so native Iceberg mutation behavior was not tested.

For identical ten-row results, ten single-row commits produced **10 snapshots
and 10 current files**; one batched insert produced **1 snapshot and 1 current
file**. This small workload demonstrates commit amplification, not a throughput
benchmark or a general cost ratio.

The first harness run stopped because `lake_iceberg.files()` rejects an empty
table with no current snapshot. Inventory collection now handles that state;
the successful run above includes empty-table inventories. Metadata, current
files, cleanup queues, and PostgreSQL logs are preserved with the run.

### Precise crash windows

Run `crash_20260914_045535` used the lab-only patch in
`deploy/docker-compose/patches/pg-lake-lab-crash-points.patch`. The
[crash assertion results](evidence/2026-09-14-pg18-crash-results.json) are
retained here; full per-boundary logs remain under `.lab/`.

| Crash boundary | Client result | Recovered state | Cleanup state |
|---|---|---|---|
| Before metadata upload completion | Connection lost | 0 rows, 0 snapshots | 4 in-progress entries discovered; VACUUM cleared them |
| After metadata upload completion, before PostgreSQL commit | Connection lost | 0 rows, 0 snapshots | 4 in-progress entries discovered; VACUUM cleared them |
| PostgreSQL post-commit callback | Connection lost | 1 row, 1 snapshot | 0 in-progress entries |

Each boundary killed the exercising backend with signal 9. PostgreSQL logged
termination of all server processes, crash recovery, and readiness afterward.
The unchanged `pg_postmaster_start_time()` is expected: the postmaster process
survives while it reinitializes its children.

The after-commit result is the important ambiguity: the client cannot know from
its disconnected session that the row and snapshot committed. A replication
worker must reconcile a durable batch identity/checkpoint with catalog state
before retrying; blindly re-running the insert can create a duplicate snapshot
and live row.

The first two points bracket `FinishAllUploads()`, which drains pg_lake's
parallel metadata uploads. Data-file production begins earlier in statement
execution, so “before upload completion” does not mean before the first object
write. The experiment proves catalog invisibility plus discoverable cleanup
records in those windows, not absence of temporary objects. This local catalog
test does not model an ambiguous remote REST-catalog response.

## Inputs inspected

- `pg_lake` revision `8180647d4535fdb1ed223f92f301ba6094a8ec3e`.
- PostgreSQL 18 official Bookworm image, manifest digest
  `sha256:1c59e2c3c818eaa0f0628f695b36e7c9e362d6b219b36a54a32df645cbd7e1af`.
- Upstream build documentation lists PostgreSQL 16–19; our initial target is 18.
  Supporting 17 and failover remains later work.

## Source findings

1. Iceberg tables are implemented as foreign tables. `USING iceberg` is the
   convenient creation syntax, not evidence of a heap-compatible table AM.
   [Upstream Iceberg documentation](https://github.com/Snowflake-Labs/pg_lake/blob/8180647d4535fdb1ed223f92f301ba6094a8ec3e/docs/iceberg-tables.md).
2. PostgreSQL 18 `CheckSubscriptionRelkind` admits ordinary and partitioned
   tables only. Foreign table targets receive a wrong-object-type error.
   Both relation mapping and partition apply validate the relation kind.
   [PostgreSQL executor source](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/executor/execReplication.c),
   [logical relation mapping](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/relation.c).
3. Upstream `test_logical_replication.py` checks replica identities of internal
   catalogs under a `FOR ALL TABLES` publication. It does not establish a
   heap-to-Iceberg subscription or test its apply worker.
   [Upstream test](https://github.com/Snowflake-Labs/pg_lake/blob/8180647d4535fdb1ed223f92f301ba6094a8ec3e/pg_lake_table/tests/pytests/test_logical_replication.py).
4. The file named `pg_lake_replication.c` only caches an extension identifier;
   it is not a replication consumer.
   [Upstream implementation](https://github.com/Snowflake-Labs/pg_lake/blob/8180647d4535fdb1ed223f92f301ba6094a8ec3e/pg_lake_engine/src/extensions/pg_lake_replication.c).
5. Local Iceberg metadata changes run in a precommit callback. Optional REST
   catalog requests run in the commit callback, and HTTP failures can be
   reported as warnings. A local heap checkpoint therefore cannot by itself
   prove successful REST-catalog publication. The lab uses the local catalog;
   a REST catalog requires additional recovery experiments.
   [Transaction callbacks](https://github.com/Snowflake-Labs/pg_lake/blob/8180647d4535fdb1ed223f92f301ba6094a8ec3e/pg_lake_table/src/transaction/transaction_hooks.c),
   [REST publication and metadata tracking](https://github.com/Snowflake-Labs/pg_lake/blob/8180647d4535fdb1ed223f92f301ba6094a8ec3e/pg_lake_table/src/transaction/track_iceberg_metadata_changes.c).
6. The transaction callback rejects `PREPARE TRANSACTION` when Iceberg metadata
   changes are tracked. PostgreSQL two-phase commit is therefore not an available
   shortcut for coordinating these Iceberg writes with an external catalog.
   [Transaction callbacks](https://github.com/Snowflake-Labs/pg_lake/blob/8180647d4535fdb1ed223f92f301ba6094a8ec3e/pg_lake_table/src/transaction/transaction_hooks.c).

Architecture recommendation for the next prototype: use a destination-side
external `pgoutput` consumer with a durable heap journal and micro-batched SQL
application. Native apply's relation-kind barrier is now confirmed at runtime.
This does not yet establish the external consumer's crash-safety protocol; it
does establish the failure windows that protocol must reconcile.

## Experiments and exit gates

| Experiment | Harness coverage | Evidence needed |
|---|---|---|
| Build PostgreSQL 18 + pg_lake | Passed | Image and versions recorded above |
| Direct Iceberg I/U/D/TRUNCATE | Passed | Assertions and metadata/file inventories captured |
| Direct transaction visibility | Passed basic probe | No partial rows observed |
| Precommit backend termination | Passed basic probe | No visible aborted rows; full orphan accounting still needed |
| Native initial copy | Blocked at subscription creation | SQLSTATE 42809, foreign-table target |
| Native apply, copy disabled | Blocked at subscription creation | Same explicit rejection, separate probe |
| Native mutations | Conditional basic checks | Cannot run if target relation rejected |
| True source TOAST transitions | Pending | Unchanged marker plus inline/external/null transitions |
| Cross-table transaction visibility | Two-table PostgreSQL reader probe implemented | Independent Iceberg readers still need separate validation |
| Crash after durable commit | Passed basic probe | Both tables and checkpoint survived abrupt restart |
| Failpoints around upload completion/catalog commit | Passed for local catalog | Pre-commit absence, tracked orphans, post-commit ambiguity measured |
| File amplification | Measured | 10 snapshots/files versus 1 for equal rows |
| Consistent initial copy under writes | Pending | Snapshot/LSN barrier and catch-up comparison |

An 80KB value checks large-value SQL roundtrips; it does not alone establish
unchanged-TOAST replication handling. Backend termination before commit is a
transaction abort test, not a substitute for postmaster crashes or storage
failpoints. A PostgreSQL reader observing two tables atomically does not prove
cross-table atomicity to independent Iceberg readers.

## Recovery protocol to validate before Milestone 1

Staging must become durable before replication feedback acknowledges its WAL.
Published progress should be committed in the same destination PostgreSQL
transaction as the corresponding Iceberg catalog changes if upstream permits
that guarantee. If it does not, ambiguous catalog commits need explicit
reconciliation before retry. Recovery must prove whether the batch committed;
blind replay of INSERT is not idempotent.

Upstream has an `after-apply-iceberg-changes` injection point after
`FinishAllUploads()` in metadata tracking. A PostgreSQL build with injection
points enabled is needed to use it; `pg_config --configure` confirms the selected
distribution build does not enable it. The lab therefore applies a small,
auditable patch with equivalent process-kill boundaries. This patch must never
be included in a production image.

Separate received, staged, published and acknowledged positions. Validate
source system/database identity and deduplicate staged transactions by stable
source coordinates. A source failover must not silently change that identity.

## Build reproducibility limits

PostgreSQL's base image, MinIO images, and pg_lake source revision are immutable
pins. vcpkg currently uses a release tag; Debian packages are resolved at
build time. Upstream submodules are pinned by the pg_lake revision, while its
build may fetch further dependencies. This is a repeatable lab recipe, not yet
a fully locked or bit-for-bit reproducible supply chain. Capture image digests,
installed packages and dependency licenses before publishing binaries.

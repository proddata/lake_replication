# Direct snapshot bootstrap for existing tables

The optional direct prototype can now initialize nonempty application tables while INSERT, UPDATE and DELETE transactions continue, then catch up through the existing direct WAL publisher. It uses native heap-to-Iceberg `INSERT … SELECT`; it does not stage snapshot rows in PostgreSQL JSONB tables.

This is a bounded, manually operated frozen-schema experiment. The default journal pipeline and its installation remain unchanged.

## Publication boundary

1. Take the direct stream's exclusive advisory ownership lock. Refuse replacement of an existing stream or destination tables.
2. Hold source-table read locks on an independent connection before exporting a logical-slot snapshot. This closes the gap for table rewrites between snapshot export and copy. Ordinary INSERT/UPDATE/DELETE can proceed; row-type-changing DDL and TRUNCATE may wait on these locks. DDL must remain quiesced for the bootstrap contract.
3. Create the persistent pgoutput slot and export its consistent snapshot. On retry after an uncommitted initialization, retain that slot and use a fresh temporary slot to export a newer snapshot. Invalid, foreign or busy retained slots are refused.
4. Import the snapshot into a repeatable-read transaction. Validate the explicit unfiltered publication, supported source row types/identity and input budgets. Create and populate final Iceberg tables directly, with small per-table audit and frozen-mapping records.
5. Confirm the independent pre-export source-lock connection is still alive, then commit all tables, mappings, snapshot audit and the stream's consistent starting LSN together through the qualified local PostgreSQL/pg_lake catalog. Before this COMMIT, new tables and activation metadata remain invisible to other sessions.
6. Release snapshot ownership and run the direct worker from the recorded boundary. Changes committed after that snapshot are decoded and applied; streaming WAL feedback follows final publication COMMIT.

Slot creation establishes an initial baseline while the exported snapshot protects row visibility. The initializer sends no streaming WAL feedback. A failed attempt retains the persistent slot for retry; it does not replace it or pretend that a partial copy is ready. The snapshot owner and source lock connections remain alive until activation commits. The copy statement has a 180-second timeout; snapshot establishment can wait for older source transactions and is not a whole-operation time guarantee.

## Limits

The first implementation copies all selected tables in one transaction. Defaults and hard prototype ceilings are **100,000 rows and 64 MiB of measured JSONB input representation across all tables**. `LAKE_DIRECT_SNAPSHOT_MAX_ROWS` and `LAKE_DIRECT_SNAPSHOT_MAX_BYTES` can lower them. Budget measurement scans the exported snapshot before target writes, and the copy scans it again. These are input limits, not a measured RSS ceiling or a production capacity qualification.

It supports at most 64 explicitly published ordinary heap tables, with usable replica identity and supported built-in scalar columns. RLS, partition/inheritance layouts, publication filters/column lists, generated/identity columns, custom collations/types, unconstrained or over-precision decimals and negative numeric scales are refused. Native pg_lake validation still applies. The qualification includes JSONB numeric precision and bounded decimals through snapshot and subsequent WAL updates.

The target schema must have no colliding destination names. Existing mirrors are never overwritten. A repeated initializer reconciles an already committed matching stream and leaves it intact; this is not an automatic rebuild operation. The experimental schema is recreated between fixture versions and has no in-place upgrade contract.

There is no PostgreSQL row-data journal or durable row spool. `snapshot_tables` and `snapshot_state` hold only table identities, counts, byte totals, snapshot identity and starting LSN. Retained source WAL and MVCC history remain necessary during copy. Large resumable copies, disk/memory admission, storage-outage/WAL-budget handling, general DDL, managed credentials/lifecycle, restore/clone/promotion and physical power-loss recovery remain separate gates.

## Qualification

[Retained native evidence](../evidence/2026-11-16-wal-direct-snapshot.json) passes **138 snapshot checks across nine fixtures**, plus **104 immediate/coalesced streaming regression checks** on the snapshot-capable runtime. Every fixture cleans up successfully. `make check`, all-feature Rust tests and `git diff --check` pass. Coverage includes:

- A 2,000-row, two-table snapshot with concurrent key changes, INSERT/DELETE, an aborted transaction, JSONB precision, DDL locking, atomic visibility and exact WAL catch-up.
- Row/byte budget refusal, partial metadata rollback, and retry using retained history.
- Worker termination after copying starts, retry from a fresh snapshot, and continued exact replication.
- Loss of the independent pre-export source-lock connection followed by a row-type rewrite: activation aborts, targets remain invisible, and fresh initialization plus streaming succeeds.
- Lost response after snapshot COMMIT, reconciliation without duplicate copying, and no replacement of committed activation state.
- Existing destination refusal, publication/RLS/identity guards, decimal mapping refusal and bounded decimal continuation.
- Real shared-PostgreSQL crashes before upload, after upload and after COMMIT: activation and audit match the actual commit outcome; committed source heaps survive; retry and subsequent streaming remain exact.
- Zero rows in PostgreSQL initial-copy chunks/rows and transaction/change journals.

These are correctness and recovery checks for this bounded slice, not snapshot performance, large-copy capacity or complete production validation. Existing streaming performance results remain tied to their recorded v4 runtime in the [coalescing report](../performance/wal-direct-coalescing.md).

## Run

Build the optional image over the qualified runtime and select the pinned storage fixtures:

```sh
docker build -f experiments/wal_direct/Dockerfile \
  --build-arg BASE_IMAGE=lake-local-iceberg:qualification-v8 \
  -t lake-local-iceberg:wal-direct-v8 .
export PUSH_PG_LAKE_IMAGE=lake-local-iceberg:wal-direct-v8
# Set PUSH_STORAGE_IMAGE and PUSH_STORAGE_INIT_IMAGE to qualified images.
make local-iceberg-wal-direct-snapshot-test
WAL_DIRECT_SNAPSHOT_CASE=stop make local-iceberg-wal-direct-snapshot-test
WAL_DIRECT_SNAPSHOT_CASE=response make local-iceberg-wal-direct-snapshot-test
```

Other cases are `existing`, `guards`, `guard_loss`, and `crash:before_upload_completion`, `crash:after_upload_completion`, `crash:after_commit`. Crash cases require the matching test-only fault image. All commands provision and remove private Compose volumes.

The experimental `lake-replication-direct-init` requires the same loopback/database/user settings and explicit opt-in as the direct worker, plus `LAKE_DIRECT_SLOT`, `LAKE_DIRECT_PUBLICATION` and `LAKE_DIRECT_TARGET_SCHEMA`. Install the private `install.sql` in that database first. There is one direct stream per database. It remains outside default bundles, launchers and extension migrations.

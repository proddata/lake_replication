# Milestone 7: consistent initial synchronization

Status: a single-table exported-snapshot copy and its transition to continuous
pgoutput replication are proven end to end.

## Consistency protocol

The initial-sync worker creates the logical replication slot with
`EXPORT_SNAPSHOT`. PostgreSQL returns both an exported snapshot identifier and
the slot's `consistent_point`. A separate read-only, repeatable-read source
transaction imports that snapshot before reading catalog metadata or table
rows. The connection that owns the exported snapshot remains open throughout
the read.

Rows are first written to ordinary PostgreSQL staging tables on the destination.
This exposes durable per-table progress without changing the readable Iceberg
table. The final destination transaction performs the following as one unit:

1. verifies that row ordinals are complete and contiguous;
2. rejects a non-empty destination unless overwrite was explicitly enabled;
3. replaces or fills the Iceberg destination;
4. marks the relation and copy ready;
5. initializes received, staged, published, and acknowledged checkpoints to the
   slot's `consistent_point`.

The normal receiver then starts at that checkpoint. Transactions committed
after the exported snapshot are retained by the slot, staged in source commit
order, and applied by the existing micro-batch publisher.

## Failure behavior

An injected failure after 50 staged rows demonstrated that partial rows remain
inspectable, the copy is marked `error`, every replication checkpoint stays at
`0/0`, and the previous Iceberg contents remain readable. Because an exported
snapshot ceases to be usable when its owning connection closes, recovery is an
explicit reset: discard the partial copy, drop the inactive slot, and start a
new snapshot/slot pair. The worker never resumes an incomplete copy against a
different snapshot.

The overwrite path also leaves the previous Iceberg state visible for the full
copy duration. `TRUNCATE` and insertion of the replacement snapshot occur only
inside the final destination transaction.

## Evidence

Run `initial_20260914_103652` passed all 15 assertions. The retained
[results](evidence/2026-09-14-initial-sync-results.json) show:

- a 100-row exported snapshot at `0/5375598`;
- an observable `copying` state and exact `ready:100` progress;
- atomic initialization of all four checkpoints at the snapshot boundary;
- one concurrent source transaction containing an update, delete, and insert;
- one canonical relation schema version across catalog inspection and pgoutput;
- a final source/destination digest of
  `100:100:f63904230484cf044b22af6db573ecf4`;
- checkpoint convergence at `0/5375C50` after WAL catch-up.

The protocol follows PostgreSQL's documented
[replication-slot snapshot contract](https://www.postgresql.org/docs/18/protocol-replication.html)
and the standard
[initial-snapshot architecture](https://www.postgresql.org/docs/18/logical-replication-architecture.html#LOGICAL-REPLICATION-SNAPSHOT).

## Remaining boundaries

- The milestone worker synchronizes one explicitly configured table per run.
  Publication discovery and coordinated multi-table cutover are pending.
- Source rows are now streamed with bounded `COPY` framing as described in
  [Milestone 8](milestone-8.md).
- Source/destination column names are validated and destination conversion is
  transactional, but a versioned cross-cluster type compatibility matrix is
  not implemented.
- Publication row filters, column lists, partitions, generated columns, and
  binary initial-copy transfer are not yet honored by this worker.
- Automatically creating the Iceberg destination is pending; this milestone
  uses a pre-created destination.
- A process failure after slot creation requires controlled rebuild. Automated
  cleanup/retry policy and WAL-retention limits remain operator responsibilities.

# Journaled and direct publication

The goal is to mirror ordinary PostgreSQL tables into pg_lake Iceberg tables. A primary deployment is one PostgreSQL instance and database containing both the application heaps and their Iceberg mirrors, with pgduck and object storage as prerequisites. A separate destination PostgreSQL remains an available topology for the journaled path.

## Two publication paths

| Contract | Journaled publication | Direct publication |
| --- | --- | --- |
| Current state | Packaged default beta pipeline | Optional feature-gated prototype; not a managed production mode |
| Deployment | Same-instance loopback or remote source | Current qualification covers same-instance/same-database |
| Data path | Logical decoding → durable PostgreSQL journal → native Iceberg writes | Logical decoding → native Iceberg writes; no durable bulk row-data journal |
| WAL acknowledgement | After durable journal admission | After final data and publication checkpoint commit together |
| Replay before publication | Durable journal plus source continuity checks | Retained source WAL; streamed spill is disposable and never authorizes acknowledgement |
| Outage trade-off | Capture and publication can progress independently within journal/retention limits | Slow or unavailable final storage retains source WAL and increases replay/backlog pressure |

Direct publication still has a worker, small PostgreSQL control/checkpoint state, native metadata writes and temporary/spill files. Removing the durable bulk journal does not remove native Iceberg write, conversion, catalog or maintenance costs.

## Intended direct support

The intended product path includes consistent initial copy followed by continuous INSERT/UPDATE/DELETE, key changes and TRUNCATE; bounded atomic handling of large/streamed transactions; safe restart/replay and unknown-COMMIT reconciliation; explicit ownership and credentials; subscription lifecycle and health/lag/pressure reporting; guarded schema changes; and documented retention, rebuild and resource limits. These are requirements to qualify, not a claim of parity with the packaged pipeline today.

The prototype already has bounded snapshot bootstrap, coalescing, streamed spill/replay, atomic checkpoint/final publication, confirmed-deadlock retry and bounded outage/history-loss recovery fixtures. Managed direct lifecycle, general DDL, large resumable snapshots, production privilege/resource admission and broader failover/soak qualification remain open. See the [active roadmap](../roadmap/replication.md).

## Read the implementation and evidence

- [Run the private prototype](../../experiments/wal_direct/README.md).
- [Snapshot bootstrap and recovery](../qualification/wal-direct-snapshot.md).
- [Streamed transactions and replay floor](../qualification/wal-direct-streaming.md).
- [Deadlock recovery and unknown outcomes](../qualification/wal-direct-deadlock-recovery.md).
- [Current native lock experiment](../performance/wal-direct-prelock.md): the local 6/second run improves CPU and lag; the full 8/second run still misses its drain deadline. These are bounded observations, not production capacity limits.
- [Journal alternatives and acknowledgement contracts](wal-backed-iceberg-options.md).
- [Proposed write-path experiments](../roadmap/direct-write-path.md).

The optional source-push change-log transport is a different design: capture publishes intermediate logs and an importer feeds the journal. Local direct publication does not require those logs, that importer or another PostgreSQL instance.

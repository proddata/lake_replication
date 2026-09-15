# Milestone 21: slot invalidation and WAL loss

Status: a disposable PostgreSQL 18 source now proves that the supervisor fails
closed when required WAL has been removed from an inactive logical replication
slot. It neither launches the receiver nor advances any destination checkpoint.

## Failure boundary

`deploy/slot-loss/compose.yml` sets `max_slot_wal_keep_size=1MB` on an isolated
source. The probe first completes initial synchronization and publishes one live
transaction through a healthy slot. It then drains the supervisor, commits one
more source row, emits enough unrelated WAL to exceed the slot retention bound,
and forces checkpoints until PostgreSQL reports:

```text
wal_status=lost
invalidation_reason=wal_removed
```

This exercises PostgreSQL's real slot invalidation path. It does not simulate
loss by deleting the slot or changing the catalog view.

## Fail-closed contract

On restart, the supervisor performs its source preflight before starting a
logical replication connection. A lost or invalidated slot is a fatal continuity
error that requires controlled rebuild; the worker must not create a replacement
slot or resume at a newer LSN automatically.

The failure is recorded in subscription and service status. Received work is not
staged, all durable checkpoint positions remain unchanged, replication feedback
does not advance, and the row committed before invalidation remains absent from
the destination rather than being silently skipped.

PostgreSQL documents `lost` as an unusable slot and `wal_removed` as removal of
required WAL in
[the `pg_replication_slots` view](https://www.postgresql.org/docs/18/view-pg-replication-slots.html).
Its
[replication configuration](https://www.postgresql.org/docs/18/runtime-config-replication.html)
defines `max_slot_wal_keep_size` as the checkpoint-time bound used by the probe.

## Evidence

Run `slot_loss_20260915_095846` passed all eight assertions. The retained
[results](evidence/2026-09-15-slot-loss-results.json) demonstrate:

- exact publication before the retention overrun;
- clean receiver drain before forcing the slot inactive;
- source confirmation equal to the durable acknowledged checkpoint;
- actual `lost:wal_removed` invalidation;
- supervisor failure before receiver launch;
- durable error visibility;
- unchanged staged, published, and acknowledged LSNs; and
- no destination appearance of the unconsumable source row.

Run the isolated probe with `make slot-loss-test`. The probe removes its dedicated
PostgreSQL container and volume in a `finally` block and uses the main laboratory
only as its pg_lake destination.

## Remaining boundary

Recovery is deliberately operator-controlled. A production workflow still needs
to turn this error into an explicit replacement-subscription rebuild with policy,
approval, progress reporting, and retained-old-table cleanup.

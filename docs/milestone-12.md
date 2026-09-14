# Milestone 12: controlled rebuild and subscription replacement

Status: an incompatible paused subscription can be replaced through a fresh
snapshot and slot, caught up before cutover, atomically promoted, and continued
without advancing the paused slot past undecoded WAL.

## Recovery model

An incompatible schema changes the destination representation, so this workflow
uses a full replacement subscription rather than trying to reinterpret retained
WAL under a new schema:

1. Leave the paused subscription, its Iceberg table, checkpoint, and source slot
   untouched.
2. Create a replacement subscription and slot for the same source and publication.
3. Run publication discovery and initial synchronization into Iceberg tables
   whose names carry a configured shadow suffix.
4. Receive and publish replacement-slot WAL until its staged and published
   checkpoints converge.
5. Atomically rename each original table to a backup name and each replacement
   table to the canonical name.
6. Disable the old subscription, retain its slot identity in a promotion record,
   and continue the replacement subscription against the promoted table OIDs.
7. Drop the old source slot explicitly only after promotion is verified.

The fresh exported snapshot includes every commit before its consistent point,
including the transaction that the old receiver refused. The replacement slot
retains every later transaction, so the workflow does not need to skip or decode
the incompatible WAL held by the old slot.

## Promotion validation

`promote_subscription_rebuild` requires the old subscription to be paused and
the replacement to be streaming from the same source and publication set. It
also requires the replacement checkpoint to have no unpublished WAL, validates
that every shadow table is assigned to the replacement relation and every target
belongs to the old subscription, rejects pre-existing backup names, and performs
all table renames and state changes in one destination transaction.

The old Iceberg state remains readable throughout the rebuild and remains
available under the backup suffix after promotion.

## Evidence

Run `rebuild_20260914_172153` passed all 12 assertions. The retained
[results](evidence/2026-09-14-rebuild-results.json) demonstrate:

- an old slot held at `0/71FA1C0` after a `text` to `bigint` incompatibility;
- a readable two-row `text` Iceberg table during replacement;
- a replacement `bigint` snapshot containing the formerly refused third row;
- replacement-slot catch-up of a fourth row before promotion;
- atomic promotion with the prior `text` table retained as `__backup`;
- continued replication of a fifth row through the promoted replacement; and
- explicit successful removal of the old retained source slot.

## Remaining boundaries

- The workflow is assembled from SQL functions and worker invocations; a single
  supervised operator command and resumable state machine are pending.
- Promotion is subscription-wide and requires all replacement relations to be
  caught up. Per-table rebuild under a multi-table subscription is not supported.
- Backup retention and deletion are manual. Snapshot-age policy and object-file
  cleanup must be integrated before automatic removal.
- Source-side slot creation/removal cannot be part of the destination transaction.
  Promotion records both slot names so incomplete external cleanup remains visible.
- Promotion uses suffix-based names and rejects identifiers exceeding PostgreSQL's
  63-byte limit or colliding with an existing backup.
- A running supervisor must ensure old and replacement workers are not launched
  against the wrong subscription after promotion.

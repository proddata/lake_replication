# Milestone 11: fail-closed schema evolution

Status: the live receiver automatically applies safe nullable column additions
and durably pauses incompatible relation versions before staging or acknowledging
their WAL.

## Receiver reconciliation

Subscriptions initialized with a type contract open a second source connection
for catalog inspection. The replication connection remains in COPY-BOTH mode;
when pgoutput sends a relation descriptor, the catalog connection resolves the
canonical source types, nullability, and generated status for precisely those
published columns.

The receiver registers the new relation descriptor and type contract, then asks
the destination to reconcile it with the newest assigned predecessor. Legacy
subscriptions without type contracts keep the earlier registration behavior.

The PostgreSQL wire client now drains messages through `ReadyForQuery` after a
server error. This allows the worker to persist a schema diagnostic on the same
destination connection rather than losing protocol synchronization.

## Safe automatic change

An online change is accepted only when:

- the mapping version remains unchanged;
- every existing published column retains its name, order, and source type;
- the destination's existing mapped types remain compatible;
- all new columns are appended after the existing columns; and
- every appended column is nullable, mapped, and not generated.

The destination adds accepted columns with `ALTER TABLE ... ADD COLUMN`, assigns
the Iceberg table to the new relation version, and returns the relation to
`ready`. The source transaction is decoded only after this succeeds.

## Fail-closed changes

Removed, reordered, renamed, or type-changed columns; mapping-version changes;
new `NOT NULL` columns; generated columns; unmapped types; and destination ALTER
failures stop reconciliation. The new relation version and subscription are
marked `paused`, and `subscriptions.last_error` records the reason. The receiver
exits before the containing source transaction reaches the journal or receives
replication feedback.

## Evidence

Run `evolution_20260914_170156` passed all 10 assertions. The retained
[results](evidence/2026-09-14-schema-evolution-results.json) show:

- automatic addition of a nullable `extra text` column as relation version 2;
- correct update and insert publication with the pre-existing row receiving NULL;
- rejection of a subsequent `note text` to `note bigint` change as version 3;
- durable `paused` states and a precise diagnostic;
- one published transaction and no staging of the unsafe transaction;
- source slot and all checkpoints fixed at `0/6B34D00`; and
- the prior three-row Iceberg state and `text` schema remaining readable.

pg_lake documents support for adding columns to writable Iceberg tables in its
[Iceberg table ALTER guidance](https://github.com/Snowflake-Labs/pg_lake/blob/main/docs/iceberg-tables.md#altering-an-iceberg-table).

## Remaining boundaries

- There is no operator resume workflow for an incompatible paused version. Type
  migration requires a controlled table rebuild.
- Safe widening conversions and renames are conservatively rejected.
- New non-null columns with immutable defaults require backfill coordination and
  are not applied online.
- Publication column-list changes that expose an older physical source column
  can appear reordered and are paused.
- DDL changes that produce no subsequent pgoutput relation message are not
  discovered proactively.
- Schema history retention and operator-facing inspection views remain pending.

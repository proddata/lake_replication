# Milestone 3: Iceberg micro-batch publisher

Status: the insert-only source-to-journal-to-Iceberg path is implemented and
tested against the local pg_lake catalog. The receiver and publisher remain
bounded prototype processes rather than supervised services.

## Implemented slice

`migrations/0003_publisher.sql` adds three operations:

- `assign_destination` binds a relation schema version to an Iceberg foreign
  table after validating its ordered column names;
- `next_batch` chooses complete staged transactions in end-LSN order, bounded
  by configurable transaction, row, and serialized-byte thresholds; the first
  transaction is always selected even when it exceeds a threshold; and
- `publish_insert_batch` resolves tuple arrays through versioned relation
  metadata, inserts rows into each destination, and advances transaction, batch,
  and published-checkpoint state in the same PostgreSQL transaction.

The `lake-replication-publisher` executable plans deterministic batch keys and
can publish one or more batches per invocation. If no staged transactions
remain, it exits successfully without changing Iceberg metadata.

Reusing a completed batch key with the same LSN bounds returns the existing
batch with zero inserted rows. Reusing it with different bounds fails closed.
Because pg_lake's local catalog participates in the PostgreSQL transaction, an
ambiguous client result can be reconciled from the durable batch and checkpoint
instead of replaying the Iceberg insert.

## Evidence

Run `publisher_20260914_062208` passed all 14 assertions. The
[retained results](evidence/2026-09-14-publisher-results.json) cover the complete
prototype path:

```text
3 source transactions / 4 inserts
              |
              v
     durable journal at 0/342AE98
              |
              +-- batch 1: 2 transactions / 3 rows --> snapshot 1
              |
              +-- batch 2: 1 transaction  / 1 row  --> snapshot 2
```

The first completed batch was deliberately replayed and produced zero rows and
no new snapshot. The second batch used the lab-only `after_commit` crash point:
the client lost its connection, but recovery found four destination rows, two
snapshots, three published transactions, and a published checkpoint equal to
the staged LSN. A new publisher process reported no staged work.

The final Iceberg contents exactly matched the source rows. The probe cleaned
up its publication, slot, source table, destination Iceberg table, and journal
subscription.

## Current boundaries

- Only insert changes are publishable.
- Column names and ordering are checked when a destination is assigned; value
  conversion is performed by `jsonb_populate_record`, so incompatible values or
  destination types abort the whole batch at runtime.
- Row, byte, and transaction thresholds exist. A long-running scheduler and
  the approximately 30-second time threshold are not implemented yet.
- A batch may span multiple mapped Iceberg tables, but the current end-to-end
  probe exercises one table.
- These atomicity results apply to pg_lake's local PostgreSQL catalog. Remote
  REST catalog commits still require independent reconciliation.
- Updates, deletes, truncates, unchanged TOAST reconstruction, schema evolution,
  initial copy, authentication, TLS, and HA behavior remain pending.

## Next slice

1. Decode UPDATE, DELETE, and TRUNCATE with explicit replica-identity policy.
2. Add pgoutput protocol-v4 streamed transaction assembly and spill behavior.
3. Run receiver and publisher as reconnecting services with time-based flushing,
   lag state, and operator-visible errors.
4. Implement a consistent initial-copy snapshot and WAL catch-up transition.

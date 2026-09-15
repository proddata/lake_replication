# Milestone 24: bounded journal maintenance

Status: the destination-side staging journal now has configurable, incremental
retention. Successful supervisor cycles run maintenance when it is due, while a
maintenance failure is recorded without rolling back service progress or stopping
replication.

## Safety boundary

Journal rows are reclaimable only when all of these conditions hold:

- the transaction and its batch are fully `published`;
- the transaction end LSN is no later than
  `LEAST(published_lsn, acknowledged_lsn)`;
- its publication timestamp is outside the configured retention window; and
- the transaction fits inside the per-run work limit.

Using both checkpoints is necessary because the receiver acknowledges WAL after
durable local staging, before the independent Iceberg publisher necessarily
catches up. Acknowledgement alone is therefore not evidence that a journal row is
published. Staged transactions, publishing transactions, and publishing batches
are never selected, preserving the evidence needed to reconcile an ambiguous
Iceberg commit.

Deleting a transaction cascades to its normalized change rows. A published batch
record is deleted only after every transaction that refers to it has been removed.
The checkpoint itself is locked while candidates are selected and deleted, and
concurrent maintenance calls serialize on the policy and status rows.

## Configuration and operation

Every new and existing subscription receives this default policy:

```text
enabled = true
retention_interval = 1 day
cleanup_interval = 5 minutes
max_transactions_per_run = 1000
```

Configure a subscription with SQL:

```sql
SELECT lake_replication.configure_journal_maintenance(
    p_subscription_id => 1,
    p_enabled => true,
    p_retention_interval => INTERVAL '2 days',
    p_cleanup_interval => INTERVAL '10 minutes',
    p_max_transactions_per_run => 5000
);
```

`report_service_cycle` invokes due maintenance automatically after recording a
successful or paused supervisor cycle. The cleanup runs in a protected PL/pgSQL
block: an error increments its own failure counter and leaves the service cycle
successful. An operator can trigger an immediate run with:

```sql
SELECT * FROM lake_replication.run_journal_maintenance(1, true);
```

The force flag bypasses only the cleanup interval. It does not bypass a disabled
policy, the retention window, publication state, or the safe LSN watermark.

Inspect current retained work and cumulative results with:

```sql
SELECT * FROM lake_replication.journal_maintenance_health;
```

The view includes policy values, safe cleanup LSN, retained transaction/change
counts, an approximate retained payload-byte count, oldest published row, last
run details, cumulative deletion totals, and isolated failure diagnostics.
Physical PostgreSQL space is reusable after row deletion; ordinary autovacuum is
still responsible for reclaiming dead tuples within the heap and indexes.

## Evidence

Run `make journal-maintenance-test` against the main laboratory. The probe covers:

- automatic execution from successful supervisor cycle reporting;
- maintenance failure isolation from service progress;
- the minimum of published and acknowledged checkpoints;
- bounded one-transaction cleanup;
- retention of staged and ambiguous publishing work;
- batch removal only after the final referencing transaction;
- time-based retention, scheduling, force, and disable behavior;
- idempotent repeated cleanup; and
- health-view retained-storage and cumulative-work telemetry.

Run `maintenance_20260915_115653` passed all 15 assertions; its retained result is
[recorded here](evidence/2026-09-15-journal-maintenance-results.json).

## Remaining boundary

This milestone bounds PostgreSQL journal storage. It does not compact Iceberg data
or delete files, expire Iceberg snapshots, or remove orphaned objects. Those
operations require the exact maintenance primitives supported by the pinned
`pg_lake` revision and remain the next storage-maintenance milestone.

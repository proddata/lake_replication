# Milestone 1: insert-only replication

Status: durable journal and local-catalog publication boundary implemented and
tested. A live streaming replication connection, source feedback loop, initial
copy, and automated batch planner are not implemented yet.

## Implemented slice

The destination schema in `migrations/0001_journal.sql` records:

- source system identifier, database OID, and timeline;
- subscription, slot, and publication identity;
- versioned relation metadata and destination relation OID;
- complete source transaction envelopes and ordered insert changes;
- deterministic publication batches; and
- independent received, staged, published, and acknowledged LSNs.

`stage_transaction` serializes writers through the checkpoint row. An identical
transaction at the same commit LSN is an idempotent replay. Different contents
at the same commit LSN, an unknown relation version, a wrong tuple width, a new
transaction behind staged progress, and acknowledgement beyond staged progress
all fail closed.

`prepare_batch` uses a deterministic key and marks only complete staged
transactions as publishing. The caller writes Iceberg rows and calls
`finish_batch` in the same PostgreSQL transaction. With pg_lake's local catalog,
the Iceberg snapshot, batch state, transaction states, and published checkpoint
therefore share one PostgreSQL commit boundary.

## State transition

```text
pgoutput transaction
        |
        v
stage_transaction() ---- durable commit ----> staged_lsn may be acknowledged
        |
        v
prepare_batch() + Iceberg INSERT + finish_batch()
        |
        v
single local PostgreSQL commit ----> published_lsn
        |
        +-- client receives success: continue
        |
        +-- connection lost: look up deterministic batch key before retry
```

Received and staged currently advance together because the network receiver is
not implemented. They remain separate columns so transport can expose received
progress without weakening the rule that feedback never exceeds staged LSN.

## Evidence

Run `journal_20260914_051629` passed all 16 assertions. The
[retained results](evidence/2026-09-14-journal-results.json) show three staged
transactions, two published batches, three destination rows, and all four
positions converged at `0/140`.

The probe also killed the backend in pg_lake's post-commit callback. The client
lost its connection, but recovery found the destination row, deterministic batch,
and published checkpoint committed. Looking up that batch avoids replaying its
Iceberg insert.

The migration was separately applied to a clean temporary PostgreSQL database;
it created seven tables and five functions successfully. The temporary database
was removed afterward.

## Rust worker core

The dependency-free `worker` crate decodes the insert-only subset of pgoutput:
`BEGIN`, `RELATION`, `INSERT`, and `COMMIT`, including text, binary, NULL, and
unchanged-TOAST tuple markers. Its transaction assembler releases only complete
transactions and rejects inserts outside a transaction or for unknown relations.

The parser currently targets non-streamed top-level transactions. Protocol v4
stream messages, wire transport, status updates, TLS, authentication, and
PostgreSQL type conversion are intentionally pending.

## Next slice

1. Add a replication-protocol transport using `START_REPLICATION ... LOGICAL`
   with pgoutput protocol version 4 and `streaming=parallel`.
2. Persist actual decoded insert transactions through `stage_transaction`.
3. Send standby status feedback only after the staging transaction commits.
4. Restart the receiver repeatedly and prove duplicate delivery is harmless.
5. Add a batch planner driven by row, byte, and time thresholds.
6. Implement consistent initial copy with an exported snapshot and WAL catch-up.

Remote REST catalogs remain outside this atomicity claim. Their commit response
can be ambiguous independently of PostgreSQL and requires catalog reconciliation.

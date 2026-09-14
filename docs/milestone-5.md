# Milestone 5: streamed large transactions

Status: protocol-v4 streamed transactions are decoded, durably spilled by
segment, committed atomically, and discarded on abort. Mid-stream receiver loss
and replay are tested end to end.

## Protocol support

The receiver now handles:

- `STREAM START`, including first and continuation segments;
- XID-prefixed RELATION, INSERT, UPDATE, DELETE, and TRUNCATE messages;
- `STREAM STOP` segment boundaries;
- `STREAM COMMIT`; and
- both the short stream-abort frame and the protocol-v4 extended abort frame.

An active segment must contain messages for its declared XID. Duplicate first
segments, continuations for unknown transactions, nested segments, and commits
before a stop fail closed without advancing feedback.

The `START_REPLICATION` command uses pgoutput's boolean `streaming='true'`
option with protocol version 4. The `parallel` value belongs to PostgreSQL's
subscription apply configuration rather than the output plugin's boolean wire
option.

## Durable spill and recovery

Migration `0005_streaming.sql` adds `streamed_transactions` and
`streamed_changes`. At every `STREAM STOP`, the receiver commits the segment's
decoded changes into these heap tables and releases their memory. These fragment
commits intentionally do not advance received, staged, or acknowledged LSNs.

`STREAM COMMIT` aggregates the ordered fragments and calls `stage_transaction`
in the same destination transaction that deletes them. Only after that commit
succeeds does the receiver send standby-status feedback. `STREAM ABORT` deletes
the fragment transaction without entering the main journal.

If the receiver dies mid-stream, fragments remain diagnostic and recoverable,
but the source slot position stays at the last complete staged transaction. On
replay, the next first segment replaces stale fragments for the same XID and
rebuilds the transaction deterministically.

## Evidence

Run `streaming_20260914_102042` passed all 12 assertions. The
[retained results](evidence/2026-09-14-streaming-results.json) record 120 actual
stream segments for one source transaction.

During that transaction, durable fragments were visible while the main journal
still contained zero transactions and its staged LSN remained `0/0`. The probe
then terminated the receiver before source commit. Fragments survived, a new
receiver replayed all segments, and `STREAM COMMIT` exposed exactly one journal
transaction containing 120 ordered changes.

A second large source transaction was rolled back after streaming. Its abort
removed every fragment and none of its 100 rows entered the journal. A later
ordinary insert was accepted, and the publisher wrote 121 committed rows to
Iceberg with zero aborted rows. All four progress coordinates converged at
`0/474FC30`.

The lab sets `logical_decoding_work_mem=64kB`. The streaming probe additionally
sets the PostgreSQL developer option `debug_logical_replication_streaming` to
`immediate` only on its replication connection. This forces deterministic
segmentation without changing other lab consumers; production deployments
should use normal buffered behavior.

The message layouts follow PostgreSQL 18's
[logical replication protocol specification](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html),
and the deterministic lab setting follows the documented
[`debug_logical_replication_streaming` behavior](https://www.postgresql.org/docs/18/runtime-config-developer.html).

## Remaining boundaries

- Concurrent top-level XIDs have independent assemblers and spill rows, but the
  integration probe currently forces one streamed transaction at a time.
- Nested subtransaction-abort recovery is not proven. An unsupported sequence
  stops without feedback rather than risking partial application.
- Prepared transactions and protocol-v3 two-phase messages are unsupported.
- Fragment writes currently use one destination transaction per segment. Normal
  buffered segmentation is far less granular than the lab's immediate mode;
  production batching and backpressure still need measurement.
- Relation-schema changes interleaved across streamed XIDs need version-keyed
  mapping rather than the current latest-version cache.
- Unchanged TOAST reconstruction, initial copy, TLS/authentication, continuous
  service supervision, and HA remain pending.

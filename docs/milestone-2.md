# Milestone 2: live logical-replication transport

Status: a live insert-only pgoutput receiver is implemented and restart-tested.
It is a bounded one-shot prototype, not a production daemon.

## Implemented slice

The dependency-free Rust worker now speaks enough PostgreSQL v3 wire protocol
to:

- open ordinary and database-scoped replication connections;
- run destination journal queries;
- issue `START_REPLICATION ... LOGICAL` with pgoutput protocol version 4 and
  streamed transactions enabled;
- receive CopyBoth WAL and primary-keepalive frames;
- decode non-streamed `BEGIN`, `RELATION`, `INSERT`, and `COMMIT` messages;
- register relation metadata with deterministic schema versions;
- durably commit complete transactions through `stage_transaction`; and
- send standby-status feedback only after that commit succeeds.

The transaction's begin coordinate is taken from the XLogData WAL-start field;
the pgoutput `BEGIN` message's separate final-LSN field is not mislabeled as the
begin position.

After feedback is written, the worker records the sent position with
`mark_acknowledged`. On restart it begins at the durable staged checkpoint.
The source slot may redeliver around a connection failure; the journal's commit
LSN identity remains the deduplication boundary.

The wire client intentionally accepts only trust authentication in this lab
slice. It has message-size limits, validates frame structure, and fails closed
on unsupported authentication, pgoutput messages, or malformed data.

## Evidence

Run `transport_20260914_061335` passed all nine assertions. The
[retained results](evidence/2026-09-14-transport-results.json) show two source
transactions and three ordered inserts decoded by two separate receiver
processes.

The first durable journal and source slot positions matched at `0/2EEFC00`.
After restart they matched at `0/2EF3E48`. Relation metadata remained at one
schema version, a value containing a quote and backslash survived the transport,
and received/staged/acknowledged checkpoints converged. Published remained
`0/0`, as expected: this probe validates reception and staging, not the batch
publisher.

The probe removed its table, publication, slot, and journal subscription. The
post-run source inventory contained zero logical slots and zero probe
publications.

## Current boundaries

- Only INSERT is decoded and accepted.
- Protocol v4 is negotiated, but streamed in-progress transaction messages are
  not decoded yet; encountering one stops the worker visibly.
- TLS, SCRAM, password authentication, reconnect backoff, timeline-history
  validation, and HA endpoint discovery are pending.
- SQL is rendered by a narrowly scoped internal serializer. Production code
  should use bound parameters and a supported authentication/TLS stack.
- Feedback records that a status update was written to the socket; it cannot
  prove when the publisher processed that packet.
- The executable deliberately exits after a configured number of commits so
  integration tests can exercise deterministic restarts.

## Next slice

1. Add streamed-transaction protocol v4 message assembly and spill large
   transactions into the destination journal without acknowledging fragments.
2. Turn the one-shot process into a reconnecting service with health and lag
   state.
3. Add the micro-batch planner and publish staged inserts into Iceberg using the
   atomic local-catalog boundary already established in Milestone 1.
4. Add TLS/SCRAM through a reviewed permissive client stack.
5. Validate source system identity and timeline on every connection.

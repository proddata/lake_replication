# Milestone 15: source identity and slot continuity

Status: every supervised receiver cycle now validates source-cluster identity,
database identity, timeline continuity, and logical-slot safety before launching
the receiver. Unsafe continuity becomes a durable fail-closed error without
consuming or staging WAL.

## Preflight contract

The supervisor compares the live source with the subscription's durable source
record and checkpoint. It requires:

- the same PostgreSQL system identifier and database OID;
- the configured durable, non-temporary logical slot in the expected database;
- the `pgoutput` output plugin;
- no other active slot consumer;
- no `lost` WAL status, conflict, or invalidation reason; and
- a source `confirmed_flush_lsn` no later than the destination's durable staged
  LSN.

Connection I/O errors remain retryable. Identity, slot, checkpoint, and timeline
violations call `report_source_error`, set both subscription and service state to
`error`, persist the diagnostic, and terminate the supervisor before a receiver
child can start.

## Timeline validation

The current timeline may equal the recorded timeline. Regression is rejected.
For an advance, the slot must be failover-enabled and the supervisor reads the
new timeline's PostgreSQL history file. The recorded timeline must appear as an
ancestor, and its fork LSN must not precede the destination's durable staged LSN.
After successful validation, the recorded source timeline advances
monotonically.

This check establishes ancestry and protects the durable checkpoint boundary.
It does not by itself certify that a standby was synchronized before promotion;
that operational readiness check remains separate.

## Observability

Migration `0013_source_continuity.sql` appends these fields to
`service_status` and `service_health`:

- source check timestamp;
- observed system identifier, database OID, timeline, and confirmed LSN;
- cumulative source-check failures; and
- the last source-continuity error.

## Evidence

Run `source_20260914_200541` passed all ten assertions. The retained
[results](evidence/2026-09-14-source-continuity-results.json) demonstrate:

- normal replication and graceful stop after a successful preflight;
- durable observation of the exact source system identifier, database OID, and
  timeline;
- rejection of a deliberately mismatched system identifier before receiver
  launch;
- rejection of a deliberately removed logical slot before receiver launch;
- durable `error` state and diagnostic counters for both unsafe cases; and
- unchanged Iceberg rows and staged/confirmed positions when queued WAL could
  not be proven safe to consume.

Pure Rust checks additionally cover lost/invalidation state, a slot confirmed
past the durable staged LSN, timeline regression, non-failover slots after
timeline advance, missing ancestry, and a fork point before the staged LSN.

## Remaining boundaries

- The supervisor still uses one configured source endpoint; it does not discover
  a Patroni leader or rotate through candidate endpoints.
- The laboratory has one PostgreSQL primary, so the timeline-advance path is
  unit-tested rather than exercised by a real standby promotion.
- Standby readiness requires deployment checks for `sync_replication_slots`,
  `synchronized_standby_slots`, and a persistent synchronized slot before
  promotion.
- Automatic rebuild after slot loss remains operator-controlled. The supervisor
  deliberately refuses to create a replacement slot or skip ahead.
- Trust authentication and superuser access to control/timeline metadata remain
  prototype constraints.

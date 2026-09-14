# Milestone 14: destination control reconnection

Status: an established supervisor now survives loss of its destination control
connection, reconnects with bounded exponential delay, safely reacquires
subscription ownership, records the outage, and resumes replication.

## Reconnection model

The supervisor uses a distinct PostgreSQL `application_name` containing its
subscription ID. Every control-plane query runs through one reconnecting path.
An I/O failure causes the supervisor to:

1. discard the failed control session, which also releases its session advisory
   lock at PostgreSQL;
2. reconnect using exponential delay bounded by the existing supervisor backoff
   settings;
3. acquire the subscription advisory lock before issuing any further control
   operation;
4. exit rather than run concurrently if another supervisor obtained the lock;
5. durably record the reconnect count, timestamp, and triggering connection
   error; and
6. retry the interrupted control operation and continue normal worker cycles.

Receiver and publisher journal correctness remains independent of this control
session. Their existing idempotency and checkpoint ordering cover replay if a
control response is lost at an ambiguous boundary.

## Observability

Migration `0012_service_reconnect.sql` extends `service_status` and
`service_health` with:

- `reconnections`;
- `last_reconnected_at`; and
- `last_connection_error`.

These fields are appended to the existing view contract so upgrading does not
rename or reorder its previous columns.

## Evidence

Run `supervisor_20260914_194306` passed all ten assertions. The retained
[results](evidence/2026-09-14-reconnect-results.json) demonstrate that:

- child retry/backoff and duplicate-supervisor fencing still work;
- the destination backend holding the supervisor control connection was
  forcibly terminated;
- the original supervisor reacquired ownership and persisted reconnect
  telemetry;
- a source transaction committed around the outage appeared exactly once after
  recovery;
- SQL-requested graceful shutdown still completed with exit status zero; and
- source slot confirmation remained equal to the durable acknowledgement LSN.

## Remaining boundaries

- Failure of the initial destination connection still exits immediately; the
  reconnect loop begins after the service has established ownership.
- PostgreSQL server errors and protocol violations are treated as fatal rather
  than transient I/O failures.
- A competing supervisor that acquires the lock during an outage wins ownership;
  the reconnecting process exits safely and requires an external service manager
  to restart it if appropriate.
- Source identity, timeline ancestry, failover-slot synchronization, slot loss,
  and leader rediscovery remain the next high-availability gate.
- The prototype still uses trust authentication and a single configured endpoint.

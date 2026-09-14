# Milestone 13: supervised service operation

Status: the one-shot receiver and publisher now run as bounded child cycles under
a long-running supervisor with durable health, exponential retry, duplicate
process fencing, subscription-aware pausing, and SQL-controlled graceful stop.

## Runtime model

`lake-replication-supervisor` owns a PostgreSQL advisory lock scoped to one
subscription. A second supervisor exits before changing service state. While the
subscription is `streaming`, each successful cycle:

1. launches the receiver for one transaction, with a socket read timeout so an
   idle source returns control to the supervisor;
2. checks the durable staged and published checkpoints;
3. launches the publisher only when staged WAL remains; and
4. records its heartbeat and worker-cycle counters.

Receiver or publisher failure is recorded before a retry. Delay starts at
`LAKE_SUPERVISOR_BASE_BACKOFF_MS`, doubles after consecutive failures, and is
capped by `LAKE_SUPERVISOR_MAX_BACKOFF_MS`. A successful cycle resets only the
consecutive count; the lifetime failure count and last error remain observable.
Paused, disabled, or errored subscriptions do not launch children.

The default receiver idle timeout is one second. Existing direct receiver use
is unchanged because `LAKE_RECEIVER_IDLE_TIMEOUT_MS` defaults to zero outside
the supervisor.

## Control and health

Starting the binary calls `lake_replication.start_service`. Operators request a
clean stop with:

```sql
SELECT lake_replication.request_service_stop(<subscription_id>);
```

The supervisor observes this between bounded receiver cycles and while waiting
in backoff, releases its session lock by disconnecting, and leaves a durable
`stopped` status with its PID cleared.

`lake_replication.service_health` joins service status to the subscription and
checkpoint. It exposes desired and actual state, heartbeat and lifecycle times,
failure and cycle counters, retry timing, last error, all four LSNs, and current
staged transaction/change and streamed-transaction queue depths.

## Evidence

Run `supervisor_20260914_173031` passed all seven assertions. The retained
[results](evidence/2026-09-14-supervisor-results.json) demonstrate:

- a lab-injected child failure became a future-dated backoff with durable
  consecutive and total failure counts;
- a second supervisor was rejected by the subscription advisory lock;
- the original process recovered, received, and published the queued transaction
  exactly once;
- health returned to `running`, preserved one lifetime failure, reported both
  worker cycles, and showed an empty queue with converged checkpoints;
- an SQL stop request produced exit status zero and durable stopped state; and
- source slot confirmation equaled the acknowledged destination checkpoint.

## Remaining boundaries

- The supervisor is a foreground process. A host service manager is still needed
  to restart it after process, host, or destination-connection failure.
- Its control connection is not re-established in process. Source-side child
  failures are retried, but loss of the destination control connection terminates
  the supervisor so the external service manager can restart it cleanly.
- Successful child output is captured and discarded. Central log shipping,
  metrics export, alert thresholds, and heartbeat-staleness alerting remain
  deployment concerns.
- Graceful-stop latency is bounded by the configured receiver idle timeout and
  poll interval. The supervisor does not interrupt a publisher transaction.
- Authentication remains the prototype wire client's trust-authentication model.

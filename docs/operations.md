# Operations guide

This guide defines the beta monitoring contract for `lake_replication`.
Run all queries against the pg_lake destination database containing the
`lake_replication` extension.

## Subscription status

`lake_replication.subscription_status` is the primary operator interface. It
extends the detailed `service_health` view with derived heartbeat age, durable
journal lag, source-to-Iceberg lag, and a stable health classification.

```sql
SELECT subscription_name,
       health_state,
       health_reason,
       service_state,
       heartbeat_age_seconds,
       source_lag_bytes,
       journal_lag_bytes,
       consecutive_failures,
       receiver_cycles,
       publisher_cycles,
       last_failure_at,
       last_error
FROM lake_replication.subscription_status
ORDER BY subscription_name;
```

Health states have these meanings:

| State | Meaning |
|---|---|
| `healthy` | The subscription is running, its heartbeat is current, and it is not retrying. |
| `degraded` | The service is starting, paused, backing off, or has consecutive failures. |
| `critical` | The subscription/service is in error or its heartbeat is more than 30 seconds old. |
| `stopped` | An operator requested the service to stop. This is not an incident by itself. |

`source_lag_bytes` is the difference between the source's most recently
observed current WAL position and the destination's published checkpoint.
`journal_lag_bytes` is the part already received into the durable destination
journal but not yet published to Iceberg. Source lag is `NULL` until the first
successful source preflight.

The view also retains raw checkpoints and diagnostics. Use those fields when a
summary state needs investigation:

```sql
SELECT subscription_name,
       received_lsn,
       staged_lsn,
       published_lsn,
       acknowledged_lsn,
       observed_current_lsn,
       active_source_address,
       staged_transactions,
       staged_changes,
       last_source_error,
       last_source_candidate_errors,
       last_connection_error
FROM lake_replication.subscription_status
WHERE health_state IN ('degraded', 'critical');
```

## Metrics export

`lake_replication.metrics` exposes one numeric sample per metric and
subscription. `subscription_id` and `subscription_name` are the intended
labels. It can be collected by any PostgreSQL SQL exporter.

```sql
SELECT metric, subscription_id, subscription_name, value
FROM lake_replication.metrics
ORDER BY metric, subscription_id;
```

The interface currently exports:

- `lake_replication_subscription_healthy`;
- `lake_replication_heartbeat_age_seconds`;
- `lake_replication_source_lag_bytes`;
- `lake_replication_journal_lag_bytes`;
- `lake_replication_consecutive_failures`;
- `lake_replication_failures_total`;
- `lake_replication_receiver_cycles_total`;
- `lake_replication_publisher_cycles_total`;
- `lake_replication_staged_transactions`;
- `lake_replication_staged_changes`;
- `lake_replication_reconnections_total`; and
- `lake_replication_source_switches_total`.

Grant a monitoring role only the access it needs:

```sql
GRANT CONNECT ON DATABASE analytics TO replication_monitor;
GRANT USAGE ON SCHEMA lake_replication TO replication_monitor;
GRANT SELECT ON lake_replication.subscription_status,
                lake_replication.metrics
TO replication_monitor;
```

## Engine admission

Beta.3 serializes managed bulk engine transactions across destination databases.
Destination creation, additive schema changes, initial-copy publication and
rebuild promotion share one permit held through commit. Streaming publication
does not request the bulk permit; it has four independent transaction slots.
PostgreSQL staging requests neither.

As `lake_replication_admin` or an authorized database operator, inspect holders
and waiters with:

```sql
SELECT pid, database_name, admission_state, waitstart,
       wait_event_type, wait_event
FROM lake_replication.bulk_admission
ORDER BY waitstart NULLS LAST, pid;
```

An empty view means no managed bulk transaction holds or waits for the permit;
streaming workers can still be active. A queued bulk operation is normal while
another copy or schema transaction commits. Cancellation, transaction rollback
and backend exit release the permit. Temporary schema lock contention propagates
as a retryable SQL error rather than durably pausing the subscription; an
incompatible schema still requires operator attention.

Inspect streaming holders and waiters with:

```sql
SELECT pid, database_name, slot, admission_state, waitstart,
       wait_event_type, wait_event
FROM lake_replication.stream_admission
ORDER BY slot, waitstart NULLS LAST;
```

Four streaming engine transactions may be admitted alongside the bulk transaction.
Nested publication/apply calls reuse their transaction's slot. A request tries any
free slot, then joins one slot's native queue if the pool is full; this does not
promise one global FIFO. Cancellation, rollback and backend exit release slots.
These limits apply within this PostgreSQL instance to managed SQL entry points.

These limits do not bound staging memory, connections, retained source WAL or
application SQL sent directly to the engine. Continue monitoring source WAL and
journal backlog while copies wait.

## Suggested alerts

Tune lag thresholds to the workload and service-level objective. These are
safe starting points for the beta:

| Alert | Condition | Initial severity |
|---|---|---|
| Service critical | `health_state = 'critical'` for 1 minute | Page |
| Repeated retry | `consecutive_failures >= 3` for 5 minutes | Page |
| Stale heartbeat | `heartbeat_age_seconds > 30` while desired state is `running` | Page |
| Source lag | `source_lag_bytes > 1 GiB` for 10 minutes | Ticket/page according to WAL budget |
| Journal backlog | `journal_lag_bytes > 256 MiB` for 10 minutes | Ticket |
| No receiver progress | receiver cycle counter does not increase for 10 minutes while running | Ticket |
| Source switch | increase in `lake_replication_source_switches_total` | Informational; correlate with HA events |

A direct SQL alert query is useful before a metrics exporter is installed:

```sql
SELECT subscription_name, health_state, health_reason,
       heartbeat_age_seconds, source_lag_bytes, journal_lag_bytes,
       consecutive_failures, last_error
FROM lake_replication.subscription_status
WHERE health_state = 'critical'
   OR consecutive_failures >= 3
   OR source_lag_bytes > 1073741824
   OR journal_lag_bytes > 268435456;
```

## Structured worker logs

Every Rust worker writes one JSON object per line. Informational events go to
stdout; warnings and errors go to stderr. The subscription launcher redirects
both streams to its configured `lake_replication.log_directory`.

Each record has stable common fields:

```json
{
  "timestamp_unix_ms": 1789963200000,
  "level": "WARN",
  "component": "supervisor",
  "event": "cycle_failed",
  "pid": 4242,
  "message": "worker cycle failed; retrying in 1000ms: ...",
  "subscription_id": "7",
  "retry_in_ms": "1000",
  "consecutive_failures": "1",
  "error": "..."
}
```

Event-specific values are strings. Important event names include
`replication_started`, `transaction_staged`, `snapshot_acquired`,
`initial_copy_completed`, `batch_published`, `cycle_failed`,
`destination_connection_lost`, `destination_connection_restored`, and
`process_failed`.

Configure log rotation for the worker directory with the host's standard log
manager. Do not grant untrusted users write access: the PostgreSQL operating
system user must own the directory, and log records can contain relation names,
LSNs, and database error details.

## First-response checklist

1. Query `subscription_status` and record the health reason, lag, checkpoints,
   active source, and last error.
2. Inspect recent `supervisor` events for retry, source-continuity, or
   destination-connection failures.
3. If source lag grows but journal lag is near zero, investigate source access,
   the logical slot, TLS/authentication, and WAL retention.
4. If journal lag grows, investigate the publisher, pg_lake, object storage,
   destination locks, and destination disk space.
5. Do not drop or recreate a slot to clear an alert. A missing, invalidated, or
   unexpectedly advanced slot is a fail-closed condition requiring a controlled
   rebuild.
6. Use `ALTER SUBSCRIPTION name DISABLE` to stop work deliberately. Preserve
   the status rows and logs until the incident has been understood.

## Known beta boundaries

- Metrics are pulled through SQL; this project does not run an HTTP metrics
  listener.
- Lag is measured in WAL bytes, not wall-clock seconds.
- The 30-second heartbeat classification is fixed in the initial extension
  version. Exporters may apply a stricter threshold.
- [Upgrade, backup, and rollback procedures](upgrade-and-rollback.md) are
  documented separately; uninstall instructions ship inside each bundle.

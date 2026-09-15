# Milestone 16: leader-aware source endpoint selection

Status: the supervisor can discover a safe writable source from an ordered set
of PostgreSQL endpoints, route receiver cycles to the selected endpoint, switch
endpoints without losing its durable checkpoint, and retry temporary total
unavailability without poisoning the subscription.

## Endpoint selection

`LAKE_SOURCE_ADDRESSES` accepts a comma-separated ordered list of PostgreSQL
`host:port` addresses. When it is absent, the existing single
`LAKE_SOURCE_ADDRESS` setting remains the fallback.

Before each receiver cycle, the supervisor probes candidates in order. It skips
connection failures and servers where `pg_is_in_recovery()` reports a standby.
For each writable candidate, it applies the complete Milestone 15 source
identity, slot, checkpoint, and timeline validation. The first safe candidate is
passed explicitly to the receiver child as `LAKE_SOURCE_ADDRESS`.

This database-level leader check works with direct node addresses, a Patroni
leader service, or a load balancer that may briefly expose an old primary. It
does not require a Patroni REST client.

If no candidate is safe, connection failures and standby-only results are
retryable. A set consisting entirely of reachable but unsafe writable servers
fails closed. This distinction prevents a temporary leader election from
changing a healthy subscription to `error`, while still refusing a proven wrong
cluster or unusable slot.

## Observability

Migration `0014_source_endpoints.sql` adds these fields to `service_status` and
appends them to `service_health`:

- `active_source_address`;
- `source_switches`;
- `last_source_switch_at`; and
- `last_source_candidate_errors`.

Initial selection does not count as a switch. A later healthy selection of a
different configured address increments the durable counter and timestamp.
Failures encountered before a successful candidate remain visible for diagnosis.

## Evidence

Run `endpoints_20260915_043735` passed all eight assertions. The retained
[results](evidence/2026-09-15-source-endpoints-results.json) demonstrate:

- fallthrough from an unreachable first candidate to the writable source;
- exact selected-address and candidate-error telemetry;
- successful replication through the selected fallback;
- a later switch to another address for the same source and slot;
- durable switch count and timestamp;
- continued exactly-once visible rows after the switch;
- retry/backoff with the subscription still `streaming` when every endpoint is
  temporarily unreachable; and
- SQL-controlled graceful stop during endpoint outage.

The complete source identity and missing-slot integration suite was also rerun
after introducing endpoint selection and passed all ten assertions.

## Remaining boundaries

- The laboratory aliases one PostgreSQL primary through two network names; a
  real Patroni promotion and timeline advance still require a multi-node lab.
- Candidate configuration is static for the process lifetime. DNS changes are
  naturally re-resolved, but a changing explicit node list requires restart.
- Selection probes candidates sequentially and uses the operating system's TCP
  connection timeout. Per-candidate connect deadlines are not yet configurable.
- Patroni readiness validation still needs a real standby with synchronized
  failover slots and promotion fencing.
- TLS, password/SCRAM authentication, and connection-string parsing remain
  outside the dependency-free prototype wire client.

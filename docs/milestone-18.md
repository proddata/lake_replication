# Milestone 18: Patroni automatic failover

Status: a pinned two-member Patroni cluster now demonstrates automatic leader
election and PostgreSQL 18 promotion after unplanned leader loss. The
replication supervisor rediscovers the promoted endpoint and continues from the
synchronized failover logical slot without rebuilding or duplicating rows.

## Patroni laboratory

`deploy/patroni/compose.yml` builds PostgreSQL 18 with Patroni 4.1.5 and uses the
official etcd 3.6.14 image as a single-node laboratory DCS. Both PostgreSQL and
etcd base images are pinned by manifest digest. The Patroni member configuration
enables:

- synchronous mode with one eligible synchronous standby;
- `wal_level=logical` and failover logical-slot synchronization;
- Patroni-managed physical replication slots;
- `hot_standby_feedback=on` and `sync_replication_slots=on`;
- zero permitted replication lag at leader election; and
- data checksums and `wal_log_hints` for later `pg_rewind` work.

Patroni's REST `/cluster` view is used by the test to establish that one leader
and one streaming synchronous standby exist before injecting failure. Patroni's
documented `/primary` and `/read-write` endpoints remain suitable inputs for a
production load balancer; the worker itself continues to validate writability,
source identity, slot state, and timeline ancestry at the PostgreSQL layer.
[Patroni's REST API documentation](https://patroni.readthedocs.io/en/latest/rest_api.html)
defines the leader health endpoints and failover behavior, while its
[replication-mode documentation](https://patroni.readthedocs.io/en/latest/replication_modes.html)
describes the DCS-backed synchronous mode used here.

## Unplanned failure path

`make patroni-test` creates a unique source table, publication, failover slot,
and destination schema. Once the logical slot is persistent and synchronized on
the standby, the probe sends `SIGKILL` to whichever Patroni member currently
owns the leader lock. It does not call `pg_promote()`, Patroni's failover API, or
`patronictl`.

The surviving member acquires the DCS leader lock and promotes itself. The
supervisor tolerates the election interval as retryable unavailability, selects
the new writable endpoint, validates the descendant timeline and promoted slot,
then resumes receiver and publisher cycles.

## Evidence

Run `patroni_20260915_051737` passed all nine assertions. The retained
[results](evidence/2026-09-15-patroni-results.json) demonstrate:

- election of one leader and one streaming synchronous standby;
- publication through the original elected leader;
- a persistent, synchronized, valid failover slot on the standby;
- active PostgreSQL slot-synchronization prerequisites;
- automatic standby promotion after an unplanned leader `SIGKILL`;
- promotion in 19.629 seconds with the configured minimum Patroni TTL;
- exact pre- and post-failover rows with no visible duplicates;
- durable endpoint-switch and timeline-advance health telemetry;
- promoted-slot confirmation at the destination's acknowledged LSN; and
- clean SQL-controlled supervisor shutdown after the failover.

The probe removes its dedicated etcd and PostgreSQL volumes in a `finally`
block. It reuses the main lab only as the pg_lake destination.

## Safety boundary

This milestone combines Patroni synchronous election with PostgreSQL failover
slot synchronization. Milestone 17 separately proves strict
`synchronized_standby_slots` decoding fencing in the native two-node lab. The
Patroni lab does not keep a static fence after promotion because the required
physical slot name changes with the leader; production strict fencing needs
leader-aware dynamic configuration and an operational policy for loss of the
last eligible standby.

Other remaining boundaries are:

- the DCS has one etcd member and is not itself highly available;
- controlled switchover, split-brain network partitions, and DCS loss are not
  yet exercised; and
- lab authentication is trust-based and the Patroni REST API is unauthenticated.

Milestone 19 extends this laboratory through former-primary rewind, slot
resynchronization, and a second automatic failover.

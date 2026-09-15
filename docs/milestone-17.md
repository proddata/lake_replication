# Milestone 17: synchronized logical-slot failover

Status: the PostgreSQL-to-Iceberg worker now continues from a PostgreSQL 18
primary to its promoted physical standby without rebuilding the subscription,
losing acknowledged progress, or duplicating visible destination rows.

## Failover-capable initial synchronization

Initial synchronization creates logical slots with PostgreSQL 18's extended
replication syntax and enables `FAILOVER` by default. Set
`LAKE_SLOT_FAILOVER=0` only for an environment that intentionally does not
synchronize logical slots. A non-failover slot remains valid on its original
timeline, but Milestone 15 validation refuses to follow it across a timeline
advance.

## Two-node PostgreSQL laboratory

`deploy/failover/compose.yml` starts a disposable primary and physical standby
from the same pinned PostgreSQL 18 image used by the main laboratory. The
primary pre-creates physical slot `ha_standby` and configures
`synchronized_standby_slots=ha_standby`. The standby is initialized with
`pg_basebackup` and enables:

- `primary_slot_name=ha_standby`;
- `hot_standby_feedback=on`; and
- `sync_replication_slots=on`.

Together with a failover logical slot, these are the PostgreSQL prerequisites
for making the logical slot persistent and synchronized on the standby while
preventing the primary from decoding beyond the physical standby.
[PostgreSQL's failover-slot procedure](https://www.postgresql.org/docs/18/logical-replication-failover.html)
and [logical decoding standby guidance](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION)
describe these controls and the required readiness checks.

The lab deliberately performs promotion with `pg_promote()` rather than an HA
controller. This isolates and proves the replication data plane first. Patroni
leader election and service routing remain the next orchestration layer.

## Current timeline discovery

The first real promotion run exposed an important distinction:
`pg_control_checkpoint().timeline_id` describes the most recent checkpoint and
can briefly retain the old timeline after promotion. The supervisor now derives
the current timeline from the timeline prefix of the active WAL filename after
confirming that a candidate is writable. Timeline ancestry validation therefore
observes the promotion immediately instead of waiting for a checkpoint.

## Evidence

Run `failover_20260915_045438` passed all ten assertions. The retained
[results](evidence/2026-09-15-failover-results.json) demonstrate:

- creation of a persistent, valid failover logical slot;
- publication through the original primary;
- a persistent synchronized copy of the logical slot on the standby;
- active standby synchronization settings and the primary's physical-slot
  decoding fence;
- successful physical standby promotion after stopping the primary;
- exact rows before and after promotion, without duplicates;
- a durable endpoint switch and timeline advance from 1 to 2 in health state;
- agreement between the promoted slot's confirmed flush LSN and the durable
  destination acknowledgement; and
- SQL-controlled graceful supervisor shutdown after failover.

Run with `make failover-test`. The probe owns and removes its dedicated primary
and standby volumes; it does not reuse the main pg_lake destination volume.

## Remaining boundaries

- Promotion is explicit; Patroni/DCS election, fencing, leader service routing,
  and automatic old-primary rejoin are not yet exercised.
- The lab tests one standby and one promotion, not cascading replicas or
  repeated switchovers.
- Authentication is trust-based on loopback and the isolated Compose network;
  production TLS and SCRAM remain outside the dependency-free wire client.
- PostgreSQL 17 and mixed-version upgrade paths are not covered.

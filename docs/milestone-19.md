# Milestone 19: former-primary rewind and repeated failover

Status: the Patroni laboratory now repairs and rejoins a divergent former
primary, resynchronizes its PostgreSQL 18 logical failover slot, and safely uses
that member for a second automatic promotion while one replication supervisor
continues running.

## Timeline-safe rejoin

Both Patroni members enable `use_pg_rewind`, PostgreSQL data checksums, and
`wal_log_hints`. Patroni dynamic configuration also enables `check_timeline`,
preventing a node on an older history from participating in leader election
until its history is safe.

After the first leader is killed and the synchronous standby promotes to
timeline 2, the probe commits another source row. Restarting the killed member
therefore presents Patroni with a genuinely divergent timeline, not merely a
cleanly stopped replica. The test waits until Patroni reports that member as the
streaming synchronous standby and requires the member log to contain a
successful `pg_rewind exit code=0` result.

It then verifies that the rejoined member:

- retains the expected PostgreSQL system identifier;
- is running in recovery rather than accepting writes;
- has become Patroni's synchronous standby; and
- again owns a persistent, synchronized, valid copy of the logical failover
  slot.

## Second automatic failover

With the former primary caught up and its logical slot ready, the probe commits
a fourth row, sends `SIGKILL` to the current leader, and waits for Patroni to
promote the rewound member. The unchanged supervisor moves back to its original
endpoint, validates timeline 3 and the promoted slot, and publishes a fifth row.

`service_health` must report exactly two source endpoint switches and observed
timeline 3. The final promoted slot's confirmed flush LSN must equal the durable
destination acknowledgement.

## Evidence

Run `patroni_20260915_052545` passed all 18 assertions. The retained
[results](evidence/2026-09-15-patroni-rejoin-results.json) demonstrate:

- first automatic promotion in 19.506 seconds;
- exact replication through the first failover;
- successful `pg_rewind` and synchronous former-primary rejoin;
- logical failover-slot resynchronization on the rejoined member;
- uninterrupted publication while that member rejoins;
- second automatic promotion in 20.529 seconds;
- five exact visible rows across three PostgreSQL timelines;
- two durable endpoint switches with no duplicate destination rows;
- valid slot confirmation at the final acknowledged LSN; and
- clean supervisor shutdown after the repeated failover.

Run with `make patroni-test`. The probe removes all dedicated Patroni, etcd, and
PostgreSQL volumes afterward.

## Remaining boundaries

- The second killed leader is not rejoined a second time in the same run.
- Controlled Patroni switchover and rolling maintenance are not covered.
- Network partitions, DCS loss, watchdog fencing, and a multi-member etcd
  quorum remain untested.
- Rewind failure and fallback replica reinitialization need explicit tests.
- The two-data-node synchronous topology prioritizes durability; write
  availability while either member is absent depends on Patroni's non-strict
  synchronous-mode policy.

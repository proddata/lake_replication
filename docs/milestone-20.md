# Milestone 20: planned switchover and rolling maintenance

Status: the Patroni lifecycle probe now performs a targeted synchronous
switchover and restarts PostgreSQL on the demoted replica while preserving the
replication checkpoint, exact destination contents, and correct source
telemetry.

## Coordinated drain contract

An initial experiment requested Patroni switchover while the supervisor was
actively cycling a logical replication connection. PostgreSQL began fast
shutdown, but demotion did not finish within Patroni's request deadline and the
REST endpoint returned HTTP 503. No unsafe promotion occurred.

The validated planned-maintenance sequence is therefore:

1. confirm that the target member is Patroni's streaming synchronous standby;
2. confirm that its logical slot is `synced`, persistent, failover-enabled, and
   not invalidated;
3. call `lake_replication.request_service_stop()` and wait for the supervisor to
   exit cleanly, closing the logical replication connection;
4. send a targeted Patroni `POST /switchover` with both current leader and
   candidate;
5. wait until the candidate is the running leader and the former leader is its
   streaming synchronous standby; and
6. restart the supervisor, which resets desired state through
   `lake_replication.start_service()` and resumes from the durable acknowledged
   LSN.

This is an explicit drain-and-resume contract, not transparent zero-downtime
process handoff. It avoids forcing the old postmaster down and leaves Patroni's
healthy-candidate, lag, synchronous-member, and timeline checks in control of
the planned promotion. Patroni documents these checks and the request fields in
its [switchover API](https://patroni.readthedocs.io/en/latest/rest_api.html#switchover).

## Rolling replica restart

After switchover and timeline 4 continuation, the probe calls the demoted
member's Patroni `POST /restart` endpoint with `role=replica`. The role condition
prevents an accidental primary restart if cluster state changed between
planning and execution.

The test verifies that PostgreSQL's postmaster start timestamp changes, the
member returns as the streaming synchronous standby, and its synchronized
logical slot becomes ready again. A subsequent source transaction is published
without changing the worker's selected leader or incrementing the source-switch
counter.

## Evidence

Run `patroni_20260915_053457` passed all 32 assertions. The retained
[results](evidence/2026-09-15-patroni-maintenance-results.json) cover the complete
three-milestone Patroni lifecycle and specifically demonstrate:

- a clean supervisor drain before maintenance;
- HTTP 200 acceptance of the targeted synchronous switchover;
- leader/standby role exchange in 7.142 seconds;
- worker restart and exact publication on timeline 4;
- three durable source switches across four timelines;
- HTTP 200 acceptance of a role-fenced replica restart;
- a changed replica postmaster start timestamp and synchronous rejoin;
- restoration of the replica's synchronized failover slot;
- seven exact visible rows after crashes, rewinds, switchover, and maintenance;
- no spurious worker source switch during replica maintenance; and
- final slot confirmation equal to the destination acknowledged LSN.

Run the lifecycle with `make patroni-test`. Both pre-switchover and
post-switchover supervisor output are retained in the run directory, and all
dedicated Patroni/etcd volumes are removed afterward.

## Remaining boundaries

- The supervisor drain is operator-coordinated rather than an integrated
  Patroni pre-switchover callback or quiesce API.
- Primary rolling restart requires a preceding switchover; only the demoted
  replica restart is exercised directly.
- Scheduled switchovers, cancellation, and rejected unsafe candidates are not
  tested.
- Network partitions, DCS loss, watchdog fencing, and a multi-member etcd
  quorum remain outside this laboratory.

# Sustained direct publication with native maintenance

This follow-up extends the [deadlock recovery qualification](wal-direct-deadlock-recovery.md) to 15-minute mixed-write phases at 6 and 8 source transactions per second. It measures local behavior under maintenance, rather than establishing a production service limit.

## Method

Each rate uses fresh private volumes and the unchanged v13 worker/native runtime, one CPU and 512 MiB for PostgreSQL, one CPU and 2 GiB for pgduck, a 1536 MB engine query budget, and no swap. Native autovacuum runs every 20 seconds. Direct publication coalesces for up to one second or 32 complete source transactions. Deadlock recovery allows three additional attempts. The write phase lasts at least 900 seconds; final drain has a separate 90-second deadline.

Every source transaction inserts four rows into each of two tables. After the first transaction, it also updates and deletes one earlier row per table. Warmup is excluded from workload counters. Exact source/mirror contents and transaction receipts are checked after drain. Resource observers run every two seconds; CPU includes source writes, native maintenance, publication, retries and container observers, but excludes host/controller and MinIO CPU. Object requests describe the observed trace, not provider billing.

The harness now retains native maintenance and retry diagnostics even on failure. Its existing file-count sampling also records active PostgreSQL backends, wait events and blocking PIDs. These are sparse observations, not a CPU profile or an exhaustive lock trace. Longer transactions can appear in several samples. No timing wrapper, diagnostic native library or altered publication query is used in these runs.

## Results

[Retained evidence](evidence/2026-11-16-wal-direct-sustained-maintenance.json) preserves image/source identities, checks, resource endpoints, minute-by-minute progress, wait summaries and native vacuum durations. Both private fixtures clean up successfully. There is one run per rate; these are local ARM64 observations.

| Measurement | 6 transactions/second | 8 transactions/second |
| --- | ---: | ---: |
| Source transactions | 5,400 | 7,200 |
| Write phase | 900 seconds | 900-second paced workload |
| Result | Pass: exact final source/mirror rows, 25 checks | Fail: 90-second drain deadline exceeded |
| Final drain | 11.83 seconds | Not completed |
| Last regular published sample | 5,400 | 5,959 at 989.29 seconds |
| Maximum sampled backlog | 294 | 1,657 |
| Publication lag, sampled p95 upper bound | 43.52 seconds | Not computed: incomplete publication |
| Recovered foreground deadlocks | 11 | 45 |
| Maximum retries for one publication | 2 | 2 |
| PostgreSQL CPU | 478.10 seconds | 722.42 seconds through failure diagnostics |
| pgduck CPU | 199.56 seconds | 217.92 seconds through failure diagnostics |
| Total database/engine CPU | 677.66 seconds | 940.34 seconds through failure diagnostics |
| Successful native vacuum events | 74 | 60 |
| Sampled file-count drops | 43 | 45 |
| PostgreSQL cgroup lifetime memory peak | 512 MiB | 512 MiB |
| pgduck cgroup lifetime memory peak | 1,234.7 MiB | 961.0 MiB |
| OOM kills / OOM events | 0 / 0 | 0 / 0 |

The failed run's CPU window extends through resource diagnostics after worker termination. It is not the cost of publishing all 7,200 transactions and must not be compared as a completed-workload saving. No final exact-row comparison runs for this failed fixture. Its source heaps contain 21,602 rows each and the slot still has reserved WAL history in the failure diagnostic. The 5,959 publication count is the last regular sample, not a separately verified final checkpoint.

At 6/second, backlog spikes and then recovers during offered writes. The run finishes exactly, but its 43.52-second p95 and 51.47-second maximum sampled lag upper bounds do not establish a low-latency service target. At 8/second, backlog grows across the write phase and remains large after writes stop. Recovering each deadlock does not make this offered rate sustainable.

Both PostgreSQL containers reach their memory cap without OOM. The cgroup `memory.events.max` counter increases by 1,270 and 1,405 respectively; it counts encounters with the memory boundary, not OOM kills. This includes page cache and shared memory and does not establish a process-RSS leak. The direct worker itself remains small in the completed run: 14,076 KiB lifetime peak RSS and 0.84 lifetime CPU-seconds, including warmup.

## Optimization direction

The database/native path remains the useful target. PostgreSQL accounts for 478.10 of 677.66 measured CPU-seconds in the completed run; the Rust worker consumes less than one lifetime CPU-second. Previous [native profiling](wal-direct-publication-profile.md) directly measured repeated full file-catalog loads for UPDATE/DELETE. Current file counts still peak at 1,099 and 1,221 across the two mirrors despite active compaction.

Maintenance remains expensive and contested. Logged vacuum elapsed durations sum to 156.62 and 380.89 seconds; the longest events are 42.29 and 13.81 seconds. Those are wall durations that can include waits and overlap publication, so they must not be added to CPU or interpreted as an isolated maintenance percentage. Publisher samples observe advisory-lock waits 10 and 37 times, with additional extension/engine IPC waits. These samples support further investigation but do not identify a precise CPU split.

The next experiment should compare coordinated maintenance/consistent native lock ordering against the unchanged baseline, retaining deadlock retry as recovery. Then remeasure the full metadata-loading path under mixed writes. A candidate must reduce total CPU or lag under the same memory caps and complete exact-row/recovery checks before repeating both 15-minute rates. Increasing retry counts would not solve the measured throughput deficit. General mutation reordering would require separate dependency/correctness work and is not introduced here.

The [subsequent native lock experiment](wal-direct-prelock.md) repeats both rates with early full-mapping target locks. It improves the passing 6/second result to 589.03 CPU-seconds and a 6.15-second sampled p95 lag upper bound, with zero retries. At 8/second the deadline still fails despite zero retries. The baseline results above remain unchanged.

## Reproduce

Build/select the optional v13 runtime and pinned local storage images as described in the recovery report, then run these fixtures sequentially:

```sh
python3 tests/local_iceberg/wal_direct_maintenance.py --transactions 5400 --interval .16666666666666666
python3 tests/local_iceberg/wal_direct_maintenance.py --transactions 7200 --interval .125
```

The fixture permits up to 10,000 transactions. Deployment defaults and vendor sources remain unchanged. The earlier 3,000-transaction run at 10/second failed its drain deadline; the earlier 1,000-transaction run at 4/second passed. Those runs have different durations and cannot isolate the effect of rate alone.

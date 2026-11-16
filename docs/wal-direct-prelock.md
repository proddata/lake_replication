# Acquiring native table locks before direct publication

This experiment addresses the native maintenance lock cycle found in the
[15-minute baseline](wal-direct-sustained-maintenance.md). The optional direct
publisher acquires each mapped target's existing native update lock before
writing any target metadata. Whole-publication deadlock retry remains enabled.

## What changes

Native compaction holds a per-table update lock while changing file metadata.
The baseline publisher can INSERT first, modify metadata, and later request that
lock for UPDATE/DELETE. Compaction can then wait for the publisher transaction
while the publisher waits for compaction. The observed advisory lock belongs to
native lock class 101 and the target relation, rather than a global COMMIT lock.

A private SQL bridge calls the existing native `LockTableForUpdate` function.
An optional `apply_payload` wrapper first applies the existing target
eligibility/DDL guard, then acquires all frozen mapping targets in ascending OID
order. Only then does the original apply function run with its existing schema
checks and change order. It acquires the complete target set even if a streamed
chunk only mentions one table. The native transaction locks persist across
chunks until COMMIT or rollback. No source transaction is acknowledged earlier.

The bridge is superuser-only, with PUBLIC execution revoked. It is exposed only
by explicitly installed experimental SQL in a child image, not by production
extension migrations. The image retains source/library hashes and rejects an
unexpected native source hash during build. Native compaction and INSERT
functions themselves are unchanged. Default workers and the release prerequisite
patchset do not enable this wrapper. The vendor checkout stays clean.

This moves waiting before publication writes and can avoid wasted abort/replay
work. It also serializes pure INSERT publications against compaction and can
lengthen waits. It does not guarantee a throughput improvement, eliminate all
possible deadlocks, or establish compatibility with arbitrary external writers.
Snapshot copying remains unchanged; subsequent direct publication can use the
wrapper. Managed direct ownership, production privileges and native upgrade
compatibility remain separate gates.

## Qualification method

The targeted fixture holds the higher-OID target's native lock, then starts a
publication that writes the lower-OID target first. It checks that the publisher
already owns the lower native lock but has not written either target, that file
metadata remains lockable, and that checkpoint/feedback remain unchanged.
Buffered and multi-chunk streamed cases must finish exactly with zero retries
after the held lock is released, then release their transaction locks. The
fixture also checks access control for the private bridge.

Retry, batching, streamed replay, snapshot, default-worker and real native crash
fixtures run before sustained comparisons. They retain the distinction between a
confirmed COMMIT abort and an unknown outcome after transport loss.

The candidate repeats the baseline's sequential 15-minute mixed write phases at
6 and 8 source transactions/second on fresh private volumes. Batching, native
20-second maintenance, one-CPU budgets, 512 MiB PostgreSQL/2 GiB engine limits,
no swap, resource observers and 90-second drain deadline remain the same. Each
rate has one run per configuration; any difference is descriptive, not a
statistical or production capacity claim.

## Results

[Retained evidence](evidence/2026-11-16-wal-direct-prelock.json) records **398 passing native checks**: 348 checks across 11 correctness/default-worker/crash fixtures, 25 for the short 8/second pilot, and 25 for the full 6/second run. All accepted fixtures clean up successfully. The full 8/second run is a retained failed capacity test and contributes no successful full-publication qualification. `make check`, Python compilation and diff checks pass. All vendor sources remain unchanged.

### Fifteen minutes at 6 transactions/second

Both configurations publish all 5,400 source transactions with exact final rows; source/mirror row hashes also match across configurations.

| Measurement | Baseline v13 | Opt-in prelock v14 |
| --- | ---: | ---: |
| PostgreSQL CPU-seconds | 478.10 | 379.30 |
| Engine CPU-seconds | 199.56 | 209.73 |
| Total database/engine CPU-seconds | 677.66 | 589.03 |
| Sampled p95 publication-lag upper bound | 43.52 s | 6.15 s |
| Maximum sampled backlog | 294 transactions | 35 transactions |
| Final drain | 11.83 s | 1.12 s |
| Foreground deadlock retries | 11 | 0 |
| Native vacuum events | 74 | 82 |
| Longest logged vacuum duration | 42.29 s | 2.61 s |
| Maximum sampled current files, both mirrors | 1,099 | 593 |
| Observed object requests | 90,578 | 91,272 |
| Final publications, excluding warmup | 301 | 363 |

Total CPU falls by **88.64 seconds (13.08%)** in this local comparison. Engine CPU rises by 10.17 seconds while PostgreSQL CPU falls by 98.80 seconds. Maintenance scheduling, file counts and publication grouping also change, so this does not isolate only deadlock-replay cost. Object requests and publication count increase; the candidate does not improve every metric.

Both PostgreSQL configurations reach their 512 MiB cgroup memory boundary without OOM events or kills. Prelock engine lifetime peak is about 1,161.6 MiB, below its 2 GiB cap; swap remains disabled. Lower CPU and lag do not establish production memory admission or a low-latency SLA.

### Fifteen minutes at 8 transactions/second

Both configurations fail the 90-second final-drain deadline after all 7,200 source commits. The candidate has **zero foreground deadlock retries**, compared with 45 in the baseline, but backlog still grows during offered writes. Its maximum sampled backlog is **1,416 transactions**, versus 1,657 in the baseline. The last resource samples show **6,319** and 5,959 publications respectively at roughly 989 seconds; these are observations, not separately verified final checkpoints. No final exact-row comparison completes for either failed run.

Candidate CPU through post-failure diagnostics is 768.94 PostgreSQL plus 223.76 engine seconds (992.71 total). Baseline is 722.42 plus 217.92 seconds (940.34 total). Different amounts of work are published and diagnostic windows include worker termination, so these are **not completed-workload CPU comparisons**. Both retain reserved WAL history at failure and clean up their private volumes; no OOM event or kill occurs. The candidate reaches the PostgreSQL memory boundary too.

The earlier 1,200-transaction pilot at 8/second passes with exact rows and no retries, but its 150-second write phase does not predict the longer outcome. The full test demonstrates why the pilot is insufficient as a sustained-rate qualification.

## What remains

The candidate avoids the observed maintenance deadlocks in these loads and materially improves the bounded 6/second result. It remains an opt-in experiment, with retry retained for other confirmed aborts. It does not qualify 8/second or production lifecycle/upgrade behavior.

The next optimization target is repeated native file metadata work for mutations, measured in the earlier publication profile. This wrapper deliberately leaves the original mutation implementation and change order intact; it does not reduce the number of those catalog loads. A further experiment should instrument that hot path on the coordinated runtime, reduce measured metadata work, and repeat exact-row/recovery checks before rerunning the same load rates. Raising retry budgets or extending the drain deadline would not establish stable throughput.

An initial lock-fixture trial requested a 128-byte streamed chunk limit, below the existing 256-byte minimum, and failed waiting for a worker that had rejected that setting. The corrected fixture uses 256 bytes and passes both paths; no runtime limit was changed. The failed trial and cause are retained in the evidence separately from accepted checks.

## Reproduce

See the [candidate image instructions](../experiments/wal_direct/prelock/README.md)
and select the pinned storage images from the recovery report:

```sh
export PUSH_PG_LAKE_IMAGE=lake-local-iceberg:wal-direct-prelock-v14
export WAL_DIRECT_PRELOCK=1
make local-iceberg-wal-direct-prelock-test
python3 tests/local_iceberg/wal_direct_maintenance.py --transactions 5400 --interval .16666666666666666
python3 tests/local_iceberg/wal_direct_maintenance.py --transactions 7200 --interval .125
```

Fixtures retain the native marker and exact loaded harness inputs, and remove
private volumes on completion. The environment flag selects private test SQL;
it is not a production subscription setting.

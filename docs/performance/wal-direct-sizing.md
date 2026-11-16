# Direct publication load and memory sizing

This bounded experiment compares the journal and direct paths under explicit container budgets. It is not a production sizing recommendation or a 24-hour soak. The runtime adds direct-worker lifetime RSS and CPU diagnostics; publication and acknowledgement behavior are unchanged.

## Method

Use fresh private fixtures with identical runtime, storage, observer code, INSERT grouping and 16-MiB Parquet row-group targets. PostgreSQL receives one CPU and 512 MiB. The baseline engine receives one CPU, a 1-GiB container and a 768-MB internal query limit; the expanded profile uses the same CPU quota, a 2-GiB container and a 1536-MB query limit. The final memory cohort disables swap for both containers and verifies the cgroup swap limits are zero. Source logical decoding has a 64-KiB work-memory setting in the final trials, so larger transactions stream without immediate-debug mode.

The sustained phase offers 3,000 mixed transactions at 10 transactions/second, approximately five minutes of writes per mode. Each transaction inserts four rows per table and, after the first, updates/deletes preceding keys. Both pipelines coalesce at one second and at most 32 source transactions. The memory phase uses one INSERT transaction with 1,000, 5,000 or 20,000 rows per table, with 1,024-byte values made from varied MD5 blocks. The journal and streamed direct modes publish one source transaction at a time in that phase. The middle size reverses mode order. Each fixture begins with one excluded warmup transaction. Final sorted rows must hash identically across modes for each shape.

CPU totals sum the PostgreSQL and pgduck cgroups, including container observers and replication workers; host/controller/MinIO CPU is excluded. Observers wait two seconds between iterations; resource/progress queries add delay. Recorded timestamps show the actual sampling intervals for PSS, RSS, backlog and WAL retention. PostgreSQL-user observers read database/engine process memory, and a separate root observer adds the root-owned experimental worker. This addresses a gap in earlier process-memory samples; earlier cgroup CPU totals already included the worker.

Worker exit diagnostics record lifetime peak RSS and CPU, including warmup, so short bursts are not lost between samples. Its CPU is a subset of the cgroup total and must not be added again. Cgroup lifetime memory peaks include startup, caches and observer processes; they are not process RSS. Sampling can miss brief backlog or latency peaks. Publication latency is a sampled upper bound. These trials have one run per shape/mode and do not establish statistical significance, a capacity maximum or a hard RSS admission guarantee.

## Results

[Retained evidence](../evidence/2026-11-16-wal-direct-sizing.json) records five passing no-swap memory fixtures (94 checks), one PostgreSQL OOM failure, and 64 passing checks across the corrected-observer smoke modes. All fixtures clean up successfully. The smoke and earlier load-discovery cohorts preceded the explicit no-swap profile; they are not no-swap memory qualifications.

The final memory cohort produces identical source/mirror rows for every successful fixture and identical hashes across modes where both finish. CPU totals are seconds for the entire measured PostgreSQL/engine workload, including generating the application data and observing the containers:

| Rows per table in one transaction | Journal CPU seconds | Direct CPU seconds | Journal PostgreSQL peak MiB | Direct PostgreSQL peak MiB | Direct worker peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 0.632 | 0.462 | 186.0 | 154.0 | 15.5 |
| 5,000 | 2.664 | 0.962 | 400.4 | 189.3 | 13.8 |
| 20,000 | OOM | 3.293 | OOM | 334.7 | 13.7 |

Both tables participate in each transaction. The largest direct fixture carries 40,000 changes and about 39 MiB of raw text, spilling about 45 MiB. The worker's measured peak stays small as the transaction grows, while the PostgreSQL container footprint includes heaps, WAL, temporary-file cache and native apply. The engine's observed cgroup lifetime peaks are about 214–216 MiB in these INSERT trials. These are observed outcomes, not hard RSS bounds or recommended production allocations.

At 5,000 rows per table, generated WAL is 63,244,120 bytes for journal and 11,758,288 for direct in this run. At the largest size, the journal PostgreSQL container is actually OOM-killed (Docker exit 137, `OOMKilled=true`) under its 512-MiB no-swap limit. No comparative CPU or WAL total is claimed for that failed fixture. The same application shape completes through direct publication without an OOM kill. This identifies an unsupported envelope for the existing journal path, not a guarantee that direct handles arbitrary transactions within 512 MiB.

The 768-MB engine query budget also fails on mixed writes in both pipelines, although the earlier INSERT-only capped fixture passed. The engine reports an internal allocation failure and exits; Docker records no kernel OOM kill and PostgreSQL remains running. A 1536-MB engine query budget passes the short mixed smoke modes, but does not establish sustained capacity.

The two five-minute load-discovery runs offer 3,000 mixed transactions at 10 transactions/second. Both miss the subsequent 90-second drain deadline. Their last regular samples report completion upper bounds of 2,374 and 2,379 transactions for journal and direct. These runs used the older XID observer described below and did not constrain swap. Because even the upper bounds remain below 3,000, they demonstrate failure to catch up in the tested window. They are excluded from comparative CPU-saving claims and do not establish an eventual data mismatch. Increasing the engine budget avoids the early allocation failure but does not resolve this publication bottleneck.

## Measurement corrections and limits

The old observer used the highest published source XID. Allocation order differs from commit/publication order: a later bookkeeping transaction can publish while a large user transaction is still being streamed. That made early journal memory measurements stop prematurely and could produce a false row-mismatch assertion. The final observer counts the exact application XIDs instead. Bookkeeping-only transactions are separated from application publication counts, and empty bookkeeping records without a batch ID are excluded from batch-shape assertions. Early attempts remain archived and are not accepted memory comparisons.

The older overload cohort also emitted a final-hash comparison error after both fixtures had already failed: there were no successful hash groups to compare. The harness now skips such empty groups and preserves partial status for actual fixture failures. This changes benchmark reporting, not replication semantics.

No sustained production rate has been qualified here. The next gate is to profile publication cost as mirrors and file metadata grow, establish a deployment resource/transaction envelope, add measured memory admission, and repeat load/recovery trials. One run per shape is descriptive and does not establish statistical significance. Existing 12%/11% coalescing CPU observations remain tied to their earlier workload/runtime; these bulk-transaction measurements are a different experiment.

## Run

Build the optional runtime as `lake-local-iceberg:wal-direct-v10` using `experiments/wal_direct/Dockerfile` and select the pinned storage images. All commands create and remove private Compose volumes:

```sh
python3 tests/local_iceberg/wal_direct_sizing.py --phase smoke --engine-budget expanded
make local-iceberg-wal-direct-sizing
python3 tests/local_iceberg/wal_direct_sizing.py --phase memory --engine-budget expanded --continue-on-failure
```

Use `--engine-budget baseline` to reproduce the smaller envelope. `--continue-on-failure` records every requested fixture; the cohort retains a nonzero result and partial status if any fail. Individual fixture evidence includes input hashes, images, resource samples, container exit state on failure, exact row hashes and cleanup status. Earlier development harness failures are retained separately.

The [subsequent publication profile](wal-direct-publication-profile.md) locates a growing cost in full file-catalog loads for mutations and demonstrates compaction recovery. Its original shorter-interval native-maintenance pilot exposes a publisher/vacuum deadlock. The [bounded retry follow-up](../qualification/wal-direct-deadlock-recovery.md) recovers confirmed aborts and passes 1,000 transactions at 4/second with active maintenance, using 122.86 PostgreSQL-plus-engine CPU-seconds. A longer 10/second run still fails its drain deadline. Production capacity and memory admission remain unqualified.

The [15-minute maintenance follow-up](wal-direct-sustained-maintenance.md) passes 6/second with exact rows and 677.66 CPU-seconds, but shows substantial lag and PostgreSQL memory-boundary pressure. At 8/second, backlog grows and the drain deadline fails; no OOM kill occurs. These runs narrow the local working range without establishing production capacity.

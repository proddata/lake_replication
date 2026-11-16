# Direct publication with bounded coalescing

Opt-in coalescing is implemented and has native recovery qualification. In this bounded local comparison, coalesced direct publication uses about 12% less total PostgreSQL/engine CPU for INSERTs and 11% less for mixed mutations than the coalesced journal pipeline. WAL generation is about 43% and 11% lower respectively. The default journal path remains unchanged.

[Performance evidence](../evidence/2026-11-16-wal-direct-coalesced-performance.json) records eight successful fresh fixtures and 60 checks. All source input hashes and image identities match across fixtures; final source/mirror row hashes match across modes and repetitions.

## What changed

The worker buffers complete source transactions and publishes on a deadline, transaction count, row/byte pressure, or its process commit budget. Defaults remain immediate publication. Opt-in controls are `LAKE_DIRECT_PUBLISH_INTERVAL_MS` (0–60000, default 0) and `LAKE_DIRECT_BATCH_MAX_TRANSACTIONS` (1–32, default 32). Aggregate row/charged-input/serialized-input limits remain 4096 rows and 8 MiB, and can be lowered. They are input limits rather than a measured RSS ceiling.

A flush may publish earlier complete transactions while another transaction is being decoded. It never acknowledges that incomplete transaction. An idle decoder uses bounded socket waits so a deadline does not depend on another application commit. Each following live batch receives a fresh deadline. Final Iceberg changes and the checkpoint commit together before WAL acknowledgement. Only small stream/mapping metadata persists in PostgreSQL; bulk row-data journaling remains absent.

Opt-in independent INSERT grouping applies only to wholly INSERT-only batches and native-guard-qualified targets. Mixed batches retain source order. `commits` counts source transactions and `publications` counts sink commits separately. Both counters commit with the data and publication LSN.

## Comparison

Both modes use the same cached ARM64 `wal-direct-v4` runtime, source-pinned storage images, current apply functions, one-second coalescing, a cap of 32 complete transactions, and INSERT grouping enabled. A test-only journal selector caps rows/bytes at 4096/8 MiB; these limits do not bind this small workload. Each fresh fixture publishes one warmup transaction before measurement, then runs 120 transactions at four offered transactions/second, inserting four 256-byte-text rows per table into two tables. Mixed transactions also UPDATE one and DELETE one preceding-transaction row per table. Each mode/workload is repeated twice, in reversed order.

The direct prototype uses one persistent manual superuser worker; journal uses the existing unprivileged managed pipeline, supervision, generation fences and schema checks. This is a comparison of implementations, not isolated journal-write cost. CPU includes container observer overhead but excludes host/controller/MinIO work. Setup, warmup and final row scans are outside measurement. There is no application-only baseline or saturation sweep here.

The first attempt failed on its seventh fixture when the replication connection timed out following a roughly 170-second container-log gap. Its evidence is retained and excluded in full. The complete comparison below was rerun under a scoped `caffeinate -i` idle-sleep assertion. That does not qualify recovery from host suspension or prolonged storage/network outages.

| Workload | Mode | CPU seconds (range) | WAL MiB | Object requests | Publications |
|---|---|---:|---:|---:|---:|
| inserts | Journal | 6.41 (6.38–6.44) | 1.89 | 300 | 25.0 |
| inserts | Direct | 5.62 (5.45–5.80) | 1.08 | 324 | 27.0 |
| mixed | Journal | 15.98 (15.60–16.36) | 11.52 | 2,091 | 18.5 |
| mixed | Direct | 14.29 (14.13–14.46) | 10.24 | 2,106 | 20.0 |

Identical policy settings do not produce identical realized batches: direct publishes 27 groups versus 25 for INSERTs, and 20 versus 18–19 for mixed mutations. Consequently direct makes 8% more INSERT object requests and about 0.7% more mixed requests. The CPU/WAL reductions are not an object-request reduction, and should not be attributed solely to removal of the journal. Two repetitions provide descriptive medians/ranges, not statistical confidence or a general throughput claim.

| Workload | Mode | App p99 ms | Publication p95 upper bound s | Peak sampled PSS MiB | Peak observed retained WAL MiB |
|---|---|---:|---:|---:|---:|
| inserts | Journal | 7.11 | 3.13 | 208.0 | 1.06 |
| inserts | Direct | 8.08 | 2.95 | 218.4 | 0.70 |
| mixed | Journal | 8.74 | 4.16 | 264.9 | 5.77 |
| mixed | Direct | 8.39 | 3.80 | 227.3 | 8.45 |

Publication latency is a sampled upper bound with two-second observation/query delay, not exact commit-to-mirror latency. App p99 is client-observed transaction time. Sampled PSS does not establish hard memory admission: INSERT memory ranges overlap and there is no consistent cross-workload memory win.

Direct retains more peak WAL in the mixed case despite generating less WAL overall. Retention is measured from slot restart LSN, which also depends on decoding/checkpoint history. No outage capacity or WAL-budget guarantee follows from these light-load samples.

## Recovery and remaining gates

[Native runtime evidence](../evidence/2026-11-16-wal-direct-coalescing-runtime.json) covers grouped transaction ordering, mixed mutations and TRUNCATE, source commit versus publication counters, native INSERT grouping, row/byte/count limits, idle flushing, a fresh deadline after blocked publication, worker stop before publication, rollback across grouped source transactions, and actual shared-server crashes before upload, after upload and after commit. Immediate-mode regression and all-feature Rust tests remain required.

This is still an isolated frozen-schema experiment with manually quiesced empty bootstrap. Next gates are nonempty snapshot alignment, large/streamed transactions with disposable spill, bounded outage/WAL-pressure handling, managed credentials/lifecycle, restore/clone/promotion safety, sustained load and deployment-specific power-loss qualification. The current journal remains the default and supported qualification baseline.

The [bounded direct-snapshot follow-up](../qualification/wal-direct-snapshot.md) now initializes existing tables without PostgreSQL row staging, and qualifies atomic activation, concurrent catch-up and interrupted/ambiguous publication. The original initial-copy path uses PostgreSQL `initial_copy_chunks`, so the new initializer reuses the snapshot/slot principles with a native heap-to-Iceberg data path. Large resumable copies remain unimplemented.

Run on a qualified cached runtime and storage fixture:

```sh
make local-iceberg-wal-direct-coalescing-test
WAL_DIRECT_CRASH_TESTS=1 make local-iceberg-wal-direct-coalescing-test
make local-iceberg-wal-direct-compare
```

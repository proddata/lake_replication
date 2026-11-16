# Ordered DELETE key-filter optimization

These measurements predate the subsequent [coalescing-window fix](local-coalescing-windows.md). Each report compares its own contemporaneous control and candidate; their CPU reductions should not be combined as a single measured result.

The shared publisher now adds a redundant, typed key-membership predicate for eligible DELETE runs. This lets pg_lake filter rows/files before returning them to PostgreSQL. DELETEs still execute in source order, with the original null-safe predicate and an exactly-one-match check for each change. Missing or duplicate matches abort the run and roll back earlier deletions. This is implemented in `migrations/0033_delete_key_filter.sql` and requires no journal-format or transaction-boundary change.

The fast path covers single-column, non-null boolean, smallint, integer, bigint, text, varchar and UUID identities with deterministic collations. Nullable, compound, nondeterministically collated and other identity types retain the previous path. Typed PostgreSQL arrays preserve values such as bigint extrema and quoted text. No DELETE statements are combined or reordered; row and statement trigger order remains intact.

## Diagnosis

The [baseline phase profile](../evidence/2026-11-15-local-publication-phase-profile.json) instruments the existing mixed workload through temporary SQL wrappers and existing opt-in worker transaction timings. It records about 12.2 seconds across mutation calls, 6.0 seconds in publication COMMIT, 0.09 seconds in staging SQL and 0.06 seconds in connection startup across the diagnostic run. SQL operation times are nested within publication statement times; they must not be summed with that enclosing layer. These are elapsed wall times, not CPU. Setup events may be included in worker totals. Extra logging and split-phase round trips make this diagnosis, not a performance qualification.

Inspection showed the original DELETE had a PostgreSQL-side row-conversion filter and no key predicate in vectorized engine SQL. The candidate retains that predicate and adds typed membership that reaches the engine. The [optimized phase profile](../evidence/2026-11-15-local-publication-phase-profile-optimized.json) records:

| Operation | Baseline mean elapsed | Optimized mean elapsed | Calls per diagnostic run |
| --- | ---: | ---: | ---: |
| INSERT run | 9.23 ms | 9.23 ms | 240 |
| UPDATE run | 19.76 ms | 19.81 ms | 238 |
| DELETE run | 22.20 ms | 11.00 ms | 238 |
| Publication COMMIT | 103.68 ms | 103.81 ms | 58 / 54 |

The isolated profiles support DELETE filtering as the changed mechanism. They do not independently establish a speedup: the uninstrumented comparisons below provide that measurement. Connection reuse remains a smaller candidate in this workload; mutation execution and COMMIT are larger costs.

## Uninstrumented comparisons

Both experiments use the exact same cached v5 worker/native binaries and fresh private volumes for every run. The candidate installs only migration 0033 before startup. No timing wrappers are enabled. Each variant runs twice, reversing order in the second pair. Settings are one-second coalescing, INSERT grouping on and journal capacity 32 transactions/64 MiB. Each measured workload commits 120 paired-table transactions over about 30 seconds: four INSERT rows per table, followed by one UPDATE and one DELETE per table targeting the preceding transaction. Initial copy and final full-row validation are outside the resource interval.

The [small-table comparison](../evidence/2026-11-15-local-delete-filter-small.json) starts empty. The [seeded comparison](../evidence/2026-11-15-local-delete-filter-seeded.json) starts with 20,000 existing rows per table. All eight fixtures match source and mirror contents exactly, and final row digests agree across variants/repetitions. Initial-row waiting differs between the two experiments; each control/candidate pair uses identical harness code.

| Existing rows per table | Control CPU median (range) | Optimized CPU median (range) | Median reduction |
| ---: | ---: | ---: | ---: |
| 0 | 26.86 s (25.52–28.20) | 21.12 s (20.33–21.92) | 21.4% |
| 20,000 | 32.41 s (32.21–32.62) | 21.20 s (20.61–21.78) | 34.6% |

CPU includes PostgreSQL, the one pgduck engine and container observers, excluding host/controller/object-store CPU. All runs are same-instance mode on one local host. Fresh volumes do not establish cold caches. Two reversed-order pairs are descriptive local evidence, not statistical confidence intervals, production sizing or a capacity maximum.

| Existing rows/table | Variant | COMMIT p99 median | Publication p95 upper bound | WAL | Mirror S3 requests |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | control | 11.27 ms | 3.70 s | 12.36 MiB | 2416 |
| 0 | candidate | 10.84 ms | 3.59 s | 12.47 MiB | 2446 |
| 20,000 | control | 9.60 ms | 3.54 s | 12.32 MiB | 2431 |
| 20,000 | candidate | 10.77 ms | 3.46 s | 12.55 MiB | 2461 |

The CPU reduction does not establish reduced WAL, fewer storage requests or better application COMMIT latency. The seeded candidate has a higher COMMIT p99 median in these two runs. Publication delay is an upper bound including two-second sampling/query delay. Faster publication changes batch timing slightly; object and WAL counts stay similar rather than falling with CPU.

This shared publisher change is available to both same-instance and separate-instance pull deployments. Performance improvements above were measured only in same-instance mode. The earlier topology comparison used v5 before this optimization and should not be combined with these values to rank optimized local versus optimized remote pull.

## Qualification and use

The [packaged runtime qualification](../evidence/2026-11-15-local-delete-filter-runtime.json) passes 189 checks: direct DELETE/migration/trigger tests (51), functional lifecycle/schema/restart (21), scalar and compound/TOAST values (18), full-row identity (18), actual shared-server publication crashes (27), and existing remote logical upgrade/restore/replication/interrupted-copy/uninstall checks (54). `make check` also passes, including assertions that frozen beta4 SQL excludes the migration and current beta5 SQL includes it.

Migration 0033 is appended to fresh current beta5 installs and beta4-to-beta5 upgrades. Existing databases already on this unreleased beta5 need an explicit migration refresh through the installation workflow; replacing installed SQL files alone does not update their function definitions. The change only affects future publications. No host installation or production deployment was performed.

Reproduce using cached fixture images:

```sh
make local-iceberg-delete-filter-test
make local-iceberg-delete-filter-compare
python3 tests/local_iceberg/delete_filter_compare.py --seed-rows 20000
make local-iceberg-publication-profile
```

The comparison deliberately uses the v5 binary image as control and applies candidate SQL explicitly. Use a control image without migration 0033 when reproducing the before/after comparison; the correctness/profile targets can use the packaged v6 image. Timing wrappers and crash-instrumented images are confined to disposable qualification fixtures.

# Direct publication slowdown: file metadata and maintenance

The slowdown follows the growth of tiny Iceberg files. UPDATE and DELETE load and lock the mirror's current file metadata before pruning to the relevant files. Compaction restores much of the lost apply performance while preserving rows and the publication checkpoint. The original v11 shorter-interval maintenance pilot also exposes a deadlock that its one-shot publisher does not recover from automatically. The [v13 follow-up](wal-direct-deadlock-recovery.md) adds bounded whole-publication retry and qualifies a local 4-transaction/second workload with maintenance.

[Retained evidence](evidence/2026-11-16-wal-direct-publication-profile.json) includes successful profiles, the negative native candidate, normal-path regressions and the failed maintenance pilot. This is diagnosis, not production capacity qualification.

## Fixed-batch experiment

One private fixture prequeues batches of 32 source transactions. Each mixed transaction inserts four rows into each of two tables, then updates/deletes preceding keys. One direct worker publishes each whole batch as one transaction. There are 32 growth batches, four batches before compaction, native VACUUM of both mirrors, and four batches afterward. The final mirrors match the heaps exactly.

PostgreSQL has one CPU and 512 MiB; pgduck has one CPU, 2 GiB and a 1536-MB query limit. Swap is disabled. INSERT grouping and 16-MiB row-group targets are enabled. Mirror autovacuum is disabled in the fixed-batch profiles to isolate growth; manual compaction is the intervention. Queueing source transactions and reading observer/catalog statistics happen outside publication wall timing. Cgroup CPU deltas include the observers.

`LAKE_DIRECT_TRACE_PHASES=1` splits ordinary/coalesced publication into BEGIN, apply and COMMIT requests. This adds round trips and is diagnostic only. Private SQL wrappers record operation timings in an unlogged table containing counts/times, without staging user rows. Function and operation timings are nested inside apply; overlapping layers must not be summed.

The original native runtime produces these median elapsed times for the four batches immediately around compaction:

| Phase | Before compaction | After compaction |
| --- | ---: | ---: |
| Apply 32 source transactions | 4.300 s | 2.411 s |
| COMMIT | 0.140 s | 0.058 s |
| INSERT runs, inside apply | 0.510 s | 0.493 s |
| UPDATE runs, inside apply | 2.122 s | 1.190 s |
| DELETE runs, inside apply | 1.645 s | 0.703 s |

Before compaction, each mirror has 2,303 data files holding 3,457 rows. Native VACUUM reduces the total current file count from **4,606 to 6** in about **4.17 seconds**. Exact rows and the direct publication checkpoint stay unchanged. Subsequent measured batches add rows/files again, so their times are not measurements of an entirely static layout.

COMMIT is a small fraction of the observed publication time. INSERT time is roughly flat across this intervention. The growing mutation cost is the useful optimization target. These single-fixture diagnostic timings do not establish statistical significance or a sustainable offered rate.

## Native catalog hotspot

`CreateTableScanForRelation` in the pinned pg_lake `snapshot.c` fetches data files through `GetTableDataFilesFromCatalog`, using `isResultRelation` to request row locks, and then calls `PruneDataFiles`. Thus every UPDATE/DELETE in this shape first fetches and locks metadata for all current files in its target mirror.

The native query joins `lake_table.files` to partition metadata, column statistics, field mappings and `pg_attribute`. Its inner file selection includes `FOR UPDATE`. The fixed batch has 64 UPDATE and 64 DELETE runs: **128 repeated catalog loads**, even though the actual mutations each target one row.

Normal `pg_stat_statements` cannot expose this work: the upstream internal SPI helper explicitly sets `pg_stat_statements.track=none`. An unmodified visibility probe therefore fails its coverage assertion. It is retained as a failed profiling attempt rather than interpreted as zero catalog cost.

A separate diagnostic image adds explicitly enabled backend-local query/decode counters and allows this one internal catalog reader to be observed by `pg_stat_statements`. No production SQL entry point is installed by the extension. The private profile creates its own entry point and enables it only around apply. The instrumentation preserves the catalog query and locking behavior. Nested query statistics have no entry evictions in the accepted run.

The diagnostic image reports these medians:

| Catalog work per 32-transaction batch | Before compaction | After compaction |
| --- | ---: | ---: |
| Catalog loads | 128 | 128 |
| Joined metadata rows returned, summed over loads | 556,928 | 33,664 |
| File entries processed, summed over loads | 278,464 | 16,832 |
| Catalog query/setup time | 1.622 s | 0.204 s |
| Decoding returned metadata | 0.212 s | 0.012 s |
| Total apply time in this instrumented run | 4.358 s | 2.530 s |

The row/file sums count the same metadata repeatedly; they are not unique file counts. Query and decode time explain a large part of the growing apply cost. Other native execution and pruning work remains. An optimization of this path must preserve current-snapshot and file-locking semantics; removing locks without concurrency qualification is not justified by the profile.

## Negative candidate

An isolated candidate makes the position-delete receiver omit unused per-column statistics. The source consumers use paths and file-level row counts, and the candidate passes the fixed-batch exact-row/checkpoint checks. It does not materially improve this copy-on-write workload: median pre-compaction apply is 4.275 seconds versus 4.300 seconds for the original runtime, while the same growth remains. The measured hotspot is the result-scan metadata loader with `forUpdate=true`, rather than that receiver's metadata lookup.

The candidate stays in `experiments/wal_direct/metadata/` as a reproducible negative experiment. It is not promoted to the vendor checkout, release prerequisite patchset or default runtime. The native diagnostic image is likewise an investigation tool, not a release artifact.

## Maintenance pilot: failed qualification

The existing native autovacuum interval is **600 seconds** in the runtime. A private pilot changes it to **20 seconds** through ALTER SYSTEM/reload and offers 600 mixed transactions at 10 transactions/second on the original native runtime. Deployment and release defaults are untouched.

Compaction runs during publication: observed file counts fall, including one mirror shrinking to one file. The pilot then fails with PostgreSQL **40P01, deadlock detected**, after 434 application commits. The foreground publisher waits for a native per-table update lock (advisory class 101) held by autovacuum; autovacuum waits on the publisher's transaction. PostgreSQL and pgduck remain running and neither container is OOM-killed.

The one-shot direct worker exits on the server error. Its feedback follows successful publication COMMIT, so it does not acknowledge the failed batch. This run does not qualify automatic recovery, a shorter maintenance interval or a sustained throughput improvement. It demonstrates that maintenance coordination/recovery must be addressed before using more frequent compaction as a capacity fix.

The [subsequent recovery qualification](wal-direct-deadlock-recovery.md) implements bounded retry of confirmed deadlock aborts, including complete streamed replay and COMMIT aborts. It passes 1,000 transactions at 4/second with native maintenance and exact final rows. The 3,000-transaction run at 10/second still exceeds its drain deadline despite recovering 15 deadlocks. Unknown COMMIT outcomes still require restart and durable checkpoint reconciliation; capacity and the native lock-order conflict remain open.

## Validation and reproduction

The original profile passes 12 checks, the native catalog profile 14, and ordinary/coalesced regressions on the v11 worker pass 104 checks. The negative candidate passes 12 functional profile checks. All fixtures, including failed probes, clean up successfully. All-feature Rust tests, `make check`, formatting and diff checks pass.

Build the optional v11 worker over the qualified base using `experiments/wal_direct/Dockerfile`, and select the pinned storage images:

```sh
make local-iceberg-wal-direct-profile
```

For internal catalog timing, build `experiments/wal_direct/catalog_profile/Dockerfile` over that image and select it as `PUSH_PG_LAKE_IMAGE`:

```sh
python3 tests/local_iceberg/wal_direct_profile.py --windows 32 --statements --native-catalog
```

Both flags and the diagnostic image are needed for the hidden internal SQL. The `--windows 2` case is a shorter instrumentation check. The metadata candidate has its own Dockerfile and pinned source/library hashes. Both native experiments rebuild only the table library, with no ABI/header change, and record their distinct identities.

`make local-iceberg-wal-direct-maintenance` remains a **failed overload reproducer** at 3,000 transactions and 10/second on v13. The passing follow-up command is `python3 tests/local_iceberg/wal_direct_maintenance.py --transactions 1000 --interval .25`. It creates only private disposable fixtures. Production memory admission, managed direct lifecycle, power-loss/failover and sustained sizing remain open.

The [native prelock follow-up](wal-direct-prelock.md) moves the existing native per-table update locks before direct publication writes. It avoids the observed deadlocks in both 15-minute loads and improves the passing 6/second result, but does not establish stable 8/second throughput or reduce the original per-mutation catalog-load count.

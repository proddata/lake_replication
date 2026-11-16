# WAL-backed direct publication experiment

This implements the first frozen-schema slice of [option B](../../docs/wal-backed-iceberg-options.md). It is a manually operated experiment, excluded from default builds, release installation, subscription launchers and migrations. The existing journal path remains the baseline.

One PostgreSQL database holds application heaps, final Iceberg mirrors and small control records. A separate worker decodes committed pgoutput transactions, buffers bounded batches in memory or uses opt-in disposable transaction spill, invokes the existing `apply_change_run` functions, and commits the Iceberg changes with a publication checkpoint. Only then does it send source WAL feedback. There are no intermediate Iceberg logs and no inserts into the PostgreSQL transaction/change journal.

The SQL helper preserves contiguous operation runs, source order, exactly-one-match mutation checks and the existing 500-row/4-MiB run limits. Immediate mode publishes one source transaction per commit. Opt-in coalescing publishes bounded groups of complete transactions. It reuses the native target-lock guard to restrict mirrors to qualified local-catalog pg_lake scalar tables without triggers, defaults, rules or check constraints. Wholly INSERT-only batches can use the existing opt-in independent INSERT grouping; mixed batches preserve source order.

## Recovery boundary

`lake_direct.stream` holds source identity, timeline, slot/publication identity, the last published LSN, a digest of the most recent input, and separate source-transaction/publication counters. `lake_direct.mapping` holds frozen source/target descriptors. Final writes and the checkpoint update share a PostgreSQL/pg_lake transaction. On a lost response the worker exits; the next invocation reads the committed checkpoint and resumes decoding there. WAL feedback never uses the observed WAL-end pointer or a buffered transaction boundary.

The worker verifies the slot plugin, database, invalidation state, restart and confirmed-flush positions, and source system/timeline identity. An exact unfiltered application-table publication is required. A session advisory lock excludes another direct worker in the same database; PostgreSQL excludes concurrent decoding of the slot. Required history loss, descriptor changes and apply errors stop the worker without acknowledging unpublished input.

This does not establish restore/clone safety or automatic failover. Operator bootstrap and source-slot ownership remain trusted. An independent writer must not mutate mirrors; the fixture deliberately violates that rule once to verify atomic rollback.

## Bounds and unsupported work

The default, non-streamed worker accepts at most 4,096 changes and an 8-MiB serialized input per transaction and publication batch. Aggregate charged input also accounts for the pending complete batch plus the current transaction; pressure can flush earlier complete transactions before the current one finishes decoding. The worker also charges incoming row bytes and cloned relation descriptors against its buffer budget, with at most 64 relations. `LAKE_DIRECT_MAX_ROWS` and `LAKE_DIRECT_MAX_BYTES` can lower these limits. These are input bounds, not a measured RSS ceiling: allocator overhead, JSON expansion, SQL encoding and pgduck use additional memory. The existing wire reader separately caps a single server frame at 64 MiB.

Opt-in protocol streaming and quota-controlled disposable spill now support larger transactions through bounded apply chunks in one final transaction. A separate replay floor protects unfinished interleaved transactions across restart. See [streamed recovery and outage qualification](../../docs/wal-direct-streaming.md) for controls, limits and evidence. Temporary files never authorize WAL acknowledgement.

The [bounded snapshot initializer](../../docs/wal-direct-snapshot.md) now supports existing ordinary tables and concurrent INSERT/UPDATE/DELETE through an exported snapshot, direct native Iceberg writes and atomic activation. It has 100,000-row/64-MiB input ceilings and uses one transaction; large resumable copies remain unsupported. The original streaming fixtures still bootstrap empty tables explicitly. General DDL handling, managed credentials/generation fencing, process supervision, automatic rebuild and production deployment are not implemented. Object-store outage and checkpoint-enforced WAL-history loss now have bounded qualification in both direct modes. Retained WAL grows during outages; PostgreSQL retention limits can invalidate history, requiring an explicit fresh snapshot. This is not a hard disk ceiling or automatic retention controller.

## Coalescing controls

`LAKE_DIRECT_PUBLISH_INTERVAL_MS` defaults to 0 (immediate), with an opt-in range up to 60000 ms. `LAKE_DIRECT_BATCH_MAX_TRANSACTIONS` defaults to 32 and accepts 1–32. The deadline begins with the first buffered complete transaction; later live publications start fresh windows. Count, row or byte pressure can flush earlier. An idle decoder uses bounded socket waits to flush without another application commit.

`LAKE_DIRECT_MAX_COMMITS` bounds the process's source-transaction budget and flushes remaining complete work before normal exit. A forced worker stop discards provisional buffers without acknowledging them; restart replays retained WAL. Publication logs report counts, rows, bytes, LSN and flush reason. No bulk row journal or durable temporary spool is introduced.

The experimental SQL schema changed to track source commits and publications separately. Recreate disposable fixtures when changing experiment versions; there is no in-place experiment-schema upgrade contract.

## Run the isolated fixture

Use an existing qualified local runtime and source-pinned storage images:

```sh
docker build -f experiments/wal_direct/Dockerfile \
  --build-arg BASE_IMAGE=lake-local-iceberg:qualification-v8 \
  -t lake-local-iceberg:wal-direct-v9 .
export PUSH_PG_LAKE_IMAGE=lake-local-iceberg:wal-direct-v9
# Select the qualified PUSH_STORAGE_IMAGE and PUSH_STORAGE_INIT_IMAGE.
make local-iceberg-wal-direct-test
```

The fixture creates and removes private Compose volumes, installs `install.sql` only there, and runs the optional `lake-replication-direct` binary. It uses explicit loopback superuser credentials for this experimental scope. `LAKE_DIRECT_EXPERIMENT=1` is required. Nothing installs SQL or binaries on the host.

For real publication crash checks, build over the matching **test-only crash-instrumented** runtime, select that image, and set `WAL_DIRECT_CRASH_TESTS=1`. The fixture kills the shared PostgreSQL before upload completion, after upload completion and after commit. `LAKE_DIRECT_EXIT_BEFORE_FEEDBACK=1` separately exercises a worker exit after a successful publication commit.

Performance comparisons must align transaction batching and runtime, then measure CPU, WAL, retained WAL, memory, object operations, application latency and publication lag. Bounded snapshot recovery is qualified separately; streamed/outage recovery is qualified in bounded fixtures; production memory admission and sustained sizing remain open.

The [first bounded performance comparison](../../docs/wal-direct-performance.md) now measures matched per-commit modes and a coalesced journal reference across 12 fresh fixtures. Direct reduces WAL versus the matched control but shows only a small descriptive CPU difference. The coalesced journal reference remains much cheaper than unbatched direct publication. That report retains its original per-commit harness inputs. The current `make local-iceberg-wal-direct-compare` defaults to the two coalesced modes; `--modes` can select the per-commit controls. No performance overlay is installed by default.

The [coalesced comparison](../../docs/wal-direct-coalescing.md) now records eight successful matched-policy fixtures and 60 checks. Direct uses about 12%/11% less CPU and 43%/11% less generated WAL for INSERT/mixed workloads in these local trials, with slightly more publications/object requests. It retains more peak WAL for mixed mutations. These are descriptive two-run comparisons of implementations, not production sizing or proof of isolated journal cost.

## Retained qualification

The [native evidence](../../docs/evidence/2026-11-15-wal-direct-prototype.json) records 125 passing checks: 52 on the ordinary runtime and 73 on the crash-instrumented runtime. Both fixtures clean up successfully. Checks cover exact multi-table mutations, JSONB numeric precision, rollback, worker ownership, publication mismatch, row/byte bounds, schema refusal, slot replacement, restart and acknowledgement ordering. The fault fixture verifies real shared-server crashes before upload, after upload and after commit, then resumes to exact mirror contents with no bulk journal rows. `make check` and all-feature Rust tests pass.

The [current coalescing runtime evidence](../../docs/evidence/2026-11-16-wal-direct-coalescing-runtime.json) passes 180 checks: 52 ordinary coalescing checks, 76 coalescing/crash checks and 52 immediate-mode regressions. It covers count/row/byte pressure, idle flushing, fresh windows after blocked publication, stop/replay, grouped rollback and the three real server-crash boundaries. Three deterministic Rust batching tests and all-feature Rust tests also pass.

These checks establish a limited direct-publication vertical slice. The streaming-only checks do not establish general snapshot correctness, streamed-transaction recovery, outage capacity, memory admission, lineage recovery or a general performance improvement; the bounded snapshot slice below has its own evidence.


## Nonempty initialization

`lake-replication-direct-init` is gated by the same optional feature and explicit opt-in. It requires `LAKE_DIRECT_SLOT`, `LAKE_DIRECT_PUBLICATION`, and `LAKE_DIRECT_TARGET_SCHEMA` in addition to loopback/database/user settings. Install the current private SQL schema first. One direct stream is supported per database, and existing destination names are refused.

The [snapshot report](../../docs/wal-direct-snapshot.md) records 138 checks across nine fixtures, including DDL fencing before snapshot export, concurrent-write catch-up, partial-copy rollback, lost response, existing-target/type/publication refusal and three real shared-server crash boundaries. A further 104 immediate/coalesced streaming regression checks pass on the new runtime. No PostgreSQL snapshot rows/chunks or streaming transaction/change rows are staged.

Use `make local-iceberg-wal-direct-snapshot-test` for the normal case; the report lists recovery and fault cases. Long source transactions can delay snapshot establishment, and DDL/TRUNCATE may wait on held read locks. The copy statement has a timeout, but there is no complete production admission or recovery-time guarantee. Input limits are not hard RSS bounds. Large/resumable initialization, production memory admission and lineage handling remain open. Bounded streaming/spill and outage recovery are now covered by the [streamed recovery report](../../docs/wal-direct-streaming.md).

## Load and memory limits

The [sizing study](../../docs/wal-direct-sizing.md) uses the optional v10 runtime with worker lifetime RSS/CPU diagnostics. No-swap bulk transactions show a small worker footprint, but the journal path can OOM PostgreSQL at 512 MiB and both paths exceed the smaller engine budget on mixed writes. Five-minute overload trials do not establish a stable production rate. These failures are retained as configuration limits; managed lifecycle, memory admission and sustained sizing remain open.

## Publication profile

The [publication profile](../../docs/wal-direct-publication-profile.md) identifies repeated full file-metadata loads during UPDATE/DELETE and verifies compaction's effect. The v11 worker adds opt-in `LAKE_DIRECT_TRACE_PHASES=1` timing for ordinary/coalesced publication; default execution keeps its existing single query. Native counter and unused-stats images are isolated experiments with distinct library hashes, outside the release prerequisite. The original shorter-interval native autovacuum pilot deadlocks with publication. The [v13 recovery follow-up](../../docs/wal-direct-deadlock-recovery.md) retries whole publications only after protocol-confirmed 40P01 aborts and successful rollback. `LAKE_DIRECT_DEADLOCK_RETRIES` defaults to 3 additional attempts and accepts 0–8; unknown COMMIT outcomes still require restart/checkpoint reconciliation. All 345 native checks pass, including a 1,000-transaction run at 4/second with maintenance. The longer 10/second run fails its drain deadline; production capacity remains open. The v13 image includes current default workers for shared-wire regressions and uses unchanged native libraries.

The [15-minute maintenance follow-up](../../docs/wal-direct-sustained-maintenance.md) passes at 6 transactions/second with exact rows but substantial sampled lag. At 8/second backlog grows and the drain deadline fails despite automatic deadlock recovery. PostgreSQL reaches its memory boundary without OOM at both rates. Native maintenance/metadata costs remain the next optimization target; these are not production capacity limits.

The optional [native prelock child image](prelock/README.md) acquires the existing native table update locks for the full mapping before publication writes. Its [qualification report](../../docs/wal-direct-prelock.md) records 398 passing native checks and a descriptive 13.08% reduction in database/engine CPU at 6/second, with much lower sampled lag and no retries. The longer 8/second run still exceeds its drain deadline. It requires explicit `WAL_DIRECT_PRELOCK=1` in private test fixtures; no release/default direct behavior changes.

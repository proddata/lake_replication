# Chunk-backed initial-copy staging prototype

The [row-cap native comparison](bulk-copy-chunk-prototype.md#prospective-native-result)
finds a faster copy at 4,000 rows but fails all three streaming p95 limits.
Keep the production 1,000-row default. This separate experiment changes storage
for each existing transaction, not transaction size or admission limits.

## Isolated staging cost

An unchanged archived beta.4 worker/runtime runs a disposable 1,000-row SQL
profile followed by managed safety checks. Three rollback-only INSERT plans
report 3.52–3.68 ms execution and 1,000 foreign-key trigger calls each, consuming
2.10–2.16 ms (about 60% of execution). Append function plans take 3.59–4.34 ms.
The INSERT also emits about 350 KB of WAL and roughly 2,028 records per chunk
in the warmed runs. Planning is about 0.02–0.05 ms. These are isolated small-table
plans with instrumentation, not concurrent throughput or native timing evidence.
Rollback still emits WAL. The accompanying 97 archive/managed/profile assertions
pass. No constraint or trigger was disabled to obtain these measurements.

## Targeted change

The disposable SQL overlay stores the unchanged worker's JSON array as one
`initial_copy_chunks` row. Each chunk retains the existing composite parent
foreign key with `ON DELETE CASCADE`, rather than repeating that check for each
source row. A GiST exclusion constraint rejects overlapping ordinal ranges,
including ranges with different starting ordinals. Object validation, ordinal
bounds, the 1,000-row cap, statement-level generation fencing and row-progress
updates remain enforced. The same instance-wide staging permit covers the
transaction. The row-text byte target remains 8 MiB in the unchanged worker;
a single oversized row retains the existing exception. This is not a total
memory bound.

A read-only `initial_copy_rows` view expands committed chunks for existing exact
count, ordinal and publication queries. Publication still validates complete
contiguous rows before writing the destination and committing its checkpoint.
The retired physical row table must be empty and rejects new writes. The overlay
refuses installation over nonempty staging; it never converts an active copy.

The physical storage has fewer index entries and parent checks, but publication
must expand arrays and may sort or detoast more data. TOAST compression, memory,
WAL and publication costs can change. Performance must be measured across the
whole workload rather than inferred from fewer staging statements.

## Safety and scope

The frozen local safety fixture passes 131 assertions: archive identity,
checkpoint isolation, malformed/orphan/overlapping/oversized chunks, adjacent
ranges, uncommitted visibility, rollback, concurrent append and parent-delete
locking, cascade cleanup, stale-generation rejection, completed-copy rejection,
real native byte/row flushing, singleton oversized rows, exact destination data,
stop/re-enable, disconnected backends and abrupt worker death.

The hold/cancel test attaches its pause trigger to the actual physical staging
table in each mode. Bounds tests expand the physical chunks and group by their
transaction IDs, retaining the same row-text byte accounting as the control.

This is not a production migration. It adds `btree_gist`, changes the old row
relation to a read-only view and restricts append calls to the existing native
1,000-row cap. Direct row DML, SQLSTATE compatibility for overlap errors, restore
configuration, populated upgrades, rollback, wider supported inputs and the full
lifecycle/maintenance/permission contract need design and qualification before
integration. Prototype objects belong to the extension for uninstall, but that
does not qualify dump/restore. The production schema and binary defaults remain
unchanged.

## Fixed comparison

```sh
QUALIFICATION_ARCHIVE=/path/to/verified/arm64/archive.tar.gz \
  python3 tests/admission/checkpoint_compare.py --chunk-store --cpuset 0-1
```

This runs per-row/chunk/per-row storage on fresh volumes with split checkpoints
throughout, the same image and worker, independent source sessions, matched
query-return clock sampling and every existing workload gate. Input hashes and
runtime identity must match. There are no automatic retries or best-result
selection. Failed controls and candidates remain retained. A development safety
run preceded the frozen fixture; only the frozen source-matching fixture is used
for qualification claims.

A bounded native comparison is available as `staging_chunk_comparison=true`,
requiring a separate ARM64 native probe. It must follow valid local safety and
performance evidence. Do not combine it with row-cap or checkpoint layout modes.

## Local performance result

The fixed per-row/chunk/per-row cohort passes every unchanged workload gate:

| Storage | Copy seconds | Stream p95 milliseconds |
|---|---:|---|
| Per-row before | 73.11 | 463 / 458 / 461 |
| Chunk candidate | 40.58 | 376 / 429 / 367 |
| Per-row after | 72.08 | 418 / 439 / 428 |

Source schedules, matched clock sampling, four-way publication overlap, exact
data, catch-up and gap checks are valid in all three observations. No engine OOM
or PostgreSQL crash is observed. The candidate is 44.11% below the controls'
mean, with 1.41% control copy drift. This is one sequential local cohort, not a
statistical speedup claim or native qualification.

Periodic samples show about 4.1 MiB aggregate initial-sync process PSS for the
candidate, versus 6.0–6.4 MiB in controls. Destination `memory.current`, which
includes file cache, reaches about 1.11 GiB versus 3.79–3.82 GiB in controls;
engine samples reach about 223 MiB versus 227–292 MiB. These are sampled values,
not exact peaks or guarantees. Source, destination, engine and object store share
the same two CPUs; no cgroup quota throttling is observed.

[Local evidence](evidence/2026-10-30-staging-chunk-local.json) retains the isolated
plans, development-run qualification exclusion, all 131 frozen safety assertions,
full fixed performance comparison, source/archive hashes and resource summaries.
Repository `make check`, 39 admission unit tests and workflow lint pass. The
separate Docker build optimization is excluded to retain the native prerequisite
cache; it is not part of this performance experiment.

A separate one-CPU candidate sensitivity also passes every unchanged gate at
79.01-second copy duration and 1.526–1.535-second streaming p95. Source lateness
stays at about 10 ms; clock/overlap and exact-data checks pass, with no OOM or
PostgreSQL crash. This is not a matched one-CPU speedup comparison. Recorded
inputs for the frozen safety run, full local cohort and sensitivity run match
the final harness sources byte for byte.

## Prospective native result

[ARM64 comparison 41022841650](https://github.com/proddata/lake_replication/actions/runs/41022841650)
uses the clean archive from successful CI 41022649732 at `6f76a31`. The entire
fixed sequence has valid independent source schedules, query-return clock audits
and four-way publication overlap. Archive/exact-data assertions pass, with no
engine OOM or PostgreSQL crash.

| Storage | Copy seconds | Stream p95 milliseconds | Outcome |
|---|---:|---|---|
| Per-row before | 137.28 | 1,889 / 1,818 / 1,863 | PASS |
| Chunk candidate | 87.09 | 3,031 / 2,615 / 2,635 | FAIL: all three latency gates |
| Per-row after | 141.13 | 1,975 / 1,768 / 1,781 | PASS |

The candidate is 37.44% below the controls' mean copy duration, with 2.80%
control copy drift, but its streaming p95 is worse while both controls pass.
This is a valid workload finding, not a source-scheduling or measurement error.
The workflow is correctly red. Do not promote the unpaced chunk store or
re-run the unchanged treatment hoping for a passing observation.

In the candidate, all stream transactions finish publication by 61.56 seconds
from the first source commit. Bulk publication admission is first observed at
76.82 seconds. Slow stream transactions therefore precede observed bulk engine
publication. Their journal-to-batch p95 rises to 1.12–1.33 seconds and
batch-to-publication p95 to 1.44–1.73 seconds. Separate phase percentiles must
not be added. There are 84 transactions above two seconds across the three
streams, versus 7 and 13 in controls.

The first approximately 6–56 seconds of periodic cgroup samples show:

| Service | Per-row before: mean CPU cores | Chunks | Per-row after |
|---|---:|---:|---:|
| Destination PostgreSQL | 1.04 | 0.97 | 1.05 |
| Destination engine | 0.57 | 0.42 | 0.58 |
| Source PostgreSQL | 0.26 | 0.57 | 0.24 |
| Object store | 0.04 | 0.03 | 0.04 |

All four services share two CPUs. Source CPU usage approximately doubles while
the destination engine gets less CPU. Source sampled `cpu.pressure` some/avg10
reaches 86.06%, versus 30.62% and 14.68% in controls; engine pressure reaches
43.25%, versus 35.93% and 31.32%. No destination/engine cgroup quota throttling
is observed. This is consistent with a faster bulk pipeline consuming shared CPU
headroom, not proof of one particular scheduler or SQL cause. Source/object-store
CPU estimates use observer intervals because their raw dumps lack individual
return timestamps. Periodic samples cannot rule out storage or shorter bursts.

Destination memory.current samples, including cache, reach about 987 MiB for
the candidate versus 3,796–3,863 MiB in controls; this does not qualify exact
memory peaks. The workload uses repeated compressible payloads. Fewer parent
checks, fewer index entries and chunk TOAST compression can all contribute;
the copy improvement is not a guarantee for arbitrary data distributions.

[Native evidence](evidence/2026-10-30-staging-chunk-native.json) retains every
observation and failed gate, archive/input/runtime hashes, resource counters and
the stream/publication timeline. Historical row-cap and checkpoint findings are
unchanged.

## Decision and next bounded experiment

Retain chunk storage as a throughput prototype; keep production storage and
defaults unchanged. The remaining blocker is bulk/stream fairness on the shared
CPU budget. The next local experiment should pace the whole bulk pipeline via
the archived worker's existing post-commit `LAKE_INITIAL_COPY_CHUNK_PAUSE_MS`,
without holding the staging permit or generation fence while sleeping. The
source COPY callback consumes data synchronously, so this also tests source-side
backpressure rather than only limiting the engine publisher.

Prespecify a 40 ms pause per unchanged 1,000-row transaction, retain the 8 MiB
row-text target, and compare unpaced/paced/unpaced chunks with fresh volumes and
identical source/resource settings. This pause is a diagnostic candidate, not a
production default or validated fix. At 2,000 chunks per copy it adds at most
80 seconds of explicit sleep per copy; admission waits can overlap it. Actual
completion must still pass 180 seconds with headroom and every streaming gate.
Re-run cancellation/bounds safety and one-CPU sensitivity before another native
comparison. Do not add CPUs, change input cadence or loosen thresholds. A valid
local pacing result is necessary before testing the combined candidate natively;
a hardened production migration remains deferred.

The [subsequent pacing diagnostic](staging-pacing-prototype.md) executes this
40 ms candidate. Its two paced observations, one-CPU sensitivity and safety
checks pass. Both local bracketed comparisons retain unpaced overlap misses
and remain inconclusive; a prospective native comparison is required.

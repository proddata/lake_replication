# Bounded initial-copy transaction size prototype

The [independent-source native checkpoint trial](checkpoint-split-prototype.md#prospective-independent-source-native-result)
puts streaming p95 below two seconds but misses the 180-second copy gate at
182.15 seconds. The next local experiment changes initial-copy transaction
size while keeping split checkpoints, source input and all timing gates fixed.

The archived worker already supports `LAKE_INITIAL_COPY_CHUNK_ROWS` (default
1,000) and `LAKE_INITIAL_COPY_CHUNK_BYTES` (8,388,608). A disposable wrapper
configures 2,000 or 4,000 rows, retaining the existing byte budget. No worker
binary is rebuilt: the overlay compares its SHA-256 before/after installation.
It retains the canonical process command line so native stop/crash tests still
find and terminate the real worker. Production defaults remain unchanged.

The byte budget counts source row JSON text; array framing/SQL quoting and
per-row/allocator overhead are additional. Existing single oversized rows
remain permitted as singleton chunks. This is not a total RSS bound. Increasing
the row cap can grow typical buffers and row overhead even with the same byte
budget, so memory and latency observations must remain part of qualification.
The global staging permit, engine memory/slot limits and journal budgets are
unchanged.

## Safety checks

Both 2,000 and 4,000 rows pass 106 archive/lifecycle/bounds assertions locally.
Real native copies are held before engine publication and grouped by staging
transaction ID to inspect chunk sizes:

- 5,001 narrow rows reach the selected row cap without losing/duplicating IDs.
- 220 rows with 65,536-byte payloads flush on bytes: 127 and 93 rows, each below
  the original 8 MiB row-text budget.
- A 9 MiB single row retains the existing singleton exception; the remaining
  small rows form a separate chunk.
- Publication after admission release preserves exact payloads and contiguous
  ordinals; DROP removes source slots.
- Managed stop/re-enable, generation fencing, disconnect and abrupt-worker-death
  cleanup continue to pass with the unchanged archived executable.

Run safety checks with a verified archive:

```sh
QUALIFICATION_ARCHIVE=/path/to/beta.4-arm64.tar.gz \
  python3 tests/admission/checkpoint_split_probe.py --cases managed --copy-chunk-rows 4000
```

## Fixed local comparisons

`checkpoint_compare.py --copy-chunk-rows N` runs 1,000/N/1,000 on one host with
fresh volumes, split checkpoints throughout and no extra safety setup in the
performance fixtures. It requires matching archive, original image, worker
checksum and source hashes. Only the explicitly recorded wrapper configuration
and resulting image layer vary. Widened byte budgets and mixed source/clock
sampling regimes are rejected. All measured observations and failed gates are
retained; there are no automatic retries or best-result selection.

The first 2,000-row screen passes all gates at 74.89 seconds, followed by a full
1,000/2,000/1,000 sequence at 72.18 / 71.96 / 76.07 seconds. Streaming, source,
clock, overlap and exact-data checks pass. The candidate is 2.92% below the
controls' mean, inside the 5.40% control drift, so it is not a demonstrated
speedup. A 4,000-row configuration is tested prospectively with the same byte
budget rather than promoting this weak result.

```sh
QUALIFICATION_ARCHIVE=/path/to/beta.4-arm64.tar.gz \
  python3 tests/admission/checkpoint_compare.py --copy-chunk-rows 4000 --cpuset 0-1
```

## Observer clock sampling correction

The prior fairness audit paired a SQL wall timestamp near the end of several
observer queries with monotonic time at the beginning of the whole batch. A
slow earlier query could therefore resemble a wall-clock step. New records
capture monotonic start/return for each timestamp query and pair the selected
wall timestamp with that query's return time. The existing one-second audit
tolerance stays unchanged, as do all copy, source cadence, latency and overlap
gates. No extra SQL query is added to the measured path.

Tests distinguish an earlier batch delay from a real forward/backward wall-clock
step, reject malformed timing bounds and reject mixed sampling methods in a
comparison. Historical traces without query-return markers retain the old
validation path; the earlier invalid native control is not requalified.
Results identify the new sampling method as `query_return_monotonic` and must
be measured prospectively. Source commits use their recorded INSERT completion
window when available, with the same tolerance.

## Native boundary

The optional workflow `copy_chunk_comparison=2000` or `4000` requires
`native_probe=true` and `probe_arch=arm64` and cannot combine with other
prototype modes. It has a 20-minute step ceiling for three workloads. The
prerequisite recipe/cache and AMD64 build are unaffected. A passing configuration
overlay does not supply a production migration or qualify an unmodified archive.

## Selected 4,000-row local result

The first fixed 1,000/4,000/1,000 cohort is 72.90 / 69.90 / 72.77 seconds.
The candidate and first control pass every gate; the final control observes
only three overlapping publications and is ineligible. That cohort is retained
as inconclusive. One complete confirmation was declared before rerunning; no
individual observation was retried or selected.

The confirmation passes every unchanged gate at 73.33 / 71.32 / 75.59 seconds,
with source cadence, query-return clock audit and four-way overlap valid. The
candidate is 4.22% below the controls' mean; this is a modest local observation,
not statistical proof or a native speedup guarantee. Both candidate observations
remain retained. With this fixture's narrow rows, 4,000-row chunks reduce staging
transactions from 8,000 to 2,000 while retaining the same bytes per copied row.
The native synchronization cost may differ from the local M5 and must be measured.

A separate one-CPU sensitivity run passes at 115.70-second copy duration and
1.08–1.17-second streaming p95, with valid source/clock/overlap checks, exact data
and no engine OOM or PostgreSQL crash. It is a sensitivity result, not a matched
one-CPU speedup comparison.

Periodic confirmation-run samples show the four initial-sync processes reaching
about 18.6 MiB aggregate PSS with 4,000 rows, versus 5.7–6.4 MiB in the controls;
individual worker samples peak near 4.8 MiB versus 1.9–2.2 MiB. These are sampled
observations, not hard peaks, and demonstrate that the change is not memory-neutral
even though the encoded byte target and engine limits are unchanged.

All 37 admission tests, 24 analysis tests, workflow lint and `make check` pass.
The clock validator also replays the previous native cohort without changing its
valid/valid/invalid classifications. No historical control has been requalified.

[Local evidence](evidence/2026-10-30-bulk-copy-chunk-local.json) retains safety
histograms, both row-cap candidates, the inconclusive cohort, confirmation,
one-CPU run, archived-worker hashes and all failed gates.

## Prospective native result

[ARM64 run 41017649640](https://github.com/proddata/lake_replication/actions/runs/41017649640)
uses the clean CI archive from run 41017420553 at `7ad6044`, with the unchanged
archived worker and split checkpoints in all three observations. The complete
fixed sequence has valid source cadence, query-return clock audits and four-way
publication overlap. Exact-data/archive assertions pass, with no engine OOM or
PostgreSQL crash.

| Configuration | Copy seconds | Stream p95 milliseconds | Outcome |
|---|---:|---|---|
| 1,000 before | 142.60 | 1,693 / 1,873 / 1,785 | PASS |
| 4,000 candidate | 133.04 | 2,062 / 2,022 / 2,054 | FAIL: all three latency gates |
| 1,000 after | 141.78 | 1,815 / 2,058 / 1,836 | FAIL: second stream latency |

The candidate copy is 6.43% below the controls' mean, with 0.57% control copy
drift. This one bracketed sequence supports a copy-throughput observation, not
a statistical speedup claim. The candidate fails the unchanged two-second
streaming limit; the final control also misses it on one stream. All copies
finish below 180 seconds on this host, including the unchanged 1,000-row controls.
The earlier 182.15-second native result remains retained; absolute durations
across different runner allocations are not matched comparisons.

The candidate's source-to-journal p95 is 91–187 ms, journal-to-batch p95 is
914–957 ms and batch-to-publication p95 is 1,147–1,180 ms. These separate phase
percentiles must not be added. Both later phases are higher than the first
control, but the final control also varies; this does not isolate a causal lock
or SQL operation. Periodic samples show bulk staging advisory-lock waits in all
three runs. Destination CPU interval averages are about one core and engine
averages about 0.27 cores, with no cgroup quota throttling. Such samples cannot
exclude brief CPU, storage or lock contention. Initial-copy workers reach about
18.0 MiB aggregate sampled PSS in the candidate versus 6.1–6.6 MiB in controls.

[Native evidence](evidence/2026-10-30-bulk-copy-chunk-native.json) retains the
complete comparison, every failed gate, archive/harness/worker hashes and sampled
resource summaries. The workflow is correctly red because workload gates fail;
this is not a fixture, source scheduling or observer-clock failure.

Do not promote 4,000 rows. Production remains at 1,000 rows and the checkpoint
split remains a diagnostic overlay.

## Local SQL profile after the native finding

A local sequential 1,000/4,000 pair uses identical `pg_stat_statements` planning
and execution instrumentation, independent source sessions, split checkpoints,
two shared CPUs and the same local archive/worker. Both observations pass every
workload gate, copying in 76.07/71.61 seconds with streaming p95 of 423–467 /
486–513 ms. These instrumented local timings do not qualify the failed native
candidate and the unbracketed pair does not establish causality.

| Statement | 1,000 rows: calls / mean execution | 4,000 rows: calls / mean execution |
|---|---|---|
| Bulk staging permit | 8,000 / 18.94 ms | 2,000 / 71.85 ms |
| Append initial-copy rows | 8,000 / 5.87 ms | 2,000 / 24.45 ms |
| Streaming publication | 360 / 30.69 ms | 360 / 30.99 ms |
| Streaming engine permit | 720 / 0.003 ms | 720 / 0.003 ms |

Total append execution is 46.94/48.90 seconds, dominated by the nested staging
row INSERT. Enlarging chunks reduces transaction/round-trip frequency but does
not reduce the observed aggregate append execution; each admission covers a
longer append. Permit wait totals overlap between sessions and nested SQL
execution totals overlap with their callers; do not add them to copy wall time.
Streaming engine permit execution is negligible in this local pair. Neither
fact identifies the native bottleneck: aggregate local SQL profiles are not
native per-transaction traces.

[Profile evidence](evidence/2026-10-30-bulk-copy-chunk-sql-profile.json) records
all gates, source/archive/worker/profile hashes and per-database statement costs.
Raw profiles remain at the recorded local evidence paths. Both native and local
failed/inconclusive observations elsewhere in this document remain retained.

The next bounded investigation is an isolated append profile at the current
1,000-row cap: inspect INSERT/index/constraint/WAL costs, including foreign-key
trigger time, before changing the staging representation. Preserve parent/row
integrity, exact data, cancellation, generation fencing and transaction/byte
bounds. Separately obtain per-call publication timing if staging changes do
not improve latency; interval CPU samples and local SQL averages cannot choose
that fix. Require a concrete locally verified improvement before another native
comparison. Do not retry the unchanged cohort or relax thresholds. The hardened
checkpoint production migration remains deferred.

The [subsequent chunk-storage investigation](staging-chunk-prototype.md) measures
per-row foreign-key work and tests one durable chunk per unchanged 1,000-row
transaction. Its local integrity and fixed performance cohort pass; no production
row-cap change is adopted.

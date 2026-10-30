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

The next bounded native experiment is:

```sh
gh workflow run lifecycle-qualification.yml --ref main \
  -f artifact_run_id=CI_RUN_ID -f native_probe=true -f probe_arch=arm64 \
  -f copy_chunk_comparison=4000
```

Keep every observation and all existing gates. Select a production migration
only after valid native data demonstrates adequate latency and copy headroom.
Production worker defaults and extension SQL remain unchanged.

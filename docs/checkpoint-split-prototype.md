# Independent receiver and publication checkpoints: local prototype

The [native timing diagnostic](native-followup-diagnostics.md#targeted-timing-result-and-checkpoint-coupling)
shows substantial pre-insertion delay. A deterministic archive-only reproduction
proves that publication's checkpoint row lock blocks staging and acknowledgement
below the journal limit. This prototype tests removing that dependency while
keeping publication and its progress marker atomic.

`tests/admission/checkpoint_split.sql` is a disposable SQL overlay. It is not an
extension migration and is never included in release archives. The original
worker binaries and archived extension SQL are installed first. Production
migrations remain unchanged.

The overlay separates the hot paths:

- Receiver staging, journal admission and acknowledgement lock an ingress row.
- `prepare_batch` locks a publication row, then reads the committed staged
  frontier without locking ingress. Competing publishers still serialize.
- `finish_batch` updates the publication row in the same transaction as target
  changes, batch state and published journal state. Engine work and progress
  are not split into separate commits.
- Journal admission counts committed unpublished rows. An in-flight publication
  does not free capacity for other sessions until commit. A rollback leaves
  staged work available for replay.
- The existing managed generation fence also protects the new publication table.

A combined `checkpoints` view keeps the harness/worker interface. Initial-copy
promotion, subscription creation and cleanup use a routing trigger; stored views
are rebound to the combined view. The retained ingress table has an unused legacy
`published_lsn` column. This is an experiment mechanism, not the proposed final
schema or upgrade implementation.

The original checkpoint constraints span fields that would now reside in two
rows. Staging and acknowledgement retain their ingress constraints; publication
validation checks its requested end LSN against committed staging. A production
migration must explicitly preserve the full direct-write permission/constraint
contract, dump/restore, upgrade/rollback, cleanup and maintenance behavior. The
prototype grants are for disposable test databases and do not establish that
production security contract. No prototype result qualifies a release.

Run locally with a verified arm64 beta.4 archive:

```sh
QUALIFICATION_ARCHIVE=/path/to/beta.4-arm64.tar.gz \
  python3 tests/admission/checkpoint_split_probe.py --cases managed fairness --cpuset 0-1

QUALIFICATION_ARCHIVE=/path/to/beta.4-arm64.tar.gz \
  python3 tests/admission/contention.py --copies 4 --cpuset 0-1
```

All fixture services share the selected CPU affinity. The source schedule,
journal limits, copy size, latency threshold and overlap checks are unchanged.
The driver records overlay hashes and snapshots in qualification evidence.
It reuses the existing local prerequisite image; the separate pending Dockerfile
optimization is not rebuilt or validated by these runs.

Seventeen focused checks cover ingestion during held publication, acknowledgement,
full-journal retry, competing publishers, rollback, visibility before commit,
capacity after commit, replay, conflicting replay and frontier bounds. The
managed suite checks native stop/re-enable, generation fencing, source feedback,
byte/count bounds, disconnect cleanup and exact resumed data with the overlay.

The decision requires a local constrained comparison, then a targeted native
comparison with retained timing evidence. Local M5 measurements cannot close the
failed native latency gate. If separation removes lock coupling but does not
improve the measured bottleneck, do not promote it on the lock test alone.

## Initial local checks

The first two-CPU prototype run passes all checks: 17 focused checks per
fixture, the managed suite, exact data, four-copy publication overlap and all
unchanged timing gates. The sequential original-archive run also passes.

| Two shared CPUs | Copy duration | Stream p95s | Pre-insertion p95s |
|---|---:|---|---|
| Original archive | 72.85 s | 463 / 481 / 603 ms | 54 / 44 / 183 ms |
| Split overlay | 72.72 s | 461 / 467 / 454 ms | 18 / 10 / 12 ms |

These are single local observations, not a statistically established speedup.
Both variants pass, and the native multi-second delay is absent.

| One shared CPU | Copy duration | Stream p95s | Pre-insertion p95s |
|---|---:|---|---|
| Original archive | 118.04 s | 1101 / 1242 / 1149 ms | 483 / 463 / 448 ms |
| Split overlay | 117.91 s | 1086 / 1123 / 1065 ms | 13 / 9 / 8 ms |

Both one-CPU runs pass the unchanged gates, exact data and four-way overlap,
with valid clocks and source schedules. The source-to-insertion delay falls
substantially, while end-to-end gains are modest: some waiting moves to the
journal (insertion-to-batch p95 is 480–508 ms in the overlay). This supports a
targeted native diagnostic, not a claim that separation alone will meet the
native two-second target. No additional local repeats were used to select a
favorable result.

[Machine-readable evidence](evidence/2026-10-29-checkpoint-split-prototype.json)
retains all four runs, source/overlay/archive hashes and unchanged gate results.
The first two-CPU run predates the driver's automatic overlay marking of root
and case results; its retained overlay declaration was applied afterward, with
an explicit annotation. Executed sources and original overlay metadata remain
retained. The one-CPU run records the declaration automatically.

## Native diagnostic boundary

The workflow's `checkpoint_split_diagnostic=true` option requires
`native_probe=true` and `probe_arch=arm64`. It verifies the CI-built archive's
provenance first, then runs the explicit SQL overlay with managed/four-copy
checks. Evidence marks the overlay and preserves its source hashes. The
standard archive-only probe and full release qualification are unchanged.
This diagnostic uses the existing prerequisite recipe and cache; a local
Dockerfile optimization is intentionally excluded from its commit.

## Production migration requirements

If the native diagnostic supports this direction, implement an explicit,
stopped-worker upgrade with two authoritative tables and a stable combined
read interface. Remove the prototype's unused legacy publication column and
register both data tables for extension dump/restore. Preserve the frozen
prior-release SQL and qualify both clean installation and real upgrades.

The candidate constraint strategy is to enforce monotonic frontiers at all
supported write boundaries, keep `received >= staged >= acknowledged` as local
ingress constraints, and validate publication against committed staging. With
staging unable to regress, publication can validate its upper bound without
locking ingress throughout engine work. This needs adversarial concurrent
write tests and an explicit direct-DML permissions decision; merely relying on
current worker behavior is insufficient. Publication lookup must also fail
explicitly if its physical row is missing, rather than silently skipping a
progress update. Deletion/identity rules must preserve paired checkpoint rows.

Qualify initial-copy promotion/recovery, rebuild promotion, journal maintenance,
status views, stop/generation fencing, crash/replay behavior, logical restore,
upgrade rollback and uninstall against the final schema. These are promotion
requirements, not outcomes established by the performance overlay.

## Native result: latency improves, qualification still fails

[ARM64 overlay diagnostic 40957099106](https://github.com/proddata/lake_replication/actions/runs/40957099106)
at `99f9177` verifies and loads the existing prerequisite cache. Managed checks,
all focused checkpoint assertions, exact data, four-way publication overlap,
clock validity and crash/OOM checks pass. It completes with workload findings:

| Stream | Publication p95 | Source → insertion p95 | Source scheduling lateness |
|---|---:|---:|---:|
| analytics | 1883 ms | 291 ms | 385 ms |
| fair_stream_1 | 1717 ms | 39 ms | 523 ms |
| fair_stream_2 | 1757 ms | 46 ms | 677 ms |

All streaming p95s are below two seconds. However, the latter two source
schedule observations exceed the unchanged 500 ms limit, and copies take
182.86 seconds against the 180-second limit. This is not a passing workload or
release qualification; source scheduling failure limits the latency conclusion.
No timing threshold is relaxed.

The original native run had 2.579–2.945-second streaming p95 and 158.18-second
copies. Those observations come from different runner instances. Copy staging
is also slower after measured streaming has ended: samples nearest 80–120
seconds show about 66.5k rows/s in the overlay run versus 78.2k in the original.
That period contains essentially no streaming engine CPU work. This does not
establish whether the copy difference comes from host/storage variation,
fixture/setup effects or a persistent cost of the prototype. Publication counts
are similar (271 original, 275 split across the measured streams), so there is
no evidence of a large increase in engine publication count.

**Decision:** retain the prototype; do not migrate production yet. The next
performance experiment should compare original/split/original sequentially on
one ARM64 runner, reusing the same verified prerequisite image. Keep all source,
copy, latency and overlap gates, retain every result, and omit extra focused
setup work from the timed performance-only fixtures. This separates runner
variation from the schema change before selecting a copy/resource fix. Do not
repeat an unchanged full qualification or presume that the three-second copy
miss is harmless noise.

[Native evidence](evidence/2026-10-29-checkpoint-split-native.json) retains the
archive/overlay hashes, timing summaries, exact source misses and the failed
gates. The unmodified production archive remains blocked on native latency;
this overlay is still an experiment.

## Controlled comparison harness and local check

`tests/admission/checkpoint_compare.py` runs a fixed original/split/original
sequence on one host, using the same archive, destination image, harness inputs
and CPU affinity. Child fixtures have fresh volumes, and extra focused safety
operations are omitted from every performance fixture. Findings are retained
without retrying or selecting a best result; fixture/provenance errors stop
the sequence. Input and image identity must match all three observations.
The same driver supports original and split layouts, eliminating differences
between separate diagnostic drivers.

The first local startup caught an evidence-directory collision before fixture
startup. After correction the complete two-CPU sequence passes every gate:

| Observation | Copy duration | Stream p95s |
|---|---:|---|
| Original before | 72.23 s | 462 / 462 / 439 ms |
| Split | 78.47 s | 501 / 495 / 492 ms |
| Original after | 74.14 s | 457 / 482 / 451 ms |

Control copy drift is 2.64%; the split copy is 7.22% slower than the mean of
the controls. This is a cost signal, not statistical proof of a regression.
All clocks, source schedules, exact-data and overlap checks are valid. The
final classifier reproduces these retained observations, with 29 admission
unit tests and workflow lint passing.

A separate local pair enables `pg_stat_statements` with planning/I/O timing in
both layouts and captures normalized statements after measurement. These are
instrumented observations, not comparable to unprofiled qualification times.
Copies take 76.34 s original and 78.76 s split. The pending-work query's total
planning plus execution changes from 89 ms to 135 ms over 1,990 calls, too
small to explain the wall-clock difference by itself. Staging append execution
is slightly lower in the split run (48.45 vs 47.70 seconds total). Bulk
publication admission wait totals differ substantially (14.98 vs 26.54 seconds),
while nested timings overlap and must not be added. This does not identify a
specific planning fix; the controlled native comparison remains necessary.
Raw profiles and source hashes are retained separately.

Run the comparison locally with `QUALIFICATION_ARCHIVE` set:

```sh
python3 tests/admission/checkpoint_compare.py --cpuset 0-1
```

The workflow's `checkpoint_split_comparison=true` option requires a focused
ARM64 native probe and cannot combine with the overlay safety diagnostic.
It allows 20 minutes for the three workloads, each retaining all original
performance gates. Every workload finding, including an original latency miss,
causes a nonzero exit; inspect the comparison summary's `measurement_valid`
and `candidate_workload_passed` separately. Source/clock failures invalidate a
comparison even when latency is low. A comparison does not qualify a release.

The optional local profiling command is:

```sh
python3 tests/admission/checkpoint_split_probe.py --cases fairness \
  --skip-operations --variant original --profile-sql
# Repeat with --variant split, retaining both instrumented observations.
```

Profiling is disabled in the native comparison. No production SQL or binary,
prerequisite recipe or timing threshold changes in this diagnostic work.
[Local comparison and profile evidence](evidence/2026-10-29-checkpoint-comparison-local.json)
retains the full cohort, initial setup error, hashes and selected inclusive
statement totals.

## Same-runner native result and source workload correction

[Native comparison 40962971005](https://github.com/proddata/lake_replication/actions/runs/40962971005)
completes all three observations at `e3e6958` with matching archive, image and
harness identities. No fixture or exact-data error aborts the sequence.

| Serial-source observation | Copy duration | Stream p95 range | Maximum source lateness |
|---|---:|---:|---:|
| Original before | 190.35 s | 4.04–5.13 s | 2.315 s |
| Split | 176.73 s | 1.72–1.91 s | 0.653 s |
| Original after | 179.28 s | 2.29–2.74 s | 0.745 s |

The split passes copy and streaming latency gates in this cohort; its only
failed gate is source scheduling in the third stream. All three observations
miss at least one source schedule check, so the comparison is invalid for
qualification. The apparent 4.38% lower split copy time relative to the controls'
mean does not establish a speedup. These matched observations also do not
reproduce the earlier separate-run copy slowdown consistently. No gate is
relaxed and production migration remains deferred.

The harness uses one source session and one producer loop for all three
streams. In split transaction 12 the first stream starts on time, the second
83 ms late, and the third 653 ms late. In the following round the first stream
inherits 213 ms of lateness. This proves cross-stream scheduling coupling;
CPU/I/O stalls may contribute too. The old cohort remains retained as a failed
serial-source observation, not reclassified as a passing workload.

The source producer now uses one durable session/thread per stream,
with a common start and the same 500 ms schedule, 120 transactions per stream,
20 rows per transaction and all existing gates. Each stream records its own
actual commit timestamp and insertion start. One blocked source no longer
postpones other streams. Source errors signal shared cancellation, and shutdown
joins producers against one deadline. Source fsync/durability, source/destination
CPU affinity and production binaries are unchanged.

This changes the offered workload's concurrency/burst shape, so it requires a
new original/split/original cohort. New results explicitly record
`source_producer=independent_sessions`; comparison rejects mixed source regimes.
The helper is included in source snapshots and input identity checks. Historical
serial-source performance observations cannot qualify the revised workload.

[Retained native comparison](evidence/2026-10-30-checkpoint-native-comparison.json)
includes every gate, identity and the source scheduling examples.

The revised source producer passes a local two-CPU rebaseline:

| Independent-source observation | Copy duration | Stream p95s | Maximum source lateness |
|---|---:|---|---:|
| Original before | 71.58 s | 478 / 484 / 459 ms | 5.42 ms |
| Split | 73.36 s | 472 / 488 / 476 ms | 5.18 ms |
| Original after | 73.69 s | 460 / 414 / 428 ms | 10.18 ms |

All three observations pass every unchanged gate, with matching archive/image
and harness identities. The 1% split-to-control-mean copy difference is a single
local observation, not statistical proof. It cannot be combined with the older
serial-source cohorts. Thirty-three admission unit tests, twenty-two analysis
tests, workflow lint and the full `make check` pass. New tests cover blocked
stream isolation, shared cancellation, bounded join and mixed-regime rejection.

The final producer additionally records monotonic insert completion and commit
observation completion, so future source delays can be split into durable INSERT
and timestamp-observation round trips. These non-gating markers were added after
the measured local cohort and pass ordering tests/full checks. They do not change
source SQL, cadence or acceptance limits. The next qualification is one prospective
native cohort, not an unchanged retry of the invalid serial-source workload.

[Independent-source local evidence](evidence/2026-10-30-independent-source-local.json)
retains the full three-run cohort and its hashes. Production migration remains
unimplemented pending valid native evidence.

## Prospective independent-source native result

[Native comparison 41007547690](https://github.com/proddata/lake_replication/actions/runs/41007547690)
at `7c5d901` completes original/split/original with matching archive, destination
image, harness and independent-source regime. All three observations meet the
unchanged 500 ms source-start lateness limit. The serial source scheduling
coupling is removed in the measured native workload.

| Observation | Copy duration | Stream p95 range | Maximum source lateness | Measurement validity |
|---|---:|---:|---:|---|
| Original before | 183.17 s | 2.43–3.96 s | 3.64 ms | Valid; copy/latency gates fail |
| Split | 182.15 s | 1.71–1.74 s | 199.97 ms | Valid; only copy gate fails |
| Original after | 178.26 s | 1.90–2.15 s | 141.96 ms | Invalid observer clock audit |

The candidate passes exact data, overlap, source cadence, clock validity and
streaming latency/catch-up/gap checks, with no engine OOM or PostgreSQL crash.
Its remaining failed gate is 182.15-second copy completion against 180 seconds.
The valid first control also misses copy completion and streaming latency.
This supports independent checkpoints as the latency direction; it does not
qualify the candidate or prove a statistical copy performance result.

The final control reports `observer wall clock differs from monotonic trace`.
The root cause of that audit mismatch is not established; it must not be
reclassified as a passing observation or silently discarded. Consequently the
full bracketed comparison remains invalid, even though source cadence is valid
in every observation. The summary reporter now includes clock/OOM/crash
validity failures explicitly in `failed_gates`; thresholds and acceptance logic
are unchanged.

Source completion markers show durable INSERT round trips reaching roughly
560–699 ms in the split run, while subsequent source-start lateness remains
62–200 ms, within the limit. The observation queries are much shorter (at most
about 12 ms). Independent schedules prevent those delays from cascading across
all streams as in the old shared source loop.

**Decision:** keep the split checkpoint prototype, and keep production migration
deferred. The next runtime work is local bulk-copy throughput/headroom, using
the retained staging and publication profiles and preserving row integrity,
transaction/memory/admission bounds, exact data and all existing timing gates.
Do not loosen the three-minute limit or launch another unchanged native run.
Requalify the complete workload and clock audit after a concrete, locally tested
copy improvement; then implement the hardened production schema/upgrade.

[Prospective native evidence](evidence/2026-10-30-independent-source-native.json)
retains the valid pair, invalid final control, source round trips and all failed
gates. This remains an experiment; the production archive still uses the shared
checkpoint row.

The subsequent [bounded copy transaction prototype](bulk-copy-chunk-prototype.md)
retains the 8 MiB row-text target and original worker binary while testing
larger row caps. Its safety checks, local controlled cohorts and one-CPU
sensitivity pass. It also corrects the observer's timestamp pairing with the
same clock tolerance, leaving this historical invalid control unchanged.
The [prospective native configuration trial](bulk-copy-chunk-prototype.md#prospective-native-result)
passes measurement validity and improves copy duration, but the larger chunks
fail all three streaming latency gates. The 1,000-row controls also vary. Keep
the production row cap and investigate staging/publication costs locally; no
production migration has been performed.

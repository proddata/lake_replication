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

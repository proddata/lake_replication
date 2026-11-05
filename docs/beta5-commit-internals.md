# Beta.5 commit internals and manifest cleanup candidate

The earlier native trace identified commit/finalization as the largest elapsed
publication phase. This follow-up measures its internal work locally, using the
clean ARM64 CI archive at `170839d` and pinned pg_lake `8180647d4535`. No production
default or submodule revision changes here.

## Internal timing

The diagnostic verifies cached source hashes before rebuilding affected pg_lake
libraries. It reuses the original prerequisite image and its compiled objects;
DuckDB is not rebuilt. Patched sources, header, recipe and original/rebuilt
library hashes are retained. These are timer-only binary overlays, not release
qualification artifacts.

The first internal profile passes the four-copy workload and matches all 318
publications to their internal spans:

| Mean elapsed time across the three streams | Range |
|---|---:|
| Complete commit | 381–386 ms |
| Metadata preparation | 154–156 ms |
| Iceberg metadata generation | 207–211 ms |
| Upload completion | 19–20 ms |
| Remaining commit work | 1.0–1.5 ms |

The deeper profile also passes, matching all 330 publications. Its main costs
are scanning the previous snapshot's files (125–129 ms) and finalizing the new
snapshot (160–164 ms). Catalog reads take 0.25–0.40 ms; loading statistics for new
files takes 0.39–0.55 ms. Metadata JSON reads take 3.6–4.2 ms, and metadata writes
take 14.8–18.3 ms. These nested spans must not be added to their parents. Different
instrumentation and host drift prevent treating the profiles as a speed
comparison. The remainder includes other callbacks, logging and durability;
it is not a standalone WAL measurement.

Internal markers are attached to the next completed server message from the
same database/backend, then joined to the already matched publication COMMIT
sequence. The analyzer rejects missing parents, inconsistent upload counts,
missing messages and child spans that exceed their enclosing spans.

## Guarded candidate

`RemoveDeletedManifestEntriesInternal` reads each old manifest to remove entries
marked DELETED. When a manifest is known to contain live entries and zero deleted
entries, that read can return the original manifest unchanged. The prototype
tests this shortcut behind a diagnostic GUC.

Unknown counts require special handling. The existing reader maps an Avro NULL
integer to zero. The candidate retains the integer's existing value and adds a
separate `deleted_files_count_known` flag. It handles plain integers and nullable
unions. Unknown deleted-entry counts, negative counts, manifests containing deleted entries,
and empty manifests retain the original cleanup path. The Iceberg specification
[defines these counts and requires unknown counts to be treated conservatively](https://iceberg.apache.org/spec/#manifest-lists).

The added internal field requires a coherent rebuild of pg_lake_iceberg and
pg_lake_table. The diagnostic removes both modules' old objects before building
them together; only those modules use this struct in the pinned source tree.
The proposed [upstream patch](../tests/admission/patches/pg_lake_skip_clean_manifests.patch)
contains the count-presence tracking and shortcut without the diagnostic GUC
or timers. It is not automatically applied by production builds.

## Correctness checks

Both final control and candidate fixtures pass 16 assertions each. Their 13
normalized snapshots match exactly, including independent Iceberg file and
manifest observations. Cases cover initial empty tables, appends, manifest
merging, savepoint rollback, DELETE, UPDATE, VACUUM, deleting every row,
TRUNCATE, insertion after TRUNCATE and full transaction rollback. The fixture
explicitly handles tables that have no current snapshot yet.

The control records zero shortcut witnesses; the candidate records five. Both
use identical rebuilt library hashes. The actual C predicate is also compiled
and checked for unknown, empty, deleted, negative and large counts. The actual
reader helper is compiled against the cached libavro and tested with plain
zero, nullable zero, NULL and missing counts. These focused checks do not replace
the full upstream regression suite or native qualification.

## Performance comparison

The fixed local sequence is control/candidate/control. The archive, binaries,
source/harness hashes and workload are identical across trials. Only the
diagnostic shortcut setting changes. The harness verifies that all seven
databases inherit the declared setting, internal timers are off, and worker
query tracing is off. All timing, copy, admission, exact-data and safety gates
remain in force. Failed controls remain failures and are retained.

All three local trials pass with valid source/observer timing, exact data,
concurrent publication, queue observation and no OOM or PostgreSQL crash:

| Trial | Four-copy completion | Three streaming p95 values |
|---|---:|---|
| Control | 126.75 s | 1.307 / 1.285 / 1.296 s |
| Candidate | 125.85 s | 0.611 / 0.591 / 0.628 s |
| Control | 131.50 s | 1.876 / 1.899 / 1.855 s |

The controls show host drift. The candidate stays below both controls while
copy time remains similar; this supports a native comparison, not a statistical
speedup claim. The original local cohort ran its two correctness fixtures
separately first. The runner now automates those same safety checks, including
the real-libavro count test, before the three fixed performance trials.

[Machine-readable evidence](evidence/2026-11-05-beta5-commit-internals.json)
retains source/library/patch hashes, internal spans, correctness snapshots,
reader checks and every control/candidate result. The native workflow's
`manifest_cleanup` diagnostic runs the guarded comparison on a selected
architecture and keeps any failed control red.

## Native comparison and delivery verification

The fixed AMD64 [native comparison](https://github.com/proddata/lake_replication/actions/runs/42092260358)
at `fb29d5d` completes with valid measurements. Both correctness fixtures pass
with identical normalized snapshots. All three workload trials pass exact-data
and safety assertions, with no OOM or PostgreSQL crash:

| Trial | Four-copy completion | Three streaming p95 values |
|---|---:|---|
| Control | 161.94 s | 2.274 / 2.171 / 2.268 s |
| Guarded candidate | 160.76 s | 1.584 / 1.545 / 1.678 s |
| Control | 161.07 s | 2.301 / 2.356 / 2.107 s |

The candidate passes every workload gate. The workflow remains red because both
controls fail the unchanged two-second latency limit. This is evidence for the
shortcut, not release qualification.

A separate local build applies the minimal unguarded patch to the same original
prerequisite image used by the earlier local cohort. It rebuilds Iceberg and
table objects together and leaves the engine unchanged. The pipeline and
managed-worker cases pass, as do all 166 assertions. However, its fairness case
fails valid streaming p95 at 3.015 / 2.993 / 2.974 seconds. Copy completion passes
at 136.16 seconds; no OOM or PostgreSQL crash occurs. This finding is retained
and prevents qualifying that run. The different case order, rebuilt binaries
and host conditions prevent assigning a cause from this observation alone.

A predeclared local guarded/minimal/guarded sequence then isolates the same
fairness case on fresh fixtures, with the same archived workers and prerequisite
base. Internal timers and worker query tracing remain disabled:

| Trial | Four-copy completion | Three streaming p95 values |
|---|---:|---|
| Guarded candidate | 121.59 s | 0.532 / 0.539 / 0.490 s |
| Minimal unguarded patch | 117.29 s | 0.457 / 0.484 / 0.483 s |
| Guarded candidate | 118.89 s | 0.465 / 0.446 / 0.466 s |

All three trials pass every gate with valid measurements. There is no
reproducible slowdown from removing the diagnostic code in this cohort. This
does not explain away the earlier failure or establish a statistical speedup.
The exact unguarded patch still needs native verification before delivery.

The unguarded patch's separate correctness fixture also passes all 15 assertions.
Its 13 normalized row/file/manifest snapshots match the original control exactly,
including maintenance and rollback cases. This fixture has no diagnostic GUC
assertion, accounting for one fewer assertion than the guarded fixtures.

[Delivery evidence](evidence/2026-11-05-beta5-manifest-delivery.json) records the
native trials, unsuccessful initial unguarded verification and complete fixed
follow-up cohort, including artifact, image, library and raw-result hashes.

The standalone `tests/admission/manifest_patch_verify.py` runner verifies pinned
source hashes, applies the minimal patch and records the original and rebuilt
libraries. Its default sequence is pipeline, managed-worker and fairness cases;
`--cases fairness` isolates the unchanged performance workload. The separate
`--correctness-only` mode runs the same independent row/file/manifest fixture
without diagnostic GUCs or internal timers. Use an isolated checkout with the
committed prerequisite recipe and an explicitly selected CI archive. These
binary overlays do not qualify the archived production prerequisite.
The native workflow's `manifest_patch` option runs the independent fixture
followed by the default pipeline/managed/fairness sequence, without timers or
diagnostic GUCs. Every unchanged latency and safety gate remains enforced.

A verified production prerequisite containing the reviewed fix is still
required before this can close the beta latency gate. Longer-term growth and
maintenance behavior also remain part of full qualification and the soak.

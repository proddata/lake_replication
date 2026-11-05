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

Native comparison and a production prerequisite containing the reviewed fix are
still required before this can close the beta latency gate. Longer-term growth
and maintenance behavior also remain part of full qualification and the soak.

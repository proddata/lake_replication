# Current-runtime engine admission results

Bulk-only admission is the candidate to qualify next. Four concurrent large
copies expose a current streaming-latency finding; limiting bulk work to one
transaction while leaving streaming unrestricted passes that same diagnostic.
Production scheduling is unchanged until the candidate is qualified and promoted.

The current beta.2 runtime passes the original four-database/sixteen-subscription
restart burst and single-copy diagnostic without admission. The historical OOM
did not reproduce. Reserving and serializing streaming capacity adds observed
latency without a demonstrated benefit in those cases.

[Retained evidence](evidence/2026-10-22-engine-admission-results.json) records
checks, input hashes, archive/image identities, timing audits and log hashes.
The [protocol](engine-admission-experiment.md) describes the workload and limits.
These are single local arm64 diagnostics, not paired statistical performance
qualification or a supported capacity ceiling. No causal claim is made about
which prior lifecycle/receiver change removed the earlier failure.

## What was tested

The prototype applies admission to seven engine-capable SQL entry points while
retaining the current production launcher and workers. It permits one bulk and
one streaming transaction concurrently. PostgreSQL-only staging remains outside
the gate; transaction commit still owns atomic copy/checkpoint publication.

The initial function-entry policy queued ready-schema reconciliation behind
bulk copy. It failed the original two-second p95 target in every stream. Six
observations show streaming databases waiting on the bulk lane while the copy
holds it. The source snapshots for this failed policy are retained with hashes;
its results were not replaced by the revision.

The revision moves schema admission to actual DDL. Thirty-eight assertions cover
the revised reserved/global lock fixtures, including existing-table and
ready-schema progress while permits are held, cross-database exclusion,
cancellation, timeout, savepoint rollback and backend-death cleanup. The revised
copy trace has no streaming database waiting for bulk and three observations
with both lanes admitted.

| Diagnostic | Current package, no admission | First reserved policy | Revised reserved policy |
| --- | ---: | ---: | ---: |
| Four databases / sixteen subscriptions, live and restarted exact data | Pass | Pass | Pass |
| Two-million-row copy duration | 16.55 s | 16.15 s | 17.65 s |
| Per-stream publication p95 | 258–267 ms | 2,149–2,333 ms | 1,092–1,404 ms |
| Largest publication gap | 0.65 s | 3.94 s | 1.31 s |
| Largest final catch-up | 0.17 s | 0.27 s | 0.50 s |
| Minimum commits per stream during copy | 33 | 26 | 35 |
| All coexistence limits | Pass | Fail: p95 | Pass |
| Exact final rows, IDs and payloads | Pass | Pass | Pass |
| Engine OOM / unexpected PostgreSQL crash markers | None | None | None |

Every measured coexistence case passes the original wall/monotonic clock audit.
Memory snapshots are not peaks and do not establish a resource bound. The table
does not establish a statistically measured regression ratio; it does show that
the diagnostic supplies no reason to accept the prototype's extra serialization.

All private containers/volumes were removed. Raw runs and exact prototype source
snapshots remain under `.qualification/qualification_20261022_054553_reserved_admission/`,
`qualification_20261022_054732_reserved_admission/`,
`qualification_20261022_055643_reserved_admission/` and
`qualification_20261022_055907_reserved_admission/`. The archive is the locally
qualified beta.2 arm64 candidate, SHA256
`963e6dd28b4b20fa443e9f43a72f510bb7d7a73c1297ef66d510b610bf69d17e`;
the production worker/migration/extension sources match `9cfee8f`. Experimental
images rebuild the C library and add scheduling SQL; they are not release packages.

## Four simultaneous copies: current finding and narrower policy

The follow-up runs four two-million-row copies into four databases while the
same three streaming subscriptions remain active: seven databases and eight
million copied rows, under the same 2 GB/one-thread engine. Four copy-publication
function backends overlap while the source is offering streaming transactions.
This counter measures function activity, including permit waiters, not four
simultaneously executing internal engine INSERT statements.

| Four-copy diagnostic | No admission | Bulk-only permit |
| --- | ---: | ---: |
| Group copy duration, first start through last commit | 37.34 s | 42.57 s |
| Per-stream publication p95 | 3,071–3,360 ms | 874–924 ms |
| Largest publication gap | 0.93 s | 0.82 s |
| Largest final catch-up | 0.16 s | 0.12 s |
| All coexistence limits | Fail: p95 | Pass |
| Exact copied/streaming rows, IDs and payloads | Pass | Pass |
| Engine OOM / unexpected PostgreSQL crash markers | None | None |

The no-admission latency increase overlaps copy publication. The bulk-only
variant leaves streaming unqueued and serializes bulk admission; 25 observations
contain queued bulk work, at most one permit is admitted and no streaming permit
is requested. It trades a longer overall copy interval for lower observed stream
lag. Both runs pass clock and source-schedule audits. This is one diagnostic per
variant, not a statistical guarantee or a universal capacity bound.

The bulk-only run passes 99 assertions across lock handling, the original
sixteen-subscription restart and the new four-copy fixture. Raw runs and exact
tested source snapshots are retained in
`.qualification/qualification_20261022_060830_reserved_admission/` and
`qualification_20261022_061312_reserved_admission/`. All private containers and
volumes were removed; `make check` passes.

## Next engineering decision

Implement the bulk-only candidate in production source, retaining the base
lifecycle integration and current streaming concurrency. The [operational follow-up](bulk-admission-operations-results.md)
fixed a transient schema-error handling defect and passes 39 assertions for
actual engine-request cancellation, independent schema/rebuild concurrency,
rollback and lock-order abort/retry. The corrected image also passes the
four-copy diagnostic again. The [paired performance follow-up](bulk-admission-performance-results.md)
passes five pairs each for steady streaming, catch-up and single-copy overhead.
Production migration, qualification of both native packages and the release soak
remain pending.

The bulk-only limit does not bound PostgreSQL staging memory, source snapshot/WAL
retention, wrapper/connection growth or arbitrary application engine queries.
Neither its diagnostic pass nor the absence of an OOM proves a total memory bound.

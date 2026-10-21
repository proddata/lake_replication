# Admission performance and large-copy coexistence

Evaluation completed on 2026-10-21. Five steady-load and five timing-validated
catch-up pairs are retained, alongside both coexistence cases. The clock-disrupted
catch-up attempt was rejected and replaced, not mixed into the accepted pairs.
See the [retained results](evidence/2026-10-21-base-admission-performance-results.json).

Bounded admission remains useful for engine safety, but the fixed one-permit
policy is too restrictive for the large-copy responsiveness targets in this
evaluation. Keep the base lifecycle integration; do not promote the current
admission policy unchanged on the strength of the earlier functional restart
pass. The receiver also has a separate empty-stream correctness finding.

## Scope

The [protocol](pg-extension-base-admission-performance-protocol.md) compares the
exact qualified base images before/after admission, with identical 2 GB/one-thread
engines, 64 PostgreSQL worker slots and commit timestamp tracking. Production C,
Rust and migrations were unchanged in these measurement images. Steady/coexistence data come from
`.qualification/qualification_20261020_061805_admission_measure/`; accepted
catch-up data come from `.qualification/qualification_20261021_054133_admission_measure/`.

## Matched single-subscription measurements

Each cohort contains five whole alternating pairs. All source insertion windows,
publication observation windows and warm-ups pass the realtime/monotonic audit.
Source elapsed times also stay within one second of the prescribed write window.
The accepted catch-up driver used scoped idle-sleep prevention. The interrupted
unmatched case and [clock-disrupted cohort](evidence/2026-10-21-admission-rejected-clock-run.json)
are excluded. Regression tests reject clock steps while accepting genuinely slow
SQL with consistent clocks.

| Median metric | Without admission | One permit |
| --- | --- | --- |
| Steady publication p95 | 266.18 ms | 258.18 ms |
| Steady publication throughput | 201.09 rows/s | 201.02 rows/s |
| Catch-up publication p95 | 16.25 s | 17.27 s |
| Catch-up publication throughput | 414.56 rows/s | 400.12 rows/s |

Steady absolute lag/catch-up/backlog/scheduling checks pass. Its paired p95 ratio
median is `0.9684`, with 95% descriptive interval `[0.8966, 1.0561]`, within the
proposed `1.10` ceiling. Steady throughput is offered-load limited.

The clean catch-up result is **inconclusive**, not a margin pass. Its paired
throughput ratio median is `0.9792`, interval `[0.8386, 1.0291]`, crossing the
proposed `0.95` floor. Its p95 ratio median is `1.0407`, interval
`[0.9499, 1.3580]`, crossing the `1.10` ceiling. One pair has substantially worse
bounded performance; its clocks are consistent, so it remains in the assessment.
These five local pairs do not establish either equivalence or a causal regression.
The backlog workload offers about 1,000 rows/s, so high lag is expected and is not
judged against the steady two-second limit.

| Median sampled CPU, percent of one core | Without admission | One permit |
| --- | --- | --- |
| Steady destination / engine | 22.7 / 9.4 | 22.7 / 9.0 |
| Catch-up destination / engine | 39.1 / 20.4 | 39.9 / 20.4 |
| Idle destination, steady fixtures | 10.7 | 11.0 |

CPU counters include sampling overhead and describe these runs; they do not prove
equal resource cost. Memory snapshots are not peaks. The cohorts ran in separate
sessions, with complete pairs kept within each session. No engine OOM or unexpected
PostgreSQL crash appears in accepted benchmark logs. All 260,000 benchmark rows
and 4,014,400 coexistence rows were verified. Private containers/volumes are removed;
cached images remain. `make check` passes, including five timing regression tests.

## Existing large table copied while three subscriptions stream

Three databases receive 120 transactions each, with 20 rows per transaction at
500 ms intervals. A fourth copies two million existing rows with 128-byte
payloads. The streaming source continues for about one minute. Bulk dataset
preparation precedes the replication slots to isolate admission from the
independent receiver defect below. One diagnostic case per variant is not a
statistical performance guarantee.

| Metric | Without admission | One permit |
| --- | --- | --- |
| Initial-copy duration | 24.53 s | 58.97 s |
| Per-stream publication p95 | 293–311 ms | 1,602–1,829 ms |
| Largest publication gap per stream | 0.748–0.754 s | 7.78–8.18 s |
| Catch-up after last source commit | 0.130–0.134 s | 6.92–7.12 s |
| Commits per stream during copy | 49 | 103–104 |
| Engine OOM / unexpected PostgreSQL crash | None | None |
| Final Iceberg data checks | Passed | Passed |

Both variants make concurrent progress. The larger number of commits during the
bounded copy reflects its longer duration, not higher throughput. The baseline
passes all local coexistence targets. The bounded case passes the two-second
p95 target but fails the five-second maximum-gap and catch-up targets in every
stream. Source schedules stay within the offered interval.

The bounded trace contains 14 observations with all two million rows staged,
the copy still unfinished, the copy backend admitted, and all three streaming
databases waiting for its permit. These span approximately elapsed seconds
57.77–64.54. The initial-copy group finish inserts the staged relation into
Iceberg inside one transaction; its permit is held through commit. This is a
concrete cause of long publication gaps that the overall p95 can understate.

Admission also wraps initial-copy staging and other guarded operations, including
work that only changes PostgreSQL control/staging tables. This gives the simple
prototype a broad safety boundary but adds unnecessary contention for the engine
permit. The ownership validation and resource-admission policy should be separate
so tightening scheduling does not weaken the identity checks.

## Independent receiver correctness finding

The first large-copy fixture bulk-loaded the unrelated source table after the
stream subscriptions existed. A receiver then repeatedly failed with an empty
streamed transaction and stopped progressing. The same error reproduces without
admission. Moving preparation earlier isolates this measurement; it is not a
runtime fix. See the [diagnostic and reproducer](empty-filtered-stream-diagnostic.md).

The 200,000-row pilot verified its exact data and latency but provided only a
2.25-second copy interval with four commits per stream. It cannot pass the
five-commit overlap requirement. Its result is retained separately; the larger
case did not relax that requirement. An initial observer-formatting failure is
excluded from runtime/performance conclusions.

## Next execution-policy experiment

First fix and qualify empty filtered-stream commits with durable checkpoint,
acknowledgement, replay and restart tests. That receiver issue affects both
admission variants and is a release correctness follow-up.

For admission, distinguish actual engine work from PostgreSQL-only staging while
preserving all ownership checks. Then test a small bounded policy that leaves
capacity for streaming while one bulk copy runs, under the same engine limit and
the original 16-subscription restart burst. Merely raising an unreserved global
limit can still let bulk copies occupy every permit. Splitting final copy work
into smaller transactions is an alternative only if atomic publication and
failed-copy cleanup remain correct; removing that atomicity is not an evaluated
optimization.

These are candidates to test, not qualified implementations. The present permit
still coordinates one PostgreSQL instance only. Wrapper/connection growth,
many simultaneous copies, long-term fairness, amd64 and package upgrades remain
outside this evaluation.

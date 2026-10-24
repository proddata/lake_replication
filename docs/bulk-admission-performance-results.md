# Bulk-only admission: paired performance qualification

Historical prototype qualification. The policy is now implemented in the
[beta.3 production candidate](stream-pool-migration-results.md); its package
qualification is tracked separately.

The bulk-only candidate passes the local paired performance gates. Combined
with the [operational checks](bulk-admission-operations-results.md) and the
[four-copy diagnostic](engine-admission-results.md), these results support
implementing the policy in production source while retaining pg_extension_base
and unrestricted streaming concurrency. Production scheduling is still unchanged.
Packaged qualification on both architectures and the release soak remain required.

[Retained evidence](evidence/2026-10-22-bulk-performance-results.json) contains
all 30 case summaries, paired ratios, checks, input hashes, exact image/archive
identities and raw-result/log hashes. The runtime image is identical to the
corrected operational candidate, `sha256:ddf2a8b27ea9caf70f20e25a04a6da2be1263be814f151a58cee185684691268`.

## Protocol

Five complete pairs per profile alternate baseline/candidate order (AB, BA, AB,
BA, AB). Each case starts fresh disposable containers and volumes. Both variants
use the same exported beta.2 arm64 archive and the same 2 GB, one-thread engine
with 64 PostgreSQL worker slots. The candidate overlays only the experimental
bulk permit and SQL guards; streaming requests no permit.

Steady and catch-up cases each measure 120 transactions of 100 rows with 128-byte
payloads, after ten warm-up transactions and five idle seconds. The journal
observer polls every 100 ms. Offered transaction intervals are 500 ms for steady
and 100 ms for catch-up. Publication latency uses actual source/destination
commit timestamps. All warm-up and measured phases pass wall/monotonic clock
audits. Every case verifies 13,000 exact rows, unique IDs and payloads.

Single-copy cases retain a two-million-row copy while three streams each receive
120 transactions of twenty rows at 500 ms intervals. They verify exact copy and
stream contents plus the existing absolute coexistence limits. A queued bulk
waiter is not required when only one bulk job is offered; contention exclusion
is established separately by the multi-copy and operational fixtures.

The margins were fixed before execution: candidate/baseline p95 ratio at most
1.10; catch-up throughput ratio at least 0.95; single-copy duration ratio at most
1.10. The table reports the median of five whole-pair ratios and the exact
resampled median's 95% interval (3,125 samples). These intervals describe this
local cohort, not a universal hardware or workload guarantee. Inconclusive or
incomplete cohorts cannot pass.

## Results

| Gate | Median candidate/baseline | Paired 95% interval | Limit | Result |
| --- | ---: | ---: | ---: | --- |
| Steady publication p95 | 1.039 | 1.037–1.061 | ≤ 1.10 | Pass |
| Catch-up publication p95 | 1.002 | 0.988–1.009 | ≤ 1.10 | Pass |
| Catch-up publication throughput | 1.000 | 0.995–1.008 | ≥ 0.95 | Pass |
| Single-copy group duration | 0.992 | 0.976–1.038 | ≤ 1.10 | Pass |

Steady p95 ranges from 242–252 ms for baseline and 251–263 ms for the candidate.
The small measured increase is inside the proposed margin; it is not zero
overhead. Steady throughput is offered-load limited, approximately 201 rows/s
for both variants, and is not treated as a capacity gate. All steady cases pass
the two-second p95 and final-catch-up limits, backlog-at-most-four check and
source-schedule audit.

Catch-up throughput ranges from 321–326 rows/s for baseline and 322–325 rows/s
for the candidate. Its roughly 24-second p95 reflects deliberate backlog; this
profile uses relative latency and throughput gates rather than the steady
two-second limit.

Single-copy durations range from 15.36–16.23 seconds for baseline and
15.63–15.94 seconds for the candidate. All thirty live-stream summaries pass
their coexistence limits, with p95 below 289 ms. All ten copies verify two
million exact rows each. No engine OOM or unexpected PostgreSQL crash markers
were observed in any accepted run.

## Evidence and remaining work

The first attempt failed its engine health check before any measurement and is
excluded. Its cause was not established; the retry used identical image IDs and
completed all 30 cases. That startup failure and its source/log fingerprints are
retained separately. Successful workload measurements do not establish that
startup is free from intermittent failures.

Raw runs and exact tested sources remain under
`.qualification/qualification_20261022_202910_bulk_performance/` (excluded startup)
and `.qualification/qualification_20261022_203158_bulk_performance/` (accepted).
Private fixture containers and volumes from both attempts were removed.
`make check` passes after the final code changes. The retained analysis identifies
`bounded` as a legacy helper alias for this run's `bulk_only` candidate; raw cases
retain their actual variant names.

Next, introduce the proven bulk policy through a production migration, including
the transient schema-error handling fix. Qualify fresh install and upgrades,
native cancellation/recovery and both exported architectures, then run the
release-candidate soak. The policy serializes managed bulk transactions; it does
not bound staging memory, connections, source WAL retention or arbitrary
application engine SQL. These local results do not establish a capacity ceiling
or beta release readiness.

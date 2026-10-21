# Extended launcher evaluation

Status: matched measurements and stopped-state upgrade testing completed.
Production adoption remains unqualified.
See the [frozen protocol](pg-extension-base-performance-protocol.md) and the
[first evaluation](pg-extension-base-evaluation-results.md) for scope.
The experimental C launcher, Rust workers, migrations and upstream pin stay
fixed; the new work changes measurement and adds an isolated upgrade fixture.

## Observer calibration

The [poll-frequency cohort](evidence/2026-10-18-observer-poll-calibration-results.json)
used three static-baseline repetitions each at 100 ms and 1,000 ms polling.
Observer SQL wait time fell from a median 2.081 s to 0.259 s per measured phase,
while completion-rate medians were 452 and 448 rows/s. Paired differences varied
in direction. This does not establish a consistent throughput penalty, and the
one-second observer has a much wider final-visibility interval. Do not subtract
observer wait time from replication time or equate it with CPU consumption.

The [journal/visibility calibration](evidence/2026-10-18-journal-observer-calibration-results.json)
used two repetitions per observer at a 200 rows/s target. Publication-commit
p95 medians were 224 ms with Iceberg observation and 227 ms with journal
observation. Observer SQL wait time was 2.795 s versus 0.512 s. Throughput here
was limited by the offered load, so this is not a capacity improvement claim.
All 240 measured publication commit timestamps fell within the independently
recorded first-observation intervals, including every directly observed Iceberg
transaction in this calibration.

Source XIDs are matched to published journal rows. Reading
`pg_xact_commit_timestamp(xmin)` identifies the publication transaction's commit;
the journal's `published_at` value is set before that commit and is not used as
a substitute. Commit timestamp tracking is enabled on both servers. The journal
count-to-marker mapping is specific to the fresh single-publication fixture.
Final Iceberg data is checked independently, and journal polling is never
reported as direct Iceberg query visibility.

## Extended matched cohorts

The protocol separates approximately one minute at 200 offered rows/s from a
1,000 rows/s backlog/catch-up workload. Each uses five fresh static/base pairs,
100 rows per transaction, a separate ten-transaction warm-up and 100 ms journal
polling. Analysis uses exact resampling of whole pairs, keeps absolute
latency/backlog checks separate from relative comparisons, and records the
analysis input/source hashes.

The [steady-load cohort](evidence/2026-10-18-launcher-steady-results.json)
completed all ten fixtures and matches the frozen protocol. All absolute checks
passed: p95 publication-commit lag below two seconds, publication catch-up within
two seconds, no more than four transactions unobserved at end of source writes,
and source scheduling lateness within one offered interval.

| Metric | Static, median (range) | Base, median (range) |
| --- | --- | --- |
| Publication-commit p95 | 274.6 ms (257.0–282.7) | 285.1 ms (247.6–290.9) |
| Completion throughput at 200 offered rows/s | 201.19 rows/s (200.87–201.35) | 201.11 rows/s (201.03–201.28) |

The finite schedule starts its first transaction immediately, so completion
rates can slightly exceed the nominal offered rate. These figures do not
measure maximum capacity. The largest end-of-write backlog upper bound was one
transaction; the largest publication catch-up was 0.226 seconds. All 130,000
destination rows passed count, distinct-ID and marker/payload predicate checks.

Median paired p95 ratio was 1.0423 (base/static). Its exact paired-bootstrap
95% interval was 0.9348–1.1095. This narrowly crosses the proposed 1.10 margin:
the relative latency result is **inconclusive**, despite passing the absolute
local latency/backlog limits. Do not round the interval into a pass or move the
margin after seeing the data.

The [backlog/catch-up cohort](evidence/2026-10-18-launcher-catchup-results.json)
also completed ten fixtures and matches the protocol. All 130,000 destination
rows passed the generated-data checks. The offered rate exceeded completion
capacity, so accumulated latency is expected and the two-second steady-load
limit does not apply to this cohort.

| Metric | Static, median (range) | Base, median (range) |
| --- | --- | --- |
| Publication completion throughput | 453.1 rows/s (423.4–472.8) | 473.0 rows/s (462.3–473.7) |
| Publication-commit p95 under backlog | 14.00 s (12.88–15.84) | 12.89 s (12.85–13.41) |

Median paired throughput ratio was 1.0450, with a 95% paired-bootstrap interval
of 1.0020–1.0918. The cohort meets the proposed no-more-than-5%-loss margin.
The p95 ratio was 0.9203 with interval 0.8468–0.9979, also within the proposed
relative margin. This supports absence of a throughput regression for this
fixture; do not generalize a small local advantage into a framework speedup
claim or sustained capacity guarantee. The steady-load latency comparison
remains inconclusive and is not superseded by this different workload.

The fixture inventory also records an unrelated running PostgreSQL container.
It is left untouched; these are matched local observations, not claims from an
exclusive production-capacity test host.

## Stopped-state migration

The separate upgrade-only fixture prepares a real packaged static installation,
drains its subscription and external processes, stops PostgreSQL and pgduck,
then replaces the image with the same hardened base-worker implementation.
The experimental target `0.1.0-beta.2-prototype` adds the SQL entry point and
registration through an actual extension UPDATE script.

The [upgrade run](evidence/2026-10-18-static-to-base-upgrade-results.json) passed
42 checks. It starts from the real release archive, verifies that the original
static launcher is running, drains the old subscription and external executable
processes, and confirms no accidental postmaster restart during the drain.
After replacement, SQL metadata stays on the old version with no static/base
owner dispatch until the extension UPDATE is committed. A rolled-back UPDATE
leaves the old SQL version and no registration or dispatch; the committed retry
adds exactly one base owner and its direct dependency on pg_extension_base.

The old explicit preload, session preload and database-name settings remain
configured. The subscription stays stopped until explicit enable, keeps its
checkpoint/source slot and completed copy, publishes changes made during the
handoff, and streams after another server restart without repeating the copy.
The test also verifies unique rows and subsequent source-slot cleanup.

For this tested restart contract, exclusion comes from draining and stopping
all old processes, then using a new binary that disables static registration.
This does not establish a mixed/live-owner interlock or recovery by reverting
native binaries against newer SQL metadata.
This is a stopped-server transition, not a live handoff, fresh-install package,
SQL downgrade, or clone/restore identity policy.

## Decision scope

The final `make check` passed, including six measurement/decision tests.
All disposable evaluation containers and volumes were confirmed removed.
Recorded analyses reproduce from the retained cases/parameters, and source,
protocol and analyzer fingerprints were verified. A final
[two-variant runner smoke test](evidence/2026-10-18-measurement-validation-smoke-results.json)
also passed after tightening the post-measurement predicate to reject NULL
markers/payloads. The matched cohorts retain their exact pre-tightening runner
snapshots; the timed workload and experimental C runtime did not change.

Performance evidence cannot clear database identity, physical takeover,
forced-crash cleanup, multi-database/subscription capacity, mixed mutations,
amd64 or the 24-hour soak. The shipped runtime remains unchanged and its
existing correctness release blockers remain open.

Recommendation: continue with pg_extension_base as the candidate foundation.
The stopped-state handoff is feasible in the tested fixture and catch-up shows
no throughput loss. Keep the steady-load relative latency comparison open;
its interval narrowly misses the frozen margin. Next evaluate forced-crash
ownership/cleanup and clone/restore identity, then scale and architecture/soak
gates before promoting the prototype to shipped code.

## Subsequent crash qualification

The [forced-crash evaluation](pg-extension-base-fault-results.md) found failures
outside the successful graceful lifecycle and stopped-state upgrade tests.
A replacement can permanently fence itself on transient source-slot contention
after a launcher/wrapper crash. The current recommendation is **revise and
retest**, not production adoption; the earlier performance evidence is unchanged.

The subsequent [bounded-recovery revision](pg-extension-base-recovery-results.md)
passes the failed crash regressions with Docker init and refuses persistent
slot contention. Production adoption, other ownership topologies and performance
of the revised supervisor remain unqualified.

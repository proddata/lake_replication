# Launcher evaluation: first implementation and screening

Status: evaluation in progress; production adoption is not qualified.
The [extended evaluation](pg-extension-base-extended-results.md) now covers
longer matched runs and the real packaged-static to base-worker upgrade fixture.

The experimental static and base-worker variants now share the same hardening:
handle allocations outlive SPI, retired handles are freed, worker exit
notifications are enabled, tracked workers are drained, and Rust descendants
have explicit process-group/subreaper cleanup. Steady-state waits use SIGCHLD
and PostgreSQL latches; only bounded shutdown retains 10 ms polling. Binding
errors update subscription rows only when their diagnostic state changes.
Production sources, release packaging, and the data path remain unchanged.

The expanded functional runs passed
[111 base-worker checks](evidence/2026-10-18-base-event-wait-results.json) and
[81 applicable static-baseline checks](evidence/2026-10-18-static-shared-hardening-results.json).
Both include a paused supervisor disable with explicit cleanup and unchanged
postmaster start time, followed by ten enable/disable cycles. In both variants,
the named handle context returned to exactly 240 used bytes within its 8,192-byte
allocation after retirement, matching the baseline. This checks handle retention
across those cycles, not whole-process longevity. The base run also confirms
unchanged binding errors do not rewrite subscription rows and passes the
multi-database/extension lifecycle cases. Production release blockers remain.
The final `make check` passed, evidence fingerprints were verified against
their retained sources, and disposable evaluation containers/volumes were
confirmed removed.

## Initial A/B screening

[Raw retained evidence](evidence/2026-10-18-launcher-screening-results.json)
contains five alternating repetitions per variant, fresh fixtures each time,
with pinned upstream/release sources, image IDs, and host information. The
ignored run directory also retains matching source snapshots and build/log
files: `.qualification/qualification_20261018_200202_comparison/`.

Each repetition used one destination and one subscription, 40 transactions of
100 inserts offered every 100 ms, an equal warm-up, and ten seconds of idle
observation. Both phases used the actual supervisor/receiver/publisher. Every
fixture ended with 8,000 rows and distinct IDs as expected.

| Metric | Static, median (range) | Base, median (range) |
| --- | --- | --- |
| Rows/s through final visibility | 444 (385–450) | 399 (343–455) |
| p95 observed commit lag | 4.74 s (4.70–5.94) | 5.76 s (4.62–7.36) |
| Destination idle CPU, percent of one core | 10.30% (6.18–11.28) | 11.07% (8.45–12.00) |
| Stable wrapper voluntary context switches/s while idle | 0 | 0 |

Base medians were approximately 10.2% lower for throughput and 21.4% higher for
p95 lag. This is a performance concern to investigate, not evidence that the
framework itself causes those differences. Ranges overlap substantially and
the workload is short. The median CPU difference is 0.77 percentage points of
one core; this includes real idle supervisor cycles and sampling overhead.
Zero wrapper context switches support removal of periodic wrapper polling;
they do not imply that the entire replication service consumes no idle CPU.

Actual source transaction commit timestamps come from
`pg_xact_commit_timestamp`; visibility uses the first destination polling query
to observe a transaction. Both servers share the Docker VM's wall clock. Polling
interval and query time add observation delay. Throughput includes catch-up,
so these results must not be quoted as sustained capacity. Memory counters are
endpoint snapshots, not peaks. The first sampler misses the base launcher's
process title, so this run makes no per-launcher PSS claim; subsequent sampling
identifies its PID from the registration view.

The proposed adoption thresholds are not satisfied by this screening evidence.
Do not waive the differences because correctness passed, or reject the framework
on these short noisy runs. Repeat the screening, then calibrate longer workloads
and observer overhead if uncertainty remains. A control with the same explicit
preload setting does not isolate library loading: source inspection and startup
logs show that the base control-file annotation already preloads the library
at server startup. Do not attribute cohort differences to dynamic connection-time
loading or to the presence of the explicit setting.

## Repeatability control

The [second cohort](evidence/2026-10-18-launcher-repeatability-results.json)
used three alternating repetitions per variant with `--base-preload`. It kept
the same workload and shared C implementation, added worker logs and explicit
base-launcher PID sampling, and retained both resource counters and source
snapshots. Every fixture again had the expected 8,000 distinct rows.

| Metric | Static, median (range) | Base, median (range) |
| --- | --- | --- |
| Rows/s through final visibility | 437 (434–441) | 447 (435–453) |
| p95 observed commit lag | 4.93 s (4.90–5.00) | 4.79 s (4.67–4.90) |
| Destination idle CPU, percent of one core | 8.85% (8.51–11.20) | 10.41% (8.15–11.77) |
| Stable wrapper voluntary context switches/s while idle | 0 | 0 |

Here base throughput was approximately 2.5% higher and p95 lag 2.9% lower.
This reversal means the initial slowdown is not yet established as repeatable
framework overhead. Three short pairs also do not establish equivalence or a
performance improvement. Automatic startup preloading occurs in both base
configurations; the explicit-setting change cannot establish a loading cause.
Do not pool the cohorts into a production performance guarantee.

Next measurement work should calibrate longer durations and the observer's cost,
then compare steady offered load below capacity and a separate saturation case.
Freeze the absolute latency budget and workload before applying acceptance
thresholds. Keep identity/upgrade/failure gates open independently of performance.

## Remaining decision gates

The [operational contract](pg-extension-base-operational-contract.md) proposes
conservative rename/clone/restore behavior and a stopped-state upgrade with an
exclusive-owner interlock. Those behaviors are not implemented or tested by the
new measurements. Database names participate in pinned pg_lake catalog identity,
so a connection-name rewrite alone does not qualify rename safety.

Multi-database/subscription capacity, mixed mutations, production static-to-base
packaging/recovery beyond the tested stopped-state prototype, SIGKILL/postmaster crash recovery, namespaced logs/worker
names, amd64, and a 24-hour soak remain open. Existing production release
blockers remain in force. Follow the [evaluation plan](pg-extension-base-evaluation-plan.md)
before making the production switch.

# Beta.5 latency gate investigation

The patched archive at `820f402` fails the valid streaming latency gate in
[native run 41289535953](https://github.com/proddata/lake_replication/actions/runs/41289535953).
This supersedes the earlier pre-patch pass as evidence of the current candidate's
release readiness. It does not invalidate the historical result.

| Architecture | Four-copy completion | Streaming p95, three databases |
|---|---:|---|
| ARM64 | 133.22 s | 2.040 / 2.088 / 2.133 s |
| AMD64 | 162.47 s | 2.132 / 2.102 / 2.250 s |

Both measurements are valid: source scheduling, clock validation, concurrent
publication, admission queue observation, exact final data, catch-up, maximum
publication gap, and the 180-second copy deadline pass. Neither run has an OOM
or unexpected PostgreSQL crash. Nine other admission cases pass, alongside the
333 admission assertions. Earlier steps pass storage preflight (2 checks),
fresh lifecycle (119), verified TLS (19), fault handling (17), receiver
regressions (96), and cross-version physical recovery (30). The later beta.1
handoff step is skipped after admission fails; this is not a complete native
qualification pass.

Latency accumulates mainly between journal insertion and batching, then between
batching and publication. The measurements do not establish a single cause.
The C extension and upstream base image are unchanged between the successful
pre-patch and failed patched native runs, but they ran on different hosts.
The rustls security update remains required.

## Fixed local comparisons

All local end-to-end comparisons use one shared CPU on the Mac, the committed
original upstream build recipe, fixed control/candidate/control order, and
unchanged workloads and gates. They are diagnostics, not substitutes for native
qualification. No retries or best-run selection are used.

| Sequence | Copy completion, in sequence | Streaming p95 range, in sequence |
|---|---|---|
| Pre-patch / patched / pre-patch archives | 123.72 / 124.19 / 129.71 s | 1.136–1.189 / 1.217–1.254 / 1.460–1.521 s |
| Patched archive, 40 / 50 / 40 ms pauses | 127.12 / 142.69 / 122.70 s | 1.499–1.590 / 1.085–1.217 / 1.130–1.205 s |
| Patched archive, PGLZ / LZ4 / PGLZ, all 40 ms | 119.43 / 117.62 / 120.81 s | 0.730–0.814 / 0.734–0.757 / 0.842–0.917 s |

Every run passes. The patched archive lies between the pre-patch controls;
there is no consistent local evidence attributing the native failure to the
security update. Increasing the pause to 50 ms makes copying 14.24% slower than
the control mean, while latency overlaps the later control. This does not
justify changing the production default.

An exploratory append API microbenchmark uses the patched archive, 2,000 chunks
of 1,000 rows, and PGLZ/LZ4/PGLZ order. It takes 1.321 / 0.644 / 1.272 seconds;
the LZ4 relation uses 13,123,584 bytes versus 11,763,712 bytes for PGLZ. This
single-transaction loop excludes worker RPC, pacing, and publication costs.
It motivates an end-to-end codec comparison but cannot establish its benefit.

The complete codec comparison also passes every validity, exact-data, safety,
and performance gate. LZ4 is 2.08% faster than the control mean, but its latency
overlaps the first control. This small local result does not establish enough
headroom to resolve the native failure. Keep the current pacing and compression
defaults; profile batching and publication costs before another full native run.

## Reusable diagnostic

`tests/admission/copy_configuration_compare.py --mode toast` fixes the order to
PGLZ/LZ4/PGLZ while keeping the 40 ms pause. `--mode pacing` fixes 40/50/40 ms.
Both preserve the exported archive, executable image, harness inputs, workload,
and every gate. Each trial records the server configuration and actual worker
pacing; codec trials also verify the compression method of stored chunks in
all four copy databases. No SQL schema/function or executable overlay is used.

The workflow exposes this as an explicit native diagnostic. All controls remain
in the report, and any failed control keeps the workflow red. A passing
candidate is not qualification of the archived defaults. Native comparison,
an implemented and documented configuration decision, full qualification, and
the release soak remain separate steps. No production default has changed.

`tests/admission/sql_profile_probe.py --cpuset 0` separately runs the exported
archive with the existing `pg_stat_statements` profiler. It installs only the
profiling extension and enables nested statement/planning and I/O timing.
`sql-profile.json` retains per-database statements. Profiling adds overhead;
nested inclusive execution times must not be summed or treated as CPU time.
These timings cannot qualify the uninstrumented archive.

The profiler also enables duration-only server logs and summarizes all seven
worker databases. PostgreSQL's simple-query path logs duration after transaction
finalization; the `pg_stat_statements` COMMIT utility timer does not cover that
whole boundary. See the [PostgreSQL 18 source](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c)
and [duration logging documentation](https://www.postgresql.org/docs/18/runtime-config-logging.html#GUC-LOG-DURATION).
These complete message durations include parsing, fences and transaction work;
they do not identify an individual statement or measure CPU time. SQL text is
not enabled in the duration logs. Retained logs can be summarized again with
`sql_profile_probe.py --analyze-log PATH`.

## Profile observations

Three instrumented local workloads pass exact data, safety and workload gates,
with copy completion of 119.46, 120.46 and 120.28 seconds. These are exploratory
profiles with different instrumentation, not a performance comparison.
Publication SELECTs average about 37–39 ms in the utility-timing profile, with
maximum times of 79–97 ms across the three streams. The final duration-log
profile records worker message p95 of 411–418 ms and maxima of 826–851 ms in
those databases. Message statistics mix several operations and include setup;
these are not matched per-query populations and must not be subtracted to
estimate commit cost. The gap motivates tracing whole publication transactions,
including finalization and queueing, before changing resource settings again.
It does not establish a single cause of the native failure.

The duration run's original postprocessor rejects the log prefix because it
expects a space before `LOG`. Its workload passes, but its original runner
status remains `error` in the evidence. The corrected parser reads the same
retained log successfully and finds all seven databases; the workload was not
rerun to repair the parser. Raw-result and log hashes retain that distinction.

[Machine-readable evidence](evidence/2026-11-01-beta5-latency-investigation.json)
retains archive identities, raw-result hashes, failed gates, local controls,
and the limits of each comparison.

# Beta.5 publication transaction tracing

The native latency failure remains open. This work implements the first step of
the next-step plan: matched observations of complete publication transactions.
It changes no admission, pacing, compression, or publication defaults.

## Local findings

Two instrumented four-copy workloads on one shared local CPU pass all measured
validity, exact-data, safety and performance checks. Both use locally built
dirty diagnostic archives based on `8968a7e`, the committed original upstream
recipe, and pinned pg_lake `8180647d4535`. These are not release artifacts or
native qualification results.

The first run preserves the single-request transaction shape. All 352 successful
publications match server durations by database, backend PID and complete
connection message sequence, and match their emitted batch ID and published
LSN. Per-stream mean client elapsed time is 293–307 ms, mean server duration is
292–306 ms, and mean residual is 0.68–0.75 ms. Publisher connection establishment
averages 8–9 ms. Publisher startup before connecting averages less than 1 ms.
Connection/startup measurements count each connection once, rather than once
for every batch it publishes.

The second run splits transactions into separate begin, fence, statement and
commit requests. It matches all 348 successful publications with no correlation
errors. All 244 publisher spawns have a recorded connection. Mean server time:

| Phase | analytics | fair_stream_1 | fair_stream_2 |
|---|---:|---:|---:|
| Begin | 0.01 ms | <0.01 ms | 0.03 ms |
| Generation fence | 0.21 ms | 0.34 ms | 0.19 ms |
| Publication statement | 37.68 ms | 38.45 ms | 39.91 ms |
| Commit/finalization | 280.66 ms | 281.21 ms | 278.43 ms |

Commit p95 is 563–570 ms. These are complete server request durations, not
`pg_stat_statements` COMMIT utility counters. The split diagnostic adds round
trips and can perturb scheduling and lock durations. It identifies the largest
measured local phase; it is not a speed comparison against the first run and
does not prove the cause of the failed native observations.

This evidence supports inspecting commit/finalization before optimizing process
startup or generation fences. In pinned upstream code,
`pg_lake_table/src/transaction/transaction_hooks.c` invokes
`ConsumeTrackedIcebergMetadataChanges` during pre-commit. That path applies
Iceberg metadata changes and calls `FinishAllUploads`. Other transaction work,
including PostgreSQL durability, is also included in the measured commit phase;
the current trace does not attribute time to individual callbacks or uploads.

[Machine-readable evidence](evidence/2026-11-01-beta5-transaction-trace.json)
retains archive identities, runtime/harness input hashes, wrapper provenance,
raw log/result hashes, matched counts and per-database summaries. Full per-batch
matches and source snapshots remain in the referenced local qualification
directories. Publication batches can combine source transactions; batch counts
are not source transaction counts.

The final diagnostic archive also passes the ordinary pipeline, managed-worker
and fairness cases with all tracing and profiling disabled: 166 assertions,
121.27-second four-copy completion, and streaming p95 0.879–0.924 seconds.
This verifies the local default path; it does not replace native qualification.

## Native AMD64 confirmation

[Native diagnostic 41324056257](https://github.com/proddata/lake_replication/actions/runs/41324056257)
uses the clean `fac7079` archive from
[CI 41323582724](https://github.com/proddata/lake_replication/actions/runs/41323582724).
CI, CodeQL and both archive security audits pass. The native diagnostic remains
red: analytics streaming p95 is 2.007067 seconds, exceeding the unchanged
two-second gate. The other streams reach 1.927332 and 1.918589 seconds. Copies
finish in 144.12 seconds; exact data, timing validation, concurrent publication,
queue observation, catch-up and maximum-gap checks pass. No OOM or unexpected
PostgreSQL crash occurs.

All 265 successful publications match, with no correlation errors. All 94
publisher spawns have a recorded connection. Mean server phase durations:

| Phase | analytics | fair_stream_1 | fair_stream_2 |
|---|---:|---:|---:|
| Begin | 0.01 ms | 0.01 ms | 0.01 ms |
| Generation fence | 0.26 ms | 0.28 ms | 0.23 ms |
| Publication statement | 60.55 ms | 62.21 ms | 56.55 ms |
| Commit/finalization | 513.73 ms | 517.77 ms | 518.94 ms |

Commit p95 is 988–1055 ms. Outer publisher connections average 9–10 ms;
startup before connecting averages 1.4–2.3 ms. Commit dominates this measured
native path as well. It remains necessary to separate metadata preparation,
upload completion, internal engine connection work and PostgreSQL durability.
Outer connection timings do not measure connections created inside pg_lake.
The split instrumentation perturbs execution, so this run cannot qualify the
normal transaction path or establish a causal regression against an older VM.

## Reproduce and interpret

Build the archive from an isolated checkout, then set `QUALIFICATION_ARCHIVE`
to that archive and run:

```sh
python3 tests/admission/sql_profile_probe.py --cpuset 0 --trace-workers
python3 tests/admission/sql_profile_probe.py --cpuset 0 --trace-workers --trace-phases
```

`--trace-workers` adds a supervisor launcher wrapper that exports
`LAKE_TRACE_TRANSACTIONS=1`; its descendants inherit the flag. The wrapper
preserves and hashes the archived supervisor executable. The trace records
worker role, connection sequence, backend PID, numeric subscription/generation,
operation label, status and elapsed time. It does not log SQL text, credentials,
row payloads or PostgreSQL cancellation secrets. COPY and replication message
streams are not timed; the matcher covers publisher simple-query sessions.

`--trace-phases` additionally enables `LAKE_TRACE_TRANSACTION_PHASES=1`.
Transactions retain their admission and generation fences and return results
only after successful commit. Synchronized server errors still roll back;
protocol failures still poison/close the connection. Session-lease acquisition
followed by a failed fence still closes the session. Both flags are off by
default, and the ordinary transaction stays one wire request.

The matcher rejects missing/duplicate messages, inconsistent identities,
unmatched batch events, incomplete phase groups and impossible enclosing
timings. Orphan publisher spawns remain visible in coverage. Residual elapsed
time includes transport and scheduling; it is not a CPU measurement. Startup
estimates use millisecond wall-clock timestamps and are not precise CPU times.

The native workflow exposes `transaction_trace` and
`transaction_trace_phases` through its existing diagnostic-choice input. Use
`native_probe=true` and an explicit architecture. These jobs keep the workload
gates and retain failures; neither qualifies production defaults.

## Next decisions

1. Completed locally: verify the new archive with tracing disabled, including
   pipeline, managed-worker safety and the unchanged four-copy workload.
2. Completed: short matched AMD64 phase diagnostic from clean CI artifacts.
   Commit dominates; the valid latency miss is retained.
3. Instrument commit metadata preparation, internal engine connection work,
   file uploads and durability waits separately. Preserve the pinned upstream source
   as the control and clearly label any diagnostic instrumentation.
4. Implement one evidence-backed change, then use fixed local control/candidate/
   control runs and focused native tests on both architectures. Retain the
   two-second streaming and 180-second copy gates and seek repeatable headroom.
5. Repeat full archive qualification, including the skipped beta.1 handoff,
   before the 24-hour soak and signed prerelease verification.

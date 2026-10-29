# Follow-up native failures and targeted diagnostics

[Native run 40932877134](https://github.com/proddata/lake_replication/actions/runs/40932877134)
fails at `a817ce9`; its build CI passes.

- ARM64 passes all managed checks with zero heartbeat lock errors. Copies
  complete in 156.46 seconds and exact data checks pass. Streaming p95 remains
  2.161–2.718 seconds, above the unchanged two-second limit.
- AMD64's SDK cleanup increases free disk from 13 to 30 GiB. Its cold upstream
  build times out after 60 minutes while still compiling JSON/ICU loadable
  extensions, around 73% of the default build. Replication does not start.

The heartbeat correction addresses the reproduced lock conflict. It does not
close the native performance gate. CPU pressure is substantial in the retained
ARM64 samples, and no streaming-permit waiters are observed. Those samples do
not identify the remaining latency's dominant phase.

## Latency instrumentation

The fairness harness now collects existing transaction/batch timestamps after
the measured workload, without additional hot-path queries. It preserves the
existing source-to-publication lag and adds:

1. Source commit timestamp → journal row insertion (`staged_at`).
2. Journal insertion → publication batch creation.
3. Batch creation → publication commit timestamp.

`staged_at` is an insertion marker, not a durable staging commit timestamp.
Consequently, the middle interval also includes staging commit time. Phase
durations sum to end-to-end lag for each transaction; independently computed
phase percentiles must not be added together. Out-of-order markers invalidate
the diagnostic rather than producing misleading negative durations.

The local two-CPU run passes all unchanged workload gates: 71.66-second copies,
0.437–0.490-second publication p95, exact rows and four-copy overlap. This tests
the instrumentation; it does not reproduce or resolve native latency. Three
new timing tests pass alongside the existing twenty admission unit tests.

Focused probes can select `probe_arch=arm64` or `probe_arch=amd64`. Full
qualification retains both architectures. This permits an ARM64 timing
diagnostic without repeating the unrelated AMD64 prerequisite build.

## Prerequisite build work

The pinned shared-library target links all required static extensions. Its
dependency graph does not require the duplicate loadable-extension artifacts.
A local container check deletes those artifacts and the shared library, then
rebuilds the `duckdb` target. It reproduces a byte-identical `libduckdb.so`
without recreating any of the sixteen loadable artifacts. Required static
dependencies are retained; an initial probe incorrectly assumed the core
static archive was unnecessary and was corrected after dependency inspection.

The build recipe optimization is prepared separately from the latency
diagnostic because changing the recipe invalidates the prerequisite cache.
The first targeted ARM64 diagnostic should retain the existing recipe/cache;
the shared-target recipe can then be checked as a separate build change.
Its cold native build duration remains unverified.

[Machine-readable evidence](evidence/2026-10-29-native-followup-diagnostics.json)
retains native findings, local timestamp measurements, input hashes and the
shared-library relink check. No additional runtime policy or latency threshold
is changed by this diagnostic work.

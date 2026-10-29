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

## Targeted timing result and checkpoint coupling

[ARM64 diagnostic 40951067352](https://github.com/proddata/lake_replication/actions/runs/40951067352)
at `9e21a4a` restores the cached prerequisite and completes the probe. Managed
checks and exact data pass, copies finish in 158.18 seconds with four-way
publication overlap, and neither engine OOM nor a PostgreSQL crash occurs.
Streaming p95 is still 2.579–2.945 seconds; the release gate remains failed.

| Stream | End-to-end p95 | Source → insertion p95 | Insertion → batch p95 | Batch → commit p95 |
|---|---:|---:|---:|---:|
| analytics | 2579 ms | 1585 ms | 484 ms | 845 ms |
| fair_stream_1 | 2945 ms | 1722 ms | 856 ms | 876 ms |
| fair_stream_2 | 2772 ms | 1696 ms | 834 ms | 954 ms |

These column percentiles are independent and must not be summed. The
pre-insertion interval includes source delivery/decoding, staging SQL and
backlog caused by acknowledgement of earlier transactions. Destination CPU
pressure remains high, and sampled transaction-ID waits are present, but the
existing samples do not identify blocking PIDs or queries.

Source inspection identifies a concrete coupling: `prepare_batch` locks the
subscription's `checkpoints` row before engine publication and retains it
until commit. `stage_transaction`, journal admission and `mark_acknowledged`
also lock that row. The receiver waits synchronously for acknowledgement
recording before consuming its next source message. Publication can therefore
stall ingestion even when journal capacity is available.

The artifact-only local diagnostic reproduces this without relying on machine
speed or an engine slowdown:

```sh
QUALIFICATION_ARCHIVE=/path/to/beta.4-arm64.tar.gz \
  python3 tests/admission/probe.py --cases checkpoint
```

It prepares a real batch in an open transaction, verifies spare journal
capacity, observes staging and acknowledgement lock timeouts, rolls back the
publication transaction, and verifies both operations resume with the original
staged transaction preserved. All seven diagnostic checks pass. This proves
the lock dependency, not its exact share of native latency. The diagnostic is
opt-in and is excluded from default release qualification.

Next: locally prototype separate ingress/acknowledgement and publication
checkpoint writes. Preserve atomic publication plus its progress marker,
durable staging before source feedback, monotonic frontiers, serialized
publisher/replay behavior, journal bounds and generation fencing. First prove
staging can progress while publication is held open below capacity, and blocks
or retries safely at capacity. Then compare the unchanged two-CPU workload
with the baseline. Do not move the publication marker into a separate commit
or loosen the latency gate to hide this coupling.

[Checkpoint evidence](evidence/2026-10-29-checkpoint-coupling.json) retains the
native phase summaries and local archive/input hashes and assertions. The
local prerequisite image was reused; the pending Dockerfile optimization was
not rebuilt or qualified by this diagnostic.

The [local split-checkpoint prototype](checkpoint-split-prototype.md) now
removes this lock dependency in deterministic checks and passes managed and
one-/two-CPU workload checks. It sharply reduces pre-insertion delay locally,
but some waiting shifts into the journal, so end-to-end gains are modest.
The native overlay diagnostic subsequently passes stream latency but fails copy/source-scheduling gates; see the [retained native result](checkpoint-split-prototype.md#native-result-latency-improves-qualification-still-fails). Production migration remains deferred pending a matched runner comparison.

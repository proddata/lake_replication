# Native qualification failures

[Packaged run 40175931014](https://github.com/proddata/lake_replication/actions/runs/40175931014)
qualified the native CI archives at `8bcd5b218da761ed737f0fcb2b82e9487566ccf5`.
Both architectures passed source-storage preflight, 119 fresh lifecycle checks,
17 forced-child-crash checks, and 96 receiver checks. The run failed and does
not qualify the candidate for release. [Retained summary](evidence/2026-10-25-native-qualification-failures.json)
records archive evidence hashes and the failed metrics; full raw results are in
the run's `lifecycle-qualification-arm64` and `lifecycle-qualification-amd64`
artifacts (30-day retention).

ARM64 passed admission locks, pool cleanup/progress, schema, rebuild,
cancellation, beta.2 upgrade and sixteen-stream live/restart cases. The
four-copy fairness case completed all exact data checks without an observed
engine OOM or PostgreSQL crash. Copies completed in 173.32 seconds, within the
180-second limit. Streaming p95 lag was 91.02–96.16 seconds against a two-second
limit, and final catch-up was 91.82–96.85 seconds against a five-second limit.
Source scheduling also exceeded its half-second tolerance. The wall/monotonic
clock audit passed. The first eleven transactions in each stream published before bulk copying with
95–357 ms lag; lag then increased during copying. The cause of the throughput
shortfall remains unconfirmed.

AMD64 failed while building the historical beta.2 fixture: dpkg and Docker
reported `No space left on device`. Its admission and beta.1 upgrade cases did
not run. The workflow now prunes Docker build cache after the source-storage
build and before each historical-package build, recording disk usage before
and after cleanup. It preserves tagged prerequisite and fixture images and
volumes. The extra buildx builder previously introduced before beta.1 is
removed; both historical builds use the same Docker builder. The disk fix still
requires a complete native workflow run to verify it.

The diagnostic workflow input `fairness_diagnostic=true` runs one copy, four
copies, then one copy, all against the same candidate archive and independently
initialized volumes. It attempts all three cases even if an earlier case fails,
then fails the job if any case fails. It skips upgrade and other qualification
lanes and cannot substitute for a full qualification pass. Each copy retains
two million rows, three live streams and the original timing limits; the
single-copy control does not require a bulk waiter or four overlapping copies.
This comparison tests sensitivity to copy contention; it does not establish
whether the streaming pool itself regresses performance.

The resource sampler excludes its own `python3 -c` process, whose embedded
source previously matched the supervisor substring. It now identifies engine
and PostgreSQL processes and records CPU throttling, affinity, cgroup limits,
pressure and free space. The previous cgroup totals remain valid; the old
per-process wrapper entry can include the sampler. Fairness cases now collect
resource snapshots approximately every ten seconds, source/storage counters,
and the production streaming admission view. These diagnostic reads can add
measurement overhead; compare cases with the same instrumented harness.

Reproduce a control locally with an explicit CI archive:

```sh
QUALIFICATION_ARCHIVE=/absolute/path/to/candidate.tar.gz \
  python3 tests/admission/probe.py --cases fairness --fairness-copies 1
```

Use `--fairness-copies 4` for the unchanged release workload. Resolve the cause
before changing runtime admission policy or claiming a performance pass.

## Local constrained-CPU diagnostic

The unchanged `8bcd5b2` ARM64 CI archive was also exercised using the instrumented
`02e34ff` harness, with source, destination, engine and storage containers sharing
CPU affinity `0-3`. The host observer was outside that affinity restriction.
Both runs used fresh volumes and identical harness inputs. The order was four
copies followed by one copy; these are single observations, not a paired
statistical qualification. [Evidence and exact fixture driver](evidence/2026-10-25-local-copy-contention.json)
retain input hashes and activity/pressure summaries.

| Simultaneous copies | Copy duration | Streaming p95 range | Result |
|---|---:|---:|---|
| Four | 44.46 s | 1,521–2,005 ms | Finding: one stream exceeded the unchanged 2,000 ms limit |
| One | 16.05 s | 250–265 ms | Passed |

All exact data checks and timing audits passed. Neither run observed an engine
OOM, PostgreSQL crash or streaming-admission waiter. The four-copy activity
samples included concurrent staging calls waiting on WAL initialization,
WAL writes and data-file writes; destination CPU pressure increased during the
run. This supports investigating contention outside engine admission. It does
not establish which resource caused the much larger native-run slowdown or
prove that serializing staging is the right fix.

At harness revision `02e34ff`, [CI 40198268702](https://github.com/proddata/lake_replication/actions/runs/40198268702)
passed checks and both native bundle builds. The
[native one/four/one diagnostic](https://github.com/proddata/lake_replication/actions/runs/40198461763)
and [full native qualification rerun](https://github.com/proddata/lake_replication/actions/runs/40198597757)
were dispatched against those bundles. Both runs subsequently failed. See the [completed native results and local
investigation](local-copy-contention-investigation.md): native single-copy
controls also failed, and a local two-CPU four-copy workload reproduces a backlog.
Staging pacing passes that local case at the cost of longer copy time.
Production runtime and timing limits remain unchanged.

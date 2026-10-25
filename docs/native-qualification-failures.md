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

# Local investigation of native copy contention

The native failures are reproducible as a streaming backlog in a local,
CPU-constrained fixture. Pacing initial-copy staging eliminates that backlog in
the measured local case while extending copy time. This is a diagnostic result,
not a production resource policy or release approval. No new GitHub workflow
was dispatched for this investigation. [Retained evidence](evidence/2026-10-27-local-contention-investigation.json)
contains case metrics, clock audits, resource summaries, raw-result hashes,
source fingerprints and diagnostic drivers.

## Completed native runs

The [one/four/one comparison](https://github.com/proddata/lake_replication/actions/runs/40198461763)
failed on both architectures. Both runners exposed CPU affinity `0-1` and
substantial CPU and I/O pressure. ARM64's first one-copy control had p95 lag of
38.45–40.87 seconds, the four-copy case 87.48–91.57 seconds, and the final control
36.44–37.62 seconds. The final ARM64 control also failed the observer timing
audit, so its metrics must not be treated as valid qualification evidence.
AMD64's first control reached 67.06–68.98 seconds, its four-copy case exceeded
the observation deadline, and its final control reached 58.48–60.25 seconds.
Completed cases passed their exact data checks, with no observed engine OOM or
PostgreSQL crash. These results do not establish that two CPUs alone explain
the native performance: the local one-copy control below is much faster.

The [full rerun](https://github.com/proddata/lake_replication/actions/runs/40198597757)
failed too. ARM64 passed admission locks, pool, schema, rebuild, cancellation,
beta.2 upgrade and sixteen-stream scale cases; its four-copy case reached
97.72–99.63 seconds p95 lag. The beta.2 fixture build succeeded, verifying that
the original disk failure did not recur through that build on ARM64. AMD64
passed fresh lifecycle and forced-child-crash lanes, then hung in the interrupted
receiver test before reaching beta.2. Its disk fix is not fully verified.
The separate AMD64 performance fixture had only 4.5 GiB free at the start and
1.2 GiB at the end of the timed-out four-copy case, another resource constraint
that needs attention before any future remote qualification.

## Local two-CPU reproduction

Source, destination PostgreSQL, pgduck and storage share affinity `0-1` in the
local Linux Docker VM. The observer runs outside that affinity restriction.
The runtime is the unchanged `8bcd5b2` ARM64 CI archive, SHA256
`4494a0ce95c45613753abd4529dc00ec04f446516a76a012eb15ce6e98be7164`.
The fixture retains two million rows per copy, three streams, 120 transactions
per stream, 20 rows per transaction, and a 500 ms offered interval. Limits remain
2 seconds p95 lag, 5 seconds final catch-up and 180 seconds copy completion.

| Case | Streaming p95 range | Copy duration | Result |
|---|---:|---:|---|
| One copy, unchanged archive | 280–299 ms | 18.54 s | Passed |
| Four copies, unchanged archive | 17.82–18.79 s | 73.34 s | Finding |
| Four copies, 50 ms pause after each staging chunk | 428–464 ms | 139.04 s | Passed |
| Four copies, 50 ms pause, reusable-driver repeat | 348–350 ms | 136.79 s | Passed |
| Four copies, 25 ms pause | 751–894 ms | 94.50 s | Passed |

All five cases passed exact final data checks and clock audits, with no
observed engine OOM or PostgreSQL crash. No streaming-admission waiter was
sampled. The unpaced four-copy case also missed source scheduling tolerances;
the paced case passed them. During the backlog, activity samples showed
concurrent `append_initial_copy_rows` calls, including WAL/data-file I/O waits,
while final engine publications remained serialized by the existing bulk gate.

The paced image adds only an initial-sync shell wrapper that sets the existing
`LAKE_INITIAL_COPY_CHUNK_PAUSE_MS` diagnostic knob before executing the unchanged
archived binary. It changes no SQL or Rust source. The wrapper is an explicit
image overlay and must not be presented as an unmodified package qualification.
A 50 ms pause after each default 1,000-row chunk is not a fixed throughput cap:
actual rate also depends on processing time, row width and the number of copies.
Its aggregate admitted rate rises as more copies are started.
At 50 ms, copy time increased by about 90%. A 25 ms pause reduced that cost
but raised p95 lag. Fifty milliseconds per 1,000-row chunk adds a minimum of
100 seconds of waiting per two-million-row copy; 25 ms adds 50 seconds. The 139-second result on the local M5 leaves
insufficient evidence of headroom on slower machines; the native one-copy
controls were already much slower than the local control. A fixed 50 ms pause
must not be adopted as a default from this result. The diagnostic establishes a
mechanism and a tradeoff, not a production configuration.

### Resource sensitivity on the M5

With the same 25 ms setting but one shared CPU (`--cpuset 0`), four copies took
153.03 seconds and streaming p95 reached 16.92–19.28 seconds, with final catch-up
17.76–19.65 seconds. Exact data, source scheduling and the clock audit passed;
there was no observed engine OOM or PostgreSQL crash. This is a deliberate
reduction in available compute, not an exact model of GitHub's processors.
It demonstrates that the local two-CPU pass does not establish adequate margin.

Neither fixed pause is a production recommendation. The next design comparison must specify the source/storage/destination
resource topology, control aggregate staging work across copies, and establish
the capacity needed to meet both latency and copy-time limits. If the host
has insufficient capacity for the offered workload, pacing alone cannot promise
both deadlines. Keep the existing limits and failed gates visible.

### Separating source and storage CPU work

A further unpaced control kept destination PostgreSQL and pgduck on `0-1`,
assigned source PostgreSQL to CPU `2` and storage to CPU `3`. It completed copies
in 55.75 seconds with streaming p95 of 2.46–2.71 seconds. It still failed the
unchanged 2-second lag limit; exact data, source scheduling, final catch-up and
the clock audit passed. The services still share the local VM, memory and disk.
This isolates CPU placement, not separate physical hosts or storage devices.

CPU competition from packing the source and storage into the same two-CPU
budget is a substantial contributor in this local fixture. The residual lag
failure also shows that moving those services out does not, by itself, establish
a passing destination budget. Specify the qualification topology and resource
envelope before selecting a production backpressure policy or rerunning native
qualification. These observations do not justify a fixed sleep default or
weakening the limits.

The evidence supports testing backpressure before final engine publication.
It does not prove that a fixed 50 ms pause is suitable across machines, nor that
serializing whole initial copies would suffice: even native one-copy controls
failed. A production design still needs a defined scope and configurable budget,
restart/cancellation behavior and measurements across resource envelopes.

## Reproduction

`tests/admission/contention.py` keeps diagnostic images and fresh volumes
separate from production installation. It records the driver hash, exact driver
source, archive identity, CPU affinity, pacing flag and image identity alongside
raw fairness evidence. A zero pause uses the unchanged candidate image.

```sh
export QUALIFICATION_ARCHIVE=/absolute/path/to/candidate.tar.gz
python3 tests/admission/contention.py --copies 1
python3 tests/admission/contention.py --copies 4
python3 tests/admission/contention.py --copies 4 --pause-ms 25
python3 tests/admission/contention.py --copies 4 --pause-ms 50
# Sensitivity and CPU-topology controls:
python3 tests/admission/contention.py --copies 4 --pause-ms 25 --cpuset 0
python3 tests/admission/contention.py --copies 4 --source-cpuset 2 --storage-cpuset 3
```

Each invocation starts with independent volumes. These exploratory observations
are not randomized paired performance qualification or a confidence interval.

## Receiver hang and harness correction

The remote AMD64 wire test reached a durably retained uncommitted stream, then
`docker compose exec ... pkill` timed out. Cleanup retried the kill with another
20-minute timeout, followed by a 20-minute log collection timeout. The final
exception prevented `results.json` from being written. The original Docker/host
hang remains unexplained; it must not be labeled a Rust receiver defect without
further evidence.

Receiver kill attempts now have 15-second deadlines. Local docker-exec processes
get bounded terminate/kill attempts. Finalization writes the primary result
before trying Docker, records host load/free space and Linux memory/pressure
counters without Docker, independently attempts logs and fixture removal, and
persists each cleanup error. A passing test remains in `finalizing` state until
cleanup succeeds, so cancellation cannot leave a false completed pass. Log
commands have 20-second deadlines and teardown has a 60-second deadline. Failure to collect evidence still fails the lane.
Three regression tests force Docker timeouts and verify preservation of the
primary failure, teardown attempts and bounded local process cleanup.

Two full local ARM64 receiver lanes with shared two-CPU affinity passed all
96 checks each. The final run also verified the new host-resource capture.
The checks cover interrupted-stream kill, replay, disable, source outage and
drop. This verifies the normal path and does not reproduce or close the AMD64
host/Docker hang. The test harness fix does not change production binaries.

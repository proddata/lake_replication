# Streaming-only capacity baseline

This local diagnostic asks whether the three-stream workload meets its existing
latency target without competing large copies. It uses the unchanged retained
beta.3 archive and fresh volumes, with no shared-budget binary or SQL overlay.
The three subscriptions still perform their ordinary empty initial sync before
measurement. No large-copy table, publication, destination database or
subscription is created. This is a capacity control, not release qualification.

The source offers 120 transactions of 20 rows to each of three subscriptions at
500 ms intervals. The observer, 2 GB engine limit, single engine query thread,
and CPU affinity settings match the earlier contention diagnostics. Exact final
Iceberg data, source schedule, p95 below two seconds, publication gaps within
five seconds, catch-up within five seconds, clock continuity and engine/crash
observations remain required. Copy duration, progress and overlap checks do not
apply and are omitted, rather than reported as passing. Samples have no copy
object; the database observer clock is audited against monotonic elapsed time.

```sh
export QUALIFICATION_ARCHIVE=/absolute/path/to/candidate.tar.gz
# S1: all four services share one CPU.
caffeinate -is python3 tests/admission/contention.py --copies 0 --cpuset 0
# S2: all four services share two CPUs.
caffeinate -is python3 tests/admission/contention.py --copies 0 --cpuset 0-1
```

Run these profiles sequentially to avoid mutual interference. A passing control
shows that the workload is feasible on that tested profile; it does not predict
that an adaptive copy controller will preserve it. A failing control means copy
throttling alone cannot be assumed to meet the target at that capacity. The
local M5 remains a capacity experiment, not a cloud performance guarantee.


## Local results, 2026-10-28

[Retained evidence](evidence/2026-10-28-streaming-only-capacity-results.json)
includes all five attempts: an archive-selection error, a zero-copy summary bug,
and three completed measurements. The two harness failures are excluded from
performance conclusions. The summary bug was fixed before the valid runs.

| Profile | Streaming p95 range | Final catch-up range | Result |
|---|---:|---:|---|
| S1, first valid run | 1.86–2.37 s | 224–266 ms | Two streams fail p95 |
| S2 | 254–275 ms | 143–156 ms | All targets pass |
| S1, repeat | 399–451 ms | 156–165 ms | All targets pass |

All three valid measurements passed source scheduling, exact final data
(2,400 distinct correct rows per stream), clock continuity and the five-second
publication-gap limit. No engine OOM or PostgreSQL crash was observed. They
were run sequentially in S1/S2/S1 order. Each used the same archive SHA-256
`4494a0ce95c45613753abd4529dc00ec04f446516a76a012eb15ce6e98be7164`.

The first S1 run's largest lag occurred around transactions 89–100, rather than
at startup. The passing repeat prevents treating the first finding as proof
that one CPU can never meet the target. Conversely, the failing run prevents
calling this a robust passing baseline. The cause of the variation is not yet
isolated; CPU affinity does not isolate shared host/VM storage and memory.
These are exploratory samples, not statistical qualification.

The earlier S1 four-copy/16 MiB/s run reached 52.59–54.86 seconds p95. The new
controls show that this much delay is not intrinsic to every streaming-only
run at that affinity, which supports further copy-pressure experiments. The
runs are not paired estimates of a controller's achievable improvement.

Continue an isolated pressure-aware prototype on S2, where this control has
latency headroom. Include S1 as a sensitivity profile and retain repeated
streaming-only controls to distinguish baseline variability from controller
behavior. Observe committed unpublished work and receiver progress across all
streaming databases; stream-admission occupancy alone missed earlier staging
contention. A destination queue can also be empty while the receiver is behind,
so its absence cannot by itself authorize more copy work. Test slow/missing
telemetry, cancellation, aggregate fairness and recovery before any production
policy change. The interrupted-run engine OOM remains a separate open finding.


The subsequent [visibility diagnostic](pressure-visibility-prototype.md) confirms
that queue-only feedback misses receiver-side backlog. Its additional
instrumented streaming-only control passes; retain it separately from the
three baseline runs above. Receiver progress/freshness is the next prerequisite
for an adaptive controller.

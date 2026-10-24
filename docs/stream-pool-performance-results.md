# Four-slot streaming pool qualification

The local performance and operational checks support adopting four streaming
transaction slots alongside the existing bulk permit. The production source now
contains that policy; its [exported arm64 package qualification](stream-pool-migration-results.md)
also passes. Native amd64, final-commit CI and the release soak remain open.
[Retained evidence](evidence/2026-10-24-stream-pool-qualification-results.json)
records all 40 performance cases, the initial inconclusive result, the fixed copy
extension and 25 operational checks, with exact input/image/archive fingerprints.

## Comparison and protocol

The baseline is the unchanged bulk-only beta.3 arm64 archive, SHA256
`ea10f47b5cedf7084fa34dd682c2df14d23f21d5c9d5e3bf2a312411e4a7ec37`.
The candidate adds only the experimental four-slot streaming overlay. Its image
matches the one that passed three sixteen-subscription restart diagnostics.
Both variants retain a 2 GB, one-thread engine and 64 PostgreSQL worker slots.

Five alternating pairs each measure steady streaming and catch-up: 120
transactions of 100 rows with 128-byte payloads, ten warm-up transactions, five
idle seconds and a 100 ms journal observer. Offered intervals are 500 ms and
100 ms respectively. Publication latency uses actual commit timestamps. The
copy control uses two million copied rows alongside three live streams.

The proposed limits stay unchanged: p95/duration ratio at most 1.10 and catch-up
throughput ratio at least 0.95. Steady and catch-up measure overhead with one
streaming subscription; copy controls have three. They do not measure queueing
cost with a full four-slot pool. Busy-pool behavior is checked separately.

## Results

| Metric, pool/baseline | Median ratio | Paired interval | Assessment |
| --- | ---: | ---: | --- |
| Steady publication p95, five pairs | 0.993 | 0.958–1.063 | Pass |
| Catch-up publication p95, five pairs | 0.991 | 0.849–1.004 | Pass |
| Catch-up publication throughput, five pairs | 1.005 | 1.001–1.114 | Pass |
| Original copy duration, five pairs | 1.022 | 0.955–1.111 | Inconclusive |
| Combined copy duration, ten pairs | 1.009 | 0.955–1.034 | Pass |

Every steady case passes the absolute latency, catch-up, backlog and source
schedule checks. All 40 cases pass exact data checks and timing audits; no engine
OOM or unexpected PostgreSQL crash markers are present. One catch-up baseline
was noticeably slower, so these results support the regression margins rather
than a claim of improvement.

The first copy cohort is retained as inconclusive: one pair was 11.1% slower,
slightly outside the 10% margin. After observing that result, the fixed extension
rule was to collect exactly five more pairs, reverse their starting order, keep
all ten and preserve the 1.10 limit. This balances five AB and five BA pairs.
No observations were excluded or replaced and no further extension was used.

For ten pairs, `pair_extension.py` computes the exact whole-pair median
resampling distribution by weighting 92,378 count vectors, representing all
10^10 ordered resamples. Independent exhaustive enumeration validates the
algorithm for one through six pairs. Intervals remain descriptive conditional
on these local runs; no sequentially adjusted coverage guarantee is claimed.

## Operational checks and adoption boundary

All 25 checks in the separate operational run pass, including archive checks.
The tested pool spans two databases, reuses nested slots, releases savepoint
acquisitions while preserving outer holdings, propagates timeout/cancellation,
and releases slots after backend death. Eight queued transactions all progress
once holders release, with observed occupancy at most four. A deliberately
inverted bulk/stream order aborts with SQLSTATE `40P01`, releases both lanes and
can retry. This tests abort/retry, not prevention of every lock-order cycle.

The production helper preserves the tested acquisition body after renaming its
exported symbol. Production SQL uses explicit function replacements rather than
the prototype's dynamic overlay. Full native package, upgrade and recovery
qualification is separate. Per-slot queues do not promise one global FIFO; the
pool does not reserve memory or cover arbitrary application engine SQL. Native
amd64 CI, final-commit checks and the release soak remain required.

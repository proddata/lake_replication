# Extended launcher performance protocol

Frozen before the extended A/B cohorts; this is a local prototype evaluation,
not a supported product capacity or latency guarantee. Production sources and
shared experimental C hardening remain unchanged throughout the measurements.

## Observer calibration

Use only the static baseline, three repetitions each at 100 ms and 1,000 ms
polling, reversing order on alternate repetitions. Each fresh fixture has ten
warm-up transactions, three seconds idle, then 100 transactions of 100 inserts
at 100 ms intervals. This intentionally creates backlog. Record the observer's
query count/time and final visibility interval; compare throughput intervals
rather than treating polling completion time as the exact publication time.
This calibrates observation during backlog/catch-up, not every possible load.

Keep 100 ms polling for the matched A/B cohorts so observation resolution and
observer work are the same for both variants. Report calibration differences
and uncertainty; do not subtract a guessed observer cost from measured lag.

Before extended A/B runs, also compare Iceberg scanning against a lightweight
journal observer on the static baseline: two repetitions per observer, 60
transactions at 500 ms intervals, otherwise ten warm-up transactions and three
seconds idle. Enable commit timestamp tracking on both servers. Match source
transaction IDs to journal rows marked published, and read each journal row's
publication transaction commit timestamp through its `xmin`. Check those
timestamps against the independent first-observation intervals and verify final
Iceberg row counts. This validates the measurement relationship for this fixture;
it does not prove visibility semantics for every catalog or concurrent reader.

The extended cohorts use the journal observer to avoid repeated Iceberg scans.
Primary latency is **source commit to publication transaction commit**, measured
after the phase. Journal first-observation delay is reported separately. Final
Iceberg IDs, transaction markers and payloads must match the generated grid.
Do not label journal polling as directly measured Iceberg query visibility.

## Matched A/B cohorts

| Setting | Steady offered load | Backlog/catch-up |
| --- | --- | --- |
| Database / subscription count | 1 / 1 | 1 / 1 |
| Repetitions per variant | 5 | 5 |
| Warm-up | 10 transactions | 10 transactions |
| Measured transactions | 120 | 120 |
| Rows per transaction / payload | 100 / 128 bytes | 100 / 128 bytes |
| Source transaction interval | 500 ms (200 rows/s target) | 100 ms (1,000 rows/s target) |
| Source write duration | approximately 60 seconds | approximately 12 seconds |
| Idle observation | 5 seconds | 5 seconds |
| Observer / interval | journal / 100 ms | journal / 100 ms |
| Catch-up timeout after source finishes | 90 seconds | 90 seconds |

Use fresh fixtures, the default base-worker loading configuration, the same
release data path and pinned upstream, and alternating static/base order.
Resource sampling is outside timed write/observation phases. Retain images,
source fingerprints/snapshots, other running Docker containers, source commit
timestamps, visibility intervals, backlog traces, process counters and logs.
Verify row count, distinct IDs, and postmaster continuity in every fixture.

## Interpretation gates

For steady load, require p95 publication-commit lag below **2 seconds**, publication catch-up
within **2 seconds**, and at most **4 transactions** still unobserved at the
last poll completed while the source was writing. The last figure is a
conservative end-of-write backlog bound, not a direct instantaneous queue size.
Check first/last-quarter lag and backlog traces for accumulating delay; reaching
all rows eventually is insufficient. Confirm the requested source schedule was
actually delivered rather than silently reduced by source-side cost.

For comparison, retain the proposed **5% throughput / 10% p95 publication-commit lag** regression
thresholds. Apply them only when the difference is larger than observation
uncertainty and repeat variability. In the steady cohort, throughput is capped
by the offered load and cannot establish maximum capacity. In the backlog
cohort, completion throughput measures catch-up for this transaction shape,
not sustained streaming capacity or all workloads.

Primary completion throughput uses source-server phase-start time through the
last publication commit, so its endpoint does not include observer polling delay.
Keep commit timestamp tracking enabled for both variants and disclose this
instrumented configuration. Preserve first-observation bounds separately.

Report median paired ratios, their ranges and an exact paired bootstrap interval
(resampling whole A/B pairs, not individual transactions). Keep observation
bounds separate from between-run uncertainty. With five pairs, an interval may
remain wide: label that inconclusive rather than passing equivalence from a
non-significant difference. No pooling with earlier short/cohort configurations.

A performance result does not clear database ownership/upgrade, clone/restore,
crash recovery, multi-database capacity, mixed mutations, architecture, or soak
gates. If either cohort exposes a repeatable regression, investigate before
production migration. If neither establishes one, continue correctness and
operational qualification; do not call the entire adoption gate passed.

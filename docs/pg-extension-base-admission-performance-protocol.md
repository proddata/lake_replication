# Admission overhead and coexistence protocol

This evaluates the qualified base prototype before/after the one-transaction
admission overlay. It does not change production sources or either immutable
runtime image. Both variants use Docker init, 64 worker slots, commit timestamp
tracking and the same 2 GB/one-query-thread pgduck engine. The baseline is the
base launcher without admission, not the static launcher.

```sh
python3 experiments/base_workers/admission_measure.py --copy-rows 2000000
```

## Streaming during initial copy

Three destination databases each stream their own publication. A fourth database
copies an existing two-million-row source table with 128-byte payloads. Each
stream receives 120 transactions, 20 rows per transaction at 500 ms intervals
(120 aggregate offered rows/s). The source begins five seconds before native
CREATE SUBSCRIPTION starts the large copy. Poll journal/copy/admission state at
500 ms intervals using persistent psql sessions. Final source/publication commit
timestamps supply latency; journal polling is not direct Iceberg visibility.
Verify exact final Iceberg rows, IDs and payloads in every database.

The local evaluation targets, chosen before the measured large-copy case, are:

- Per-stream publication p95 below two seconds.
- Maximum publication gap within five seconds from the first source commit
  through the final publication, including backlog drain.
- At least five commits in each stream during the copy interval.
- Catch-up within five seconds after the final source transaction.
- Source schedule lateness within one offered interval; copy finishes within
  three minutes, with multiple observed progress samples.
- Actual admission contention is observed in the bounded case; sampled admitted
  count never exceeds one. No engine OOM or unexpected PostgreSQL crash.

These are evaluation targets, not a production latency/fairness guarantee. One
case per variant is diagnostic; it is not a repeated statistical comparison.
Longer-running fairness, more simultaneous copies, mutations and sustained
overload need separate qualification.

A 200,000-row pilot finished in 2.25 seconds: data/latency checks passed but only
four commits per stream overlapped the copy. The larger copy creates a meaningful
contention interval without lowering the overlap target. A preceding observer
JSON formatting failure is excluded from all performance conclusions.

### Dataset preparation and independent receiver finding

Prepare the large source table before creating any streaming slots. The original
fixture order bulk-loaded it after the three stream subscriptions existed. That
exposed an independent receiver failure on an empty filtered streamed transaction:
the receiver repeatedly reports `streamed transaction ... has no changes` and
does not progress. It reproduces with admission and without admission. Its
diagnostics are retained separately, not counted as admission fairness results.

Reproduce the original order explicitly:

```sh
python3 experiments/base_workers/admission_measure.py --skip-comparison \
  --fairness-variants unbounded --copy-rows 2000000 --seed-after-subscriptions
```

Moving preparation before slot creation isolates admission but does not fix or
qualify concurrent large unrelated source transactions. That receiver defect
remains a correctness follow-up. Do not skip transactions or advance feedback
ad hoc: durable staging, acknowledgement and publication checkpoints must remain
consistent when implementing its recovery.

## Matched one-subscription comparison

Five alternating matched pairs each for steady and backlog workloads, with fresh
fixtures per variant/repetition. Reuse the existing calibrated journal observer:
10 warm-up transactions, 120 measured transactions, 100 rows/transaction,
128-byte payload, five idle seconds, 100 ms polling. Source intervals are 500 ms
for steady load and 100 ms for backlog, with a 120-second catch-up timeout.
Compare the exact immutable pre/post-admission images; all other settings match.

Retain the raw transaction timings and resource snapshots. Assess paired p95
ratios against a proposed 1.10 ceiling and catch-up throughput ratios against a
0.95 floor using the existing exact paired-bootstrap analyzer. Intervals describe
these local runs. Fewer than five complete pairs cannot pass a comparison margin.
Steady throughput remains offered-load limited; check absolute p95/catch-up at
two seconds, end backlog at most four transactions and source scheduling.

Report failed/uncertain margins and coexistence failures separately. A fast
single-subscription workload cannot override a large-copy fairness finding.

## Clock validation and resumed measurements

Resuming only the catch-up cohort uses `--skip-fairness --profiles catchup`.
Each source commit must fall inside its monotonic insertion interval, and each
publication commit inside its monotonic first-observation bounds, allowing one
second for local query/observer round trips. Reject a comparison before applying
any margin if its warm-up or measured phase fails this clock check. This detects
gross host/VM clock steps; it is not sub-second clock calibration.

The first resumed cohort completed data checks but its last two baseline cases
had database timestamp durations of roughly 10 and 46 minutes against about
26 seconds of monotonic observation. Their nominal margin pass is withdrawn.
The [rejected run and audit](evidence/2026-10-21-admission-rejected-clock-run.json)
preserve the raw records. The completed steady/coexistence records pass the audit.
Rerun the full catch-up cohort, keeping whole pairs together, with scoped idle
sleep prevention and clock validation. Do not merge its rejected pairs into the
new qualification interval.

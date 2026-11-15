# Coalescing windows between publications

The managed supervisor now rechecks coalescing policy after each publication on a live stream. Previously, it waited before launching a publisher, but that child could drain up to ten batches immediately. Source transactions arriving during publication could therefore bypass the next coalescing window.

With a nonzero `lake_replication.publish_interval_ms`, each live-stream publisher invocation now commits at most one batch and returns control to the supervisor. The supervisor checks stop state, pending work and journal pressure again. Count pressure or 75% of the payload-byte budget still overrides the delay. Immediate mode (`publish_interval_ms=0`) and successful receiver-exit draining retain their configured multi-batch budgets. Explicit manual publisher invocations also retain their own budgets.

A coalescing window starts when the supervisor observes pending work after the previous publication. Publication execution time adds to that wait; the setting is not a hard end-to-end latency guarantee. The same count/byte admission limits and source acknowledgment rules apply. Source operation order and atomic publication checkpoints are unchanged.

## Deterministic regression

The [before/after regression](evidence/2026-11-15-local-coalescing-followup-regression.json) blocks the first publication behind a private advisory-lock trigger. Two source transactions fill the journal and start that publication. While it is blocked, the test raises journal capacity and stages a third transaction.

After releasing the barrier, the control publishes all three immediately despite a sixty-second window: that is the expected failing check. The candidate publishes only the first two and holds the third. Renewed journal pressure then publishes the third/fourth promptly. A fifth remains staged through a responsive stop and publishes after restart with immediate mode. The candidate passes 13 checks and cleans up its slot.

The first fixture attempt was corrected to account for MVCC: while publication is uncommitted, its first two receipts still appear staged to another reader, along with the new third. A preliminary timing-harness assertion was also corrected to distinguish live-stream flushes from intentional receiver-exit draining. Neither incomplete attempt contributes to the final performance results.

## Measurements

The [retained comparison](evidence/2026-11-15-local-coalescing-windows.json) contains eight sequential fresh fixtures, two runs per worker variant/workload with order reversed in the second pair. Both runtimes include the same extension SQL and prior DELETE-filter optimization. Only worker versions differ. Both use one-second coalescing, INSERT grouping enabled, a 32-transaction/64-MiB journal, the same storage and resource observers.

Mixed workload: 120 transactions at four offered transactions/second, four INSERT rows per table and subsequent UPDATE/DELETE of preceding keys in two tables. Medians of the two per-run measurements:

| Metric | Control | Candidate |
| --- | ---: | ---: |
| PostgreSQL + engine CPU | 24.45 s | 16.89 s |
| Published batches | 57 | 17.5 (17–18 observed) |
| Mean source transactions/batch | 2.11 | 6.86 |
| Observed mirror S3 requests | 2,476 | 2,081 |
| WAL | 12.53 MiB | 11.57 MiB |
| Application COMMIT p99 | 8.55 ms | 9.03 ms |
| Publication p95 upper bound | 3.32 s | 4.26 s |
| Post-write drain | 0.90 s | 1.69 s |

Median CPU falls about 31%, publications about 69%, mirror requests about 16% and WAL about 8%. This comes with a higher sampled publication-delay upper bound and a larger waiting backlog (maximum sampled backlog ranges 7–9 versus 9–12 source transactions). The application COMMIT p99 figures do not establish an improvement. CPU ranges are 24.27–24.64 seconds for control and 15.64–18.15 seconds for candidate. This is the measured cost/latency trade-off of enforcing the opt-in window, not a free reduction in lag.

High-rate INSERT workload: 1,000 transactions at 100 offered transactions/second, four rows per table. Both variants use exactly 32 publications, average 31.25 transactions per batch and 384 mirror requests. Candidate flushes are driven predominantly by journal pressure and receiver exits. Exact final rows match in every fixture.

INSERT CPU ranges are 7.07–9.71 seconds for control and 6.37–6.79 seconds for candidate. One control run also has a higher application p99 (24.2 ms versus 3.8 ms in the other control); those observations are retained. The unchanged batch/request counts support pressure-path behavior, while the short noisy runs do not establish a general INSERT CPU speedup.

CPU includes PostgreSQL, its workers, the one engine and container observers; it excludes host/controller/MinIO CPU. Initial copy, final row verification and batch/log collection are outside measurement. Publication delay is an observed upper bound including two-second sampling/query delay. Values are medians of per-run measurements or per-run quantiles, not pooled quantiles. All containers share one local host, without cold-cache clearing or host-wide isolation. Two reversed-order pairs provide descriptive evidence, not statistical significance, production sizing or a capacity maximum.

## Flush telemetry

The supervisor emits `publication_flush` events with `reason`, configured interval, permitted batch count and locally observed pending duration. Reasons are `interval`, `journal_pressure`, `receiver_exit` or `immediate`. The publisher's existing `batch_published` event now includes `flush_reason` alongside source transaction/row/byte counts; direct invocations default to `manual`.

`pending_elapsed_ms` is the supervisor's monotonic waiting observation, not source COMMIT-to-visible latency. Events contain no row values, SQL or credentials. Logs are operational telemetry; durable journal/batch records remain authoritative after a crash or a lost COMMIT response. The benchmark counts transactions from those durable records and collects logs only after the resource interval.

## Reproduce

Use a control image with the previous workers and a candidate image with the updated workers; both must contain identical extension SQL. Select cached storage fixture images and run:

```sh
python3 tests/local_iceberg/coalescing_compare.py \
  --control-image lake-local-iceberg:qualification-v6 \
  --candidate-image lake-local-iceberg:qualification-v7
PUSH_PG_LAKE_IMAGE=lake-local-iceberg:qualification-v7 make local-iceberg-coalescing-followup-test
```

The comparison supports `--workload mixed|inserts`, `--repetitions 1..3` and `--smoke`. All data and credentials belong to private disposable qualification fixtures. No new SQL migration or configuration parameter is required by this worker change. The existing default interval is zero; opting into a window trades publication latency against batching cost.

## Runtime qualification

The [packaged worker qualification](evidence/2026-11-15-local-coalescing-runtime.json) passes 304 checks across 14 fixtures: the 12-fixture managed local suite (223), existing logical compatibility/upgrade/restore/uninstall cases (54), and actual publication crash boundaries (27). This includes immediate-mode lifecycle, initial-copy interruption, streaming reconnects, reader isolation, blocked publication, object-store outage, maintenance, coalescing count/byte pressure, follow-up arrivals, resource caps, INSERT grouping, WAL-retention loss and DELETE semantics. Every fixture cleans up successfully. `make check` also passes.

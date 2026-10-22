# Current prototype scaling and execution-model decision

> Historical prototype record. Commands and source paths below refer to the
> [preserved implementation](https://github.com/proddata/lake_replication/tree/7ef18381810075dddd26137ee959770591b95863/experiments/base_workers) before cleanup.
> Current lifecycle tests live in `tests/lifecycle`; retained admission helpers
> are described in [experiments/admission](../experiments/admission/README.md).

The base framework remains a viable lifecycle layer. The current unbounded
per-subscription execution model needs resource admission before broader use.
Neither switching to the static launcher nor increasing worker slots alone
addresses both limits found here.

This report measures the pre-admission overlay. The subsequent
[bounded-write prototype](pg-extension-base-admission-results.md) passes the
reproduced 64-slot/2 GB restart case; its performance remains to be qualified.

## Scope and reproduction

Run `python3 experiments/base_workers/scale.py`. The fixture uses the immutable
publication-qualified image `sha256:0a962ccd1865c9f97709eb2d53383dd607150af073eea5f0989dd13ffe78334f`,
including the experimental recovery supervisor and publication/origin checks.
Production C, Rust and migration sources remain unchanged. The image, archive,
source snapshots, host information, case logs and inventories are retained under
`.qualification/qualification_20261020_045053_scale/`.

PostgreSQL 18.6, Linux arm64, private same-host source/destination/storage. Docker
reports 11 CPUs and 7.65 GiB RAM; an unrelated PostgreSQL container remains running.
Subscriptions in the table are totals, distributed evenly across databases. Each
subscription has its own source publication/table/slot and real Iceberg target.

The revised fixture gives pgduck 2 GB and one DuckDB query thread. An earlier
1 GB/default-thread attempt exhausted engine memory during the four-subscription
restart. Its logs are retained under
`.qualification/qualification_20261020_044742_scale/scale-1db-4sub-24slots/compose.log`.
That run was interrupted after diagnosis and is not a completed cohort.

## Functional scaling

| Databases | Total subscriptions | Worker slots | Result |
| --- | --- | --- | --- |
| 1 | 1 | 24 | Passed |
| 1 | 4 | 24 | Passed |
| 1 | 16 | 24 | Passed |
| 4 | 4 | 24 | Passed |
| 4 | 16 | 24 | Worker slots exhausted; only 13 subscription wrappers started |
| 4 | 16 | 64 | Initial copy/live updates passed; restart publication exhausted engine memory |

Passing cases check initial copy, concurrent source updates, actual Iceberg rows,
destination restart and subsequent publication, disable/drain, native source-slot
cleanup and extension removal. Disabled inventories have no sampled Rust children
or subscription wrappers. These are small functional workloads, not multi-database
throughput measurements, leak soaks or resource peaks. Completion timings include
sequential Docker/psql observation overhead.

### Worker slots

The observed steady registered-worker demand is `3 + 2D + S` for `D` databases
and `S` enabled subscriptions in this pinned fixture:

- One server starter, one pg_lake cache worker and one logical replication launcher.
- Two base registrations per database: pg_lake's existing worker and our replication launcher.
- One PostgreSQL subscription wrapper per enabled replication subscription.

Ordinary client backends, auxiliary processes, I/O workers and raw Rust children
are separate process/resource costs; their presence in `pg_stat_activity` does
not mean each consumes a `max_worker_processes` slot. Temporary database discovery,
parallel work and other extensions need additional headroom. `27` is the observed
steady demand for 4/16, not a recommended production setting.

The C launcher attempts to register every enabled subscription and logs/retries
when no slot is free. It has no configured active-subscription ceiling, queue
fairness policy or cross-database resource admission. The base callback does not
require this one-wrapper-per-subscription topology.

### Shared engine memory

The 64-slot 4/16 repeat started all wrappers and published live updates. After
destination restart, concurrent publication through the shared pgduck engine hit
its 2 GB limit. The engine logged an unrecoverable DuckDB allocation failure,
including a failed 76.5 MiB block allocation. Subsequent PostgreSQL statements
reported `could not start query engine`, and the first checked row remained at
its previous value. This case is a finding, not a restart pass.

More slots therefore move the bottleneck. This does not establish an intrinsic
subscription limit, nor prove that base caused the memory failure. A matched
static multi-database control is unavailable: the static launcher supports one
configured database. The relevant architecture property is uncontrolled combined
publication demand against a shared engine. Initial copy and independent pg_lake
maintenance can also consume that engine's resources.

## Current single-subscription performance

Both variants use the same recovery supervisor and origin/publication guards.
The static control rebuilds the extension from production source with shared
hardening and those guards, retaining static launcher registration. The base
image is the exact publication-qualified image. Both engines use the revised
2 GB/one-thread settings. Earlier engine configurations are not direct controls.

Five alternating matched pairs per cohort use 120 measured transactions of 100
rows, 10 warm-up transactions, 128-byte payload, five idle seconds and 100 ms
journal polling. Actual source and publication commit timestamps determine
latency; actual final Iceberg rows, IDs, markers and payloads are checked. The
fixture catch-up timeout is 120 seconds; measured completion remains far below
that timeout. This is the existing workload/analysis protocol on the latest
prototype, not a multi-subscription mutation benchmark or capacity guarantee.

The steady cohort offers about 200 rows/second:

| Metric | Static | Base |
| --- | --- | --- |
| Median publication p95 | 263.78 ms | 250.50 ms |
| Median publication throughput | 201.21 rows/s | 201.26 rows/s |

All steady absolute latency, catch-up, scheduling and backlog checks passed.
The paired base/static p95 ratio has median `0.9853` and exact paired-bootstrap
95% descriptive interval `[0.8962, 1.0486]`, inside the proposed `1.10` ceiling
for this local cohort. Throughput is offered-load limited and is not a capacity
gate. All 130,000 steady-cohort rows were checked. This supports a local latency
margin pass; it does not establish a speedup or production adoption.

The backlog cohort offers about 1,000 rows/second:

| Metric | Static | Base |
| --- | --- | --- |
| Median publication p95 | 12.909 s | 12.953 s |
| Median publication throughput | 472.07 rows/s | 471.04 rows/s |

The paired throughput ratio has median `0.9957` and 95% descriptive interval
`[0.9920, 0.9990]`, inside the proposed no-more-than-5% throughput-loss margin.
Base was slightly slower in every pair; this is a small measured difference,
not proof of exact equivalence. The p95 ratio interval is `[1.0015, 1.0145]`,
also within the proposed 10% latency margin. High lag is expected under this
offered overload and is not assessed against the steady two-second gate.

All 260,000 rows across both cohorts were verified. Results apply to one
database/subscription and inserts; mixed mutations, many simultaneous writers,
amd64, soak, latest-overlay upgrades and production fencing remain unqualified.
The resource failures above remain findings despite these performance passes.

The [retained evidence](evidence/2026-10-20-base-current-scaling-performance-results.json)
contains raw transactions, analyses, fingerprints, worker inventories and log
hashes/excerpts for both resource failures. All private containers and volumes
were removed after the run; cached images remain. Nothing was committed or pushed.

## Recommended next design

Keep base registration as the database lifecycle boundary. Introduce explicit,
bounded publication concurrency across the destination instance, with visible
waiting state, cancellation, fair progress and crash-safe permit release. A
per-database ceiling alone cannot bound aggregate demand on a shared engine.
Validate permit ordering against the existing per-subscription transaction and
ownership locks. Include initial copy in the resource policy.

This alone leaves the PostgreSQL wrapper/process count proportional to enabled
subscriptions. Evaluate a bounded worker pool next if that count is unacceptable:
workers should rotate fairly among subscriptions while preserving source-slot,
checkpoint and single-owner correctness. Receiver continuity, WAL retention and
restart fairness need explicit tests; simply selecting the first N subscriptions
would starve the rest.

An external, long-running Rust service could own scheduling and multiplex source
connections without PostgreSQL wrappers per subscription. It adds a service to
operate and still needs publication admission, database lifecycle coordination
and ownership/recovery guarantees. It is an alternative execution model, not a
requirement of the base framework.

Do not adopt the full current execution model unchanged on the strength of its
single-subscription benchmark. Larger engine memory is a sizing experiment, not
a replacement for a bounded-resource policy.

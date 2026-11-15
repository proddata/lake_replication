# Mirror local PostgreSQL tables directly into Iceberg

The intended deployment is one PostgreSQL instance containing both ordinary
application tables and their Iceberg mirrors. The same database holds replication
control state and its durable journal. No separate destination PostgreSQL,
intermediate Iceberg change logs, or push importer is required.

```text
One PostgreSQL instance/database
  application heaps → logical WAL decoding → local durable journal
                                            ↓
                                      Iceberg publisher
                                            ↓
                                   Iceberg mirror tables
                                            ↓
                                       object storage
```

The receiver connects back to the same instance over loopback. This uses the
existing receiver, initial-copy, journal and publisher code. The pgduck engine is
still a pg_lake prerequisite; it is a separate engine process, not another
PostgreSQL instance containing a destination database. Application tables retain
their ordinary PostgreSQL storage and write semantics.

## Setup

Install the matching default lake_replication bundle and its qualified pg_lake
runtime on this instance. Configure `wal_level=logical`, enough replication
slots/WAL senders and worker processes, the pg_extension_base preload, the
pgduck engine, and your object-store credentials/location. See the main
installation instructions for those prerequisites. Restart when required.

As the installer, in the application database:

```sql
CREATE EXTENSION pg_lake CASCADE;
CREATE EXTENSION dblink;
CREATE EXTENSION lake_replication;

CREATE ROLE local_source LOGIN REPLICATION;
GRANT USAGE ON SCHEMA public TO local_source;
GRANT SELECT ON public.orders, public.customers TO local_source;
CREATE PUBLICATION local_iceberg_pub FOR TABLE public.orders, public.customers;

CREATE ROLE local_worker LOGIN;
CREATE SCHEMA iceberg_mirror;
```

Apply the default bundle's `worker-grants.sql` in this same database with
`-v worker_login=local_worker -v destination_schema=iceberg_mirror`. These are
trusted database-level worker grants, not tenant isolation. Configure local
PostgreSQL authentication for both logins. Create a server-side credential file
readable only by the PostgreSQL OS account, for example:

```text
LAKE_SOURCE_USER=local_source
LAKE_DESTINATION_USER=local_worker
LAKE_SOURCE_TLS_MODE=disable
LAKE_DESTINATION_TLS_MODE=disable
```

Add the corresponding password settings when required by local authentication.
Keep credentials out of SQL connection strings and source control. The example
uses TCP loopback on port 5432; choose this instance's actual port/database.
Then create the managed local mirror:

```sql
CREATE SUBSCRIPTION local_iceberg
CONNECTION 'host=127.0.0.1 port=5432 dbname=application user=local_source sslmode=disable'
PUBLICATION local_iceberg_pub
WITH (
    lake_target='iceberg',
    lake_target_schema='iceberg_mirror',
    lake_credential_file='/absolute/server/path/local-iceberg.env'
);
```

The supervisor copies existing rows consistently, then streams subsequent
commits into `iceberg_mirror.orders` and `iceberg_mirror.customers`. Keep the
publication restricted to the application heaps: do not add the mirror or
replication control tables. Do not let independent writers mutate the mirrors.

Inspect `lake_replication.subscription_status` for the subscription's health,
checkpoints and errors. Existing `ALTER SUBSCRIPTION ... DISABLE/ENABLE` and
`DROP SUBSCRIPTION` lifecycle commands apply. This is asynchronous mirroring;
application COMMIT does not wait for an Iceberg publication.

## Batch small publications

The default `lake_replication.publish_interval_ms=0` publishes pending journal
work immediately. To coalesce small application commits into fewer Iceberg
publications, an administrator can opt in on a current-core installation:

```sql
ALTER DATABASE application SET lake_replication.publish_interval_ms=1000;
ALTER SUBSCRIPTION local_iceberg DISABLE;
ALTER SUBSCRIPTION local_iceberg ENABLE;
```

The setting is captured by the supervisor's database connection when a receiver
cycle starts. Disable/enable applies a database-level setting to an existing
subscription. The allowed interval is 0–60000 milliseconds; it is a superuser configuration
parameter. A longer interval trades publication latency for fewer small
Iceberg commits. It is a coalescing window, not a hard end-to-end latency promise.

The supervisor continues checking stop state and reporting heartbeats while it
waits. Reaching the journal transaction limit or 75% of its payload-byte limit
ends the delay early. Normal journal admission limits still apply. A stop retains
admitted work for replay after enable. This does not increase journal capacity,
change source COMMIT semantics or require intermediate Iceberg capture logs.

## Qualification and limits

`make local-iceberg-test` uses `deploy/local/compose.yml`, a disposable fixture
with test-only credentials and no host PostgreSQL port exposure. Select cached
`PUSH_PG_LAKE_IMAGE`, `PUSH_STORAGE_IMAGE` and `PUSH_STORAGE_INIT_IMAGE` images.
The PostgreSQL image must contain the default qualified lake_replication bundle.
The test needs neither optional capture nor optional push extension.

Single-instance measurements must include PostgreSQL, its workers and its one
pgduck engine. Earlier two-instance push benchmarks measured an intermediate
Iceberg transport architecture and do not measure this topology. Performance,
production sizing and unsupported schema operations need their own qualification.

The [single-instance functional qualification](evidence/2026-11-15-local-direct-iceberg.json)
retains 21 passing checks covering initial copy, committed/aborted mutations,
primary-key changes, DELETE/reinsert, nullable-column evolution, stop/start,
shared PostgreSQL restart, TRUNCATE, explicit publication selection and cleanup.
The source and journal identities match, the worker login is unprivileged, and
neither optional source-capture nor optional push extension is loaded.

`make local-iceberg-fault-test` requires a **test-only crash-instrumented** image.
It kills a publisher backend before upload completion, after upload and after
COMMIT, restarting the same PostgreSQL that holds application rows and the local
journal. Never install that fault image on an application host.

The [shared-instance crash qualification](evidence/2026-11-15-local-direct-iceberg-crashes.json)
passes 27 checks. Committed application heaps and admitted journal boundaries
survive every injected server crash. Publication checkpoints follow the actual
COMMIT outcome, and repeated publication retries preserve exactly the source
rows. This fixture uses a manually controlled superuser receiver/publisher;
managed-runtime fault injection and local initial-copy crash testing remain open.

The current-core [local runtime qualification](evidence/2026-11-15-local-runtime-qualification.json)
retains functional, managed initial-copy interruption, replication-connection
recovery, repeatable-read mirror readers, blocked publisher/application progress
and object-store outage checks. During the outage, application commits proceed,
committed journal counts and bytes stay bounded, and retained local WAL supports
catch-up after storage returns. These bounds do not cover physical WAL disk
exhaustion or unlimited outages.

Additional targets are `make local-iceberg-recovery-test`,
`make local-iceberg-contention-test`, `make local-iceberg-outage-test`, and
`make local-iceberg-coalescing-test`. The last requires the current core and
supervisor supporting `lake_replication.publish_interval_ms`.
`make local-iceberg-benchmark` compares fresh application-only and local-mirror
fixtures. The benchmark script accepts `--publish-interval-ms`, `--transactions`,
`--rows`, `--interval` and `--order`. Run timed comparisons without competing
qualification jobs. Its two-second publication observer reports upper bounds,
not exact publication latency.

## Measured publication cost

The [same-runtime local comparison](evidence/2026-11-15-local-publication-source-impact.json)
uses 120 transactions at four transactions per second, with ten rows per table
in two application tables and a fresh application-only baseline for each setting.
Both modes complete with exact mirror contents. One local ARM64 fixture per mode
and setting gives the following descriptive results:

| Measurement | Immediate publication | 1-second coalescing |
|---|---:|---:|
| PostgreSQL + pgduck CPU seconds | 19.72 | 9.33 |
| Observed S3 requests for final mirrors | 1,442 | 942 |
| Application COMMIT p99 | 3.29 ms | 5.85 ms |
| Observed publication-latency upper bound, p95 | 2.41 s | 3.02 s |
| WAL generated | 4,740,960 bytes | 4,064,256 bytes |

The corresponding application-only baselines use 2.57 and 2.52 CPU seconds.
Coalescing reduces total database/engine CPU by about 53% in this workload while
increasing publication delay. The two-second observer means the latency column
is an upper bound including sampling delay, not an exact latency measurement.
The mode order differs between settings and these are small local experiments,
not production sizing or statistical confidence estimates.

Both mirrored runs still write 240 Parquet files: table switches preserve
source operation order and end contiguous write runs. Coalescing reduces
Iceberg metadata publications; it does not consolidate those data files.
The journal's WAL, receiver overhead, maintenance and small-file growth remain
relevant to production sizing.

The [coalescing qualification](evidence/2026-11-15-local-publication-coalescing.json)
passes 18 checks, including early flushing for count and byte pressure and a
0.28-second stop during a configured 60-second window. `make check` passes.
Final-runtime publication-crash repetition passes all 27 checks, and the
[default logical regression cases](evidence/2026-11-15-local-logical-regressions.json)
pass 54 checks, including upgrade/restore, replication, interrupted initial copy
and uninstall. Earlier Docker ENOSPC/startup failures are retained under
`.qualification`; they are not counted as passing checks.

## Local maintenance

The [local maintenance qualification](evidence/2026-11-15-local-maintenance.json)
creates repeated small local publications, then runs native `VACUUM` on each
Iceberg mirror. Each current data-file set compacts to one file without changing
source or mirror rows. This does not immediately delete older objects: snapshot
retention and pg_lake's delayed deletion policy still govern physical storage.

Published, acknowledged journal history can be reclaimed with
`configure_journal_maintenance` and `run_journal_maintenance`. The fixture uses
zero retention only in its disposable database to verify a three-transaction
cleanup bound, unchanged publication/ACK checkpoints, and successful mutation
replication afterward. Production retention must follow recovery requirements.

## Type fidelity and query-memory budgeting

The current publication SQL parses PostgreSQL JSONB wire text explicitly instead
of storing that text as a JSON string. The
[primary-key](evidence/2026-11-15-local-scalar-types.json) and
[full-row identity](evidence/2026-11-15-local-scalar-types-full.json) fixtures each
pass 18 checks, including JSON objects, strings, arrays, arbitrary-precision JSON
numbers, SQL NULL versus JSON `null`, exact decimal/temporal/binary values,
compound-key changes, deletes and unchanged external TOAST values. Existing
journals carry PostgreSQL text values; JSONB strings therefore include their JSON
quotation marks. The SQL correction is migration `0031_jsonb_wire_values.sql`.
It leaves frozen earlier extension versions unchanged. If an older build already
streamed JSONB values into a mirror, a fresh snapshot/rebuild is needed to repair
those existing values; this correction applies to subsequent publications.

The [resource-budget fixture](evidence/2026-11-15-local-resource-budget.json)
passes 13 checks with one CPU core each, a 512-MiB PostgreSQL cgroup cap and a
1-GiB pgduck cgroup cap. Its engine query budget is 768 MB, one query thread,
16-MB target Parquet row groups, and one admitted journal transaction at a time.
It publishes 100 transactions/1,000 rows per table without OOM kills; recorded
cgroup peaks are about 153 MiB for PostgreSQL and 214 MiB for pgduck. These are
tested limits for a controlled two-table flow, not minimum requirements or an
arbitrary-workload guarantee. Progress is observed through PostgreSQL while
writers run; mirror-content scans follow after publication is stopped.

Earlier 256–416 MB internal query-budget attempts failed at DuckDB buffer
allocation, including attempts with a 512-MiB engine cgroup. Those are retained
as failures, not counted as supported configurations. Internal query-memory
accounting and observed resident memory are different quantities; these failures
do not establish a 1-GiB resident-memory minimum. Production limits must account
for concurrent query and writer buffers as well as journal payload size.

Use `make local-iceberg-types-test` and
`LOCAL_ICEBERG_FULL_IDENTITY=1 make local-iceberg-types-test` for type coverage,
and `make local-iceberg-resource-budget-test` for the bounded budget fixture.
`make local-iceberg-suite` runs the non-crash local gates sequentially; publication
crash tests require the separate test-only instrumented image.

The [1,000-transaction comparison](evidence/2026-11-15-local-sustained-1000.json)
extends each mode to about 250 seconds at four transactions per second and ten
rows per table. With a one-second coalescing window, all 10,000 rows per mirror
match. PostgreSQL plus pgduck CPU is 93.50 seconds (about 0.37 cores on average)
versus 19.82 seconds (0.08 cores) for the fresh application-only baseline.
Application COMMIT p99 is 8.00 ms versus 7.87 ms. The observed publication-latency
upper bound is 3.18 seconds at p95, including two-second sampling delay. This
run observes published source XIDs in the journal instead of repeatedly scanning
the growing Iceberg tables; its exact method and runtime are retained. It is
not directly comparable to the shorter scan-observed measurements or a
production sizing result.

## Transaction shape and INSERT grouping

A [bounded load map](evidence/2026-11-15-local-capacity-map.json) compares different
transaction shapes instead of requiring a production throughput estimate. At the
same offered 800 rows/second across two tables, four 100-row transactions per
second kept up; forty 10-row transactions built a backlog; one hundred four-row
transactions needed about 32 seconds of drain after the ten-second write phase.
Four 1,000-row transactions per second also kept up. These are short local
experiments, not sustainable capacity maxima. Transaction overhead and small
Iceberg writes matter independently of row/byte throughput.

The optional `lake_replication.group_independent_inserts` setting addresses this
specific small-transaction case. It defaults to `off`. When enabled, an entirely
INSERT-only publication can group rows by destination table while retaining row
order within each destination and committing all rows/checkpoints atomically.
The existing 500-row/4-MiB write-run bounds still apply. Mixed INSERT/UPDATE/DELETE/
TRUNCATE batches keep their original global operation order.

The fast path is restricted to permanent local-catalog pg_lake Iceberg targets
with supported built-in scalar types and no triggers, rules, defaults, generated
columns, identity columns, CHECK constraints, row security or inheritance.
Unsupported targets fall back to the existing path. The native guard checks
INSERT permissions, acquires destination locks in OID order, and checks current
catalog metadata after waiting. Registry mappings are pinned for the publication;
locks fence later DDL. Tests cover trigger fallback/global order, permission denial
and trigger creation racing an old repeatable-read snapshot.

To opt in on a current installation, use a privileged configuration session:

```sql
ALTER DATABASE application SET lake_replication.group_independent_inserts=on;
ALTER SUBSCRIPTION local_iceberg DISABLE;
ALTER SUBSCRIPTION local_iceberg ENABLE;
```

The guard qualification confirms ten source transactions produce one new data
file per table instead of ten. This is not a blanket license to reorder arbitrary
mutations or side effects. `make local-iceberg-insert-groups-test` exercises the
native eligibility and fallback fences. `make local-iceberg-capacity` runs the
bounded load map; its script supports `--case`, `--group-inserts` and
`--journal-transactions` for controlled comparisons.


The [same-runtime comparison](evidence/2026-11-15-local-independent-insert-performance.json)
uses 1,000 paired-table transactions at an offered 100 transactions/second,
four rows per table and a one-second coalescing window:

| INSERT grouping | Journal transaction limit | PostgreSQL + engine CPU | Drain after writes | Maximum observed backlog |
| --- | ---: | ---: | ---: | ---: |
| Off | 8 (default) | 47.30 s | 38.79 s | 744 transactions |
| On | 8 | 25.63 s | 21.97 s | 616 transactions |
| On | 32 | 6.34 s | 1.44 s | 39 transactions |

Backlog here means committed application transactions awaiting publication,
including transactions still in WAL; it is not the admitted journal count.
Every mirror matches its source. The three fresh application-only baselines use
1.99–2.03 CPU seconds. These sequential local pairs include observer cost and
warm-cache/run-order effects; they establish a useful operating point, not a
sustainable capacity maximum or a production recommendation. A larger journal
window lets grouping amortize Iceberg writes across more source transactions;
it also admits more pending work, within the unchanged payload-byte limit.
The defaults remain grouping off and eight transactions.

For an equivalent controlled experiment, an administrator can update only the
selected subscription's journal transaction bound (the byte limit stays intact):

```sql
UPDATE lake_replication.journal_limits limits
SET max_transactions=32
FROM lake_replication.subscriptions subscription
WHERE limits.subscription_id=subscription.subscription_id
  AND subscription.subscription_name='local_iceberg';
```

The [grouping guard checks](evidence/2026-11-15-local-independent-insert-groups.json)
pass 18 checks. The [grouped crash fixture](evidence/2026-11-15-local-independent-insert-crashes.json)
passes 29 checks including reduced data-file counts after an actual shared-server
crash. The same runtime passes [54 default logical compatibility checks](evidence/2026-11-15-local-insert-runtime-logical-regressions.json).

## When local WAL history is lost

An object-store outage or stopped mirror can leave WAL retained by the local
logical slot. Journal admission limits do not bound retained WAL. Set and monitor
PostgreSQL's `max_slot_wal_keep_size` and free disk space for the deployment;
checkpoint enforcement can invalidate a slot once its required history is gone.

The [local retention qualification](evidence/2026-11-15-local-wal-retention.json)
passes 17 checks with a private one-MiB retention limit and bounded WAL generation.
After PostgreSQL reports `lost:wal_removed`, the managed mirror reports critical
health with the source error. It preserves staged/published/acknowledged
checkpoints, source confirmation and the last complete mirror contents. It does
not replace the invalidated slot or skip ahead. Application heap commits continue.
An explicit fresh snapshot restores exact contents and subsequent streaming.
This tests retention loss, not physical PGDATA/WAL ENOSPC.

Treat a lost slot as requiring a rebuild. Disable the subscription, record its
status/error, then drop it to release the invalidated owned slot. Preserve the
old mirror until the replacement is verified. Create an empty replacement mirror
schema, apply the worker grants to it, and create a new subscription targeting
that schema using the setup above. Verify initial copy and streaming before
switching readers or retiring the old mirror. Merely enabling the failed
subscription cannot reconstruct missing WAL.

`make local-iceberg-wal-retention-test` executes the disposable retention/rebuild
fixture. Its tiny WAL limit is test-only and is not a deployment recommendation.


The [10,000-transaction grouped comparison](evidence/2026-11-15-local-grouped-sustained-10000.json)
extends the 100-transactions/second operating point to 100 seconds of writes with
four rows per table, grouping on, one-second coalescing and a 32-transaction journal.
All 40,000 rows per table match. PostgreSQL plus engine CPU is 88.15 seconds versus
19.23 seconds for the fresh application-only baseline; application COMMIT p99 is
5.26 ms versus 5.88 ms. Post-write drain is 4.61 seconds. Observed backlog fluctuates
up to 386 transactions and sampled publication-latency upper bound p95 is 4.98
seconds. Thus the short comparison's 39-transaction peak is not a general backlog
bound. One mirror-first pair does not establish long-duration stability or a
sustainable capacity maximum. The retained evidence includes exact fixture inputs
and runtime image identities.


## Physical WAL filesystem exhaustion

The [WAL ENOSPC qualification](evidence/2026-11-15-local-wal-enospc.json) passes
12 checks. The fixture places WAL in a private 128-MiB tmpfs, fills only that
filesystem, and observes an actual `pg_wal/xlogtemp` allocation failure with
`No space left on device`. PostgreSQL PANICs and exits after recovery also runs
out of space. After removing the private filler, the test restarts PostgreSQL
with its original options. Committed application heaps and mirrors match, new
application writes replicate, one owned slot remains and cleanup succeeds.

This confirms the shared-instance availability boundary: a full WAL filesystem
can stop application PostgreSQL. Journal limits and asynchronous publication do
not prevent it. Monitor retained WAL and disk headroom; bound retention below
available space with room for ordinary database WAL and checkpoint activity.
Retention invalidation trades mirror continuity for releasing old WAL and requires
a fresh snapshot as described above.

`make local-iceberg-wal-disk-test` uses a separate fault-only Compose override and
keeps the container alive while repairing the database. Its volatile WAL tmpfs
is unsuitable for real data. The test establishes neither container/host power-loss
recovery while full PGDATA ENOSPC is covered by the separate fixture below.


The [full PGDATA ENOSPC qualification](evidence/2026-11-15-local-pgdata-enospc.json)
passes 12 checks with a private 512-MiB data filesystem shared by PostgreSQL and
its engine. An application INSERT encounters actual `base/...` file-extension
ENOSPC and aborts. Previously committed heaps remain intact. Removing the filler
restores writes; exact mirror comparison, subsequent streaming, slot ownership
and cleanup all pass. This is a bounded local data-file failure, not a general
no-data-loss guarantee for every disk, checkpoint or ambiguous COMMIT failure.

`make local-iceberg-pgdata-disk-test` runs this separate volatile fault fixture.
Neither disk profile belongs in an application deployment. Production filesystem,
persistence/power-loss behavior, transaction mix and sustained contention still
need qualification on representative disposable infrastructure.


## Comparison with separate-instance pull

The [counterbalanced local comparison](local-pull-comparison.md) uses the same
runtime, publisher, grouping, journal limits and storage for both topologies.
Eight fresh fixtures pass 56 checks. Total CPU is similar across the tested
INSERT and mixed workloads; remote pull substantially reduces application-side
CPU, memory and WAL by placing journal/publication work on its destination.
Same-instance mirroring consolidates deployment and shares the application
instance’s resource/failure boundary. These results do not establish a production
capacity maximum or a general performance winner. Reproduce with
`make local-iceberg-pull-compare`.


## DELETE predicate performance

The [ordered DELETE optimization](local-delete-filter.md) adds typed engine key
filtering while preserving per-change order, trigger behavior and exact-match
checks. Counterbalanced local mixed-workload comparisons reduce median CPU by
21% on initially empty mirrors and 35% with 20,000 existing rows per table.
These gains do not establish lower WAL or application COMMIT latency. Packaged
correctness, type, migration, crash and logical compatibility checks pass; see
the report for eligibility, measured ranges and reproducible commands.


## Coalescing between batches

The [publication-window report](local-coalescing-windows.md) fixes a bypass where
a launched publisher drained follow-up arrivals immediately. With a nonzero
interval, a live-stream child now publishes one batch before the supervisor
rechecks delay, pressure and stop state. Immediate mode and receiver-exit draining
retain their existing budgets. New flush-reason events explain each decision.
In repeated local mixed workloads, median CPU falls about 31% and mirror requests
about 16%, with a higher sampled publication-latency upper bound. The report keeps
that latency trade-off and the pressure-path measurements alongside the gains.


## Wide UPDATE row conversion

The [wide UPDATE report](local-update-conversion.md) describes a convert-once
path for supported scalar targets with at least 32 assigned columns and 16 rows
per run. Reversed-order full-pipeline trials reduce median CPU about 10% in the
48-column, 32-row workload. Narrow and single-row updates retain the prior path;
no general UPDATE or latency speedup is established. JSONB handling, partial
values, exact-match checks, schema evolution and wide crash recovery are qualified.

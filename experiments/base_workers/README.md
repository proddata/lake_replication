# pg_extension_base launcher prototype

Run `python3 experiments/base_workers/probe.py` after building a native release
archive with `make install-bundles`. The native archive must match the current
worker, migration, and extension sources. The probe uses disposable qualification
containers and volumes, and always removes them. Results and logs go under
`.qualification/`. It never operates on the development lab.

The minimal adapter deliberately exposes migration gaps. To test the prototype
with inherited-binding fences and explicit process cleanup, run:

```sh
python3 experiments/base_workers/probe.py --hardened
```

This is an isolated adapter for the real C launcher and the real Rust workers.
It is not part of the release bundle. `adapt.py` edits only a copy inside the
experimental Docker image; the production extension source is unchanged.

The adapter disables static background-worker registration, adds the
pg_extension_base preload annotation and direct dependency, and registers an
`internal -> internal` SQL entry point during extension creation/upgrade. That
entry point uses the database connection already provided by the base framework
and returns a positive restart delay after the original launcher loop stops.
The disposable destination explicitly preloads only `pg_extension_base` and
does not configure `lake_replication.database_name` or session preloading.

Coverage includes the existing package/upgrade/replication/credential/recovery
probes, extension CREATE/DROP rollback, two simultaneous destination databases,
launcher replacement without postmaster restart, server restart, database
template cloning, failed and committed DROP DATABASE, and extension removal
while a real external supervisor is active. Failed exploratory cases are
retained as findings while remaining independent cases continue.

The stronger shutdown test pauses the external supervisor before dropping its
extension. Ordinary idle supervisors can exit when their control tables vanish;
that alone does not prove explicit cleanup. The hardened adapter uses separate
process groups, an interruptible child wait, clean cancellation, and a Linux
child subreaper to collect orphaned receiver/publisher descendants. Merely
killing a process group can leave those descendants adopted by a PostgreSQL
postmaster running as PID 1 and trigger a cluster restart.

Copied subscriptions are fenced with a durable error instead of being silently
redirected into a cloned Iceberg catalog. The prototype compares database names;
that is deliberately conservative and also fences renames. A production design
needs an explicit database identity and clone/restore/rename policy.

Production adoption still requires an upgrade strategy that prevents the old
static launcher and new per-database base workers from running together. This
experiment does not change the shipped launcher, packaging, or supported
deployment contract, and cannot by itself qualify an in-place migration.

The retained investigation is in
[`docs/pg-extension-base-prototype.md`](../../docs/pg-extension-base-prototype.md).
The minimal adapter recorded 81 passing and three failing assertions; the
hardened adapter originally passed all 85 checks. The expanded event-driven
version passes 111 checks; the comparable static baseline passes 81. See the
[evaluation results](../../docs/pg-extension-base-evaluation-results.md).

## Comparable launcher variants

Shared hardening now lives in `harden()` in `adapt.py` and `runtime.c.inc`.
Both experimental variants use durable handles with explicit release, shutdown
notifications, child process groups/subreaping, bounded descendant shutdown,
transition-only binding diagnostics, and SIGCHLD/latch-driven steady-state waits.
The remaining 10 ms polling is restricted to the bounded shutdown callback.
The production source and release archive remain unchanged.

Run the static comparison baseline through the applicable packaged replication,
credential, upgrade/backup, recovery, and repeated worker-retirement probes:

```sh
python3 experiments/base_workers/probe.py --static
python3 experiments/base_workers/probe.py --hardened
```

The static baseline supports its existing single configured destination; base-only
multi-database and extension ownership assertions are intentionally excluded from
that baseline. Both hardened variants also exercise ten enable/disable cycles
and compare the launcher's handle-context usage before/after retirement.

## Performance screening

```sh
python3 experiments/base_workers/compare.py
```

This builds both variants from the same checked release data-path sources and
runs five alternating repetitions in fresh disposable fixtures. Defaults use
one database/subscription, 40 transactions of 100 inserts at a requested 100 ms
interval, a separate equal warm-up, and ten seconds of idle observation.
Measurements include real source commit timestamps (`track_commit_timestamp`),
first observed destination visibility, correctness checks, CPU/memory counters,
WAL snapshots, and stable wrapper context switches. Persistent psql sessions
keep Docker client startup outside each timed SQL operation. Read the raw
observer query times alongside the configured polling interval: reported lag
includes observation delay and is not an exact publication timestamp.

The source, receiver, and publisher are active together through the real
supervisor. That supervisor still runs receiver/publisher cycles sequentially;
this is an end-to-end workload, not a change to its pipeline architecture.
Reported rows/second includes backlog catch-up and is not a saturation capacity
claim. Resource snapshots include sampler overhead and are not sampled peaks.

To repeat with an identical explicit preload setting:

```sh
python3 experiments/base_workers/compare.py --base-preload
```

This keeps `lake_replication` in the explicit setting for both variants while the
base adapter still suppresses its static launcher and uses base-worker ownership.
The control-file annotation already causes startup preloading in base mode, so
this does not isolate loading cost or imply per-connection dynamic loading.
Treat it as a configuration/repeatability control, not a distinct loading path.

Results, source snapshots, image/host provenance, per-run logs, and summaries
are retained under `.qualification/*_comparison/`. A successful run establishes
measurement completion and row correctness; adoption/performance gates require
review of the results and their variability. The initial single-subscription
screening does not qualify capacity, mixed mutations, database identity,
production upgrades, architecture compatibility, or long-running leak behavior.

See the [evaluation plan](../../docs/pg-extension-base-evaluation-plan.md) and
[proposed operational contract](../../docs/pg-extension-base-operational-contract.md).

## Extended measurement and analysis

The [extended protocol](../../docs/pg-extension-base-performance-protocol.md)
separates below-capacity load from backlog/catch-up, with five matched pairs
per cohort. It records source scheduling, backlog traces, first-observation
bounds and source/publication transaction commit timestamps. The experimental
C runtime stays fixed across the cohorts.

```sh
# Observer frequency calibration, reversing order each repetition.
python3 experiments/base_workers/compare.py --variants static \
  --poll-intervals-ms 100 1000 --repetitions 3 --transactions 100 \
  --warmup-transactions 10 --idle-seconds 3 --interval-ms 100

# Validate journal polling against direct Iceberg observation at lower load.
python3 experiments/base_workers/compare.py --variants static \
  --observers iceberg journal --repetitions 2 --transactions 60 \
  --warmup-transactions 10 --idle-seconds 3 --interval-ms 500

# Matched steady-load cohort; use --interval-ms 100 for backlog/catch-up.
python3 experiments/base_workers/compare.py --observer journal --repetitions 5 \
  --transactions 120 --warmup-transactions 10 --idle-seconds 5 --interval-ms 500
python3 experiments/base_workers/analyze.py .qualification/<run>/results.json \
  --profile steady --output .qualification/<run>/analysis.json
```

Journal polling counts committed published transactions in this isolated fixture.
Its marker mapping requires exactly one benchmark insert transaction per marker,
no other publication traffic, and a fresh fixture. After each phase, source XIDs
must match all expected published rows and non-null destination commit timestamps.
This is test instrumentation, not a general replication monitoring API.

`pg_xact_commit_timestamp` is enabled on both servers. The destination row's
`xmin` identifies its publication transaction; `published_at` alone is a timestamp
inside that transaction and is not used as its commit time. Publication-commit
latency and throughput are separate from first-observed completion. Journal
polling is not labeled as direct Iceberg visibility; final Iceberg contents are
verified independently. Earlier retained files use older measurement field names;
read the schema and observer type before comparing results.

Analysis resamples whole matched pairs and records the input/analyzer hashes.
It distinguishes an offered-load ceiling from capacity, tests the frozen local
latency/backlog limits, and labels wide comparisons inconclusive. It never
qualifies production adoption. Timing-model and decision tests run in `make check`.

## Stopped-state upgrade experiment

`upgrade.py` and `Dockerfile.upgrade` prepare a separate upgrade-only fixture.
The starting image receives the real packaged static extension. With its
subscription disabled and all external processes drained, destination and pgduck
stop before the image is replaced. The experimental update target is
`0.1.0-beta.2-prototype`; its SQL adds only the base-worker entry/registration.

The test retains the old explicit/session preload and database settings,
exercises a rolled-back SQL update and a committed retry, verifies one base
owner with no static owner, then resumes from the retained source slot and
checkpoint and tests a further restart. This target is not a release version,
a fresh-install package, a restart-free transition, or an SQL downgrade path.
Run separately from performance measurements:

```sh
python3 experiments/base_workers/upgrade.py
```

The upgrade experiment does not implement clone/restore identity, a live-owner
handoff, or physical takeover safety. Those gates remain separate.

## Forced-crash ownership experiment

```sh
python3 experiments/base_workers/faults.py
# Isolate orphan reaping from PostgreSQL as container PID 1:
python3 experiments/base_workers/faults.py --with-init \
  --cases launcher-paused-children-kill wrapper-paused-children-kill
```

Each injection uses a fresh private fixture. Process signals verify PID start
time, and paused cases require an actually stopped receiver descendant.
Expected PostgreSQL crash recovery is counted separately from untracked
external children. Results retain source snapshots, processes and logs under
`.qualification/*_faults/`; a nonzero exit can mean a runtime finding or a
fixture precondition failure, which the report distinguishes. See the
[forced-crash findings](../../docs/pg-extension-base-fault-results.md) before
interpreting earlier successful lifecycle or performance cohorts.

### Bounded source-slot recovery overlay

```sh
python3 experiments/base_workers/faults.py --with-init --recovery
```

`Dockerfile.recovery` rebuilds only the Rust supervisor from a disposable copy
patched by `recovery.py` and `recovery.rs.inc`. It runs the seven original Rust
supervisor tests plus three recovery tests before installation. The archived
receiver, publisher and initial-sync binaries and production source tree stay
unchanged. This is an experimental image, not a new release archive.

The supervisor validates source identity, slot shape, WAL/checkpoint and timeline
safety before deciding whether an active slot is temporarily retryable. A
per-endpoint monotonic budget of five seconds survives retry cycles. A validated inactive
endpoint clears the contention budgets, including stale alternative endpoints. Backoff is capped by the remaining
budget; expiry produces a durable source error. Query/connection execution can
add time beyond the budget, so it is not an absolute process execution deadline.
Workers never consume an active slot or terminate its holder.

The full command runs the six process-kill cases plus an independent
`pg_recvlogical` holder case. The latter verifies that persistent contention
expires, retains the same external slot holder, and leaves destination contents
unchanged. Fatal source errors still require explicit validated recovery;
ENABLE does not automatically clear them.

## Destination origin and cleanup experiment

```sh
# Probe the exact recovery-qualified local image by its verified image ID.
python3 experiments/base_workers/identity.py
# Overlay origin tuple checks; the full Iceberg restore limitation still yields findings.
python3 experiments/base_workers/identity.py --binding
# Recheck paused-owner crashes against the combined overlay.
python3 experiments/base_workers/faults.py --with-init --binding \
  --cases launcher-paused-children-kill wrapper-paused-children-kill
```

`binding.py` modifies only a disposable C source copy. `binding.sql` records
system identifier/OID for new native subscription configurations without
backfilling inherited rows. The checks cover launcher/wrapper dispatch and
source-slot cleanup; they do not implement publication-side leases or rebind.
The identity report separates `binding_safety_passed` from overall `passed`:
full restoration of an existing writable Iceberg location remains a finding.

The control-plane restore uses an explicitly stopped ordinary placeholder
relation to resolve regclass metadata. It does not restore the Iceberg data.
Same-name inherited bindings stay stopped on the baseline; the revised image
also tests that attempted ENABLE is fenced. Cloned DROP is deliberately tested
only in this private fixture, with the original stopped, to detect unintended
source-slot deletion. See the [identity results](../../docs/pg-extension-base-identity-results.md).

## Fresh rebuild and rollback rehearsal

```sh
python3 experiments/base_workers/rebuild.py
```

This reuses the exact identity-qualified local image, checked by immutable image
ID and recorded runtime fingerprints. It creates a fresh database from
`template0`, checks empty control-plane state and storage, and creates a new
source slot. It tests current-state initial copy, mixed mutations, unchanged
original data/metadata/checkpoints, restart, retained-slot rollback and candidate
cleanup while an inherited clone stays fenced. It never switches consumer
traffic or rebinds existing Iceberg objects. See the [procedure](../../docs/pg-extension-base-rebuild-procedure.md).

## Publication transactions and physical copies

```sh
python3 experiments/base_workers/ownership.py
```

This builds a separate publication overlay on the exact local identity-qualified
image. It adds startup deployment identity and guarded native worker SQL entry
points, then tests lock ordering, real receiver/publisher rejection, and physical
backups under different/empty runtime identities. The copied servers disable
independent Iceberg autovacuum; no shared-object takeover is attempted. Default
empty identity fails closed. Startup profiles must be supplied independently of
PGDATA, and a duplicated profile still duplicates authorization. See the
[publication results](../../docs/pg-extension-base-publication-results.md).

## Manually fenced cold transfer rehearsal

```sh
python3 experiments/base_workers/transfer.py
```

This reuses the exact publication-qualified image. It backs up a drained owner,
keeps the candidate quarantined, refuses transfer while either original writer
container runs, checks a frozen checkpoint/config/catalog manifest, then grants
the external profile and resumes with the same source slot and Iceberg location.
The candidate uses its own engine/socket. It also refuses stale failback after
source acknowledgements advance. Docker inventory inspection is a private
controller test, not a production fencing service. See the
[cold-transfer procedure/results](../../docs/pg-extension-base-cold-transfer.md).

## Current prototype scaling and matched performance

```sh
python3 experiments/base_workers/scale.py
# Run either part independently:
python3 experiments/base_workers/scale.py --skip-performance
python3 experiments/base_workers/scale.py --skip-scale
```

The harness verifies the immutable publication-qualified image and source/archive
provenance. It covers 1/1, 1/4, 1/16, 4/4 and 4/16 database/total-subscription
combinations. Every case checks real initial copy, concurrent updates, restart,
disable/drain, source-slot cleanup and extension removal. Inventory captures
PostgreSQL worker types, base registrations, raw child processes and cgroup
memory. A failing case is retained and repeated with 64 worker slots after the
default 24-slot attempt; the higher-budget result does not erase the finding.

The test engine uses 2 GB and one DuckDB query thread. The original fixture's
1 GB/default-thread engine exhausted memory on four concurrent updates after
restart. This resource setting is explicit and identical for both launchers;
results cannot be compared directly to the earlier engine configuration.

Performance uses the existing steady/catch-up protocol with five alternating
matched pairs each. `Dockerfile.current-static` builds the static control on the
same publication-qualified image, retains its recovery supervisor, and applies
the same binding/publication guards. Only the base variant registers its launcher
with `pg_extension_base`. The static control remains single-database. All test
fixtures are private and removed after each case; production sources are unchanged.

See the [scaling results and execution-model decision](../../docs/pg-extension-base-scaling-results.md).

## Bounded write admission

```sh
python3 experiments/base_workers/admission_probe.py
```

This adds one transaction-scoped permit across all destination databases, before
the existing ownership row check in the ten guarded native worker entry points.
It tests queue ordering, cancellation, transaction/savepoint cleanup, queued
subscription disable/resume and forced-holder crash recovery, then repeats the
previously failing 4-database/16-subscription restart with 64 worker slots and the
same 2 GB engine. The admin `prototype_writer_admission` view reports waiting and
admitted backends. It remains a fresh-install experiment; wrapper counts are
unchanged and post-admission performance is not qualified. See the
[admission results](../../docs/pg-extension-base-admission-results.md).

## Admission throughput, lag and coexistence

```sh
python3 experiments/base_workers/admission_measure.py --copy-rows 2000000
# Resume only the five-pair catch-up cohort:
python3 experiments/base_workers/admission_measure.py --skip-fairness --profiles catchup
```

This compares the exact pre/post-admission base images. It records source and
publication commit times, copy progress, permit waits and final Iceberg data.
Clock steps against monotonic insertion/observation windows invalidate a cohort
before margin assessment. On macOS, prefix the command with `caffeinate -i` to
prevent idle sleep only for that process; clock validation still applies.

The [results](../../docs/pg-extension-base-admission-performance-results.md) retain
both valid measurements and the rejected clock-disrupted run. Steady load passes;
catch-up margins remain inconclusive and large-copy maximum-gap/catch-up targets
fail. Dataset preparation before slots isolates the independent
[empty-stream receiver defect](../../docs/empty-filtered-stream-diagnostic.md);
it does not fix that defect.

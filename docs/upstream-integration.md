# Upstream source and integration

## Project rename

The project and GitHub repository are named `lake_replication`; the former
repository name was `pg_lake_replication`. Release archives now use
`lake_replication-VERSION[-linux-ARCH].tar.gz`. The PostgreSQL extension remains
`lake_replication` and the worker executable names remain `lake-replication-*`.
Upstream `pg_lake` and its own source filenames retain their upstream names.
Retained qualification JSON describes the original artifacts and is unchanged.

Local checkout directory names are independent of the repository name; an
existing checkout can keep its current directory. Set its Git remote to
`https://github.com/proddata/lake_replication.git`.

Docker Compose project names and image names now start with `lake-replication`.
Existing containers and volumes retain their old names. For an existing lab,
use `docker compose -p pg-lake-replication-lab -f deploy/docker-compose/compose.yml stop`
before starting a new stack. To reuse that lab's existing data, supply its old
project name explicitly with the renamed image:

```sh
make lab-build
docker compose -p pg-lake-replication-lab -f deploy/docker-compose/compose.yml up -d
```

The same rule applies to the demo and other labs. Default Make targets select
the new project names and therefore new named volumes. No existing containers,
volumes, or installed configuration directories are renamed automatically.

## Local source and builds

`vendor/pg_lake` is a Git submodule pinned to the qualified revision
`8180647d4535fdb1ed223f92f301ba6094a8ec3e`. Its Git tree is the source of truth
for inspecting upstream APIs and building the development lab and demo.

```sh
git submodule update --init vendor/pg_lake
make lab-build
# Or:
make poc-build
```

The Make targets export a local Git bundle to `.upstream/pg_lake.bundle`.
Docker clones that bundle and checks out the selected commit. This preserves
upstream Git metadata and nested submodule declarations without passing the
parent repository's `.git` directory into Docker. The raw vendor tree is also
excluded from the Docker context to avoid sending it twice. Preparation never
fetches, switches the submodule checkout, or includes uncommitted source edits.
Commit upstream edits before building them; tracked edits are rejected.

For a direct Docker build, first run `make upstream-source`, then pass
`--build-arg PG_LAKE_REV=<full SHA>` if selecting a revision other than the
Dockerfile's default. Make-based lab/demo builds select the submodule HEAD by
default. To test another already available revision:

```sh
make lab-build PG_LAKE_REV=f609ecb17891383f021aebb0e249fe2994ed57bb
```

Compatibility qualification prepares the same bundle automatically when it
needs to build a prerequisite image. Every selected revision must be reachable
from the local submodule HEAD, branches, or tags. To make a new upstream
revision available, explicitly fetch it into a local branch first:

```sh
git -C vendor/pg_lake fetch origin <full-SHA>:refs/heads/qualification-candidate
```

Updating the default pin is a deliberate change: check out the new commit in
the submodule, qualify it, then stage `vendor/pg_lake` in the parent repository.
Update the Dockerfile/Compose fallback revisions and compatibility documentation
when adopting a new default.

This does not make the entire build offline. Docker images, Debian packages,
vcpkg, nested upstream submodules, and the DuckDB release tag still need their
initial downloads. The exported customer artifact continues to contain only
lake_replication, and does not bundle or replace pg_lake.

## Worker integration assessment

An isolated adapter has now been exercised against the real launcher and Rust
workers. The minimal adapter passed 81 checks and exposed three failed assertions;
the adapter with lifecycle fixes passed all 85. See the
[prototype investigation](pg-extension-base-prototype.md) for evidence, the
production handle-lifetime issue, cleanup requirements, and remaining migration
work. The shipped extension still uses the static launcher.
Follow-up work has extracted common experimental hardening for a fair static/base
comparison and added performance screening; see the
[evaluation results](pg-extension-base-evaluation-results.md). Production
adoption remains gated on that evaluation.
The [extended evaluation](pg-extension-base-extended-results.md) adds longer
matched performance runs and a successful 42-check stopped-state upgrade from
the actual release archive. Catch-up meets the local throughput margin;
steady-load relative latency remains inconclusive. The shipped runtime and
release blockers remain unchanged.

Assessment against the pinned source: adopt `pg_extension_base` for extension
loading and the per-database launcher lifecycle in a separate runtime change.
Keep replication state, subscription supervision, and the SQL publication path
owned by `lake_replication`.

The relevant upstream contracts are available locally:

- `vendor/pg_lake/pg_extension_base/README.md`: library preloading and base workers.
- `vendor/pg_lake/pg_extension_base/pg_extension_base--1.6.sql`:
  `extension_base.register_worker(text, regproc)` and restricted registration access.
- `vendor/pg_lake/pg_extension_base/src/base_worker_launcher.c`: database discovery,
  extension ownership, worker restart/backoff, and DROP lifecycle handling.
- `vendor/pg_lake/pg_extension_base/tests/pg_extension_base_test_scheduler/`:
  an example SQL registration and C worker entry point.
- `vendor/pg_lake/pg_lake_table/pg_lake_table--3.0.sql`: pg_lake's own vacuum
  worker registration using the same infrastructure.

Today, our `_PG_init` registers one static launcher, and
`lake_replication.database_name` selects its single database. Base workers
already provide an extension-owned worker in each database where registered,
including restart, CREATE/DROP EXTENSION, and database template lifecycle.
This can replace static launcher registration and the single-database setting.

The migration should:

1. Declare `pg_extension_base` as an explicit dependency and use its control-file
   preload annotation after verifying hook ordering and fresh-session behavior.
2. Expose a C SQL function `(internal) RETURNS internal` and register it from
   extension install/upgrade SQL. A base worker is already connected to its
   database: extract the launcher loop instead of calling its current entry
   point, which calls `BackgroundWorkerInitializeConnection` again.
3. Preserve the per-subscription worker handles, generation fencing, and
   fork/wait supervision. Base workers do not directly supervise external Rust
   processes or replace subscription retry/credential semantics.
4. Derive child connection settings from the worker's current database and
   prevent both legacy and base-worker launchers from managing it during upgrade.
5. Verify committed/aborted CREATE and DROP, server restart, two destination
   databases, template cloning, failed DROP DATABASE, launcher failure,
   subscription disable/drop, and external-child shutdown. Account for the
   increased `max_worker_processes` capacity required by the base framework.

Upstream documents worker stopping as best effort. Existing extension-presence
checks, interrupt handling, and child termination remain necessary.

No direct `pg_lake_engine` C integration is needed for this migration. The
engine supplies query execution; it is not the extension lifecycle layer.
Retain the current `pg_lake` dependency and SQL/FDW publication boundary until
a measured requirement justifies a deeper API dependency. Runtime migration
has not been implemented or qualified by this source/build change.

The [identity evaluation](pg-extension-base-identity-results.md) shows another
layer that pg_extension_base lifecycle registration does not provide: destination
origin and source-slot cleanup authorization. The isolated tuple checks fence
same-name copies and prevent cloned DROP from deleting the original slot.
A full dump of a writable Iceberg table still cannot recreate its non-empty
location; restore/rebuild/rebind semantics must be qualified with pg_lake before
production adoption.

A [fresh rebuild rehearsal](pg-extension-base-rebuild-procedure.md) now passes
on the combined prototype, including real Iceberg copy/streaming, restart and
replication rollback to the retained old slot. This provides an evaluated new-
destination recovery route without bypassing pg_lake location checks. Existing-
object rebind, physical takeover and production adoption remain unqualified.

The [publication ownership experiment](pg-extension-base-publication-results.md)
now guards native worker write transactions and rejects physical copies under
different/empty external startup identities. It also shows why lifecycle and
replication checks alone do not establish exclusive access to shared storage:
pg_lake maintenance is an independent writer, and duplicated runtime profiles
still duplicate authorization. Qualify external fencing or a storage-fenced
lease before supporting physical takeover.

A [controlled cold transfer](pg-extension-base-cold-transfer.md) now resumes
the same Iceberg objects and source slot after both old PostgreSQL and pgduck
stop, with maintenance disabled and the copied snapshot validated. This provides
a narrow evaluated physical-move procedure, while production fencing, automatic
failover and arbitrary logical restore remain outside the qualified scope.

The [current scaling/performance evaluation](pg-extension-base-scaling-results.md)
supports retaining base lifecycle management: five matched pairs in each
single-subscription workload pass the proposed latency/throughput margins with
the latest recovery/publication checks. It also rejects adopting the execution
model unchanged: four databases with 16 subscriptions exhaust 24 worker slots,
and the 64-slot repeat exhausts the shared 2 GB engine during restart recovery.
Bound aggregate publication/initial-copy concurrency and evaluate a fair worker
pool before broader adoption; switching lifecycle frameworks does not supply
that resource policy.

An isolated [bounded-write admission overlay](pg-extension-base-admission-results.md)
now passes 118 checks, including the previously failing 4-database/16-subscription
restart under the same 2 GB engine limit. One transaction permit spans the native
publication/initial-copy entry points across databases; queue cancellation and
forced-holder crash recovery pass. This does not reduce wrapper counts or close
production release gates. The [follow-up evaluation](pg-extension-base-admission-performance-results.md)
now passes the steady-load margin, leaves clean catch-up margins inconclusive,
and reproduces roughly eight-second stream gaps during a large copy. A
clock-disrupted cohort is explicitly rejected and replaced. The independent
[empty filtered-stream receiver defect](empty-filtered-stream-diagnostic.md)
also reproduces before/after admission. Fix that durable receive/checkpoint path
and refine admission for bulk work before broader adoption; retain base lifecycle
management as a separate decision.

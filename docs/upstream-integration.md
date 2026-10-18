# Upstream source and integration

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

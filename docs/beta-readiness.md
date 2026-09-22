# Beta readiness plan

Status: planning document, not a completed milestone. It sets the scope and
order for turning the current lab/PoC prototype into something a design
partner can install next to their own `pg_lake` and run unattended. See
[the feasibility report](feasibility.md) for the evidence this plan is
reacting to, and [milestones 1 through 25](milestone-1.md) for what already
works end to end.

## Distribution constraint: a separate artifact, installed next to pg_lake

This shapes almost every other decision below, so it comes first.

This project must ship as something a customer installs **onto their own,
already-running PostgreSQL + `pg_lake` instance** -- not as a fork, not as a
container image that builds `pg_lake` from source, and not as anything that
requires replacing how they already installed `pg_lake`. Today's
`deploy/docker-compose/` lab and `deploy/poc/` demo both build `pg_lake` from
source inside their own image; that is correct for a lab/demo (it pins an
exact, reproducible environment to test against) and wrong as a shipped
artifact (a real customer's `pg_lake` install is not ours to bundle, pin, or
rebuild).

Concretely, the shipped beta artifact is three things, each installable
independently against an existing `pg_lake`-enabled PostgreSQL:

1. **`lake_replication`, packaged as a real, versioned PostgreSQL extension**
   (`CREATE EXTENSION lake_replication;`), not the current raw numbered
   `migrations/*.sql` files applied by hand or by `docker-entrypoint-initdb.d`
   mounts. This needs a `lake_replication.control` file, a base
   `lake_replication--X.Y.sql` built from the current migrations concatenated
   in order, and `--X.Y--X.Z.sql` upgrade scripts going forward so
   `ALTER EXTENSION lake_replication UPDATE` is how a beta customer moves
   between releases -- the same mechanism `pg_lake` itself and every other
   real Postgres extension uses. This is a mechanical but non-trivial
   conversion. The initial `0.1.0` base extension is now generated from the
   ordered migrations by `extensions/lake_replication`; every migration was
   written to run once, forward-only,
   inside `docker-entrypoint-initdb.d`; converting to extension-versioned
   upgrade scripts means auditing each one for idempotency and ordering
   assumptions.
2. **`lake_subscription_sugar`, packaged the same way**, once it clears the
   hardening work below. It already builds as a normal PGXS module
   (`extensions/lake_subscription_sugar/Makefile`); the packaging step itself
   is small. The hardening is not.
3. **The four Rust worker binaries as a versioned release artifact** --
   cross-compiled ahead of time for the platforms we support (starting with
   the same Debian bookworm glibc the lab targets), published with a version
   number, and installed to a directory on `PATH` (matching where
   `lake_subscription_sugar` already expects them:
   `WORKER_BIN_DIR "/usr/local/bin"` in `lake_subscription_sugar.c`). A
   customer never needs a Rust toolchain.

A compatibility matrix must ship with every release: supported PostgreSQL
major version(s) (today: 18 only -- 16/17/19 are explicitly untested per the
feasibility report), and the `pg_lake` revision range validated against. The
lab's exact-SHA pin (`PG_LAKE_REV`) becomes "validated against pg_lake commit
X" in release notes, not a requirement that the customer's `pg_lake` matches
that SHA exactly -- that needs its own compatibility-testing workstream (see
below), since we do not control what `pg_lake` revision a customer runs.

`deploy/docker-compose/` (the milestone lab) and `deploy/poc/` (the demo)
both stay as they are for their own purposes -- pinned, from-source, easy to
reset. Neither becomes the beta install path. A new `deploy/install/` (or
similar) artifact is what a customer actually uses: install docs plus the
packaged extensions/binaries above, explicitly assuming "you already have
PostgreSQL and `pg_lake` running."

## Known defect, deferred: REST-catalog destinations

Status: root cause confirmed; fix deliberately deferred, not scheduled for
beta. See [Milestone 26](milestone-26.md) for the full writeup.

`pg_lake`'s local Iceberg catalog is itself Iceberg-JDBC-catalog-compatible
(`iceberg_tables` matches the Iceberg catalog JDBC driver's schema), so
Spark/pyiceberg/iceberg-rust readers already work against it directly. A
customer who only needs external tools to *read* their Iceberg tables never
needs a REST catalog, and is already covered by all of this project's
existing crash-safety evidence (feasibility report, Milestone 1's journal,
Milestone 24's maintenance bounds). **Beta targets that path only.**

A REST catalog (Polaris, Unity Catalog OSS, a managed Glue/Nessie endpoint,
etc.) is a materially different code path some customers will eventually
want, e.g. to share write ownership of a table's catalog entry with a
multi-engine lakehouse. Milestone 26 confirmed it has a real gap: `pg_lake`'s
REST-catalog commit POST runs in the `XACT_EVENT_COMMIT` callback --
strictly after the local PostgreSQL transaction is already durable -- and on
any HTTP failure it only logs a `WARNING` and unconditionally discards the
pending request, with no retry queue or durable failure record anywhere in
`pg_lake`. A checkpoint this project advances past a REST-catalog-backed
batch cannot be trusted to mean the catalog agrees, and `pg_lake` will never
tell us otherwise later.

**Decision: known defect, not a beta blocker.** Beta ships with REST-catalog
destinations explicitly unsupported/undocumented (or flagged experimental
if a customer insists on trying it), rather than delaying beta on building
a crash-injection lab and the read-back-verification logic this would need.
When picked back up, the fix is known and unchanged from Milestone 26: this
project would need to independently read back the REST catalog's
`metadata_location` after each publish and treat a mismatch as "retry the
write" or "flag for manual reconciliation" -- real worker-side logic, not a
config flag -- validated against a real REST catalog (the single-tenant
reference fixture used for the Milestone 26 spike needed two lab-only shims
that a multi-tenant implementation like Polaris or Lakekeeper should not).

## Ordered workstreams (local catalog)

### 1. Harden the subscription control surface

Status: all four sub-items below are done. See
[Milestone 27](milestone-27.md) and [Milestone 28](milestone-28.md) for the
full writeups.

`lake_subscription_sugar.c` said what it was in its own top-of-file comment:
"Experiment only." Specific gaps identified, and their current state:

- **Authorization -- done.** Any role that could load the extension and
  issue `CREATE/ALTER/DROP SUBSCRIPTION ... WITH (lake_target = 'iceberg')`
  used to get full dblink-based source access and the ability to spawn OS
  processes as this backend's user, with no gate at all. `RequireAuthorized()`
  now requires superuser or membership in a new `lake_replication_admin`
  role before any of that happens.
- **Credential handling -- documented, not eliminated.** `record_pending_cleanup`
  still stores the raw `CONNECTION` string verbatim, including any embedded
  password -- the same tradeoff PostgreSQL's own `pg_subscription.subconninfo`
  makes, and `pending_cleanup` now carries an explicit `REVOKE ALL FROM
  PUBLIC` matching that table's protection. Milestone 27 also surfaced a
  real, previously-undocumented constraint: `dblink` refuses any
  non-superuser connection unless the source's auth method actually
  verified a password during the handshake (`trust`/`peer` never counts,
  regardless of what the connection string contains) -- so a non-superuser
  `lake_replication_admin` member can only use this against a
  SCRAM-authenticated source (Milestone 22), and that password necessarily
  ends up in `pending_cleanup`. Beta install docs should say this plainly:
  SCRAM on the source whenever anyone other than a superuser will run
  `CREATE SUBSCRIPTION`.
- **Configurable worker log paths -- done.** Hardcoded `/tmp/lake_sugar_*.log`
  is now `lake_subscription_sugar.log_directory` (a `PGC_SUSET` GUC).
- **Process supervision -- done.** The supervisor is no longer a raw
  double-forked, detached process. A single, permanently-running launcher
  background worker (registered via `shared_preload_libraries`, the same
  mechanism `pg_cron` is built on) starts each subscription's supervisor
  as a genuine PostgreSQL background worker, restarts it automatically if
  it dies (verified: `kill -9` on the supervisor produces a new PID within
  about a second, with zero disruption to the rest of the database), and
  cleanly terminates it (never leaving it to be silently respawned) on
  `DROP`/`ALTER ... DISABLE`. This needed a real fix along the way: naively
  `execve()`-ing the supervisor binary directly into the background worker
  process made an ordinary application-level error (calling
  `std::process::exit(1)`) look to PostgreSQL exactly like a real crash,
  which terminated every other backend and reinitialized shared memory --
  see Milestone 28 for the fork/waitpid fix and why `bgw_restart_time`
  could not be used for the obvious reason. Real, confirmed cost: this
  needs `shared_preload_libraries` plus a new
  `lake_subscription_sugar.database_name` GUC, both requiring a restart to
  take effect -- exactly the tradeoff flagged when this option was chosen
  over shelling out to `systemctl`, now paid and working rather than
  estimated.
- **Error surfacing.** Failures still mostly land in per-subscription log
  files and `lake_replication.service_status.last_error`, with no alerting
  path. See observability, below.

### 2. Observability

`crunchy_iceberg.stat_subscription[_rel]` (in
`extensions/lake_subscription_sugar/monitoring_views.sql`) is explicitly
"Experiment only" and SQL-view-based -- fine for demoing, not sufficient for
an on-call engineer. Beta needs the service-status/health data already
tracked in `lake_replication.service_status` exported as real metrics
(Prometheus-style `/metrics` endpoint on the supervisor, or push to whatever
the customer's stack expects) covering at minimum: subscription state,
consecutive failures, receiver/publisher cycle counts, replication lag
(current LSN vs. source), and last error. Structured (not println!)
logging in the worker binaries is a prerequisite.

### 3. Packaging and supply chain

- Convert `migrations/*.sql` into a versioned `lake_replication` extension
  as described above. **In progress:** the generated `0.1.0` base extension,
  PGXS packaging, transaction-wrapper validation, and a clean install/drop/
  recreate integration probe are implemented; upgrade-fixture, artifact, and
  external-instance tests remain.
- Cross-compile and publish the four worker binaries per supported
  platform; stop requiring a Rust toolchain on the target host (today's
  `tests/subscription_sugar/probe.py` cross-build-via-`rust:bookworm`
  fallback is a test convenience, not a distribution mechanism).
  **In progress:** `deploy/install` now produces amd64 and arm64 Linux
  filesystem bundles containing release worker binaries and both extensions,
  with a PostgreSQL 18/pg_lake/architecture-checking installer and an
  exact-artifact install/link/load smoke probe. Tag-driven GitHub automation
  generates SPDX SBOMs and SHA-256 manifests, keylessly signs every published
  file, and creates the release. The first real tagged release and isolated
  clean-host coverage remain.
- Pin and record exact versions the way `docs/dependencies.md` already
  starts to (it flags this as unfinished: no SBOM, no locked transitive
  dependencies, no automated license check in CI). A beta release needs
  that closed out for every dependency that ships in the release artifact
  -- not the lab/lab-only dependencies like MinIO or the crash-injection
  patch, which never leave the lab. **Partially done:** release workflow
  dependencies are commit-SHA pinned and each architecture gets an SPDX JSON
  SBOM; locking Debian packages and automated license-policy enforcement
  remain.
- Publish the compatibility matrix (PostgreSQL major version x validated
  `pg_lake` revision range x OS/glibc target) with every release, and add a
  CI job that installs the packaged extension against at least one
  `pg_lake` build the release was not developed against, to catch silent
  incompatibilities early.

### 4. Compatibility testing beyond the pinned lab SHA

The lab intentionally pins one exact `pg_lake` commit. A beta customer will
not be on that exact commit. Before beta, add a compatibility-testing lane
that runs the existing probe suite (or a trimmed subset of it) against a
small matrix of `pg_lake` revisions/PostgreSQL versions, to establish (and
keep re-validating) the actual supported range, rather than asserting one
pinned SHA works and hoping nearby revisions do too.

## Explicit non-goals for beta

- **REST-catalog destinations.** Known defect (Milestone 26); deliberately
  deferred past beta. See above.
- **Native PostgreSQL `CREATE SUBSCRIPTION` targeting Iceberg directly.**
  Blocked upstream at `CheckSubscriptionRelkind` (SQLSTATE 42809); not this
  project's to fix. The external-consumer architecture is the answer for
  beta too, not a stopgap.
- **Multi-tenant / SaaS-hosted operation.** Beta is "installs into one
  customer's own Postgres," not a hosted multi-tenant control plane.
- **PostgreSQL versions other than 18**, until workstream 4 above actually
  validates them.
- **Automatic replacement of the demo web console.** `deploy/poc/webui` stays
  a demo tool; it is not on the path to being a customer-facing product UI.

## Suggested order

1. Harden the subscription control surface (credentials, process
   supervision, authorization) -- done (Milestones 27, 28). Move on to
   observability, since it touches the same worker/supervisor code paths.
2. Convert `lake_replication` to a proper versioned extension; package
   worker binaries as release artifacts.
3. Stand up the compatibility-testing lane; publish the first compatibility
   matrix alongside the first beta release.
4. Revisit the REST-catalog known defect (Milestone 26) once beta is
   shipped against the local catalog, if/when a customer needs it.

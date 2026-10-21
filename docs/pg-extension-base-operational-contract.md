# Proposed operational contract for base-worker adoption

Status: design constraints for evaluation, not implemented production behavior.
The [extended evaluation](pg-extension-base-extended-results.md) now includes a
42-check stopped-state upgrade experiment. The proposed live/identity behaviors
below remain separate from that tested transition.

A launcher framework change must preserve exclusive ownership of replication
and Iceberg destinations. The earlier prototype fenced only copied destination names. The isolated
[origin-check revision](pg-extension-base-identity-results.md) also checks cluster
system identifier/database OID at dispatch and before source-slot cleanup. It
does not establish the full identity/upgrade contract below.

## Database identity

A candidate origin record combines the PostgreSQL cluster system identifier,
database OID, and database name recorded when the destination is bound. A clone
normally changes the OID; a logical restore elsewhere changes the OID and/or
cluster identifier. A rename preserves the OID but changes the name. Check the
binding before dispatching a supervisor, and check it again in the supervisor
before publication so existing processes cannot bypass a new fence.

This tuple is a detection aid, not a universal uniqueness guarantee. Physical
cluster copies can preserve every field. Supporting physical restore/failover
requires an explicit exclusive-writer procedure or a destination-scoped lease;
never claim that a copied UUID or cluster identifier proves exclusive ownership.
An upgrade must not simply bless inherited bindings without operator review.

| Operation | Proposed behavior |
| --- | --- |
| Ordinary restart in the same database | Resume from durable checkpoints after validating the binding. |
| Database rename | Stop and require explicit validation/rebind; do not silently rewrite the name. |
| Template clone | Keep inherited subscriptions stopped with a durable diagnostic. |
| Logical restore into another database/cluster | Keep inherited subscriptions stopped until an explicit rebuild or validated exclusive takeover. |
| Physical restore / failover | Resume only under a documented exclusive-writer procedure; automatic identity checks alone are insufficient. |
| Empty database / fresh extension installation | Register one launcher; create destination bindings only when subscriptions are created. |

Rename needs particular care: pinned pg_lake uses the current database name in
Iceberg catalog identity (for example
`vendor/pg_lake/pg_lake_iceberg/src/iceberg/catalog.c`,
`ErrorIfSameTableExistsInExternalCatalog`). Writable REST catalogs also derive
their catalog name from the current database
(`src/rest_catalog/rest_catalog_ops.c`, `GetRestCatalogName`). This does not
prove that every catalog mode fails on rename. It does mean that rewriting only
our worker connection string is insufficient evidence of a safe rename.
Test the supported PostgreSQL catalog mode before offering a rename procedure.

Rebuild/rebind must explicitly cover source slot ownership, destination table
and object locations, existing checkpoints, credentials, and other live writers.
Never automatically resume copied launch rows based only on matching names.

## Static-to-base transition

Prefer a documented stop/restart transition for the first supported migration,
subject to testing, rather than promising a restart-free binary replacement:

1. Disable subscriptions and confirm all old wrappers and Rust descendants exit.
2. Stop PostgreSQL before replacing a library already loaded into its processes.
3. Install the qualified package and configure the required base preload.
4. Start PostgreSQL with automatic replication ownership still gated. Any old
   static owner must refuse databases whose lifecycle mode belongs to base workers.
5. Upgrade SQL metadata and register the base worker transactionally. Validate
   destination bindings and confirm exactly one owner per database.
6. Explicitly enable subscriptions and verify checkpoint continuity and source
   slot exclusivity before declaring the transition complete.

For a mixed/live transition, the concrete implementation needs a durable lifecycle
mode and an ownership interlock checked by both launcher variants before dispatch.
A startup lock alone is insufficient if old owners can keep publishing outside
its lifetime. The narrower tested restart transition instead drains and stops
old processes and suppresses static registration in the replacement binary;
it does not establish that mixed/live interlock.
Specify how workers are drained before ownership changes and what happens if
upgrade fails at each step. No SQL downgrade path is currently advertised;
recovery must not assume one exists. Test a backed-up, stopped-state recovery
procedure with the correct package version instead of calling it a downgrade.

These are proposed requirements, not a qualified upgrade recipe. Production
adoption remains gated on executable upgrade, restore, clone, rename, and
failure-recovery tests in the [evaluation plan](pg-extension-base-evaluation-plan.md).

## Crash recovery contention

The [forced-crash tests](pg-extension-base-fault-results.md) show that PostgreSQL
recovery can start a replacement while an old receiver still briefly owns the
source slot. The shared Rust source-preflight path currently makes this fatal,
and a subsequent supervisor stays paused on the durable error. Lifecycle
framework selection does not resolve that shared state-machine issue.
Qualification must distinguish bounded refusal/retry during cleanup from an
unrelated active writer. Explicit ENABLE currently changes desired dispatch
state only; it must not be advertised as validation or clearance of a fatal
source error.

The isolated [recovery revision](pg-extension-base-recovery-results.md) validates
source continuity before bounded contention retries, without clearing arbitrary
fatal errors or taking over a busy slot. This addresses the transient recovery
race in tested init-backed fixtures; an explicit validated recovery API for
persistent errors is still required.

The origin-check tests reproduce and fix cloned DROP deleting the original
source slot in the disposable overlay. They also fence same-name copies with
changed database OIDs and leave unbound legacy rows stopped. Full writable
Iceberg restore and an explicit validated rebind remain open.

The [fresh rebuild procedure](pg-extension-base-rebuild-procedure.md) now has a
disposable executable rehearsal. It allocates a new database, slot, object prefix
and checkpoints, with retained-slot replication rollback. It does not authorize
existing-object rebind, copied physical-cluster takeover or consumer cutover.

The [publication experiment](pg-extension-base-publication-results.md) adds
transactional worker write-entry checks and an external startup identity,
including actual physical-copy tests with different/empty profiles. This is
manual deployment fencing, not a distributed lease: copied runtime authorization
can still permit duplicate writers. All old storage writers, including pg_lake
maintenance, must be considered in an exclusive-writer transition.

The [cold-transfer rehearsal](pg-extension-base-cold-transfer.md) now tests
manual profile transfer after all tracked old writers stop and candidate
configuration/checkpoints/Iceberg snapshot validate. It refuses incomplete
fencing and stale failback. Its private Docker controller is not production
fencing or proof against an independently restarted/unknown writer.

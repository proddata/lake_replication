# Publication ownership and physical-copy evaluation

Status: scoped publication/deployment fencing passes in the isolated prototype.
This does not qualify automatic takeover, distributed exclusion or production
adoption. Production sources and release packaging remain unchanged.

## Prototype revision

`publication.py`, `publication.sql` and `Dockerfile.publication` extend the
origin-check image. Native subscription creation records an external deployment
identity supplied through `lake_replication.prototype_instance_id`. This is a
`PGC_POSTMASTER` string: its default is empty and an SQL session cannot change it.
Existing rows are not backfilled. Dispatch, wrapper execution and source cleanup
now require a non-empty stored identity that matches the current runtime profile,
as well as the original cluster/OID/name tuple.

The identity is a deployment label, not a secret or a distributed lease. Supply
it independently of copied PGDATA; duplicating the label and its configuration
can duplicate authorization. `ALTER SYSTEM`, copied server configuration or an
operator reusing the same profile can defeat the intended deployment separation.
The application cannot establish that the old machine has been fenced merely
by comparing these strings.

Ten native worker SQL write entry points acquire an ownership check before
DDL/data effects: publication, destination creation/schema reconciliation,
initial-copy begin/append/finish/group finish/reset/recovery, and destination
assignment. The assertion locks the launch-config row `FOR SHARE` until the
transaction ends and validates the row returned by that locking read. It avoids
re-reading the origin through a potentially older statement snapshot after
waiting for a concurrent update.

This covers the worker entry-point paths, not arbitrary SQL writes, direct calls
to every low-level helper, hostile superusers or pg_lake's independent maintenance
writers. The tested workers execute as the destination administrator. Delegated
role/ACL compatibility and performance of the extra checks remain unqualified.

## Transaction tests

A real receiver stages a source insert while the normal subscription wrapper is
disabled. The real publisher then starts while another transaction holds an
uncommitted ownership revocation. The publisher waits on the ownership row;
after revocation commits, it fails with `publication ownership rejected`.
The staged transaction, publication checkpoint, table rows and Iceberg metadata
pointer remain unchanged. Direct publication and initial-sync-group completion
also reject the revoked owner before mutation. Controlled fault repair on the
original fixture allows the same pending transaction to publish successfully.
That repair is test instrumentation, not an exposed rebind API.

The inverse race holds a validated ownership transaction open. A concurrent
origin rewrite times out on its row lock; the old identity remains unchanged.
Normal streaming then resumes successfully. These tests establish transaction
ordering in one PostgreSQL cluster; local locks cannot exclude another cluster.

## Physical-copy tests

The fixture runs actual `pg_basebackup` into a separate data volume, then starts
a second PostgreSQL server. Both servers retain the same cluster system
identifier, database OID, database name and origin metadata. The old tuple alone
passes on the copy.

Under a different external deployment identity, and again under an empty
identity, the copied server rejects the binding. Attempted ENABLE returns to
stopped, no subscription wrapper starts, direct publication and initial-copy
publication fail, checkpoint/metadata pointers stay unchanged, and copied DROP
cannot delete the original source slot. A session cannot replace the startup
identity. The original restarts under its unchanged profile and continues
streaming after both copied-server tests.

Copied servers use Docker init and explicitly set
`pg_lake_iceberg.autovacuum=off`. Their tests inspect PostgreSQL catalog metadata
without scanning or rewriting shared Iceberg data. This prevents the independent
Iceberg autovacuum path from undermining the narrow worker-fencing experiment;
it is not evidence that this replication guard controls every storage writer.

## Evidence and limits

The completed cohort passed **63/63 assertions** on PostgreSQL 18.6/Linux arm64.
It exercises real source staging, the real Rust publisher and a real physical
backup; it does not use a simulated cluster identifier or an Iceberg placeholder.
Only the scoped instrumented paths above are claimed. The SQL installer asserts
ten matching function signatures before modifying their bodies; this is a
fresh-install experiment, not a qualified in-place package upgrade.

The [retained results](evidence/2026-10-19-base-publication-ownership-results.json)
record the archive/image identity, harness/adapter fingerprints, checks and log
hashes. Exact sources and logs remain under
`.qualification/qualification_20261019_111922_ownership/`. Earlier exploratory
runs exposed two fixture issues: holding the origin lock before receiver schema
reconciliation blocked staging, and standalone receivers require the native
source-credential-loading flag. The retained run stages with the real receiver
first, then tests publication lock ordering with the normal credential path.

Next qualify a manual exclusive-writer transition or implement a destination
lease with storage-level fencing. A duplicate runtime profile is an explicit
counterexample to universal physical-copy exclusion. A safe transition must
fence every old writer, including supervisors, publisher connections, pgduck and
maintenance; validate source slot/checkpoint continuity and the exact Iceberg
catalog/snapshot; and define abort/recovery at each stage. A time-limited lease
alone is insufficient if an expired writer can still commit to storage.

The existing fresh rebuild route remains the evaluated recovery alternative.
Automatic existing-object rebind/takeover is still unsupported. Earlier
performance, upgrade, crash and rebuild evidence predates this additional
publication layer and cannot qualify its full regression matrix or overhead.

`make check` passed, retained source hashes match the tested adapters, and all
private primary/copy containers and volumes were removed. Production worker,
migration and extension files still match the archived source revision. New
work remains local, uncommitted and unpushed.

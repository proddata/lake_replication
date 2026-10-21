# Destination identity and cleanup evaluation

Status: origin checks pass the scoped prototype safety cases. Full Iceberg
restore, explicit rebind/takeover and production adoption remain unqualified.

## Baseline findings

The exact recovery-qualified image was reused by immutable image ID, with
fresh private source, destination and object-storage volumes. The [refined baseline](evidence/2026-10-19-base-identity-baseline-results.json)
completed 39 assertions: 35 passed and four exposed these gaps:

| Case | Name-only prototype result |
| --- | --- |
| Template copy to another name | Durable error; attempted ENABLE returns to stopped. |
| Metadata restore to another name | Same fence; original source slot remains intact. |
| Database rename | Durable error; no automatic connection-name rewrite. |
| Template copy back to the same name with another OID | No fresh inherited-binding error. Kept stopped; dispatch not attempted. |
| Metadata restore to the same name with another OID | Same missing identity fence. Kept stopped; dispatch not attempted. |
| DROP SUBSCRIPTION in a fenced clone | Deleted the stopped original's source slot. |
| Full logical restore of a real writable Iceberg table | Fails recreating the table at its existing non-empty object location. |

A copied diagnostic is not proof of current detection. For the same-name
cases, the fixture cleared only the copied diagnostic while launch intent
stayed stopped, then waited for the launcher to detect the changed identity.
The name-only implementation did not. These cases establish eligibility/fence
gaps, not successful cross-database publication; copied destinations were never
enabled on that image.

The cleanup finding is an actual side effect in the private fixture. Native
DROP reads the copied `pending_cleanup` and source credentials. It deleted the
source slot despite the dispatch fence. A launch-only fence is insufficient.

## Origin-check revision

`binding.py`, `binding.sql` and `Dockerfile.binding` add an isolated overlay to
the recovery-qualified image. New subscription creation records the destination
cluster system identifier and database OID alongside its existing destination
name. Copied and legacy rows are never backfilled with the current identity.
Origin columns without recorded values remain NULL and fail the check.

The tuple is checked in the launcher before dispatch and in the PostgreSQL
subscription wrapper before executing Rust. DROP SUBSCRIPTION checks it before
removing launch metadata or contacting the source. A mismatch rejects cleanup
with an explicit error and preserves both the source slot and local metadata.
These are dispatch/cleanup checks, not a destination lease or publication-time
ownership enforcement inside every receiver/publisher transaction.

The [revised identity run](evidence/2026-10-19-base-identity-origin-results.json)
completed 56 assertions: **55 passed**; the sole
remaining finding is full Iceberg restore. Its scoped `binding_safety_passed`
result is true. It covers different-name clones/restores, rename fencing,
same-name copies with changed OIDs, attempted ENABLE of those copies, unchanged
diagnostic tuple versions, unbound legacy launch rows, and cloned DROP refusal.
The original still streams after clone rejection. Restoring the original
name/OID tuple also permits its legitimate source-slot cleanup.
That cleanup control does not qualify Iceberg publication after a rename.

## Restore scope

A full custom-format dump of the actual replicated Iceberg table fails at:

```
CREATE FOREIGN TABLE ... OPTIONS (catalog 'postgres', location '<existing location>')
ERROR: location ... is not empty
HINT: Please use an empty location.
```

The pinned upstream explicitly checks this in
`vendor/pg_lake/pg_lake_table/src/ddl/create_table.c`,
`ErrorIfLocationIsNotEmpty`; its `test_create_table_at_non_empty_location`
regression test expects the rejection. Do not bypass that guard or rewrite a
dump's location and assume its journal/checkpoint/catalog bindings remain valid.

To isolate the replication control plane, the fixture separately dumps its
19 registered extension configuration tables. `relations.destination_relation`
contains regclass values, so a named relation must exist for COPY input even
with triggers disabled. The metadata-only fixture creates an ordinary local
placeholder table before restoring those rows. That placeholder is never
published to and does not recreate or validate the original Iceberg table.
Metadata-only success is not evidence of a working data restore or rebind.

## Remaining work

A safe explicit rebuild/rebind procedure must handle source slot ownership,
Iceberg table/catalog/object bindings, checkpoints, credentials and any old
writer. Specify restore/import/rebuild semantics against pg_lake before offering
a writable takeover. Its documented external Iceberg metadata-file interface is
a snapshot read/import mechanism, not evidence of a supported writable takeover.

Physical copies can preserve system identifier, database OID and name together;
the tuple cannot detect them. A destination-scoped lease or explicit exclusive
writer procedure is still required. Publication-side validation, cross-cluster
restore, rollback/recovery and migration of existing unbound rows remain open.
The overlay is fresh-install experimental SQL, not a shipped upgrade package.

The [combined crash retest](evidence/2026-10-19-base-identity-crash-results.json)
passed 30/30 assertions across the two paused launcher/wrapper kill regressions
with Docker init and bounded Rust contention retries. Earlier performance and upgrade cohorts predate these
origin checks and do not qualify the combined implementation. Production worker,
migration and extension sources remain unchanged. `make check` passed; the final
combined CLI path was exercised by the crash retest and Python compilation.
Source fingerprints were verified, and all private test containers and volumes
were removed. New work remains uncommitted and unpushed.

Retained JSON records image IDs, archive/runtime/harness fingerprints and checks.
The ignored `.qualification/<run>_identity/` and `_faults/` directories retain
matching harness/adapter snapshots and full logs; their hashes are retained in
the evidence.

The subsequent [fresh rebuild rehearsal](pg-extension-base-rebuild-procedure.md)
passes 69 assertions with a new database, slot, storage prefix and initial copy,
plus retained-slot replication rollback. It uses actual writable Iceberg tables
without adopting the existing table location. The full restore/rebind limitation
reported above remains unchanged.

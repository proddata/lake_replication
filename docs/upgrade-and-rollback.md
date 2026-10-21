# Upgrade, backup, and rollback

This procedure applies to the `v0.1.0-beta.2` candidate on PostgreSQL 18 with a
supported pg_lake installation. Rehearse it on a restored copy before changing
a production-like destination.

## Before upgrading

Keep the previous archive and its checksum. Record the installed versions:

```sql
SELECT extname, extversion
FROM pg_extension
WHERE extname = 'lake_replication'
ORDER BY extname;

SELECT subscription_name, health_state, source_lag_bytes,
       journal_lag_bytes, last_error
FROM lake_replication.subscription_status;
```

Disable each managed subscription and wait until its health state is
`stopped`:

```sql
ALTER SUBSCRIPTION orders_to_lake DISABLE;
```

With replication quiesced, take a coordinated backup of:

- the destination PostgreSQL cluster, including roles and configuration;
- the complete object-store prefix used by pg_lake, with versioning or an
  immutable snapshot; and
- the credential file, CA files, previous release archive, and PostgreSQL
  configuration through the host's secret/configuration backup system.

The PostgreSQL catalog and object-store snapshot are one recovery unit. A
database-only backup can reference Iceberg metadata or data files that are
missing from an independently restored object store. The `beta.1` extensions
register their durable tables with `pg_dump`, but a physical backup plus an
object-store snapshot remains the recommended beta recovery mechanism.

For a rehearsed logical restore, use a custom-format archive and restore into
an empty database in three phases as a PostgreSQL superuser: `pre-data`, `data`
with `--disable-triggers`, then `post-data`. (`--disable-triggers` only applies
to a data-only restore.) Subscription inserts normally initialize maintenance
rows through a trigger; disabling triggers during the data phase prevents
those defaults from colliding with the retained policy and status rows in the
archive. Validate the result against the matching object-store snapshot before
use.

## Upgrade to beta.2

1. Verify the new archive checksum and unpack it.
2. Stop PostgreSQL. Run the new bundle's `install.sh` as root; it replaces the
   program files but does not alter database objects.
3. Ensure `pg_extension_base` remains in `shared_preload_libraries`, then start
   PostgreSQL while subscriptions remain disabled. Retained explicit
   `lake_replication` preload entries and `lake_replication.database_name` are
   accepted for handoff compatibility; the new binary never registers the old
   static launcher. They can be removed in a later configuration cleanup.
4. Verify this is the original destination cluster/database, not a template copy,
   restore or physical takeover. Record its system identifier and database OID:

```sql
SELECT system_identifier FROM pg_control_system();
SELECT oid, datname FROM pg_database WHERE datname=current_database();
```

Legacy beta.1 rows do not record an origin, so the upgrade cannot establish that
provenance automatically. Existing bindings require explicit verified adoption.
Copies require a rebuild. Upgrade each installed destination database separately:

```sql
BEGIN;
SET LOCAL lake_replication.allow_legacy_binding_upgrade = on;
ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.2';
COMMIT;
```

5. Confirm the versions and query `lake_replication.subscription_status`.
6. Enable one subscription, verify that lag returns to its normal range and
   new source changes reach Iceberg, then enable the remaining subscriptions.

The upgrade is forward-only and preserves checkpoints, completed copies, stored
credentials and source slots. The explicit beta.1-to-beta.2 path adds empty-commit
handling and the per-database base-worker registration. A direct development
`0.1.0`-to-beta.2 path is also included. An empty installation does not need the
adoption setting. SQL upgrade fails if subscriptions are enabled or old launcher/
wrapper processes are present. Do not replace loaded libraries or workers online.

After SQL upgrade, verify exactly one registration for this extension/database in
`extension_base.list_base_workers()` and no legacy `lake_replication launcher`
backend. Subscriptions remain stopped until explicitly enabled. The base-worker
entry point cannot be called from ordinary SQL (`internal -> internal`). Its
shutdown stops and reaps subscription wrappers and their Rust descendants.

Each new native subscription records the local system identifier/database OID.
Copied metadata or database renames are fenced rather than implicitly rebound;
cloned DROP cannot delete the original source slot. These local origin checks
are not a distributed lease or authorization for physical takeover.

## Rollback

There is intentionally no SQL downgrade script. Running old workers against a
newer control schema, or manually changing `pg_extension.extversion`, is not a
supported rollback.

If the failure occurs before `ALTER EXTENSION`, stop PostgreSQL,
reinstall the previous archive, restore the previous configuration, and start
PostgreSQL. The database schema has not changed.

After the extension upgrade, prefer fixing forward. If rollback is required:

1. Stop all application and replication writes.
2. Stop PostgreSQL.
3. Restore the pre-upgrade PostgreSQL backup and its matching object-store
   snapshot as one unit.
4. Restore the previous credential/configuration state and reinstall the
   previous checked archive.
5. Start PostgreSQL, verify extension versions and subscriptions, then resume
   one subscription at a time.

Do not delete a source replication slot during a failed upgrade. Preserving
the slot retains the WAL needed to catch up after recovery. If a slot was lost
or its confirmed position is ahead of the restored checkpoint, leave the
subscription stopped and follow the rebuild procedure instead of forcing the
checkpoint forward.

## Current evidence boundary

The packaged lifecycle lanes exercise fresh install, baseline upgrade, a stopped-
server beta.1-to-beta.2 handoff with retained data/slot/checkpoint, rolled-back
upgrade, explicit origin adoption, and resumed replication. See the migration
qualification report for the exact archive and platform results. A full physical
backup/object-store restore drill, SQL downgrade, online handoff, distributed
fencing and automatic physical takeover are not covered by those lanes.

# Upgrade, backup, and rollback

This procedure applies to the `v0.1.0-beta.1` package on PostgreSQL 18 with a
supported pg_lake installation. Rehearse it on a restored copy before changing
a production-like destination.

## Before upgrading

Keep the previous archive and its checksum. Record the installed versions:

```sql
SELECT extname, extversion
FROM pg_extension
WHERE extname IN ('lake_replication', 'lake_subscription_sugar')
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

## Upgrade to beta.1

1. Verify the new archive checksum and unpack it.
2. Stop PostgreSQL. Run the new bundle's `install.sh` as root; it replaces the
   program files but does not alter database objects.
3. Start PostgreSQL while subscriptions remain disabled.
4. Upgrade both extensions in the destination database:

```sql
ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.1';
ALTER EXTENSION lake_subscription_sugar UPDATE TO '1.1';
```

5. Confirm the versions and query `lake_replication.subscription_status`.
6. Enable one subscription, verify that lag returns to its normal range and
   new source changes reach Iceberg, then enable the remaining subscriptions.

The `lake_replication` upgrade from the installable `0.1.0` development
baseline is forward-only and preserves control-plane rows and checkpoints. It
adds the observability contract and logical-backup registration. The
`lake_subscription_sugar` `1.0` to `1.1` upgrade registers its durable launch
and cleanup tables for logical backup without changing their shape.

## Rollback

There is intentionally no SQL downgrade script. Running old workers against a
newer control schema, or manually changing `pg_extension.extversion`, is not a
supported rollback.

If the failure occurs before either `ALTER EXTENSION`, stop PostgreSQL,
reinstall the previous archive, restore the previous configuration, and start
PostgreSQL. The database schema has not changed.

After either extension upgrade, prefer fixing forward. If rollback is required:

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

The clean-host qualification installs only the exported archive and exercises
the `0.1.0` to `0.1.0-beta.1` and `1.0` to `1.1` upgrade paths with retained
control rows. A real beta-to-beta upgrade and full backup/object-store restore
drill remain required before the upgrade gate can be considered complete.

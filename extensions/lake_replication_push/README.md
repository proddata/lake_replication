# Optional destination source-push transport

This PostgreSQL 18 companion enables the bounded `iceberg_push` prototype over
lake_replication beta.5's journal, native lifecycle and publisher. Default
`logical_pull` installations do not require it. Install the matching default
bundle first, then this separately built companion and its feature-enabled
supervisor/importer. The source separately needs `lake_replication_capture`.

The current supported group is two explicitly registered heap
tables with primary keys and boolean/integer/bigint/text columns. Empty, paused
and bounded online baselines, row mutations, conservative retention and crash
recovery have [retained evidence](../../docs/design/push-replication.md). Frozen schema is
the default. Explicit `schema_mode=ordered` activation supports bounded nullable
additions/column renames through the optional ordered publisher; see the
[activation contract](../../docs/design/push-schema-evolution.md). Automatic failover,
unbounded snapshots and general release support remain open.

## Build the optional archives

Select cached native prerequisite images explicitly. These example names are
local ARM64 qualification cache names, not published image references:

```sh
python3 deploy/push/build-bundles.py \
  --source-base pg-lake-runtime:arm64-b6794e8209f18b90cf80 \
  --destination-base lake-replication-qualification-destination:qualification_20261113_192054 \
  --output dist/push
python3 tests/push_capture/packaging.py
```

The source and destination `.tar.gz` archives include checksums, exact input and
image provenance, native dependency notices and `install.sh`. They do not include
PostgreSQL, pg_lake, pgduck or credentials. Only the native ARM64 runtime in the
retained qualification is currently qualified. Stop managed subscriptions and
the target server before installing/replacing workers or libraries. Set `PGDATA`
and optionally `PG_CONFIG` when running the extracted installer. `DESTDIR` is for
staging files; qualification uses it only in disposable containers. Keep the
matching default bundle for rollback after removing push subscriptions.
The installer requires the recorded PostgreSQL version string and, for a real
installation, the exact recorded pg_lake runtime marker. Rebuild and qualify
the native prototype for a different runtime before installation.
The destination archive also pins the prerequisite core extension and default
receiver, publisher and initial-sync worker contents by checksum.
File replacement is prepared first and rolled back on ordinary errors or
termination signals. The Linux installer requires `flock`; its lock also covers
surviving installer children. After SIGKILL during file publication, keep the
server stopped and run the same extracted bundle with the same `PGDATA` and
`PG_CONFIG`, adding `--rollback`. This explicit repair verifies the prepared
journal, bundle identity, allowed targets and backup checksums before restoring
old contents/modes and removing newly installed files. Backups survive a failed
repair so the same command can be retried. Active installers, incomplete journals,
corrupt backups and unexpected targets are refused. Keep refused recovery records
for inspection. After a committed installation is interrupted during staging
cleanup, use
`install.sh --finish` with the same bundle and stopped-server settings. Every
installed payload and mode is verified before deleting staging data; committed
installations refuse `--rollback`. New installation journals record their complete
staging plan before creating external staging directories. After an interruption
during planning or preparation, `install.sh --discard` removes only unpublished
staging data and preserves live files. Publication journals require `--rollback`;
committed journals require `--finish`. Keep the server stopped for each action.
Power-loss recovery, interruption while deleting the journal, automatic startup
recovery and version upgrades remain unqualified.

Source installation is independent and described in
[the source companion README](../lake_replication_capture/README.md). For the
destination, restart with the normal pg_extension_base prerequisite and create:

```sql
CREATE EXTENSION lake_replication_push;
```

Creating the companion does not change existing subscriptions or start a push
subscription. It refuses unsupported core SQL/TLS interfaces.

## Create a new push mirror

An administrator obtains the immutable stream identity/generation and table
registry from the source. `lake_capture_log.progress` exposes capture identity;
`lake_capture_log.registrations` exposes frozen descriptors. Retain the group
registry with your deployment configuration. For example:

```sql
-- On the source, as an authorized reader:
SELECT * FROM lake_capture_log.progress;
SELECT jsonb_agg(to_jsonb(r) ORDER BY table_id)
FROM lake_capture_log.registrations r;
```

Use a separate read-only source LOGIN with USAGE/SELECT on `lake_capture_log` and
pg_lake `lake_read` membership. It needs no REPLICATION capability or capture
management functions. Use a dedicated destination LOGIN with the existing
[worker grants](../../deploy/install/worker-grants.sql). Its normal database-wide
control-plane permissions are a trusted worker boundary, not tenant isolation.
The creation API grants it USAGE and DML on the new private mirror schema/tables.

Provide an absolute, worker-OS-owner-only credential file containing explicit
`LAKE_SOURCE_USER` and `LAKE_DESTINATION_USER`, passwords as needed, and your TLS
settings. The `destination_user` below must match that file. Conninfo supplies the
managed source TLS policy; an explicit `sslmode=disable` is for private fixtures
only. Configure verified TLS and its root/server name for remote deployments.
Do not put secret file contents in process arguments or exported source logs.

```sql
SELECT lake_replication.create_push_subscription(
  'application_push',
  '{"mode":"iceberg_push",
    "stream_id":"REPLACE_WITH_STREAM_UUID",
    "system_identifier":"REPLACE_WITH_SYSTEM_IDENTIFIER",
    "database_oid":12345,
    "capture_generation":1,
    "timeline":1,
    "source_address":"source.example:5432",
    "source_database":"application",
    "destination_user":"replication_worker",
    "conninfo":"host=source.example port=5432 dbname=application user=log_reader sslmode=verify-full sslrootcert=/credentials/source-ca.crt"}'::jsonb,
  'REPLACE_WITH_SOURCE_REGISTRY_JSON_ARRAY'::jsonb,
  'application_mirror',
  '/credentials/push.env'
);
ALTER SUBSCRIPTION application_push ENABLE;
```

Replace the illustrative identity values with the source values; the importer
validates the actual stream, generation, schema registry and retained continuity
before journal admission. The registry must have two descriptors containing
`table_id`, `schema_version`, `source_namespace`, `source_relation`,
`replica_identity`, and `columns`. Each column has `name`, numeric `type_oid`,
`type_modifier` and key `flags`. Optional `destination_relation` selects its name
inside the new target schema; otherwise the source relation name is used.

The schema must be new. The API never attaches a second writer to an existing
mirror, converts an old pull subscription, creates a source slot, or starts work
before its transaction commits. New subscriptions start stopped. The existing
supervisor selects the importer from the immutable push binding and uses the
normal managed generation fence and publisher.

## Observe and control

```sql
SELECT * FROM lake_replication.subscription_modes;
SELECT * FROM lake_replication.push_status;
ALTER SUBSCRIPTION application_push DISABLE;
ALTER SUBSCRIPTION application_push ENABLE;
DROP SUBSCRIPTION application_push;
```

`push_status` separates imported and published coordinates. Only actual
publication is eligible for source retention acknowledgement; source consumer
registration and acknowledgement remain explicit source-owner operations.
Dropping a push subscription removes destination control state and leaves its
published mirror data and source-owned capture slot intact. Stop/drop the source
stream separately when intended. Remove all push subscriptions before dropping
the optional companion or its dependencies. Pull subscriptions can keep running
while the optional companion is removed.

Use enough dynamic worker capacity for every active database and subscription;
the multi-database private qualification fixture uses
`max_worker_processes=24`. Source capture/log/upload and maintenance budgets are
finite. A stalled destination can require explicit source budget repair or a
controlled new snapshot generation; it cannot authorize skipping source history.

## Restore and rebuild

Dump/restore retains the optional stream/table bindings and imported receipts.
Copied destination configuration is fenced by the existing destination origin
checks. It does not authorize a copied worker to write or clean source slots.
The runtime still refuses recreating writable Iceberg mirrors at their live,
non-empty object locations. The control-plane restore qualification uses empty
placeholder relations and does not restore mirror data or qualify writable
object adoption. Follow the existing controlled-rebuild procedure and create a
new private target schema/generation; do not enable an inherited binding.

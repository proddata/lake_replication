# pg_lake_replication

[![CI](https://github.com/proddata/pg_lake_replication/actions/workflows/ci.yml/badge.svg)](https://github.com/proddata/pg_lake_replication/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](LICENSE)

Logical replication from ordinary PostgreSQL tables into
[pg_lake](https://github.com/Snowflake-Labs/pg_lake) Iceberg tables.

`pg_lake_replication` runs beside an existing PostgreSQL 18 + pg_lake
installation. It takes an initial snapshot, consumes logical replication,
stages source transactions durably, and publishes bounded micro-batches into
Iceberg tables. A PostgreSQL extension provides familiar
`CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg')` lifecycle commands.

> [!WARNING]
> This project is approaching its first beta and is not production-ready yet.
> The release gates, remaining work, and evidence are tracked in
> [BETA-RELEASE.md](BETA-RELEASE.md). The supported beta target is pg_lake's
> local PostgreSQL catalog; REST catalogs are not supported.

## Why this exists

PostgreSQL's built-in logical-replication apply worker cannot write directly to
a pg_lake Iceberg foreign table. This project provides a separate replication
path designed for that boundary while retaining PostgreSQL-native setup and
control commands.

It currently supports:

- initial snapshot copy followed by continuous logical replication;
- `INSERT`, `UPDATE`, `DELETE`, primary-key changes, and `TRUNCATE`;
- transaction-aware, row- and byte-bounded Iceberg micro-batches;
- publication table discovery, column lists, and row filters;
- automatic creation of missing Iceberg destination tables;
- safe nullable-column additions and fail-closed incompatible schema changes;
- crash recovery, idempotent replay, and ambiguous-commit reconciliation;
- supervisor restart, retry, health state, and journal retention;
- source continuity checks and ordered failover endpoints; and
- PostgreSQL 18 packages for Linux amd64 and arm64.

## How it works

```mermaid
flowchart LR
    A[Source PostgreSQL heap tables] -->|publication + logical slot| B[Receiver]
    B -->|durable source transactions| C[(lake_replication journal)]
    C --> D[Publisher]
    D -->|bounded SQL batches| E[pg_lake Iceberg tables]
    F[Supervisor] -. starts, retries, and monitors .-> B
    F -. starts, retries, and monitors .-> D
    G[CREATE / ALTER / DROP SUBSCRIPTION] --> F
```

The destination PostgreSQL database contains the control schema and journal.
The four installed executables perform initial copy, receive WAL, publish
batches, and supervise the service. `lake_subscription_sugar` intercepts only
subscriptions marked with `lake_target = 'iceberg'`; normal PostgreSQL
subscriptions continue through PostgreSQL unchanged.

## Compatibility

| Component | Beta target | Validated configuration |
|---|---|---|
| PostgreSQL | 18.x | 18.6 |
| pg_lake | 3.6, local PostgreSQL catalog | commit `8180647d4535fdb1ed223f92f301ba6094a8ec3e` |
| Operating system | Linux with Debian 12-compatible glibc | Debian 12 build images |
| Architecture | amd64, arm64 | native release archives for both |
| Object storage | S3-compatible storage supported by pg_lake | MinIO in the integration lab |

The pg_lake commit above is a validated point, not a claim that every adjacent
revision is compatible. See the complete
[compatibility contract](deploy/install/COMPATIBILITY.md).

## Quick start

The following walkthrough assumes a source database named `application` and a
pg_lake-enabled destination database named `analytics`. It is intended for a
disposable evaluation environment; review the [beta limitations](#beta-limitations)
before using real data.

### 1. Prepare the source

The source must have logical replication enabled and be reachable from the
destination host. Set appropriate capacity for the expected subscriptions and
restart the source after changing these server settings:

```conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Then create a table and one publication:

```sql
-- Run in the source database: application
CREATE TABLE public.orders (
    id bigint PRIMARY KEY,
    customer text NOT NULL,
    total numeric(12, 2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE PUBLICATION orders_pub FOR TABLE public.orders;
```

Updates and deletes require a usable replica identity, normally a primary key.

### 2. Install beside PostgreSQL and pg_lake

The first signed prerelease is not published yet. Until it is, build the native
bundle from a clean checkout:

```sh
make install-bundle
tar -xzf dist/pg_lake_replication-0.1.0.tar.gz -C /tmp
cd /tmp/pg_lake_replication-0.1.0
sudo ./install.sh
```

The installer verifies the bundle, PostgreSQL major version, pg_lake and
`dblink` availability, and CPU architecture before copying anything. It
installs four executables under `/usr/local/bin` plus the two PostgreSQL
extensions. It does not change PostgreSQL configuration or database objects.

To build release-style archives for both supported architectures instead, run
`make install-bundles`; the results are written to `dist/release/`.

### 3. Configure the destination server

Append `lake_subscription_sugar` to any existing
`shared_preload_libraries` value and choose the pg_lake destination database:

```conf
# postgresql.conf
shared_preload_libraries = 'pg_extension_base,lake_subscription_sugar'
lake_subscription_sugar.database_name = 'analytics'
lake_subscription_sugar.log_directory = '/var/log/postgresql/pg_lake_replication'
```

The exact existing preload list depends on the pg_lake installation; preserve
all of its entries when appending `lake_subscription_sugar`.

Create the log directory with ownership and permissions suitable for the
PostgreSQL operating-system user, then restart PostgreSQL. In the destination
database, install the extensions and activate the SQL hook for future sessions:

```sql
-- Run in the pg_lake-enabled destination database: analytics
CREATE EXTENSION lake_replication;
CREATE EXTENSION lake_subscription_sugar CASCADE;

ALTER DATABASE analytics
    SET session_preload_libraries = 'lake_subscription_sugar';
```

Reconnect after changing `session_preload_libraries`. Subscription management
requires superuser or membership in the extension-created admin role:

```sql
GRANT lake_replication_admin TO replication_operator;
```

### 4. Create an Iceberg subscription

Run this in `analytics`:

```sql
CREATE SUBSCRIPTION orders_to_lake
  CONNECTION 'host=127.0.0.1 port=5432 dbname=application user=postgres'
  PUBLICATION orders_pub
  WITH (lake_target = 'iceberg');
```

The command discovers the publication, creates corresponding
`USING iceberg` tables in `public`, performs the initial copy, and starts the
supervisor. Use `lake_target_schema` to select another destination schema:

```sql
CREATE SUBSCRIPTION orders_to_lake
  CONNECTION 'host=127.0.0.1 port=5432 dbname=application user=postgres'
  PUBLICATION orders_pub
  WITH (lake_target = 'iceberg', lake_target_schema = 'warehouse');
```

Exactly one publication is supported per subscription in the current beta.

### 5. Verify replication

Inspect service state on the destination:

```sql
SELECT subscription_name,
       subscription_state,
       service_state,
       heartbeat_at,
       consecutive_failures,
       staged_transactions,
       received_lsn,
       published_lsn,
       last_error
FROM lake_replication.service_health;
```

Then insert a row on the source and query its Iceberg counterpart on the
destination:

```sql
-- application
INSERT INTO public.orders (id, customer, total)
VALUES (1, 'Ada', 42.00);

-- analytics
SELECT * FROM public.orders WHERE id = 1;
```

## Operating subscriptions

The familiar lifecycle commands are routed to this project's control plane for
subscriptions created with `lake_target = 'iceberg'`:

```sql
ALTER SUBSCRIPTION orders_to_lake DISABLE;
ALTER SUBSCRIPTION orders_to_lake ENABLE;
DROP SUBSCRIPTION orders_to_lake;
```

Useful first checks are:

- `lake_replication.service_health` for state, heartbeat, retries, failures,
  cycle counts, queue depth, checkpoints, and source continuity;
- PostgreSQL's server log for launcher/background-worker events; and
- the configured `lake_subscription_sugar.log_directory` for per-subscription
  worker output.

The complete operator runbook and metrics/alerting interface are release-gate
work in progress. Do not treat the current status view as a finished monitoring
contract.

## Beta limitations

- PostgreSQL 18 and pg_lake 3.6 with the local PostgreSQL catalog are the only
  current targets. PostgreSQL 16, 17, and 19 are not supported.
- REST-catalog destinations are intentionally unsupported because pg_lake's
  catalog update occurs after PostgreSQL commit and does not provide the
  durable reconciliation contract this project requires.
- Only one publication is accepted per subscription.
- The SQL convenience launcher currently starts workers as database user
  `postgres` and does not inject a per-subscription password. Although the
  worker binaries support SCRAM and certificate-verified TLS, secure secret
  delivery through `CREATE SUBSCRIPTION` is not yet a supported deployment
  path. The quick start is therefore for co-located evaluation environments
  where the worker's local connection is authorized without an injected
  password.
- Compatibility has been validated against one pg_lake revision. Clean-host,
  upgrade/rollback, extended compatibility, observability, and release-candidate
  soak gates remain open.
- No signed GitHub prerelease has been published yet. Source checkouts and CI
  artifacts are development outputs until `v0.1.0-beta.1` is released.

For the complete current decision record, see
[BETA-RELEASE.md](BETA-RELEASE.md).

## Try the local demo

The repository includes a self-contained Compose demonstration with source and
destination databases, pg_lake, object storage, continuously running workers,
and a small SQL console:

```sh
make poc-build
make poc-up
```

Open <http://127.0.0.1:8080>. The initial build compiles pg_lake and can take a
while. This stack uses loopback-only trust authentication and no TLS; it is a
demo, not a deployment template. See the [demo walkthrough](deploy/poc/README.md).

Stop it while keeping its data with `make poc-stop`, or remove its containers
and volumes with `make poc-down`.

## Development

Requirements for repository checks are Docker with Compose, Rust 1.85, Python
3, and Make.

```sh
make check                 # formatting and offline/static checks
make install-bundle        # native installable archive
make install-bundle-test   # test the exact exported archive in the lab
```

The integration lab contains correctness, crash, schema, failover, TLS, SCRAM,
slot-loss, and maintenance probes. It builds a large development image and is
kept separate from the quick start. See the [feasibility report](docs/feasibility.md)
and [historical evidence](docs/) before running individual lab targets.

## Documentation

- [Beta release gates](BETA-RELEASE.md)
- [Compatibility contract](deploy/install/COMPATIBILITY.md)
- [Bundle and installer details](deploy/install/README.md)
- [Local demo](deploy/poc/README.md)
- [Feasibility and correctness report](docs/feasibility.md)
- [Dependency and license inventory](docs/dependencies.md)
- [Why native PostgreSQL apply cannot target Iceberg](docs/native-subscription-spike.md)

## License

This project is licensed under the [PostgreSQL License](LICENSE).

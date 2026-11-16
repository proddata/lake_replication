# Non-superuser destination worker grants

Use the bundle's `WORKER_GRANTS.sql` to provision a fresh destination runtime
login for PostgreSQL 18 and the qualified pg_lake runtime. The recipe grants
specific existing objects and capabilities; it does not grant superuser,
replication-administrator membership, database/schema ownership, role creation,
server-file access, or blanket/default privileges on application tables.

Run setup as the extension installer in the destination database:

```sql
CREATE ROLE lake_worker LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;
CREATE SCHEMA replicated_data;
```

Set a password using `psql`'s `\password lake_worker` when the destination
requires password authentication. Keep credentials in the private worker file,
then apply the script from the unpacked matching archive:

```sh
psql -X -v ON_ERROR_STOP=1 --dbname analytics \
  -v worker_login=lake_worker -v destination_schema=replicated_data \
  --file WORKER_GRANTS.sql
```

The login and dedicated schema must already exist. The script rejects privileged
role attributes, administrator/elevated role membership, `NOINHERIT` logins and
shared/reserved destination schemas. Validation and grants are transactional;
applying the script twice is supported. Use a fresh login: this additive recipe
does not remove pre-existing privileges or audit every inherited application ACL.
Reapply and requalify after extension or pg_lake upgrades that change the runtime
objects or permissions.

Select the same schema and an explicit login file in the subscription, using
the administrator's destination connection:

```sql
CREATE SUBSCRIPTION orders_to_lake
  CONNECTION 'host=source.example dbname=application user=lake_source sslmode=verify-full sslrootcert=/etc/lake_replication/source-ca.crt'
  PUBLICATION orders_pub
  WITH (lake_target='iceberg', lake_target_schema='replicated_data',
        lake_credential_file='/etc/lake_replication/lake_worker.env');
```

The source needs its separately configured authentication and replication
permissions. The destination file contains a non-empty
`LAKE_DESTINATION_USER=lake_worker`, the destination password if needed and the
appropriate destination TLS settings. See the [login requirement](https://github.com/proddata/lake_replication/blob/main/docs/guides/worker-login-requirement.md).
No source or destination passwords are stored by this grant script.

## Permission boundaries

| Capability | Why it is required |
|---|---|
| CONNECT and TEMPORARY on the destination database | Worker connections and Iceberg UPDATE tracking |
| `lake_replication_worker` membership | Protected source credential/TLS getters |
| `lake_read_write` membership | pg_lake read/write and Iceberg creation capabilities |
| USAGE and CREATE on the dedicated destination schema | Workers create and own destination tables, including nullable-column schema evolution |
| Named control-table/view and column grants | Initial copy, journal/checkpoints, publication, continuity and service/maintenance status |
| USAGE on two named journal/batch sequences | Allocating transaction and publication identifiers |
| EXECUTE on named restricted admission functions | Runtime fences, copy leases, streaming admission and staging |

The script deliberately leaves launch configuration read-only, raw source
credentials and cleanup metadata unreadable, and source identity/policy fields
outside the granted update columns. It adds no `ALTER DEFAULT PRIVILEGES` or
`GRANT ALL ON ALL TABLES` rules. Existing destination tables need reviewed
ownership/permissions: the worker must own tables it will alter. The tested
path lets it create those tables in its dedicated schema.

This is a **trusted replication data-plane login for one database**. Control
grants and credential-getter membership currently span subscriptions. pg_lake's
`lake_read_write` role also conveys engine/storage capabilities; SQL ACL denial
on an unrelated application table is not object-store isolation. Do not use
this profile for mutually untrusted tenants. Per-subscription authorization,
journal/staging data isolation, storage isolation and aggregate resource limits
remain separate work. Cluster/default PUBLIC privileges in other databases must
be reviewed independently; this script does not revoke them globally.

## Qualification

The packaged regression is `tests/security/worker_grants_probe.py`. It starts
from a fresh login without superuser, CREATEDB, CREATEROLE, REPLICATION,
BYPASSRLS or administrator membership, removes PUBLIC CONNECT/TEMPORARY on the
private destination database, and applies the exact script extracted from the
archive. It covers copy interruption, streaming mutations and schema evolution,
worker/source/destination restart, credential rotation, maintenance, cleanup
and explicit access denials. The expanded local ARM64 regression passes all 61 checks. Repository `make check` also
passes. The earlier failure without TEMPORARY is retained as evidence that
Iceberg UPDATE tracking needs that capability. See the
[structured results](https://github.com/proddata/lake_replication/blob/main/docs/evidence/2026-11-13-worker-runtime-grants.json)
for exact archives, inputs, denied operations and excluded exploratory attempts.

These are local results for the tested PostgreSQL 18/pg_lake runtime and worker
login code. Final-commit native AMD64/ARM64 qualification remains required. No
subscription or object-store isolation is established, and no 24-hour soak or
release publication is performed. These grants do not inherit the earlier
`d5e05d2` archive qualification.

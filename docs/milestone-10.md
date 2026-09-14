# Milestone 10: automatic Iceberg DDL and versioned type contracts

Status: initial synchronization can create a missing Iceberg destination from a
validated source schema and rejects incompatible pre-created destinations before
staging snapshot rows.

## Type contract

Source catalog inspection now records a separate type contract for each relation
schema version. Keeping this contract separate from the pgoutput relation
descriptor prevents catalog-only type names and nullability from creating false
pgoutput schema versions.

Mapping version 1 supports 13 canonical PostgreSQL types:

`smallint`, `integer`, `bigint`, `boolean`, `text`, `real`, `double precision`,
`date`, `timestamp without time zone`, `timestamp with time zone`, `uuid`,
`jsonb`, and `bytea`.

Each mapping records its destination type and whether binary representation is
considered compatible. Contracts also retain source column order, nullability,
and generated-column status. Generated columns and unmapped types fail visibly.

## Destination validation and creation

`ensure_iceberg_destination` resolves the configured schema-qualified target.
When it is absent and `LAKE_CREATE_TABLES_USING_ICEBERG=1`, the function emits a
quoted `CREATE TABLE ... USING iceberg` definition from the selected publication
columns and their versioned mappings.

Whether a table is new or pre-created, validation requires:

- a writable Iceberg foreign table;
- the exact published column names and order;
- the exact mapped PostgreSQL types; and
- no destination `NOT NULL` constraint on a nullable source column.

A destination may be more nullable than its source because that does not reject
replicated source values. Automatically created tables retain source `NOT NULL`
constraints. Destination assignment occurs only after validation.

## Evidence

Run `schema_20260914_165528` passed all nine assertions. The retained
[results](evidence/2026-09-14-auto-schema-results.json) demonstrate:

- creation of a writable `pg_lake_iceberg` foreign table;
- canonical `bigint`, `text`, `boolean`, and `timestamp with time zone` columns;
- exact source nullability and initial row values;
- a persisted four-column contract at mapping version 1;
- rejection of a pre-created `text` column where `bigint` was required;
- zero initial-copy rows and `0/0` checkpoints after rejection; and
- preservation of the incompatible destination's previous readable row.

pg_lake documents writable Iceberg creation through standard
[`CREATE TABLE ... USING iceberg`](https://github.com/Snowflake-Labs/pg_lake/blob/main/docs/iceberg-tables.md).

## Remaining boundaries

- Mapping version 1 intentionally covers a conservative fixed set. Numeric
  precision/scale, varchar/char typmods, arrays, enums, domains, composites,
  geometry, and custom types need explicit policies and tests.
- Cross-cluster type identity currently uses canonical `format_type` text. User
  type namespaces and stable type identifiers are not represented.
- Automatic schema creation is not performed; the destination schema must exist.
- Generated columns are rejected even though PostgreSQL 18 and pg_lake can
  represent some generated-column configurations.
- Primary keys, defaults, checks, collations, identities, and destination
  partitioning are not copied. Replication identity remains journal metadata,
  not a destination constraint.
- Schema evolution after initial synchronization still pauses through relation
  versioning; additive-column automation is pending.

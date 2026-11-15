# Ordered schema evolution

Ordered schema support is an explicit opt-in for the PostgreSQL 18 two-table
push prototype and the qualified local PostgreSQL pg_lake catalog. Frozen mode
remains the default. Supported changes are one nullable column addition without
a default, or one column rename, per DDL statement. Key-column renames preserve
column identity. Defaults, conversions, rewrites, identity/generated columns,
custom collations, inheritance, table lifecycle and namespace changes are rejected.

## Controlled activation

Create and initialize the source stream using the existing empty, paused or
bounded online baseline. Stop capture and wait for its actor to exit, then call
`lake_capture.enable_schema_auditing(stream_id)` as the owning replication LOGIN.
The function requires READ COMMITTED, a complete baseline, the original source
binding, explicit table access, both application locks and the capture lease.
It validates retained definitions before changing the protected flag. A stream
with existing consumers cannot switch modes; use a new source stream and mirror.

Register ordered consumers explicitly with
`lake_capture.register_consumer(stream_id,true)`. The original one-argument call
continues to register frozen consumers and is rejected for ordered streams.
Application DDL still requires ordinary table ownership. The capture operator
receives no application write or ownership privileges and cannot set the schema
flag through direct stream INSERT or UPDATE grants.

For a new destination subscription, pass `"schema_mode":"ordered"` alongside
`"mode":"iceberg_push"` in `create_push_subscription` source configuration.
Each of its two table registrations must include a `definition` containing the
retained version-one JSON definition, not a later live catalog description:

```sql
SELECT jsonb_agg(to_jsonb(r) || jsonb_build_object('definition',d.definition_json)
                 ORDER BY r.table_id)
FROM lake_capture_log.registrations r
JOIN lake_capture_log.schema_definitions d USING(stream_id,table_id)
WHERE d.capture_generation = 1 AND d.schema_version = 1;
```

Use the actual capture generation. Destination creation validates definitions,
creates a fresh private mirror including nullability, initializes history and
starts stopped. Existing mirrors cannot be upgraded in place. Managed enable
selects the optional `lake-replication-push-publisher`; frozen push and ordinary
logical pull retain the existing publisher. Schema initialization also requires
a stopped destination service and no previously admitted transactions.

## Historical definitions and source capture

Eligible initial definitions are recorded inside source registration while both
application tables are locked. Registration requires READ COMMITTED and native
post-lock relation validation. Legacy frozen streams with defaults or other
columns outside the stricter evolution contract remain supported, but receive
no invented strict history. Missing initial definitions require a new registration.

The event trigger records immutable normalized before/after definitions in the
application DDL transaction. Definitions use stable PostgreSQL attribute numbers,
UTF8 names bounded to 63 bytes, primary-key flags and nullability, with the four
supported scalar types. `LKDSC1` definitions and `LKDDL1` changes have independently
checked native, Rust and SQL encodings. Audited DDL in REPEATABLE READ fails closed.
Default frozen guards use a current snapshot even if the caller's transaction
began before registration. Private schema and extension guards remain active.

Native callbacks consume historical audit tuples and emit ordered tableless
Schema controls. Row versions change at those ordinals, including multiple
revisions and both tables in one commit. Callbacks perform no SPI or object I/O.
Large external TOAST audit payloads are supported; size checks precede detoasting.

Each durable capture receipt stores the post-transaction versions of both tables.
A new decoding read loads the newest receipt at or below the slot's confirmed
position. Capture may be ahead after a lost COMMIT response, and PostgreSQL may
rewind confirmation after a crash. Replaying later immutable audit events restores
the correct state without guessing from current catalogs. Definitions and compact
receipts remain available throughout the bounded capture generation.

## Admission and publication

The importer retains initial registrations separately from later versions and
normalizes every row against the definition at its source ordinal. Original
ordinals, Schema barriers and progress controls enter the durable journal.
Versions registered during admission remain unpublished until their DDL is applied.

Schema, progress and row metadata all count toward the existing journal byte
quota. A late admission failure rolls back the journal, new core relation versions,
retained definitions, schema heads, receipts and imported checkpoint together.
Retry and replay preserve original batch identity and payload digest.

Ordered publication reuses the core batch preparation, grouped mutation helpers
and checkpoint completion. It flushes DML before each schema barrier, applies
validated DDL, assigns the new relation version and continues later DML in the
same PostgreSQL transaction. DDL-only and progress-only commits advance the same
publication boundary. The ordinary publisher cannot mark an unapplied ordered
transaction complete, including transactions without an apply marker.

Destination OIDs are pinned by administrator initialization and cannot be
rewritten by runtime credentials. Privileged DDL is confined to those permanent
Iceberg foreign tables in the local PostgreSQL catalog. The publisher validates
types, names and nullability after taking ordinary transaction relation locks.
Readers can hold the old group while DDL waits; schema, rows, apply markers and
publication checkpoint become visible together after commit. Published source
acknowledgements and pruning still derive only from committed publication.

## Bounds and retained qualification

A generation permits 32 revisions per table, ending at version 33. Manifests
permit two distinct table identities and up to 66 table/version references with
a 4 KiB header. Existing 64 MiB complete-batch, 8 MiB event, 100,000-event and
1,600-column limits remain; ordered normalized journal JSON is separately capped
at 64 MiB. Oversized or unrepresentable work stops without advancing import or
publication. An evolved stream requires drop/new registration for recopy.

The [initial history qualification](evidence/2026-11-14-push-schema-history.json)
covers registration locks, stale snapshots, descriptor codecs and selected
regressions. The [ordered pipeline qualification](evidence/2026-11-15-push-ordered-schema.json)
retains 588 passing normal checks across 38 fixtures, 135 source crash checks and
58 publication crash checks, including 37 for ordered schemas. The pipeline exercises delayed capture,
DDL-only and mixed commits, quotas, ordinary ownership, managed runtime logins,
reader exclusion, replay after pruning, and real source/publication backend
crashes around upload and COMMIT. These are ARM64 local-catalog prototype results;
REST catalogs, automatic failover, sustained sizing and general release support
remain unqualified.

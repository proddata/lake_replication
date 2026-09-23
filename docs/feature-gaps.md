# Beta feature boundaries

This document separates product capability gaps from the release-process gates
in [`BETA-RELEASE.md`](../BETA-RELEASE.md). The first beta is intentionally a
focused PostgreSQL-to-Iceberg replication product, not a general logical
replication or hosted lakehouse control plane.

## Required before beta

### Unified PostgreSQL integration

Status: **implemented and qualified on the native arm64 artifact**. See the
[retained qualification summary](evidence/2026-09-23-unified-extension-results.json).

The former prototype split between the `lake_replication` SQL extension and a
separate `lake_subscription_sugar` hook library has been removed. The packaged
product now has one extension, one shared library, one schema, and one version
boundary named `lake_replication`. Installation, upgrade, backup, launcher
state, monitoring, and removal all follow that single lifecycle.

The artifact qualification covers fresh installation, the supported `0.1.0`
upgrade, logical dump and restore of durable state, subscription management,
credential rotation with generation fencing, recovery, redaction, slot cleanup,
and conservative uninstall.

### Common parameterized PostgreSQL types

Status: **implemented and qualified on the native arm64 artifact**. See the
[retained qualification summary](evidence/2026-09-23-common-parameterized-types-results.json).

The original mapping contract accepted thirteen exact canonical types but
rejected common application columns such as `numeric(12,2)` and
`varchar(64)`. That also made the README's `numeric(12,2)` quick-start example
inaccurate.

Mapping version 1 now additionally accepts:

- `numeric(precision,scale)` when precision is at most 38 and scale is between
  zero and precision, preserving the exact decimal contract;
- bounded and unbounded `character varying`; and
- bounded `character`.

Forms that cannot be mapped losslessly to the supported Iceberg target remain
fail-closed. In particular, unconstrained `numeric`, precision above 38,
negative scale, and scale greater than precision are rejected.

The packaged qualification covers automatic destination DDL, initial copy,
live insert/update, nullable additive decimal evolution, extension upgrade,
artifact-only installation, restart and outage recovery, and uninstall. The
final release commit still needs the ordinary manual amd64 qualification gate.

### Per-subscription credential rotation

Status: **implemented and qualified on the native arm64 artifact**. See the
[retained qualification summary](evidence/2026-09-23-credential-rotation-results.json).

Each subscription can select an absolute owner-only credential file with
`lake_credential_file`. Reapplying that option after an atomic secret rotation
increments a durable launch-configuration generation, gracefully stops the
old supervisor, and starts only that subscription again with freshly read
credentials. The instance-wide setting remains the default for subscriptions
that do not select a path.

## Desirable after the first beta

- Additional scalar types such as `time`, `time with time zone`, and
  `interval`, where pg_lake and Iceberg semantics can be preserved.
- Arrays with an explicit element-type and nullability policy.
- Enums and domains with a stable cross-database representation.
- Generated columns where source and destination evaluation semantics are
  demonstrably equivalent.
- Multiple publications in one subscription.
- Automatic creation of a missing destination schema.
- PostgreSQL 16, 17, and 19 compatibility lanes.

## Explicitly outside the first beta

- REST-catalog-backed destinations, because the current pg_lake commit path
  cannot provide the durable acknowledgement required by the replication
  checkpoint contract.
- Destructive or rewriting online schema changes. They pause replication with
  a durable diagnostic and require an operator-controlled rebuild.
- Native PostgreSQL apply directly into an Iceberg foreign table.
- A hosted multi-tenant control plane, customer-facing web UI, or embedded HTTP
  metrics service.

Unsupported features must fail visibly before advancing a replication
checkpoint. They must never be approximated silently.

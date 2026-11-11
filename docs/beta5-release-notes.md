# v0.1.0-beta.5 release notes — draft

This is preparation for an unpublished release. The 24-hour soak,
final-commit qualification and signed publication/independent
verification remain open. Resolve these gates and update the evidence below
before using this text as published release notes.

## Replication and lifecycle

`lake_replication` replicates ordinary PostgreSQL tables into pg_lake Iceberg
tables through initial snapshot copy and continuous logical replication. It
supports inserts, updates, deletes, primary-key changes, truncation, publication
filters/column lists and safe nullable-column additions. PostgreSQL subscription
commands manage copy, streaming, pause, resume and cleanup through one extension.

Beta.5 splits ingress and publication checkpoints, stages incoming changes as
chunks, and preserves the logical row interfaces and previously staged data.
Initial copies use four instance-wide session leases before source snapshots
and a default 40 ms pause after committed chunks. Existing journal admission
bounds remain eight unpublished transactions and 64 MiB of normalized payload
per subscription. These controls are not a total CPU or memory guarantee.

The candidate includes receiver progress/recovery fixes and the rustls 0.23.46
security update. Linux amd64 and arm64 archives contain the workers, extension,
dependency notices and an offline copy of the separately installed pg_lake
prerequisite patchset.

## Supported configuration and limits

The target is PostgreSQL 18 and pg_lake 3.6 with the local PostgreSQL catalog,
Debian 12-compatible glibc, and the `lake-replication-manifest-cleanup-v1`
prerequisite. See the shipped compatibility contract for exact tested revisions;
the accepted CI archive passes 57 checks per endpoint on native ARM64 at
`8180647d4535fdb1ed223f92f301ba6094a8ec3e` and
`f609ecb17891383f021aebb0e249fe2994ed57bb`. Full native AMD64 qualification
covers the first endpoint separately. These two tested endpoints do not
establish every intervening revision.

REST catalogs, PostgreSQL 16/17/19, multiple publications per subscription,
`enabled`/`copy_data` creation options, resumable initial-copy chunks, distributed
fencing and SQL downgrade are outside the beta scope. Unsupported source types
and incompatible schema changes fail closed. Source connection passwords remain
accessible to privileged database/backup readers. See
[feature boundaries](feature-gaps.md) and the main README for the full contract.

## Installation and upgrades

Authenticate the published assets using the
[independent verification procedure](release-verification.md). Install the
supported pg_lake prerequisite separately; the worker installer does not enforce
its revision or patch it automatically. Preserve both prerequisite libraries
together on upgrade or rollback.

Drain subscriptions and take a coordinated PostgreSQL/object-store backup before
installing new binaries and upgrading the extension to `0.1.0-beta.5`. Follow
the bundled `UPGRADE.md` for the exact stopped-server procedure, legacy binding
adoption and runtime validation. No SQL downgrade is supplied.

## Evidence to finalize

The accepted development candidate `b0a703a` passes 662 native checks on each
architecture, covering lifecycle, TLS/SCRAM, crash recovery, physical rollback,
admission and historical package upgrades. Its ten-minute soak and paired
maintenance experiments pass; they do not satisfy the day-long gate.

Before publication, replace this section with the final source commit and
archive hashes, compatibility results, security audit identity, 24-hour soak
result and final native qualification. Retention/storage policy remains an
operator concern: short experiments show Iceberg metadata growth, and disposable
120-second physical retention is not a production recommendation.

# Milestone 29: installable beta artifact foundation

Status: the destination control schema is packaged as a PostgreSQL extension,
and a native-platform release bundle installs both extensions and all four
workers next to an existing PostgreSQL 18 plus pg_lake installation.

## PostgreSQL extension

`extensions/lake_replication` generates the `lake_replication--0.1.0.sql` base
extension from the 15 ordered migrations. The generator validates and removes
only each migration's outer `BEGIN`/`COMMIT`, because PostgreSQL controls the
extension installation transaction. The original migrations remain usable by
the laboratory installer.

The extension declares pg_lake as a prerequisite, owns its control-schema
objects, drops cleanly without removing pg_lake, and recreates cleanly. Run
`make lake-replication-extension-test` against the main lab.

## Distribution bundle

`deploy/install/Dockerfile` builds, but does not ship, pinned PostgreSQL 18,
Rust 1.85, and Debian Bookworm build environments. Its exported artifact
contains only:

- stripped release builds of the receiver, publisher, initial-sync worker, and
  supervisor;
- the `lake_replication` SQL extension;
- the `lake_subscription_sugar` extension and shared library;
- installer documentation, license, source revision, target architecture, and
  SHA-256 manifests.

The installer verifies every payload checksum before writing, requires
PostgreSQL 18, pg_lake, dblink, and a matching CPU architecture, and does not
edit configuration, restart PostgreSQL, or create/drop database objects.
Artifacts are generated as a deterministic tar archive with normalized
ownership and timestamps. A build from an uncommitted tree is explicitly
marked `-dirty` rather than claiming exact commit provenance. Two builds from
the same source state produced the same archive SHA-256.

Run `make install-bundle` to build the native artifact and
`make install-bundle-test` to build and exercise the exact export against the
main PostgreSQL 18 lab. The test checks the archive and source identity, verifies
all payload checksums, runs the installer, loads both packaged extensions, and
verifies every native file links on the target system.

Run `install_20260920_152332` passed all six artifact assertions. Its retained
result is [recorded here](evidence/2026-09-20-install-bundle-results.json).
The reproducible arm64 archive checksum was
`d3a89ad0046b9c462bc74861de8c11c019e059ce4fdb8a6972a2e7076076ab36`.

## Remaining boundaries

- Only the native arm64 artifact was built in this run; amd64 and a true
  multi-platform release job remain.
- The smoke target uses the pinned lab as its existing pg_lake installation;
  an isolated clean-host and broader pg_lake compatibility lane remain.
- The archive has checksums but is not signed and has no SBOM or provenance
  attestation yet.
- `lake_replication` has its initial base version but no real released-version
  upgrade fixture yet.
- The build images are digest-pinned, but Debian packages resolved during the
  extension build are not locked to a snapshot repository.

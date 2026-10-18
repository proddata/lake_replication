# Binary installation bundle

This directory builds the artifact installed next to an existing PostgreSQL 18
and pg_lake installation. It does not contain PostgreSQL, pg_lake, MinIO, or the
laboratory crash patch.

Build the native-platform bundle with:

```sh
make install-bundle
```

The output is a reproducible `dist/lake_replication-VERSION.tar.gz` plus
its checksum and an unpacked copy for inspection. On the destination host,
unpack it and run `install.sh` as a user allowed to write PostgreSQL's extension
directories and `/usr/local/bin`. The installer verifies every bundled file
against `SHA256SUMS` before copying anything.

The installer refuses PostgreSQL versions other than 18, an installation where
`pg_lake.control` is absent, and an artifact built for a different CPU
architecture. `DESTDIR` is supported for package construction and filesystem
tests. `PG_CONFIG` can select a specific PostgreSQL installation.

The bundle installs:

- the receiver, publisher, initial-sync, and supervisor executables;
- the `lake_replication` SQL extension and shared library.

Source credentials for launcher-managed subscriptions can be supplied in the
standard `CREATE SUBSCRIPTION ... CONNECTION` string. They are stored in an
extension-owned table with row-level security, so `pg_read_all_data` alone
cannot read them. Grant `lake_replication_worker` only to the login used by
workers. The extension installer, superusers, backups, and SQL statement logs
remain trusted; stronger at-rest secrecy requires a key outside the database.

For destination authentication or source TLS settings, optionally set
`lake_replication.credential_file` to an absolute path owned by the
PostgreSQL operating-system user with mode `0600`, or select an absolute path
per subscription with `WITH (lake_credential_file = '/path/to/file')`.
Supported `KEY=value`
entries are the source/destination user, password, TLS mode, root certificate,
and optional TLS server name variables documented in the main README. Symlinks,
group/other permissions, duplicate keys, unknown keys, and files larger than
64 KiB are rejected before a network connection is attempted.

Rotate a source password using `ALTER SUBSCRIPTION name CONNECTION '...'`,
preferably while the subscription is disabled. After atomically rotating a
file, run `ALTER SUBSCRIPTION name SET
(lake_credential_file = '/path/to/file')`. The launcher gracefully replaces
only that subscription's supervisor, which reloads the file; PostgreSQL does
not need to restart.

It does not edit `postgresql.conf`, restart PostgreSQL, create extensions in a
database, or remove data. Those remain explicit operator actions.

For an existing installation, disable subscriptions and take the coordinated
PostgreSQL/object-store backup described in the bundled `UPGRADE.md` before
replacing files. After installing this bundle, upgrade the database objects
with:

```sql
ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.1';
```

## Removal

Disable or drop managed subscriptions first, remove
`lake_replication` from `shared_preload_libraries`, and stop PostgreSQL
before removing its shared library. From the unpacked bundle, run:

```sh
sudo ./uninstall.sh
```

The uninstaller removes only the four executables and the extension program,
control, and SQL files. It intentionally preserves PostgreSQL configuration,
roles, extension objects, replication slots, Iceberg tables, object-storage
data, credentials, and logs. Use `DROP EXTENSION` separately only when database
objects should also be deleted. Reinstalling the same archive restores the
program files.

## Release artifacts

Build both supported Linux architectures with Docker Buildx and QEMU:

```sh
make install-bundles
```

This produces architecture-qualified archives and checksum files under
`dist/release/` for `linux-amd64` and `linux-arm64`. A release build refuses a
dirty source tree so its recorded revision is unambiguous. For a local test of
uncommitted packaging changes, opt in explicitly with
`ALLOW_DIRTY=1 make install-bundles`.

Pushing a version tag such as `v0.1.0-beta.1` runs the release workflow. The tag must
match the worker and SQL-extension versions. The workflow builds both bundles,
generates an SPDX JSON SBOM for each, creates aggregate SHA-256 checksums,
keylessly signs every archive, SBOM, and checksum manifest with Sigstore
Cosign, and publishes them in a GitHub release. All third-party workflow
actions are pinned to immutable commit SHAs.

The bundle and GitHub release both include `COMPATIBILITY.md`. It distinguishes
the supported beta target from the exact configuration already validated; in
particular, the recorded pg_lake commit is evidence, not an exact-revision
installation gate.

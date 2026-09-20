# Binary installation bundle

This directory builds the artifact installed next to an existing PostgreSQL 18
and pg_lake installation. It does not contain PostgreSQL, pg_lake, MinIO, or the
laboratory crash patch.

Build the native-platform bundle with:

```sh
make install-bundle
```

The output is a reproducible `dist/pg_lake_replication-VERSION.tar.gz` plus
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
- the `lake_replication` SQL extension; and
- the `lake_subscription_sugar` extension and shared library.

It does not edit `postgresql.conf`, restart PostgreSQL, create extensions in a
database, or remove data. Those remain explicit operator actions.

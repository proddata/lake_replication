# Clean-host qualification

This fixture verifies a release archive on disposable PostgreSQL 18 hosts. The
destination image starts with PostgreSQL and the pinned supported pg_lake
revision, then installs pg_lake_replication only from the supplied archive. No
repository source tree is mounted into a running container.

The probe checks:

- the external archive checksum and its embedded checksums;
- installation and extension loading on a fresh destination;
- in-place extension upgrade from the installable development baseline with
  retained control state and no advertised SQL downgrade;
- SCRAM authentication using an owner-only worker credential file;
- initial copy, live insert/update, disable/enable catch-up, and restart
  recovery;
- the SQL status/metrics contract, source-outage backoff and recovery, resumed
  replication, and structured failure logs without credential values; and
- removal of project executables and extension program files while preserving
  pg_lake.

Build the native release archive, then run:

```sh
make install-bundles
make qualification-test
```

To qualify an already downloaded native archive, keep its adjacent
`.sha256` file and select it explicitly:

```sh
QUALIFICATION_ARCHIVE=/path/to/pg_lake_replication-0.1.0-beta.1-linux-arm64.tar.gz \
  make qualification-test
```

The first run builds the pinned pg_lake prerequisite image and can take a long
time. Later runs reuse that image unless `QUALIFICATION_REBUILD_BASE=1` is set.
Result JSON and captured logs are written under `.qualification/`. The probe
always removes its containers and volumes; it does not start, stop, or modify
the development lab.

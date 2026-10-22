# Clean-host qualification

This fixture verifies a release archive on disposable PostgreSQL 18 hosts. The
destination image starts with PostgreSQL and the pinned supported pg_lake
revision, then installs lake_replication only from the supplied archive. No
repository source tree is mounted into a running container.

The probe checks:

- the external archive checksum and its embedded checksums;
- installation and extension loading on a fresh destination;
- in-place extension upgrade from the installable development baseline with
  retained control state and no advertised SQL downgrade;
- SCRAM authentication with source credentials in `postgres://` and
  `postgresql://` connection URLs and an owner-only destination credential
  file;
- automatic persistent source slot creation and explicit errors for unsupported
  PostgreSQL subscription options;
- initial copy, interrupted-copy restart from a fresh snapshot, live
  insert/update, disable/enable catch-up, and restart recovery;
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
QUALIFICATION_ARCHIVE=/path/to/lake_replication-0.1.0-beta.2-linux-arm64.tar.gz \
  make qualification-test
```

The first run builds the pinned pg_lake prerequisite image and can take a long
time. Later runs reuse that image unless `QUALIFICATION_REBUILD_BASE=1` is set.
Result JSON and captured logs are written under `.qualification/`. The probe
always removes its containers and volumes; it does not start, stop, or modify
the development lab.

The `pg-lake-base` build target contains only PostgreSQL and pg_lake
prerequisites. Qualification rejects an image that already contains replication
libraries or workers, so source-built files cannot mask omissions from the
archive. If an older cached prerequisite image triggers that guard, rerun with
`QUALIFICATION_REBUILD_BASE=1`.

The renamed arm64 archive from commit `6772922` passed all 36 checks with
the upstream-only fixture. See the [retained results](../../docs/evidence/2026-10-18-renamed-package-qualification-results.json)
and [build/fixture provenance](../../docs/evidence/2026-10-18-renamed-package-qualification-provenance.json).

The subsequent [fresh-submodule run](../../docs/evidence/2026-10-18-fresh-submodule-qualification-results.json)
also passed all 36 checks after completing the full upstream build from the
local bundle. Its [provenance](../../docs/evidence/2026-10-18-fresh-submodule-qualification-provenance.json)
records the build retries and the fix to refresh Debian package indexes before
installing late build dependencies.

The GitHub Actions CI workflow intentionally does not run this expensive build
for ordinary pushes or pull requests. To qualify the amd64 artifact remotely,
open the **CI** workflow in GitHub Actions and choose **Run workflow**. A manual
run builds both release artifacts and runs `qualify-amd64` after the amd64
bundle is available.

For the full beta.2 lifecycle suite on both native architectures, run
**Packaged lifecycle qualification** with the ID of a successful **CI** run.
It downloads that run's amd64/arm64 bundles and checks out the exact candidate
revision. Checksums, embedded source revision and generated SQL are verified
before testing. Each architecture runs fresh lifecycle checks, Rust-child
crashes, receiver regressions, and a stopped-server beta.1 upgrade. The old
package is rebuilt from preserved commit `85a32b4`; it is not a fabricated
beta.1 configuration on the new binary. Receiver regressions retain their SQL
overlay fixture, with a byte comparison proving the overlay matches the archive.

```sh
gh workflow run lifecycle-qualification.yml -f artifact_run_id=RUN_ID
```

Evidence and the rebuilt old package are uploaded separately per architecture
and retained for 30 days. Download and retain accepted results before expiry.
This workflow does not cover the long soak or the physical/object-store
recovery drill.

The committed lifecycle candidate `85a32b4` subsequently passed all 57
checks, including source connection credentials, existing slots, and interrupted
initial-copy recovery. See its [retained results](../../docs/evidence/2026-10-18-lifecycle-qualification-results.json).
The earlier 36-check results above describe the earlier package and probe.

## pg_lake compatibility lane

The same artifact-only probe can build and qualify disposable hosts at the two
exact pg_lake 3.6 revisions selected for the beta compatibility range:

```sh
QUALIFICATION_ARCHIVE=/absolute/path/to/lake_replication-0.1.0-beta.2.tar.gz \
  make compatibility-test
```

The defaults are commits `8180647d4535fdb1ed223f92f301ba6094a8ec3e` and
`f609ecb17891383f021aebb0e249fe2994ed57bb`. Override them only with two or
more distinct full commit SHAs via the space-separated
`COMPATIBILITY_PG_LAKE_REVISIONS` variable. A base image is cached separately
for each revision; the first run of a new revision is therefore expensive.

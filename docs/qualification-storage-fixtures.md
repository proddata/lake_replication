# Source-pinned qualification storage fixtures

Fresh native CI runners could not pull the previously pinned Quay MinIO server
and client images: both requests returned 401. The prior native qualification run
failed at that pull after compiling pg_lake; the beta.3 retry was cancelled once
the same registry failure was confirmed. CI checks and native bundle builds had
passed. This was fixture availability, not a replication test result.

Qualification now builds the same source releases directly from immutable
upstream revisions, using the official Go 1.24.2 builder pinned by image digest:

| Fixture | Source release | Immutable source revision |
| --- | --- | --- |
| MinIO server | RELEASE.2025-04-22T22-12-26Z | [0d7408fc9969caf07de6a8c3a84f9fbb10a6739e](https://github.com/minio/minio/tree/0d7408fc9969caf07de6a8c3a84f9fbb10a6739e) |
| MinIO client | RELEASE.2025-04-16T18-13-26Z | [b00526b153a31b36767991a4f5ce2cced435ee8e](https://github.com/minio/mc/tree/b00526b153a31b36767991a4f5ce2cced435ee8e) |

The Dockerfile verifies each checkout, uses its committed module checksums,
disables automatic Go toolchain changes, and statically builds the native Linux
binaries. Source licences are retained in the fixture images. These are
independently compiled fixtures, not claimed to be byte-identical to the retired
vendor images. They and their build dependencies are excluded from the shipped
lake_replication bundle.

`tests/qualification/storage.py` keys local tags by source revision, Dockerfile
hash and architecture. It validates cached image labels/architecture and records
actual image IDs in every qualification result. Explicit image overrides must be
locally available and match the native architecture; they are recorded as
overrides rather than pinned-source builds. `build_images` ensures fixtures are
available before starting expensive pg_lake compilation.

Both native CI qualification paths now run `storage_preflight.py` first. It
creates a bucket, uploads/downloads exact bytes and deletes the object, then
removes its containers and volumes. Existing cached source-built images are
reused across the subsequent package lanes. Build logs and provenance remain
under `.qualification/`; a fixture preparation failure stops before the long
prerequisite build.

```sh
python3 tests/qualification/storage_preflight.py
QUALIFICATION_ARCHIVE=/path/to/lake_replication-0.1.0-beta.3-linux-arm64.tar.gz make lifecycle-test
```

The source-built arm64 fixtures pass the local S3 preflight. The CI-built beta.3
arm64 artifact has a verified checksum and source revision `fdb3484`; every
executable and extension payload matches the earlier locally qualified archive.
All 119 fresh lifecycle checks pass against that CI artifact with the replacement
fixtures. The two-check S3 preflight and three fixture-selection unit tests also
pass. [Retained local evidence](evidence/2026-10-25-source-storage-fixture-results.json)
records fixture identities, artifact/source hashes, checks and private-log hashes.
Native amd64 and final-commit CI qualification remain required.

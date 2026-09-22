# Milestone 30: multi-architecture release automation

Status: amd64 and arm64 Linux artifacts build from one command, and a tagged
release is configured to publish checksummed, SBOM-described, keylessly signed
assets.

## Release construction

`make install-bundles` invokes `deploy/install/build-release.sh`, which builds
architecture-qualified `linux-amd64` and `linux-arm64` archives through Docker
Buildx. A release build refuses a dirty worktree; `ALLOW_DIRTY=1` exists only
for exercising uncommitted packaging changes. `check-release-version.sh`
requires the release tag, Rust worker version, and `lake_replication` extension
version to agree.

The pull-request/main CI workflow runs the existing full check target and
builds both distributable archives. The tag workflow additionally:

- generates an SPDX JSON SBOM for each extracted artifact with Syft;
- creates per-artifact and aggregate SHA-256 manifests;
- keylessly signs every archive, checksum manifest, and SBOM with Cosign; and
- creates the GitHub release and uploads all assets and signature bundles.

Every third-party GitHub Action is pinned to an immutable full commit SHA with
its human-readable release version retained as a comment.

## Validation

The dirty-tree guard rejected an ordinary release build. With the explicit
test override, the same arm64 host successfully built and exported both target
architectures. Both external archive checksums and both embedded payload
manifests verified. `file` identified the four workers and C extension in the
amd64 archive as x86-64 ELF objects, and their counterparts in the arm64
archive as AArch64 ELF objects. The artifacts recorded the expected target and
the intentionally dirty source revision.

The exact test artifact sizes and hashes, plus the validation commands, are in
[the retained result](evidence/2026-09-22-multiarch-release-results.json).
`make check`, shell syntax checks, workflow YAML parsing, and
`git diff --check` all passed.

## Remaining boundaries

- The release workflow is configured but has not yet been exercised by an
  actual pushed tag, so GitHub OIDC signing and release publication still need
  a first-release proof.
- Only PostgreSQL 18 on Debian Bookworm/glibc is supported. A clean-host test
  and a broader pg_lake revision compatibility matrix remain.
- Debian packages installed while building the extension are not locked to a
  snapshot repository, and automated license-policy checking remains.
- The extension has a `0.1.0` base version but no released-version upgrade
  fixture yet.

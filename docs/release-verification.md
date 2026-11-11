# Independent release verification

Run this procedure on a non-build Linux machine after publication. Until then,
G9 remains open. Use the exact release tag approved in `BETA-RELEASE.md`; the
commands below use `v0.1.0-beta.5`. Install GitHub CLI, Cosign 3 and GNU coreutils
through a trusted channel first. A download or signature failure stops the
verification; do not skip certificate or transparency-log checks.

## Download and authenticate the assets

Use a fresh directory so old assets cannot satisfy missing downloads:

```sh
set -eu
tag=v0.1.0-beta.5
repository=proddata/lake_replication
download=$(mktemp -d)
gh release download "$tag" --repo "$repository" --dir "$download"
cd "$download"

identity="https://github.com/$repository/.github/workflows/release.yml@refs/tags/$tag"
for architecture in amd64 arm64; do
    archive="lake_replication-${tag#v}-linux-$architecture.tar.gz"
    for file in "$archive" "$archive.sha256" "${archive%.tar.gz}.spdx.json"; do
        cosign verify-blob --bundle "$file.sigstore.json" \
            --certificate-identity "$identity" \
            --certificate-oidc-issuer https://token.actions.githubusercontent.com \
            "$file"
    done
done
for file in COMPATIBILITY.md SHA256SUMS; do
    cosign verify-blob --bundle "$file.sigstore.json" \
        --certificate-identity "$identity" \
        --certificate-oidc-issuer https://token.actions.githubusercontent.com \
        "$file"
done
sha256sum -c SHA256SUMS
for architecture in amd64 arm64; do
    sha256sum -c "lake_replication-${tag#v}-linux-$architecture.tar.gz.sha256"
done
```

The expected identity is fixed to this repository, release workflow and tag.
A checksum alone does not authenticate a download. Retain the verification
output, asset SHA-256s, tag commit, release workflow run and verifier versions.
Compare the archive hashes with the publication audit's `results.json`; those
must identify the same source revision as the approved tag commit. Resolve an
annotated tag to its commit rather than comparing its tag-object SHA.

## Inspect and install the native archive

Select the machine's architecture explicitly (`amd64` or `arm64`), extract the
authenticated archive, and verify its internal files before installing:

```sh
architecture=amd64
archive="lake_replication-${tag#v}-linux-$architecture.tar.gz"
unpacked=$(mktemp -d)
tar -xzf "$archive" -C "$unpacked"
cd "$unpacked/lake_replication-${tag#v}"
sha256sum -c SHA256SUMS
cat VERSION TARGETARCH SOURCE_REVISION
```

Check that those three fields match the approved version, native architecture
and tag commit. Inspect the downloaded SPDX documents and compare their hashes
with the publication audit. Follow the bundled `README.md`, `COMPATIBILITY.md`
and `PG_LAKE_PREREQUISITE/README.md` to prepare PostgreSQL 18 and the supported
pg_lake runtime on the disposable verification host. Then run `sudo ./install.sh`
and complete the repository README's fresh extension installation, initial-copy
and live replication walkthrough at the authenticated release tag
(`https://github.com/proddata/lake_replication/tree/v0.1.0-beta.5`).
Record exact source/destination rows and subscription
health, then remove the test subscription and verify source-slot cleanup.

Record this installation result for each supported native architecture. Do not
use a locally rebuilt archive in place of a downloaded release asset. Publish
the resulting evidence and update G6/G9 only after all required checks pass.

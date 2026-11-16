# Milestone 31: shipped compatibility contract

Status: every install bundle and tagged GitHub release carries an explicit
compatibility matrix.

`deploy/install/COMPATIBILITY.md` separates the beta's supported target from
the exact lab configuration already validated. It records PostgreSQL 18,
Debian Bookworm-compatible glibc, amd64 and arm64, pg_lake 3.6 at commit
`8180647d4535fdb1ed223f92f301ba6094a8ec3e`, and the local PostgreSQL catalog.
It explicitly excludes REST catalogs and other PostgreSQL major versions.

The matrix is inside the archive and therefore covered by its payload checksum.
The release workflow also publishes it as a standalone, checksummed and
keylessly signed asset. The installer reports the validated pg_lake revision
without rejecting other revisions: an installed extension does not expose its
source commit, and one tested point is not yet a defensible compatibility
range.

The rebuilt native artifact contained the matrix, its embedded SHA-256 manifest
verified, and the installer contained the matching compatibility notice.
`make check`, workflow YAML parsing, shell/Python syntax checks, and
`git diff --check` passed.

Remaining work is the substantive half of compatibility: exercise the packaged
artifact against at least one independently selected pg_lake revision in CI,
then replace the single validated point with an evidence-backed revision range.

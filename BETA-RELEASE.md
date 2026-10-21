# Beta release gate

Target: `v0.1.0-beta.2` candidate

Status: **not releasable yet**. The artifact, clean-host qualification,
operations/observability contract, and operator documentation exist, but a
full recovery drill, security review, soak, and the first signed prerelease
still need evidence.

This file is the release control document. Historical `docs/milestone-*.md`
files remain the engineering evidence journal; new beta work updates the gates
below rather than creating another numbered milestone.
Product capability boundaries and deliberately deferred features are tracked
separately in [the beta feature-gap document](docs/feature-gaps.md).

Status values: `PASS`, `PARTIAL`, `NOT STARTED`, `BLOCKED`.

## Required gates

| Gate | Status | Exit criterion | Current evidence / next action |
|---|---|---|---|
| G0. Remote CI | PARTIAL | Required checks pass on the release commit in GitHub | [Run 39115397763](https://github.com/proddata/lake_replication/actions/runs/39115397763) passed checks and both bundle builds on `6772922`. The beta.2 lifecycle candidate passes local checks and packaged arm64 lanes and is preserved locally, but has not been tested in remote CI. Run CI on the final candidate, then retain its amd64 qualification result. |
| G1. Installable artifacts | PARTIAL | amd64 and arm64 archives install and load from exported artifacts; checksums verify | The local beta.2 arm64 candidate passes 279 packaged checks, with a verified documentation-only refresh preserving every executable payload. Its amd64 build/qualification and remote CI are pending. See [candidate artifact and evidence](docs/pg-extension-base-migration-results.md). |
| G2. Clean-host qualification | PASS | A fresh supported host with preinstalled PostgreSQL 18 + pg_lake installs only from the archive and completes one replication cycle | The beta.2 arm64 runtime archive passes the 119-check fresh lifecycle lane on clean database/storage volumes with no source mounts or adapter. The other candidate lanes cover a real beta.1 handoff, receiver regressions and forced Rust-child faults. See [current qualification](docs/pg-extension-base-migration-results.md); amd64 remains a G1 requirement. |
| G3. Release-candidate correctness and soak | BLOCKED | The release candidate completes the core suite plus a documented 24-hour soak with restart, storage outage, backlog, and recovery events | The migrated beta.2 source fixes handle lifetime and descendant cleanup, with 279 packaged arm64 checks. Admission/resource-policy findings and the packaged RC soak remain unresolved; the migration alone does not close this gate. See [current migration](docs/pg-extension-base-migration-results.md). |
| G4. Compatibility range | PARTIAL | The release notes name an evidence-backed pg_lake revision range, with at least two independently selected revisions passing the compatibility lane | Historical beta.1 archive `55822ef` passed all 27 artifact-only checks against pg_lake 3.6 commits `8180647d4535fdb1ed223f92f301ba6094a8ec3e` and `f609ecb17891383f021aebb0e249fe2994ed57bb`, twelve days apart on one ancestry path. The new beta.2 candidate currently qualifies only the pinned commit on arm64; re-run the additional-revision lane. See the shipped [compatibility matrix](deploy/install/COMPATIBILITY.md) and [evidence](docs/evidence/2026-09-22-pg-lake-compatibility-results.json). |
| G5. Operations and observability | PASS | Operators can detect unhealthy subscriptions, lag, retries, and last failure without reading ad-hoc files | The beta.2 fresh packaged lane verifies all documented metrics, source outage/backoff/recovery, retained last-failure evidence, structured logs and credential redaction. Worker log filenames now include the database OID. See [current qualification](docs/pg-extension-base-migration-results.md). |
| G6. Supply-chain and security review | PARTIAL | Published files have checksums, SBOMs, signatures, vulnerability results, and an approved runtime license inventory with no unresolved high-severity finding | Tag workflow generates SPDX SBOMs and keyless signatures. Add artifact vulnerability scanning, automated license policy, and review of shipped Debian/Rust dependencies. |
| G7. Upgrade and rollback | PARTIAL | Install from an earlier beta, upgrade without data loss, and execute the documented binary/config rollback procedure | The beta.2 candidate preserves frozen baseline/beta.1 scripts and adds a real 47-check stopped-server beta.1 handoff, explicit origin adoption and rollback validation. A physical PostgreSQL/object-store recovery drill remains pending; no SQL downgrade is supplied. See [procedure](docs/upgrade-and-rollback.md) and [current migration evidence](docs/evidence/2026-10-21-base-lifecycle-migration-results.json). |
| G8. Operator documentation | PASS | A new operator can install, configure, verify, monitor, troubleshoot, upgrade, and remove the beta from published documentation alone | The README covers evaluation, install, configuration, verification, and lifecycle; the operations guide covers monitoring, alerts, logs, and first response; the shipped bundle includes backup/upgrade/rollback and conservative uninstall procedures. |
| G9. Signed prerelease publication | PARTIAL | GitHub prerelease `v0.1.0-beta.1` is published and independently verified from downloaded assets on a non-build machine | Worker, primary extension, and artifact versions now match `0.1.0-beta.2`, and prerelease tags create GitHub prereleases. Publish, then verify checksums, Sigstore identities, SBOMs, and installation from downloaded assets. |

## Release decision

The [base lifecycle migration](docs/pg-extension-base-migration-results.md) is
implemented in the main extension and locally qualified with 279 packaged checks.
The candidate has direct base registration, durable handles, descendant cleanup,
origin checks and a stopped-server beta.1 handoff. It is preserved in a local commit and remains
unpublished, with admission, amd64/remote CI, soak and recovery work still pending.

The [empty filtered-stream diagnostic](docs/empty-filtered-stream-diagnostic.md)
also reproduces receiver retry without progress after a large unrelated source
transaction, before and after admission. Its [SQL fix](docs/empty-filtered-stream-fix-results.md)
passes 75 regression checks. The subsequent [receiver lifetime fix](docs/receiver-progress-fix-results.md)
passes 96 checks including native bulk progress, prompt disable/drop, source
outage recovery and interrupted-stream replay. Qualify a rebuilt artifact and
complete the remaining lifecycle work before promoting release correctness.

The beta may be published only when every required gate is `PASS`, remote CI is
green on the tagged commit, and there are no known unresolved correctness,
security, data-loss, or PostgreSQL-stability defects. Deferred features must be
listed explicitly in the release notes; REST-catalog destinations remain an
intentional non-goal for this beta.

## Execution order

1. Close G6 supply-chain review and complete the G7 physical/object-store recovery drill.
2. Run the G3 release-candidate soak from the packaged build.
3. Manually run the clean-host qualification on the final release commit.
4. Publish and independently verify G9; record final evidence directly here.

The scope is frozen to these gates. New feature work is deferred unless it
closes a gate or fixes a release-blocking defect.

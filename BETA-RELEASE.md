# Beta release gate

Target: `v0.1.0-beta.1`

Status: **not releasable yet**. The artifact, clean-host qualification,
operations/observability contract, and operator documentation exist, but the
second-architecture installation, a full recovery drill, security review,
soak, and the first signed prerelease still need evidence.

This file is the release control document. Historical `docs/milestone-*.md`
files remain the engineering evidence journal; new beta work updates the gates
below rather than creating another numbered milestone.

Status values: `PASS`, `PARTIAL`, `NOT STARTED`, `BLOCKED`.

## Required gates

| Gate | Status | Exit criterion | Current evidence / next action |
|---|---|---|---|
| G0. Remote CI | PASS | Required checks pass on the release commit in GitHub | GitHub Actions [`check` and `bundle` run 35690455931](https://github.com/proddata/pg_lake_replication/actions/runs/35690455931) passed on `7aa744e`; the uploaded bundle artifact was downloaded and inspected independently. |
| G1. Installable artifacts | PARTIAL | amd64 and arm64 archives install and load from exported artifacts; checksums verify | Both architectures build locally and in remote CI. Run 35690455931 exported amd64 and arm64 archives; both downloaded checksums passed and their embedded `TARGETARCH` values matched. The exact arm64 bundle passes all eight install/load/link assertions; clean-host installation of both remote archives remains. See [release evidence](docs/evidence/2026-09-22-multiarch-release-results.json) and [install evidence](docs/evidence/2026-09-22-install-bundle-results.json). |
| G2. Clean-host qualification | PASS | A fresh supported host with preinstalled PostgreSQL 18 + pg_lake installs only from the archive and completes one replication cycle | The artifact-only arm64 run passed checksum/install, SCRAM credentials, initial/live replication, disable/enable, restart recovery, observability, secret-free logs, slot cleanup, and removal without mounting repository source into the running hosts. See [qualification instructions](deploy/qualification/README.md) and [evidence](docs/evidence/2026-09-22-clean-host-qualification-results.json). The second-architecture archive remains tracked by G1. |
| G3. Release-candidate correctness and soak | PARTIAL | The release candidate completes the core suite plus a documented 24-hour soak with restart, storage outage, backlog, and recovery events | Existing laboratory correctness/crash evidence passes. Define and run one RC soak using the packaged binaries. |
| G4. Compatibility range | PASS | The release notes name an evidence-backed pg_lake revision range, with at least two independently selected revisions passing the compatibility lane | The same archive from `55822ef` passed all 27 artifact-only checks against pg_lake 3.6 commits `8180647d4535fdb1ed223f92f301ba6094a8ec3e` and `f609ecb17891383f021aebb0e249fe2994ed57bb`, twelve days apart on one ancestry path. See the shipped [compatibility matrix](deploy/install/COMPATIBILITY.md) and [evidence](docs/evidence/2026-09-22-pg-lake-compatibility-results.json). |
| G5. Operations and observability | PASS | Operators can detect unhealthy subscriptions, lag, retries, and last failure without reading ad-hoc files | The exact archive from `55822ef` passed packaged PostgreSQL 18 validation of all 12 metrics, an injected source outage, bounded backoff, retained failure state, automatic recovery, resumed replication, structured `cycle_failed` logging, and credential redaction. See [operations](docs/operations.md), [observability evidence](docs/evidence/2026-09-22-observability-results.json), and [qualification evidence](docs/evidence/2026-09-22-clean-host-qualification-results.json). |
| G6. Supply-chain and security review | PARTIAL | Published files have checksums, SBOMs, signatures, vulnerability results, and an approved runtime license inventory with no unresolved high-severity finding | Tag workflow generates SPDX SBOMs and keyless signatures. Add artifact vulnerability scanning, automated license policy, and review of shipped Debian/Rust dependencies. |
| G7. Upgrade and rollback | PARTIAL | Install from an earlier beta, upgrade without data loss, and execute the documented binary/config rollback procedure | The artifact ships explicit `0.1.0` to `0.1.0-beta.1` and subscription-control `1.0` to `1.1` paths, registers durable tables for logical backup, and the clean-host lane verifies retained state, real `pg_dump` inclusion, and no SQL downgrade. Complete a physical PostgreSQL/object-store recovery drill and a real beta-to-beta upgrade. See [procedure](docs/upgrade-and-rollback.md) and [evidence](docs/evidence/2026-09-22-clean-host-qualification-results.json). |
| G8. Operator documentation | PASS | A new operator can install, configure, verify, monitor, troubleshoot, upgrade, and remove the beta from published documentation alone | The README covers evaluation, install, configuration, verification, and lifecycle; the operations guide covers monitoring, alerts, logs, and first response; the shipped bundle includes backup/upgrade/rollback and conservative uninstall procedures. |
| G9. Signed prerelease publication | PARTIAL | GitHub prerelease `v0.1.0-beta.1` is published and independently verified from downloaded assets on a non-build machine | Worker, primary extension, and artifact versions now match `0.1.0-beta.1`, and prerelease tags create GitHub prereleases. Publish, then verify checksums, Sigstore identities, SBOMs, and installation from downloaded assets. |

## Release decision

The beta may be published only when every required gate is `PASS`, remote CI is
green on the tagged commit, and there are no known unresolved correctness,
security, data-loss, or PostgreSQL-stability defects. Deferred features must be
listed explicitly in the release notes; REST-catalog destinations remain an
intentional non-goal for this beta.

## Execution order

1. Qualify the second-architecture archive for G1.
2. Close G6 supply-chain review and complete the G7 physical/object-store recovery drill.
3. Run the G3 release-candidate soak from the packaged build.
4. Publish and independently verify G9; record final evidence directly here.

The scope is frozen to these gates. New feature work is deferred unless it
closes a gate or fixes a release-blocking defect.

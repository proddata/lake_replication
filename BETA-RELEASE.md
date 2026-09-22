# Beta release gate

Target: `v0.1.0-beta.1`

Status: **not releasable yet**. The artifact and release machinery exist, but
remote CI, operator observability, clean-host qualification, compatibility
range, upgrade/rollback, and the first signed prerelease still need evidence.

This file is the release control document. Historical `docs/milestone-*.md`
files remain the engineering evidence journal; new beta work updates the gates
below rather than creating another numbered milestone.

Status values: `PASS`, `PARTIAL`, `NOT STARTED`, `BLOCKED`.

## Required gates

| Gate | Status | Exit criterion | Current evidence / next action |
|---|---|---|---|
| G0. Remote CI | PARTIAL | Required checks pass on the release commit in GitHub | `make check` passes locally. Push the current commits and require the `check` and `bundle` jobs on `main`. |
| G1. Installable artifacts | PARTIAL | amd64 and arm64 archives install and load from exported artifacts; checksums verify | Both architectures build and validate locally; the exact arm64 bundle passes all eight install/load/link assertions. Remote artifact build remains. See [release evidence](docs/evidence/2026-09-22-multiarch-release-results.json) and [install evidence](docs/evidence/2026-09-22-install-bundle-results.json). |
| G2. Clean-host qualification | NOT STARTED | A fresh supported host with preinstalled PostgreSQL 18 + pg_lake installs only from the archive and completes one replication cycle | Build a disposable clean-host fixture that does not mount or compile this repository. Test install, configuration, restart, replication, and removal. |
| G3. Release-candidate correctness and soak | PARTIAL | The release candidate completes the core suite plus a documented 24-hour soak with restart, storage outage, backlog, and recovery events | Existing laboratory correctness/crash evidence passes. Define and run one RC soak using the packaged binaries. |
| G4. Compatibility range | PARTIAL | The release notes name an evidence-backed pg_lake revision range, with at least two independently selected revisions passing the compatibility lane | The shipped matrix currently records only pg_lake 3.6 commit `8180647d4535fdb1ed223f92f301ba6094a8ec3e`. Add a second-revision CI lane. |
| G5. Operations and observability | NOT STARTED | Operators can detect unhealthy subscriptions, lag, retries, and last failure without reading ad-hoc files | Add structured worker logs and a documented metrics/status interface for state, lag, consecutive failures, cycle counts, and last error. Add alert examples. |
| G6. Supply-chain and security review | PARTIAL | Published files have checksums, SBOMs, signatures, vulnerability results, and an approved runtime license inventory with no unresolved high-severity finding | Tag workflow generates SPDX SBOMs and keyless signatures. Add artifact vulnerability scanning, automated license policy, and review of shipped Debian/Rust dependencies. |
| G7. Upgrade and rollback | NOT STARTED | Install from an earlier beta, upgrade without data loss, and execute the documented binary/config rollback procedure | Add the first extension upgrade fixture and package-level upgrade/rollback probe. Database downgrades are not promised; document recovery boundaries. |
| G8. Operator documentation | PARTIAL | A new operator can install, configure, verify, monitor, troubleshoot, upgrade, and remove the beta from published documentation alone | Installation and compatibility documents exist. Add a single runbook with prerequisites, post-install verification, monitoring, common failures, backup/rollback, and uninstall. |
| G9. Signed prerelease publication | NOT STARTED | GitHub prerelease `v0.1.0-beta.1` is published and independently verified from downloaded assets on a non-build machine | Align worker/extension/artifact versions with the prerelease tag, mark the GitHub release as a prerelease, publish, verify checksums/Sigstore identities/SBOMs, and install the downloaded artifact. |

## Release decision

The beta may be published only when every required gate is `PASS`, remote CI is
green on the tagged commit, and there are no known unresolved correctness,
security, data-loss, or PostgreSQL-stability defects. Deferred features must be
listed explicitly in the release notes; REST-catalog destinations remain an
intentional non-goal for this beta.

## Execution order

1. Push the current packaging and hardening commits; make remote CI required.
2. Implement G5 observability, because an unattended beta must be diagnosable.
3. Build G2 clean-host and G4 second-revision compatibility lanes.
4. Implement G7 upgrade/rollback and finish G8 operator documentation.
5. Close G6 supply-chain review and run the G3 release-candidate soak.
6. Publish and independently verify G9; record final evidence directly here.

The scope is frozen to these gates. New feature work is deferred unless it
closes a gate or fixes a release-blocking defect.

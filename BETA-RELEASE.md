# Beta release gate

Target: `v0.1.0-beta.4` candidate

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
| G0. Remote CI | PARTIAL | Required checks pass on the release commit in GitHub | [CI 41084483405](https://github.com/proddata/lake_replication/actions/runs/41084483405) passes checks and both bundles at `2244f6f`. The [native 40 ms paced chunk candidate](docs/staging-pacing-prototype.md#prospective-native-result) independently passes every gate at 123.49-second copy duration and 1.610–1.661-second streaming p95. The first unpaced control misses overlap, invalidating full ABA; both controls retain latency failures, so the diagnostic workflow is red. Advance the working configuration to migration/concurrency-contract hardening, not release promotion. Full native package qualification and AMD64 cold-build validation remain open. |
| G1. Installable artifacts | PARTIAL | amd64 and arm64 archives install and load from exported artifacts; checksums verify | Local beta.4 arm64 archives pass fresh lifecycle, receiver, worker crash, admission and upgrade checks. Measured and final documented archives have identical production payloads; see [current qualification](docs/staging-journal-migration-results.md). Native amd64 and clean final-commit packages remain pending. |
| G2. Clean-host qualification | PASS | A fresh supported host with preinstalled PostgreSQL 18 + pg_lake installs only from the archive and completes one replication cycle | The local beta.4 arm64 candidate passes 119 fresh lifecycle checks on private clean volumes with no SQL/binary overlay, including restore, management and uninstall. See [current production qualification](docs/staging-journal-migration-results.md). Native amd64 and final-commit packages remain G1 requirements. |
| G3. Release-candidate correctness and soak | PARTIAL | The release candidate completes the core suite plus a documented 24-hour soak with restart, storage outage, backlog, and recovery events | Bulk-only admission reproduced a fatal sixteen-stream OOM at 2 GB. The adopted four-slot streaming pool passes 457 final packaged arm64 checks, including that live/restart workload and four-copy coexistence. [Performance evidence](docs/stream-pool-performance-results.md) retains the first inconclusive copy cohort and fixed ten-pair extension. Native ARM64 subsequently failed the four-copy latency limits at `8bcd5b2`; exact data and the sixteen-stream OOM regression passed. See [native findings](docs/native-qualification-failures.md). Both native architectures, final-commit qualification and the packaged RC soak remain required. See [production migration](docs/stream-pool-migration-results.md). |
| G4. Compatibility range | PARTIAL | The release notes name an evidence-backed pg_lake revision range, with at least two independently selected revisions passing the compatibility lane | Historical beta.1 archive `55822ef` passed all 27 artifact-only checks against pg_lake 3.6 commits `8180647d4535fdb1ed223f92f301ba6094a8ec3e` and `f609ecb17891383f021aebb0e249fe2994ed57bb`, twelve days apart on one ancestry path. The beta.4 candidate is locally tested only at the pinned commit on arm64; re-run the additional-revision lane. See the shipped [compatibility matrix](deploy/install/COMPATIBILITY.md) and [evidence](docs/evidence/2026-09-22-pg-lake-compatibility-results.json). |
| G5. Operations and observability | PASS | Operators can detect unhealthy subscriptions, lag, retries, and last failure without reading ad-hoc files | The beta.4 fresh and receiver lanes verify documented metrics, source outage/recovery, retained failures, logs and credential redaction. Managed admission checks verify permissions, budgets, fencing and live counters. The operations guide covers staging/journal as well as engine admission. See [current qualification](docs/staging-journal-migration-results.md). |
| G6. Supply-chain and security review | PARTIAL | Published files have checksums, SBOMs, signatures, vulnerability results, and an approved runtime license inventory with no unresolved high-severity finding | Tag workflow generates SPDX SBOMs and keyless signatures. Add artifact vulnerability scanning, automated license policy, and review of shipped Debian/Rust dependencies. |
| G7. Upgrade and rollback | PARTIAL | Install from an earlier beta, upgrade without data loss, and execute the documented binary/config rollback procedure | The beta.4 candidate preserves all eight earlier SQL files byte for byte and passes a real stopped-server beta.3-to-beta.4 handoff with rollback, checkpoint/data preservation and resumed native replication. Real beta.1 and beta.2 stopped-server handoffs pass SQL rollback, checkpoint/data preservation, native catch-up and slot cleanup. A physical PostgreSQL/object-store recovery drill remains pending; no SQL downgrade is supplied. See [procedure](docs/upgrade-and-rollback.md) and [migration evidence](docs/staging-journal-migration-results.md). |
| G8. Operator documentation | PASS | A new operator can install, configure, verify, monitor, troubleshoot, upgrade, and remove the beta from published documentation alone | The README covers evaluation, install, configuration, verification, and lifecycle; the operations guide covers monitoring, alerts, logs, and first response; the shipped bundle includes backup/upgrade/rollback and conservative uninstall procedures. |
| G9. Signed prerelease publication | PARTIAL | GitHub prerelease `v0.1.0-beta.4` is published and independently verified from downloaded assets on a non-build machine | Worker, primary extension, and artifact versions now match `0.1.0-beta.4`, and prerelease tags create GitHub prereleases. Publish, then verify checksums, Sigstore identities, SBOMs, and installation from downloaded assets. |

## Release decision

The [prospective native 40 ms pacing trial](docs/staging-pacing-prototype.md#prospective-native-result)
provides one valid working point for split checkpoints and chunk-backed staging:
123.49-second copy completion and 1.610–1.661-second streaming p95, passing every
source, clock, overlap, exact-data, catch-up and safety gate with no OOM or
PostgreSQL crash. The full bracketed comparison remains invalid because the
first unpaced control misses overlap; both controls retain latency failures.
The diagnostic workflow is correctly red. No causal/statistical performance
claim or requalification of historical controls is made.

Advance this combination to production hardening. Production schema/defaults
remain unchanged and the checkpoint/storage migration is still deferred. Resolve
populated upgrades and rollback, dump/restore, permissions/direct-write APIs,
the `btree_gist` dependency, configured chunk-size compatibility and an explicit
pacing/concurrency contract. Four-copy validation of a per-worker 40 ms pause
does not supply an aggregate rate bound for arbitrary concurrency. Then qualify
the unmodified migrated archive on both native architectures and complete the
remaining release gates.

Local paced validation passes 131 safety assertions, two independent four-copy
observations at 103.65–104.50 seconds, and one-CPU sensitivity at 124.59 seconds
with 1.173–1.264-second p95. Both local bracketed comparisons retain unpaced
overlap misses and remain inconclusive. [Pacing evidence](docs/staging-pacing-prototype.md)
records those limitations and the native working point. Earlier findings also
remain retained: the [independent-source checkpoint trial](docs/checkpoint-split-prototype.md#prospective-independent-source-native-result)
misses copy completion at 182.15 seconds; the [larger row-cap trial](docs/bulk-copy-chunk-prototype.md#prospective-native-result)
fails streaming p95; and [unpaced chunks](docs/staging-chunk-prototype.md#prospective-native-result)
copy in 87.09 seconds but fail all three latency gates. No prototype qualifies
the unmodified production archive. AMD64 cold-build validation and the separate
prerequisite recipe optimization remain open.

Beta.4 integrates the staging permit, journal count/byte admission and managed
worker generation fencing. [Current evidence](docs/staging-journal-migration-results.md)
records local dirty-build checks separately from historical beta.3 evidence.
Earlier PASS results do not qualify the new candidate. Native final-commit
packages and all remaining release gates still need qualification.

The [pooled beta.3 migration](docs/stream-pool-migration-results.md) is implemented
and locally qualified with 457 packaged arm64 checks. It retains the base
lifecycle integration, adds one bulk permit and four streaming slots, and closes
the reproduced sixteen-stream OOM in the tested 2 GB configuration. The earlier
bulk-only failure and original inconclusive copy cohort remain retained. The
candidate remains unpublished. Native amd64/final-commit CI, soak,
security, compatibility and coordinated recovery evidence remain outstanding.

The [empty filtered-stream diagnostic](docs/empty-filtered-stream-diagnostic.md)
also reproduces receiver retry without progress after a large unrelated source
transaction, before and after admission. Its [SQL fix](docs/empty-filtered-stream-fix-results.md)
passes 75 regression checks. The subsequent [receiver lifetime fix](docs/receiver-progress-fix-results.md)
passes 96 checks including native bulk progress, prompt disable/drop, source
outage recovery and interrupted-stream replay. The beta.4 archive now passes those 96 receiver checks without a SQL/binary
overlay. Native final-commit qualification and soak remain required.

The beta may be published only when every required gate is `PASS`, remote CI is
green on the tagged commit, and there are no known unresolved correctness,
security, data-loss, or PostgreSQL-stability defects. Deferred features must be
listed explicitly in the release notes; REST-catalog destinations remain an
intentional non-goal for this beta.

## Execution order

1. The [local investigation](docs/local-copy-contention-investigation.md) and [shared-budget prototype](docs/shared-staging-budget-prototype.md) are retained. Shared accounting and cancellation pass; a static 16 MiB/s limit helps two-CPU profiles but fails valid one-CPU lag checks. The [streaming-only controls](docs/streaming-only-capacity-baseline.md) pass on two CPUs but vary on one CPU. The [visibility diagnostic](docs/pressure-visibility-prototype.md) shows queue-only feedback misses receiver backlog. The [staging-gate prototype](docs/staging-gate-prototype.md), combined with the bounded receiver, passes repeated shared-two-CPU runs and one single-CPU sensitivity run without changing the timing gates. Its final 32-check lifecycle suite covers disconnected waiters and permit holders. The beta.4 implementation now provides same-backend admission, disconnect cleanup and a journal byte/count contract. Local archive lifecycle, admission, older upgrade and constrained performance checks pass, with the earlier source scheduling miss retained. Use short native probes before full qualification. The interrupted-run engine OOM remains open. Keep the failed native gate; the AMD64 Docker/receiver hang remains unresolved.
2. Qualify both native CI-built packages with all admission, lifecycle and upgrade lanes.
3. Close G4 compatibility, G6 supply-chain review and the G7 physical/object-store recovery drill.
4. Run the G3 24-hour release-candidate soak from the packaged build.
5. Qualify the final release commit, then publish and independently verify G9; retain final evidence here.

The scope is frozen to these gates. New feature work is deferred unless it
closes a gate or fixes a release-blocking defect.

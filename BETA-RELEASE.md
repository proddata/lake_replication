# Beta release gate

Target: `v0.1.0-beta.5` development candidate

Status: **not releasable yet**. Candidate `b0a703a` passes full native runtime
qualification on ARM64 and AMD64, including latency, security audits and
recovery. Its compatibility refresh now passes 57 artifact-only checks at each
exact pg_lake endpoint on native ARM64 with the supported runtime. The 24-hour
soak and first signed prerelease still need evidence. A short soak validates its
runner separately.

The [ten-minute packaged soak](docs/beta5-short-soak.md) now passes: 1,166
transactions, 45 assertions, exact data after all four faults, and complete
subscription/fixture cleanup. It is not the 24-hour gate. Object storage grows
to 912.8 MiB despite a bounded live dataset, dominated by retained Iceberg
metadata JSON. The subsequent [maintenance comparison](docs/beta5-maintenance-profiles.md)
passes both ten-minute profiles with 118 assertions each. Snapshot expiry
bounds active metadata; shortening physical retention from ten days to a
disposable 120 seconds reduces final storage from 335.09 to 99.95 MiB. Neither
profile qualifies the day-long gate. Define recovery/time-travel windows and
the corresponding storage budget before the full-day run; production defaults
remain unchanged.

This file is the release control document. Historical `docs/milestone-*.md`
files remain the engineering evidence journal; new beta work updates the gates
below rather than creating another numbered milestone.
Product capability boundaries and deliberately deferred features are tracked
separately in [the beta feature-gap document](docs/feature-gaps.md).

Status values: `PASS`, `PARTIAL`, `NOT STARTED`, `BLOCKED`.

## Required gates

| Gate | Status | Exit criterion | Current evidence / next action |
|---|---|---|---|
| G0. Remote CI | PASS | Required checks pass on the release commit in GitHub | Candidate `b0a703a` passes [CI 42201527552](https://github.com/proddata/lake_replication/actions/runs/42201527552), archive security audits and [CodeQL](https://github.com/proddata/lake_replication/actions/runs/42201526645). Recheck if the release candidate changes. |
| G1. Installable artifacts | PASS | amd64 and arm64 archives install and load from exported artifacts; checksums verify | Candidate `b0a703a` passes full native artifact-only qualification on both architectures with verified archive, source and supported prerequisite identities. See [current evidence](docs/evidence/2026-11-06-beta5-full-native-runtime-pass.json). |
| G2. Clean-host qualification | PASS | A fresh supported host with preinstalled PostgreSQL 18 + pg_lake installs only from the archive and completes one replication cycle | Both native architectures pass all 119 fresh lifecycle checks from the candidate CI archives, including restore, interrupted copy, management and uninstall. The supported pg_lake runtime is separately built and verified. |
| G3. Release-candidate correctness and soak | PARTIAL | The release candidate completes the core suite plus a documented 24-hour soak with restart, storage outage, backlog, and recovery events | Candidate `b0a703a` passes 662 checks per architecture. Valid four-copy completion is 124.46 s ARM64 / 119.80 s AMD64; streaming p95 is 0.903–1.064 s / 0.618–0.870 s, with no OOM or unexpected PostgreSQL crash. The short-run harness validation and 24-hour RC soak are separate evidence; the day-long gate remains open. |
| G4. Compatibility range | PASS | The release notes name an evidence-backed pg_lake revision range, with at least two independently selected revisions passing the compatibility lane | Accepted candidate `b0a703a` from CI run 42201527552 passes 57 artifact-only checks at each exact pg_lake endpoint 8180647d4535 and f609ecb17891 on native ARM64 with the supported patchset and frozen committed recipe. Native AMD64 full qualification separately covers the pinned endpoint. These are two tested points, not every intervening commit. See [refresh evidence](docs/evidence/2026-11-11-beta5-compatibility-refresh.json) and the [compatibility contract](deploy/install/COMPATIBILITY.md). Recheck for changed release archives. |
| G5. Operations and observability | PASS | Operators can detect unhealthy subscriptions, lag, retries, and last failure without reading ad-hoc files | Local beta.5 lifecycle and admission checks verify documented metrics, failures, fencing, live counters and cleanup. The operations guide now covers split checkpoints, chunk staging, initial-copy session admission and pacing. See [current qualification](docs/checkpoint-chunk-migration-results.md). |
| G6. Supply-chain and security review | PARTIAL | Published files have checksums, SBOMs, signatures, vulnerability results, and an approved runtime license inventory with no unresolved high-severity finding | Candidate `b0a703a` passes both CI archive audits with rustls 0.23.46 and checked dependency/toolchain notices. Publication now applies the same pinned Syft/Grype audit and high/critical gate to the exact newly built assets before signing; published SPDX SBOMs come from that audit. This workflow change still needs execution on the release commit. Published signatures and independent download verification remain outstanding. See [security review](docs/dependency-security-review.md) and [verification procedure](docs/release-verification.md). |
| G7. Upgrade and rollback | PASS | Install from an earlier beta, upgrade without data loss, and execute the documented binary/config rollback procedure | Candidate `b0a703a` passes all 30 coordinated physical recovery/rollback checks, production admission with beta.2 upgrade, and 47 beta.1 package-handoff checks on both native architectures. No SQL downgrade is supplied. See [procedure](docs/upgrade-and-rollback.md) and [native evidence](docs/evidence/2026-11-06-beta5-full-native-runtime-pass.json). |
| G8. Operator documentation | PASS | A new operator can install, configure, verify, monitor, troubleshoot, upgrade, and remove the beta from published documentation alone | The README covers evaluation, install, configuration, verification, and lifecycle; the operations guide covers monitoring, alerts, logs, and first response; the shipped bundle includes backup/upgrade/rollback and conservative uninstall procedures. |
| G9. Signed prerelease publication | PARTIAL | GitHub prerelease `v0.1.0-beta.5` is published and independently verified from downloaded assets on a non-build machine | Worker, primary extension, and artifact versions now match `0.1.0-beta.5`, and prerelease tags create GitHub prereleases. Publish, then verify checksums, Sigstore identities, SBOMs, and installation from downloaded assets. |

## Release decision

Current candidate `b0a703a` passes the complete native suite on both architectures
in [run 42201838803](https://github.com/proddata/lake_replication/actions/runs/42201838803).
Both pass 662 checks, including the corrected receiver interruption fixture and
the unchanged copy/streaming limits. The earlier failures below remain retained
as historical evidence; they do not describe the current native result.

## Historical candidate investigations

The following entries retain the findings and pending actions recorded for
earlier candidates. The current gate table and accepted `b0a703a` results above
determine present release status.

The earlier patched candidate at `820f402` **fails native streaming latency on
both architectures** in [run 41289535953](https://github.com/proddata/lake_replication/actions/runs/41289535953).
The measurements are valid; correctness, copy completion, TLS, and physical
recovery pass. The subsequent beta.1 handoff is skipped. The earlier pre-patch
full pass does not qualify this candidate. See the [investigation and retained
local controls](docs/beta5-latency-investigation.md). The failed measurements
remain retained and the release thresholds are unchanged.

The [matched publication trace](docs/beta5-transaction-tracing.md) now measures
complete worker/server transactions and separate diagnostic phases. A short
AMD64 diagnostic confirms that commit dominates (514–519 ms mean); one stream
still misses latency at 2.007067 seconds. Its failed gate remains retained.
This trace precedes the internal profiling below and does not qualify the
runtime dependency or change worker resource defaults.

The [local commit investigation](docs/beta5-commit-internals.md) now identifies
previous-snapshot scans and snapshot finalization as the main measured costs.
A guarded upstream cleanup candidate passes correctness checks and a fixed
local comparison, with streaming p95 0.591–0.628 seconds versus 1.285–1.899 seconds
in its controls. The native AMD64 guarded candidate also passes at 1.545–1.678
seconds while both controls fail at 2.107–2.356 seconds; the workflow retains
those failures. A separate local unguarded-patch run fails valid latency at
2.974–3.015 seconds despite passing correctness and copy completion. A fixed
guarded/minimal/guarded local follow-up passes all three trials; the minimal
patch reaches 0.457–0.484 seconds. The original failure is retained. Exact
unguarded native verification now passes both architectures at `e0d34f6`:
ARM64 p95 0.989–1.076 seconds and AMD64 1.711–1.756 seconds, with 125.67/160.97
second copies. Each passes all 181 assertions. The fix is delivered as the
[pinned runtime prerequisite patchset](deploy/pg_lake/README.md), selected by
qualification and shipped separately from workers. Full qualification on this
runtime, compatibility refresh, soak and final release gates remain required.
The upstream submodule and worker resource defaults are unchanged.
The supported runtime's local pipeline and managed cases pass. Its first
fairness run is rejected for a 91.97-second clock discontinuity; the retained
copy gate also fails. One frozen-input follow-up passes valid timing at
0.514–0.552 seconds p95 with 125.16-second copies. The patched runtime also
passes 57 artifact-only checks at each compatibility endpoint. Final installer
builds produce the same library bytes as these tested images. These local
checks use an explicitly selected CI archive; the final delivery commit still
needs full native qualification with its newly exported archives.

The [full supported-runtime run](docs/beta5-native-runtime-qualification.md) at
`6d1ef13` passes all 661 checks on AMD64, including valid p95 1.563–1.707 seconds
and 162.50-second four-copy completion. ARM64 passes lifecycle, TLS and crash
tests but fails when its receiver fixture's Docker exec kill command times out;
later ARM64 qualification steps are skipped. The fixture had unread subprocess
pipes. File-backed logs now remove that backpressure hazard and preserve the
output, with the same time limits and a new explicit SIGKILL-exit assertion.
The corrected local suite passes 97 checks against the exact failed archive.
The original also passes locally, so the CI timeout's cause is not conclusively
established. Both the failed ARM64 run and successful AMD64 run are retained;
full native requalification remains required.

The dependency review now patches rustls 0.23.45 to 0.23.46 for four upstream
advisories. [Security review](docs/dependency-security-review.md) records the
scope and validation. The new worker binaries require fresh archive and native
qualification; the completed full run below remains the pre-patch baseline.
Compatibility and recovery work uses explicit archive identities throughout.


[Full native qualification](docs/checkpoint-chunk-migration-results.md#full-native-arm64-and-amd64-results)
now passes on ARM64 and AMD64 at `14dc705`, with 614 checks per architecture.
That pre-patch run resolves the AMD64 cold-build, receiver and disk-cleanup
checks for its archive. Later work adds two-revision local compatibility,
patched dependency audits and native coordinated recovery. That candidate's
latency failure remains retained; the current candidate's corrected native pass
is recorded above. The 24-hour soak and signed publication remain open.


The [prospective native 40 ms pacing trial](docs/staging-pacing-prototype.md#prospective-native-result)
provides one valid working point for split checkpoints and chunk-backed staging:
123.49-second copy completion and 1.610–1.661-second streaming p95, passing every
source, clock, overlap, exact-data, catch-up and safety gate with no OOM or
PostgreSQL crash. The full bracketed comparison remains invalid because the
first unpaced control misses overlap; both controls retain latency failures.
The diagnostic workflow is correctly red. No causal/statistical performance
claim or requalification of historical controls is made.

Beta.5 now implements this combination in install/upgrade SQL and native
workers, including populated migration, writable logical interfaces, restored
operator grants, fixed privileged search paths, and four instance-wide
initial-copy leases before source snapshots. The default post-commit pause is
40 ms. See [migration contracts and current qualification](docs/checkpoint-chunk-migration-results.md).
Local packaged migration/lifecycle, sixteen-stream regression and constrained
two-/one-CPU performance checks pass. Historical beta.4 gate evidence
below is retained and must not be treated as qualification of beta.5. The clean CI archive now passes focused native ARM64 qualification (166 checks,
127.57-second copy and 1.653–1.734-second streaming p95). Full qualification on
both native architectures and the remaining release gates are still required; a per-worker pause is not an aggregate CPU or byte-rate guarantee.

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

## Open security follow-up

Worker login configuration, minimum grants, per-subscription credential
isolation, journal/staging data access, retention, and resource-limit
qualification remain open in the [security TODOs](docs/security-todos.md).
These are outstanding hardening and review tasks, not completed qualification.

## Execution order

The 24-hour run is deferred at the user's request. No endurance run is started
by the release-preparation work below, and G3 remains partial.

1. Keep the accepted `b0a703a` native and compatibility evidence tied to its exact archives. The earlier investigations and failed trials above remain historical evidence; the corrected full native run resolves their release-qualification follow-up for this candidate.
2. Validate the publication audit workflow on the final committed source. Refresh archive security evidence and qualification whenever release inputs change. Qualification now requires a successful default-branch push CI run at the exact dispatched workflow commit; it checks out that trusted commit and uses a fresh prerequisite-cache namespace. The uncommitted demo Dockerfile build experiment is excluded from accepted evidence and needs separate validation before inclusion.
3. Finalize the [draft beta.5 release notes](docs/beta5-release-notes.md) and the [independent verification procedure](docs/release-verification.md). Signed publication and G6/G9 closure wait until all prerequisites pass.
4. Before resuming endurance work, define snapshot/time-travel, longest-reader and backup/restore windows, storage capacity and a supported maintenance procedure. Validate the policy without treating disposable short-retention experiments as production evidence.
5. When authorized to resume, run the G3 24-hour release-candidate soak from the packaged build. Qualify the final release commit, publish and independently verify the downloaded release assets, then retain the final evidence here.

The scope is frozen to these gates. New feature work is deferred unless it
closes a gate or fixes a release-blocking defect.

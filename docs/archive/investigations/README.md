# Historical investigations

Completed or superseded protocols, prototypes, migration investigations and measurements are retained here. Read each report’s source revision and scope before using it; a historical proposal is not the current operating contract. [Active plans](../../roadmap/README.md) live separately.

- [Beta readiness plan](beta-readiness.md).
- [Beta.5 commit internals and manifest cleanup candidate](beta5-commit-internals.md).
- [Beta.5 latency gate investigation](beta5-latency-investigation.md).
- [Bounded maintenance investigation](beta5-maintenance-profiles.md).
- [Beta.5 publication transaction tracing](beta5-transaction-tracing.md).
- [Bounded receiver lifetime and cycle attribution](bounded-receiver-cycle-prototype.md).
- [Production bulk admission migration](bulk-admission-migration-results.md).
- [Bulk-only admission: operational qualification](bulk-admission-operations-results.md).
- [Bulk-only admission: paired performance qualification](bulk-admission-performance-results.md).
- [Bounded initial-copy transaction size prototype](bulk-copy-chunk-prototype.md).
- [Independent receiver and publication checkpoints: local prototype](checkpoint-split-prototype.md).
- [Initial dependency inventory](dependencies.md).
- [Empty filtered streamed transaction: receiver progress failure](empty-filtered-stream-diagnostic.md).
- [Empty filtered commits: SQL fix and remaining native timeout](empty-filtered-stream-fix-results.md).
- [Engine admission on the migrated runtime](engine-admission-experiment.md).
- [Current-runtime engine admission results](engine-admission-results.md).
- [Milestone 0 feasibility investigation](feasibility.md).
- [Local investigation of native copy contention](local-copy-contention-investigation.md).
- [Native beta.4 probe findings and heartbeat fence correction](native-beta4-probe-findings.md).
- [Follow-up native failures and targeted diagnostics](native-followup-diagnostics.md).
- [Native beta.4 probe preparation](native-probe-preparation.md).
- [Native qualification failures](native-qualification-failures.md).
- [Spike: could native `CREATE SUBSCRIPTION` ever target Iceberg?](native-subscription-spike.md).
- [Admission evaluation checkpoint — completed 2026-10-21](pg-extension-base-admission-checkpoint.md).
- [Admission overhead and coexistence protocol](pg-extension-base-admission-performance-protocol.md).
- [Admission performance and large-copy coexistence](pg-extension-base-admission-performance-results.md).
- [Bounded worker writes: prototype and results](pg-extension-base-admission-results.md).
- [Controlled cold writer transfer](pg-extension-base-cold-transfer.md).
- [pg_extension_base adoption evaluation](pg-extension-base-evaluation-plan.md).
- [Launcher evaluation: first implementation and screening](pg-extension-base-evaluation-results.md).
- [Extended launcher evaluation](pg-extension-base-extended-results.md).
- [Forced-crash evaluation](pg-extension-base-fault-results.md).
- [Destination identity and cleanup evaluation](pg-extension-base-identity-results.md).
- [Production pg_extension_base migration — beta.2 candidate](pg-extension-base-migration-results.md).
- [Proposed operational contract for base-worker adoption](pg-extension-base-operational-contract.md).
- [Extended launcher performance protocol](pg-extension-base-performance-protocol.md).
- [pg_extension_base launcher investigation](pg-extension-base-prototype.md).
- [Publication ownership and physical-copy evaluation](pg-extension-base-publication-results.md).
- [Fresh rebuild procedure for the combined prototype](pg-extension-base-rebuild-procedure.md).
- [Bounded source-slot recovery experiment](pg-extension-base-recovery-results.md).
- [Current prototype scaling and execution-model decision](pg-extension-base-scaling-results.md).
- [Pressure visibility before adaptive copy pacing](pressure-visibility-prototype.md).
- [Receiver connection lifetime and supervision: fix results](receiver-progress-fix-results.md).
- [Receiver progress telemetry prototype](receiver-progress-prototype.md).
- [Shared staging budget: local prototype protocol](shared-staging-budget-prototype.md).
- [Chunk-backed initial-copy staging prototype](staging-chunk-prototype.md).
- [Initial-copy staging admission prototype](staging-gate-prototype.md).
- [Staging and journal admission production migration](staging-journal-migration-results.md).
- [Post-commit bulk pacing diagnostic](staging-pacing-prototype.md).
- [Production streaming admission migration](stream-pool-migration-results.md).
- [Four-slot streaming pool qualification](stream-pool-performance-results.md).
- [Streaming admission: reproduced engine OOM](streaming-admission-results.md).
- [Streaming-only capacity baseline](streaming-only-capacity-baseline.md).
- [Update performance investigation](update-performance-investigation.md).

[Documentation home](../../README.md).

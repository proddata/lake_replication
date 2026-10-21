# Admission evaluation checkpoint — completed 2026-10-21

The resumed evaluation is complete. No benchmark is running. Private containers
and volumes were removed; cached images and disk evidence remain. The measurement
images retain the original production sources. Subsequent work adds SQL migration
`0022` and the receiver/supervisor/framing fix; C changes are comments only. The findings and source fixes are preserved in separate local commits;
release-artifact qualification and admission refinement remain pending.

[Final report](pg-extension-base-admission-performance-results.md) and
[accepted evidence](evidence/2026-10-21-base-admission-performance-results.json).

## Accepted measurements

- Steady and coexistence: `.qualification/qualification_20261020_061805_admission_measure/`.
- Clean catch-up redo: `.qualification/qualification_20261021_054133_admission_measure/`.
- Five complete pairs per benchmark cohort; all timing audits and data checks pass.
- Steady proposed latency margin passes. Clean catch-up latency and throughput
  margins are inconclusive; do not report a capacity/equivalence pass.
- The one-permit two-million-row copy causes roughly eight-second stream gaps
  and fails maximum-gap/catch-up targets. Both coexistence cases have correct data
  and no engine OOM.

The paused run's unmatched catch-up baseline is excluded. The first resumed
cohort (`qualification_20261020_215359_admission_measure`) has clock jumps in two
baselines, is marked invalid, and has its earlier nominal assessment withdrawn.
[Rejected evidence](evidence/2026-10-21-admission-rejected-clock-run.json) retains
it. The clean redo used scoped idle-sleep prevention and timing validation.

## Next implementation work

1. The [empty-commit SQL fix](empty-filtered-stream-fix-results.md) passed
   75 checks. The subsequent [receiver lifetime fix](receiver-progress-fix-results.md)
   passes 96 checks including the native two-million-row case, prompt disable/drop,
   source outage recovery, already-staged work and interrupted-stream replay.
   `make check` passes. These are source-build checks, not a new release artifact.
2. Refine admission to distinguish actual engine work from PostgreSQL staging,
   and test bounded capacity for streaming while one bulk copy runs. Preserve
   ownership validation and atomic copy publication. The fixed blanket permit
   is not a qualified final scheduling policy.

`make check` passes, including five new clock-validation regression tests. At
measurement completion, the production-source comparison and whitespace checks
passed; migration `0022` is a subsequent source change. No more measurements
are required to finish this evaluation; another implementation change is the
next task, not an automatic rerun until margins pass.

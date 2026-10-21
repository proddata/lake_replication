# Admission evaluation checkpoint — completed 2026-10-21

The resumed evaluation is complete. No benchmark is running. Private containers
and volumes were removed; cached images and disk evidence remain. Production
sources were unchanged during the measurement.

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

1. Fix the [empty filtered-stream receiver failure](empty-filtered-stream-diagnostic.md)
   with durable checkpoint/acknowledgement, replay and restart regression tests.
   It reproduces before/after admission and has not been fixed.
2. Refine admission to distinguish actual engine work from PostgreSQL staging,
   and test bounded capacity for streaming while one bulk copy runs. Preserve
   ownership validation and atomic copy publication. The fixed blanket permit
   is not a qualified final scheduling policy.

`make check` passes, including five new clock-validation regression tests. The
production-source comparison and whitespace checks pass. No more measurements
are required to finish this evaluation; implementation changes are next.

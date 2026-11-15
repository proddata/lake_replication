# Replication demo implementation plan

1. Implemented P0: Real parameterized order mutations, committed-row tracking, a shared ID window,
   destination verification, delete tombstones, and independent query errors.
2. Implemented P0: Compact four-stage pipeline, prominent actions, aligned tables, and Demo/Inspect modes.
3. Implemented P1: Bounded observed-visibility history, WAL checkpoints, actual Iceberg snapshot history,
   and journal batch links by source transaction ID where the journal retains them.
4. Implemented P2: Responsive styling, reduced-motion support, event-driven highlights, and regression checks.

Measurement contract:
- Visibility latency is the elapsed server time from source commit acknowledgement to
  the first successful destination query matching that version. It includes polling and
  query time and is an observation upper bound, not an exact engine latency.
- Staging latency uses the first journal observation, separately from query visibility.
- Only verified versions count toward the console session's observed event throughput.
  Versions overwritten before observation are marked superseded, not counted as verified.
- Delete verification requires a published journal transaction or earlier observation
  of the row in the destination. Missing/error responses alone never confirm deletion.
- Batch publication duration comes from journal timestamps. Snapshot IDs, timestamps,
  and file counts come from Iceberg metadata. No inferred batch-to-snapshot mapping.
- Session history is bounded and resets when the console server restarts. The comparison
  window is a bounded shared set of IDs, not a whole-table consistency check.

Validation: 12 backend tests plus real local PostgreSQL → Iceberg browser checks for
insert/update/delete visibility, shared IDs, tombstones, snapshot metadata, Inspect
Mode, SQL execution, mobile layout, pause controls, and reduced motion.

# Bulk-only admission: operational qualification

The corrected experimental policy passes 39 operational assertions covering
schema contention, rebuild rollback and cancellation during an actual engine
copy request. Production scheduling remains unchanged. The subsequent
[paired performance qualification](bulk-admission-performance-results.md) also
passes; production migration and packaging are next.

[Retained evidence](evidence/2026-10-22-bulk-operations-results.json) includes the
failed policy, accepted cases, excluded harness attempt, exact input hashes,
image/archive identities and raw-log hashes. These are local arm64 fixtures
against the exported beta.2 archive, not release-package qualification.

## Defect found and corrected

The first schema test reproduced a real defect: a queued admission timeout was
caught by reconciliation's DDL error handler. SQL returned success while the
subscription became durably paused with a destination-ALTER error.

The experimental SQL now acquires the bulk permit before entering that handler
and propagates transient deadlock, lock-unavailable and serialization errors
(`40P01`, `55P03`, `40001`). Ready-schema paths still bypass admission. Tests cover
both queued admission and an actual ALTER blocked by a reader. Neither timeout
leaves a partial column or durable pause; retry succeeds after contention ends.
An intentionally inverted control-row/advisory-lock order produces `40P01`,
preserves streaming metadata and succeeds on retry. A genuinely incompatible
source schema still pauses replication.

This proves safe abort and retry in the tested cases; it does not prevent all
deadlocks. Serialization failure propagation is included in the handler but was
not independently induced by these fixtures.

## Accepted checks

| Case | Assertions | Observed behavior |
| --- | ---: | --- |
| Schema | 13 | Admission/DDL timeouts and deadlock abort safely; retries preserve exact rows; incompatible schema still pauses |
| Rebuild | 14 | Queued cancellation records no promotion; independent schema work completes; rollback restores original/shadow tables; retry commits one promotion |
| Active native copy cancellation | 12 | Permit, rows and checkpoints roll back; restart copies two million exact rows and resumes live changes; drop cleans the source slot |

Schema and rebuild use SQL fixtures with real Iceberg data and DDL. Rebuild
promotion overlaps schema work on an independent target and assumes the normal
rebuild quiescence contract. This does not qualify arbitrary same-target writers.

The cancellation fixture uses a real native subscription. Before cancellation,
the backend owns the bulk permit and waits in an extension call; the engine
debug log records a COPY request after the publication function started. The
retained observation includes timestamps and the log-line hash. Cancellation
leaves zero published rows and all four checkpoints at `0/0`, drains the wrapper
and releases the permit. Re-enabling completes a fresh copy with unique IDs and
exact payloads, then publishes a new live row without restarting PostgreSQL.
This observation establishes a real engine write request, not its precise
internal progress at the cancellation instant.

One rebuild attempt is excluded: the harness awaited a schema job while an
earlier queued promotion retained the permit in its inspection transaction.
Only that idle test backend was terminated. The harness now queues and observes
the schema waiter before submitting promotion. The complete rebuild redo passed
on the same candidate image as the accepted schema and cancellation cases.
The earlier attempt and its source snapshot remain retained.

## Corrected candidate capacity repeat

The same image passed the four-copy/three-stream diagnostic again, under the
2 GB, one-thread engine configuration:

| Measurement | Result |
| --- | ---: |
| Copied rows | 8,000,000 across four databases |
| Group copy duration | 35.05 s |
| Per-stream publication p95 | 307–340 ms |
| Largest publication gap | 0.70 s |
| Largest final catch-up | 0.11 s |
| Commits per stream during copy | 70 |
| Exact final copied and streaming data | Pass |
| Clock/source-schedule audits and all coexistence limits | Pass |
| Engine OOM / unexpected PostgreSQL crash markers | None |

This single repeat supports the corrected candidate; comparing it with the
earlier runs does not establish a performance improvement. The subsequent paired
follow-up completes five matched pairs for steady-state, catch-up and single-copy
overhead.
Admission does not bound total memory, connection growth or arbitrary engine SQL.

Raw runs are under `.qualification/qualification_20261022_063209_bulk_operations/`,
`qualification_20261022_063409_bulk_operations/`,
`qualification_20261022_063904_bulk_operations/` and
`qualification_20261022_194711_reserved_admission/`, each with verified source
snapshots. All private fixture containers and volumes were removed. Full engine
debug logs remain private; only their hashes and the selected observation are
retained in repository evidence. `make check` passes after the final code changes.

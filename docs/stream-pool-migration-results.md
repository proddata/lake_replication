# Production streaming admission migration

The beta.3 production source now admits one bulk engine transaction and four
streaming engine transactions per PostgreSQL instance. The exported arm64
candidate passes all 457 packaged checks, including the sixteen-subscription
live/restart workload that exhausted the earlier bulk-only candidate's 2 GB
engine. Native amd64, final-commit CI and release-soak qualification remain open.
The locally tested archive was built from the development tree; it is not a
published release. Final-commit CI qualification is separate.

[Retained package evidence](evidence/2026-10-24-stream-pool-migration-results.json)
records exact archive/payload hashes, source fingerprints, all checks and raw
log hashes. The archive is
`dist/stream-beta3/lake_replication-0.1.0-beta.3.tar.gz`, SHA256
`5f4b659b25347b4ec450839b6fe051f8b019724bf10d1799e9a9db7250af9265`.
The [original OOM finding](streaming-admission-results.md) and
[performance/operational qualification](stream-pool-performance-results.md)
remain retained separately.

## Policy and migration

`pg_extension_base` continues to own worker lifetime. The production change adds
`lake_replication.admit_stream()` and explicit replacements of
`publish_insert_batch` and `apply_change_run`. Nested calls reuse one slot.
The existing bulk permit and five bulk entry points remain intact. PostgreSQL
staging does not request either permit. No Rust worker logic changed.

The streaming helper preserves the qualified prototype's C acquisition body
after renaming the exported symbol. It tries any free transaction-scoped advisory
slot, otherwise joins one slot's native queue. Four slots share the instance-wide
namespace (database 0, class 1280460112, objects 3–6); bulk uses object 2. Locks
last through commit and use PostgreSQL cancellation, savepoint and process-death
cleanup. Independent slot queues do not promise a global FIFO or reserve memory.

Operators inspect `lake_replication.bulk_admission` and
`lake_replication.stream_admission`. Public helper privileges are revoked and
administrators receive execute/select privileges. These limits cover managed
entry points in this PostgreSQL instance, not arbitrary application SQL, other
clusters, staging memory, connection growth or retained source WAL.

Migration 0024 includes the schema transient-error fix and the streaming pool.
It rejects enabled subscriptions or surviving supervision wrappers before changing
SQL entry points. The stopped-server procedure keeps every installed database
drained until its SQL upgrade completes. Actual beta.1 and beta.2 handoffs verify
rollback, preserved checkpoints/data, explicit adoption where required, native
catch-up and source-slot cleanup. Earlier bulk-only beta.3 archives were
unreleased development snapshots; verified SQL upgrade sources remain beta.1
and beta.2.

All six earlier generated SQL files remain byte-identical to the qualified
beta.2 archive. Compared with the bulk-only candidate, the only changed payloads
are the extension library, fresh beta.3 SQL and beta.2-to-beta.3 SQL. All four
worker executables are identical. The shipped upgrade documentation reflects the
new policy. Production installs no experimental functions or SQL rewrite overlay.

## Final arm64 package checks

| Lane | Checks | Result |
| --- | ---: | --- |
| Admission: bulk/pool cleanup, schema/rebuild, native copy cancellation, beta.2 handoff, sixteen-subscription restart, four copies | 178 | Pass |
| Fresh lifecycle, restore, management and uninstall | 119 | Pass |
| Real beta.1 stopped-server handoff | 47 | Pass |
| Forced Rust-child fault recovery | 17 | Pass |
| Receiver regressions and native recovery | 96 | Pass |
| Total, including each lane's archive checks | 457 | Pass |

The native pool checks cover two databases, reentrant/savepoint behavior,
timeout/cancellation, backend death, progress for eight queued transactions and
mixed bulk/stream deadlock abort/retry. They do not establish that all concurrent
application transactions are deadlock-free. Schema incompatibility still pauses
replication, while transient schema contention propagates for retry.

The final four-copy workload publishes eight million exact rows in 35.85 seconds,
with streaming p95 of 314–319 ms; all coexistence and timing checks pass. The
sixteen-subscription lane verifies exact data before and after PostgreSQL restart,
then drains workers and cleans every source slot. Its retained logs contain no
engine OOM or unexpected PostgreSQL restart markers. All private fixture
containers and volumes were removed.

The [paired qualification](stream-pool-performance-results.md) retains the
original five-pair inconclusive copy result and the fixed ten-pair extension.
Its margins were not widened and no samples were dropped. Local diagnostics and
457 passing checks close the reproduced resource case in the tested configuration;
they do not establish a universal capacity ceiling or beta release readiness.

Next: commit the candidate, run CI and the packaged workflow on both native
architectures, then complete the remaining compatibility, security, recovery and
24-hour release-soak gates.

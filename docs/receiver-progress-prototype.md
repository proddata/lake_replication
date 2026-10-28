# Receiver progress telemetry prototype

This is an isolated receiver-binary overlay, not a production migration or a
copy controller. It exposes facts missing from the queue-only diagnostic:
observed source commits, received WAL metadata, durable staging progress and
observation age. The archived initial-sync, publisher, supervisor, extension and
SQL recovery logic are unchanged. Source WAL feedback still uses the original
`feedback_lsn`; observing a server WAL endpoint never advances it.

## Record and lifecycle contract

The temporary receiver writes an atomic JSON snapshot into the existing private
`/worker-logs` directory. The filename identifies destination database and
subscription; records carry PID and process start time as their generation.
One subscription record replaces its preceding generation. Startup and normal
exit are explicit phases. A killed process can leave an older record; readers
must consider generation, phase and age rather than treating existence as
proof of a running or healthy receiver.

The snapshot distinguishes:

- Latest observed WAL message kind, server WAL endpoint and transmission time.
- Latest parsed source commit's end LSN and source commit timestamp, before the
  staging call starts.
- Latest transaction end LSN whose original staging function returned
  successfully, initialized from the original staged checkpoint.
- Snapshot wall time and process-monotonic elapsed time, age of the latest
  actual source message, and idle read-timeout count.

Regular observations are sampled at most every 250 ms. Pre-staging commits,
startup and normal exit force snapshots, so write cost also depends on
transaction rate. Read timeouts update the diagnostic tick without pretending
to receive a source message. Atomic rename avoids reading a partial JSON file;
no fsync or crash recovery guarantee is intended for this ephemeral metadata.
A private SQL reader in `analytics` exposes the files for the local observer.
PUBLIC access is revoked. Missing records remain missing and malformed files
cause an error; neither case fabricates a healthy state.

The [PostgreSQL replication protocol](https://www.postgresql.org/docs/current/protocol-replication.html)
defines the WAL header's time as transmission time and its endpoint as current
server WAL end. These differ from source transaction commit time and our
receiver's durable staging frontier. The experiment below demonstrates why the
distinction matters; a fresh transmission is not a catch-up assessment.

The current supervisor requests one committed transaction per receiver process
(`worker/src/supervisor.rs`, `LAKE_MAX_TRANSACTIONS=1`). Its persistent flag
preserves quiet decoding across socket timeouts; it does not remove that
transaction limit. This explains the observed generation churn. Per-process
files initially grew with transaction count, so the final prototype replaces
one record per subscription and publishes an exit snapshot. Historical early
attempts are retained. Old files fail the latest-record audit because historical
generations coexist, not because production recovery checkpoints regressed.

Records for dropped subscriptions deliberately remain until fixture teardown.
This bounds records by subscriptions created in this fixture, not by lifetime
subscription churn in a production server. A production implementation still
needs ownership/generation, retention, restart and security contracts. A fresh
`starting`, `exited` or read-timeout record also does not establish source
freshness. The observer continues to report receiver health as unknown.

## Validation and results, 2026-10-28

[Retained evidence](evidence/2026-10-28-receiver-progress-results.json) includes
all five attempts, input identities, exact Rust/SQL variants, raw-result hashes,
operational checks and independent snapshot audits. The final contention and
control builds have identical overlaid Rust source hashes. This verifies
source equivalence; it does not claim their image IDs or stripped binaries are
identical. Both use the retained beta.3 archive with SHA-256
`4494a0ce95c45613753abd4529dc00ec04f446516a76a012eb15ce6e98be7164`.

| Final overlay, S2 | Streaming p95 range | Copy completion | Result |
|---|---:|---:|---|
| Four large copies | 14.06–16.58 s | 67.58 s | Latency, catch-up and source-schedule findings |
| Streaming-only control | 279–290 ms | Not applicable | All original stream limits pass |

Both runs passed exact final data and clock continuity; neither observed engine
OOM or PostgreSQL crash. The contention run exceeded source-scheduling tolerance
for all three streams and is not a passing offered-load profile. Keep its
performance failures. The control shows that this overlay can meet the target
in the tested local profile; it is not a paired statistical overhead estimate.
Neither run establishes cloud headroom on the local M5.

In the contention run, recorded source commits reached sampled age lower bounds
of 14.68–16.96 seconds, while the largest sampled transmission-age lower bounds
were only 1.17–1.26 seconds. Each stream had 26–31 samples with an empty
destination queue and an observed commit older than two seconds. Many records
were `exited`, consistent with the one-transaction receiver lifecycle. These
samples expose delay not represented by destination queue occupancy alone.
They do not establish the cause of the delay or classify idle/filtered sources.
Ages use the stream observation preceding the file read, so they are lower
bounds on the stable local clock, not precise concurrent end-to-end timestamps.

The final contention fixture passed all 19 live operational checks:
quiet read ticks, unchanged durable frontier, source messages distinct from
timeouts, unpublished WAL, an open uncommitted transaction, blocked staging,
stale observations, distinct parsed/durable commit frontiers, unchanged actual
slot feedback while blocked, missing metadata, disable, restart generation,
one-record replacement, exact retry data, slot cleanup and stale stop evidence.
The overlay build ran four Rust tests, including both header formats and
truncation handling. `make check` passed, including 13 admission tests.

The independent audit checks clock continuity, snapshot clock arithmetic,
record uniqueness, captured final metadata and non-regressing sampled durable
frontiers. It does not classify receiver health or override failed performance
limits. The final contention trace observed 287 generations in at most eight
records (three streams, four completed copies and the retained operational
fixture). The final control observed 306 generations in three records.

## Reproduce and next boundary

```sh
export QUALIFICATION_ARCHIVE=/absolute/path/to/candidate.tar.gz
caffeinate -is python3 tests/admission/contention.py --copies 4 --cpuset 0-1 \
  --capture-stream-pressure --receiver-progress --receiver-operations
caffeinate -is python3 tests/admission/contention.py --copies 0 --cpuset 0-1 \
  --capture-stream-pressure --receiver-progress
```

Run sequentially with fresh volumes. The driver records the binary overlay,
exact injected sources and diagnostic SQL. Future runs automatically reject
invalid snapshot audits. No new GitHub workflow is part of this protocol.

The telemetry mechanism works, but a caught-up/idle contract remains necessary
before automatic copy pacing. The next comparison should distinguish delayed
publication, receiver generation gaps, and delayed source processing using
these observations and the fixture oracle, including quiet/filtered traffic.
Treat process churn as a measured candidate for further investigation, not a
proven root cause. Extending receiver lifetime without a staged-journal budget
could trade process overhead for unbounded durable backlog; do not adopt that
as a shortcut. Production policy and the interrupted-run OOM finding remain
open.


The subsequent [cycle attribution and bounded lifetime experiment](bounded-receiver-cycle-prototype.md)
measures the repeated-cycle cost and tests a 64-transaction lifetime with an
eight-unpublished-transaction guard. Its explicit topology control passes, but
the shared-two-CPU failures and remaining resource-contract work are retained.

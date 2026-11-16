# Direct publication deadlock recovery

The experimental direct worker retries a whole publication after a confirmed PostgreSQL deadlock abort. This covers ordinary/coalesced batches and streamed transactions applied from disposable spill. It addresses the foreground-worker exit found in the [native maintenance pilot](wal-direct-publication-profile.md).

## Recovery boundary

The wire client reads SQLSTATE from the ErrorResponse protocol field and makes it available only after consuming ReadyForQuery. The existing error representation remains available to other workers. Message text cannot authorize a retry. A transport failure, malformed response, missing SQLSTATE or error before ReadyForQuery leaves replay unauthorized.

Only server error **40P01** is retried. Before another attempt, ROLLBACK must succeed on the synchronized connection. The worker sends feedback for the previously safe boundary, waits for a bounded backoff, and starts the entire SQL transaction again. Buffered batches retain the same complete input. Streamed transactions rewind to byte zero, revalidate their spill records, recompute the digest and reapply every chunk before final COMMIT.

`published_lsn`, source-commit/publication counts and final mirrors commit together. In-memory publication state advances after successful COMMIT. Streamed feedback still uses the durable replay floor that protects other unfinished transactions. Temporary files never authorize acknowledgement.

A server-confirmed COMMIT abort can be retried. Losing the COMMIT response remains an unknown outcome: the worker exits and a new invocation reconciles the durable checkpoint. No blind connection retry is added. Other SQLSTATEs, timeouts, corruption, quota failures and retry exhaustion stop the worker with unpublished WAL retained.

## Bounds

`LAKE_DIRECT_DEADLOCK_RETRIES` defaults to **3 additional attempts** and accepts **0–8**. Zero disables retry. Backoff starts at 100 ms, doubles and caps at 1.6 seconds. The budget resets for each publication. Logs record retry number, limit, SQLSTATE, safe feedback boundary and backoff. Invalid settings are rejected before streaming starts.

The existing streamed per-statement timeout remains 30 seconds. There is no automatic process supervision, connection recovery, slot replacement or rebuild. Bounded retries do not guarantee success under arbitrary contention.

## Qualification

[Retained evidence](evidence/2026-11-16-wal-direct-deadlock-recovery.json) records **345 passing native checks** on the v13 runtime: 320 recovery, crash, snapshot and direct/default-worker regression checks, plus 25 checks for sustained publication with maintenance. All fixtures clean up successfully. All-feature Rust tests, `make check`, formatting and diff checks pass.

The passing sustained run applies **1,000 mixed source transactions at 4/second**, with native autovacuum every 20 seconds. Final mirrors match the source exactly. One foreground deadlock recovers with one retry; 24 successful vacuum events and 12 sampled file-count drops show maintenance is active. Final drain takes **1.18 seconds**, maximum sampled backlog is **25 transactions**, and the sampled p95 publication-lag upper bound is **5.19 seconds**.

PostgreSQL consumes **79.53 CPU-seconds** and pgduck **43.33**, totaling **122.86 CPU-seconds** over 250 seconds of offered writes plus drain. This includes source writes, publication, maintenance and observers; host workers and MinIO are excluded. Peak sampled memory is 290.3 MiB for PostgreSQL and 396.8 MiB for pgduck, under respective 512 MiB and 2 GiB limits, each with one CPU and swap disabled. Neither is OOM-killed.

The **3,000-transaction run at 10/second fails** its 90-second drain deadline. It recovers 15 foreground deadlocks, with at most one retry per affected publication; the last regular sample has 2,672 transactions published. There is no retry exhaustion or OOM kill, but the run does not reach a final exact-row comparison. This is a failed capacity test, not a successful sustained rate. The earlier v12 600-transaction pilot at 10/second passes, illustrating why the longer run matters.

These are local ARM64 results. The 4/second run establishes a bounded working point, not maximum capacity or a long soak. The native lock-order conflict remains; retry recovers confirmed aborted publications.

Targeted native cases cover a real PostgreSQL lock-cycle deadlock after writes have started, invisibility/checkpoint/feedback before abort, exact whole-batch replay, a later streamed-chunk failure, spill rewind, confirmed deferred COMMIT abort, retry exhaustion/disable, unrelated SQLSTATE with spoofed message text, response loss and restart. Protocol tests cover missing ReadyForQuery and clearing the previous SQLSTATE. Real crash fixtures separately exercise unknown outcomes around upload and COMMIT.

The repeated maintenance workload includes native compaction costs, retry work, source writes and observers in its resource measurements. Exact final rows, source receipts, no-swap limits and OOM events are checked. This is a bounded local workload; production memory admission, native lock-order improvements, longer contention/soak, managed direct lifecycle and failover remain separate gates.

The [15-minute follow-up](wal-direct-sustained-maintenance.md) passes 5,400 transactions at 6/second with exact final rows, but its sampled p95 lag reaches 43.52 seconds. At 8/second, backlog grows and the drain deadline fails despite 45 recovered deadlocks. The original shorter qualification and its 345 checks above remain historical evidence.

## Run

Build the optional runtime as `lake-local-iceberg:wal-direct-v13` over the qualified base. This image includes current default workers as well as the optional direct binaries so the shared wire-client change can receive native journal-path regressions. It does not alter the native library or release prerequisite patchset. Select pinned storage images, then run private disposable fixtures:

```sh
make local-iceberg-wal-direct-retry-test
python3 tests/local_iceberg/wal_direct_maintenance.py --transactions 1000 --interval .25
# Retained overload reproducer: 3000 transactions at 10/second
make local-iceberg-wal-direct-maintenance
```

The maintenance fixture uses a 20-second native autovacuum interval only inside its private PostgreSQL data directory. Deployment/release defaults remain unchanged. All fixtures remove their private volumes on completion.

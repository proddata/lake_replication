# Initial-copy staging admission prototype

The separate staging permit closes the remaining shared-resource performance
finding in the tested local profile when combined with the bounded receiver.
This remains a diagnostic overlay: production workers, extension SQL, versions
and defaults are unchanged.

## Mechanism

A tiny standalone PostgreSQL C library acquires one transaction-level advisory
lock with an instance-wide key. Its key is distinct from the existing bulk
publication permit and four streaming permits. The initial-sync overlay sends
one simple-query message containing:

```sql
BEGIN;
SET LOCAL client_connection_check_interval = '100ms';
SET LOCAL lock_timeout = '10s';
SELECT staging_gate.admit();
UPDATE staging_gate.counters SET chunks=chunks+1, bytes=bytes+<chunk_bytes>;
SELECT lake_replication.append_initial_copy_rows(..., <rows>::jsonb);
COMMIT;
```

The permit belongs to the backend appending the rows and lasts through its
commit. It is released by rollback or backend exit as well. There is no separate
coordinator connection, expiring lease or fixed byte-rate sleep. Source reading,
client serialization and the overall COPY snapshot remain outside the permit.
Each waiting copy retains its source snapshot and buffered chunk. This is an
append concurrency bound, not a total CPU, memory, byte or WAL-retention bound.

The accounting UPDATE and append commit together. Per-database unlogged counters
measure completed chunks and serialized UTF-8 bytes; they are test observations,
not recovery state. A retry after earlier committed chunks can increase counters
again even if the ordinary copy-reset path restores exact data. Such a run must
not be reported as a clean once-per-chunk measurement. A server crash can also
reset the counters and invalidate that accounting audit.

Placement matters for promotion: the permit precedes the append statement's
type analysis/execution, including its JSONB argument conversion. PostgreSQL's
[simple-query executor](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/tcop/postgres.c)
processes analysis/planning per statement after raw parsing. Moving admission
only inside the existing JSONB-argument function would leave argument conversion
outside that admission. Preserve the tested boundary, or validate a text-input
wrapper that acquires admission before converting JSON. Raw SQL lexing and
client-side work are still outside this prototype's permit.

## Cancellation finding and correction

The first lifecycle attempt failed before any measured workload: disabling the
subscription reaped its worker, but the disconnected backend remained queued
on the staging lock. This is retained as a failed attempt, not performance data.
PostgreSQL's default connection-check interval is zero, so connection loss may
be noticed only at a later socket interaction. The diagnostic append transaction
now enables [client connection checks](https://www.postgresql.org/docs/18/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)
at 100 ms and bounds lock waits at ten seconds. This is local closed-connection
detection, not a network-failure deadline or a general worker-stop guarantee.

The final suite checks both a waiting worker and a worker holding the permit
while an INSERT trigger blocks the append. DISABLE reaps the worker; the backend
then exits, releases the permit and rolls back both counters and staged rows,
before the unrelated trigger lock is released. Re-enable copies the exact data.
Separate checks cover cancellation, lock timeout, backend termination,
transaction rollback, savepoints, repeated admission and cross-database scope.
The staging holder can coexist with bulk and streaming permit holders.

The first corrected suite passed 30 checks, using explicit backend termination
for the blocked append. The repeated suite passed 32, replacing that step with
actual worker disconnection and checking backend exit. A builder SQL-string
quoting error was also corrected before a fixture was created; it supplies no
performance data. All attempts are identified in the retained evidence.

## Local comparison, 2026-10-28

[Retained evidence](evidence/2026-10-28-staging-gate-results.json) covers seven
attempts including the two failures above and five completed workloads. It
retains raw-result hashes, actual overlaid Rust/C/SQL variants, the helper binary
hash, per-copy counters, operational checks and independent audits. All completed serial
workloads used the same helper binary; the accounting control used a no-lock binary
compiled from the same C source. All completed runs used identical overlaid
initial-sync Rust source, including counter SQL and connection settings.

S2 shares CPUs `0-1` among source, destination/workers, pgduck and storage.
S1 shares CPU `0` among all those services. Engine configuration remains 2 GB
and one query thread. Source and storage receive no extra CPUs in these tests.
The workload remains four copies of two million rows plus three streams of
120 transactions, 20 rows each at 500 ms intervals. The original latency,
source-scheduling, copy-time, progress and exact-data gates are unchanged.

| Profile / receiver lifetime / staging mode | Stream p95 range | Copy duration | Result |
|---|---:|---:|---|
| S2 / 64 / serial | 438–465 ms | 71.80 s | All gates pass |
| S2 / 64 / accounting only | 1.17–1.70 s | 64.11 s | Two source-schedule failures |
| S2 / 64 / serial repeat | 440–466 ms | 70.48 s | All gates pass |
| S1 / 64 / serial | 1.10–1.16 s | 115.13 s | All gates pass |
| S2 / original 1 / serial | 1.12–2.65 s | 74.67 s | One stream fails latency |

The accounting-only build keeps the same append transaction, counters and
connection settings, while compiling the C helper to return without acquiring
a lock. Its audit confirms no staging locks were sampled. The serial runs
sampled at most one admitted staging backend, with native PostgreSQL managing
waiters across destination databases. Their independent audits require a holder
to have been observed. All completed cases committed exactly 8,000 chunks and
1,267,563,584 serialized bytes, with each of the four copies independently
accounted for. Later runs freeze helper operations and audit code at startup;
the evidence also re-audits all raw cases with the retained current analyzer.

All five measurements passed exact final data and clock continuity. No engine
OOM, PostgreSQL crash or warning/error event in the measured stream logs was
observed. The original-lifetime comparison has roughly 12.3 seconds of accumulated
between-cycle gaps per stream; its latency finding was not caused by a logged
transient retry. This does not prove longer lifetime is the only possible way
to remove that cost, but the combined configuration is the stronger tested
candidate. The source-scheduling failures in the accounting control remain
findings even though its p95 values pass.

This is an exploratory serial/accounting/serial sequence, not a randomized
paired cohort. The single-CPU and original-lifetime cases each have one run.
The local M5 results address the earlier headroom concern more convincingly
than a 139-second fixed-pause copy, but do not establish cloud throughput or a
supported one-CPU minimum. The earlier interrupted-run engine OOM and AMD64
qualification hang remain separate unresolved findings.

Generic resource phase labels in these retained runs put the new multi-statement
RPC under `other`; use the dedicated gate snapshots for holder/waiter counts.
The driver now labels that RPC as `copy_staging_transaction` in future runs.

## Reproduction and next engineering step

```sh
export QUALIFICATION_ARCHIVE=/absolute/path/to/candidate.tar.gz
caffeinate -is python3 tests/admission/contention.py --copies 4 --cpuset 0-1 \
  --capture-stream-pressure --receiver-progress --cycle-timing \
  --receiver-batch 64 --staging-gate --staging-gate-operations
# Same transaction/accounting with no lock; run sequentially.
caffeinate -is python3 tests/admission/contention.py --copies 4 --cpuset 0-1 \
  --capture-stream-pressure --receiver-progress --cycle-timing \
  --receiver-batch 64 --staging-gate --staging-gate-mode accounting
```

The isolated build passed three initial-sync, four receiver and ten supervisor
Rust tests. `make check` passed, including 20 admission tests. No GitHub workflow
was started and no new release archive was generated or promoted.

Carry the combined staging permit and bounded receiver into a production
candidate. Integrate admission into the managed extension rather than shipping
the diagnostic library/counters. Preserve same-backend transaction ownership,
pre-conversion admission and disconnect cleanup. Harden the journal guard into
an enforced byte/count contract and validate stopped/reconfigured-worker
fencing before choosing defaults. Then run short native ARM64/AMD64 probes
before another full qualification workflow. Keep the isolated experiments and
failure evidence until those replacements have been validated.

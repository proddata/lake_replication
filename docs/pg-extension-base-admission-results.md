# Bounded worker writes: prototype and results

> Historical prototype record. Commands and source paths below refer to the
> [preserved implementation](https://github.com/proddata/lake_replication/tree/7ef18381810075dddd26137ee959770591b95863/experiments/base_workers) before cleanup.
> Current lifecycle tests live in `tests/lifecycle`; retained admission helpers
> are described in [experiments/admission](../experiments/admission/README.md).

The isolated base-worker prototype now admits one native worker write transaction
at a time across all databases in a PostgreSQL instance. The four-database,
16-subscription restart case that exhausted the shared engine now passes with
the same 2 GB engine limit and one query thread. This is a tested resource-policy
prototype; the shipped extension and release archive are unchanged.

Run `python3 experiments/base_workers/admission_probe.py`. The final run is
`.qualification/qualification_20261020_055358_admission/`, with **118/118 checks
passing**. The [retained evidence](evidence/2026-10-20-base-bounded-admission-results.json)
includes image/source/archive fingerprints, inventories, checks and log hashes.
The new image is
`sha256:7a416dc7d646735c7c02ce69e1ee74dd2751abd2061555389e637fe4455e62b6`.

## Implementation

`admission.c.inc` adds a C function using PostgreSQL's heavyweight advisory-lock
manager. It acquires an exclusive transaction lock with database OID zero and
the prototype's reserved key (`classid=0x4c524550`, `objid=1`, `objsubid=1`). SQL
advisory locks normally use the caller's database OID; this explicit zero-OID
tag makes the permit shared by every database under this postmaster. It is
independent of any source-slot or per-subscription initial-sync advisory lock.

`admission.sql` prepends admission to `prototype_assert_writer`, before that
function takes the ownership configuration row lock. The ten previously guarded
native worker entry points therefore acquire admission before their checks,
DDL or data effects. They cover ordinary publication, destination creation/schema
reconciliation, and initial-copy/reset/finish operations. The initial copy's
individual destination transactions share this policy; it does not monopolize
the permit for the entire source snapshot/export lifetime.

Nested checks reuse the backend's existing permit. PostgreSQL holds it until
transaction completion and manages waiting, cancellation, savepoint resource
ownership, deadlock detection and cleanup. There is no shared counter or manual
PID reclamation. A normal transaction may hold the permit across multiple calls;
it is not released prematurely at the end of a SQL function.

The admin diagnostic view `lake_replication.prototype_writer_admission` exposes
the backend PID/database, `admitted` or `waiting_for_writer`, wait start and
PostgreSQL wait event. It is transient backend state, not a new durable
subscription state. Ordinary SQL does not acquire this permit automatically.

`Dockerfile.admission` rebuilds only the experimental extension copy on the exact
previous publication/recovery image, appending the new SQL to fresh-install
scripts. Base registration, existing origin checks and the Rust supervisor remain
unchanged. The permit is resource admission, not authorization or a lease.

## Validation

- Two databases contend for the same permit; only one holder is present. Repeated
  calls in the same transaction do not deadlock.
- In the controlled two-waiter test, the earlier queued writer enters first.
  All 16 real subscriptions eventually progress in the scaling case.
- Ordinary SQL remains usable while writers queue.
- Cancelling a waiter and expiring `lock_timeout` leave no pending permit request.
- Commit, rollback and rollback to a savepoint release the appropriate permit.
- Ownership validation still rejects a nonexistent subscription after admission;
  the ten native entry-point guards remain installed.
- An actual queued subscription can be disabled without an ownership-row lock
  blocking the command. It publishes no pending change in this test, drains its
  wrapper, and later resumes and catches up.
- Terminating a permit-holding client backend releases the permit. A separate
  SIGKILL injection, with replication supervisors already drained, triggers the
  expected PostgreSQL internal crash recovery. The logs prove signal 9 and
  reinitialization; the recovered server can acquire/release a fresh permit.
- Four databases and 16 subscriptions complete initial copy, source
  updates across all publications, actual Iceberg row checks, destination
  restart, further publication, disable/drain, native source-slot cleanup and
  extension removal. No source slots, subscription wrappers or sampled Rust
  children remain after their cleanup checks.

The full run uses 64 PostgreSQL worker slots to isolate the engine-memory issue;
the original 24-slot exhaustion is **not fixed**. The functional live-update and
post-restart observation rounds each completed in about 1.87 seconds. These
include sequential Docker/psql observation, are not percentile latency metrics,
and are not a performance comparison. An earlier narrower run also passed;
the retained final run includes queued-worker and forced-crash checks.

All private test containers and volumes were removed; cached images remain.
`make check` and the production-source comparison pass. Nothing was committed
or pushed.

## Supported conclusion and remaining work

Keep `pg_extension_base` as the lifecycle layer and retain this bounded-write
prototype as the next execution-policy candidate. It resolves the reproduced
restart failure in this workload without increasing engine memory. It does not
establish a universal memory bound: a single large query, ordinary SQL and
independent pg_lake maintenance can still exhaust the engine.

The initial ceiling is deliberately fixed at one. Increasing it requires fresh
measurements. Serialization can increase queueing and reduce aggregate throughput;
the earlier pre-admission benchmark does **not** qualify this overlay's performance.
The [follow-up measurements](pg-extension-base-admission-performance-results.md)
pass the steady-load margin, leave catch-up performance inconclusive, and expose
roughly eight-second streaming gaps during a large copy's final transaction.
The current one-permit policy is therefore not qualified for broader adoption;
evaluate admission specific to engine work and capacity for streaming during bulk
copy, while preserving ownership validation and atomic publication.

Waiting backends, wrappers and source connections still consume resources. A
bounded worker pool is a separate follow-up for process/connection growth.
This lock coordinates one PostgreSQL instance only; multiple PostgreSQL instances
sharing an engine are outside its boundary. No latest-overlay package upgrade,
long-running fairness/soak, amd64 or distributed takeover is qualified. Direct
low-level helper calls and arbitrary SQL outside the guarded entry points are
not constrained, and this is not a hostile-administrator security boundary.

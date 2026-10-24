# Engine admission on the migrated runtime

Historical prototype qualification. The policy is now implemented in the
[beta.3 production candidate](stream-pool-migration-results.md); its package
qualification is tracked separately.

Status: experimental; production scheduling is unchanged.

The [completed diagnostic results](engine-admission-results.md) retain the first
policy's latency failure and the revised policy's pass. A follow-up four-copy diagnostic
exposes current latency pressure and supports qualifying a bulk-only permit
with unrestricted streaming. No policy is promoted to production yet.

The previous one-permit experiment prevented an engine OOM during a
four-database, sixteen-subscription restart burst, but a two-million-row initial
copy blocked streaming for about eight seconds. It also admitted PostgreSQL-only
staging operations. The base lifecycle migration and receiver fixes are now in
production source; this experiment applies only a scheduling overlay to that
runtime.

## Policies and boundary

Compare the current package without admission, an engine-only global permit,
and a reserved policy allowing one bulk transaction and one streaming transaction
at the same time. A follow-up bulk-only policy admits one bulk transaction and
does not request streaming permits. Admitted work uses instance-wide PostgreSQL
advisory locks, shared across databases and held until transaction end. Native lock
handling supplies queueing, cancellation, savepoint rollback and backend-death
cleanup. Reserved mode rejects transactions that mix lanes instead of risking
an opposite-order deadlock.

Streaming operations are batch publication and its nested change-run apply.
Bulk operations are destination creation, additive schema reconciliation,
single/group initial-copy publication and rebuild promotion. The overlay covers
these seven entry points. The initial revision admitted every function at entry;
its ready-schema checks queued behind bulk work and missed the p95 target.
The revised schema helpers admit at actual DDL, allowing PostgreSQL-only existing
table and ready-schema paths to proceed without a permit. Other engine functions
admit before control-row locks. Late DDL admission introduces a lock-ordering
qualification requirement: PostgreSQL can detect and abort cycles, but the
prototype is not a proof that concurrent schema/rebuild operations are deadlock-free.
The [operational follow-up](bulk-admission-operations-results.md) verifies safe
timeout/deadlock abort and retry, independent schema/rebuild operations and actual
engine-request cancellation. It also fixes swallowed transient reconciliation
errors in the experimental overlay.
It leaves receiver staging,
copy-row staging, checkpoint ownership and destination binding checks unchanged.
Atomic multi-table copy publication and checkpoint advancement remain intact.

These are transaction concurrency limits, not memory reservations, a bound on
wrapper/connection count, or a guarantee that any individual query fits in the
engine. Bulk work cannot borrow the streaming lane. Sustained overload and
arbitrary application SQL accessing the engine remain outside this experiment.

## Checks and decision

Run against the exported beta.2 archive using disposable containers and volumes:

```sh
VERSION=0.1.0-beta.2 python3 experiments/admission/reserved_probe.py --variants reserved bounded --cases locks
VERSION=0.1.0-beta.2 python3 experiments/admission/reserved_probe.py --variants unbounded reserved --cases scale fairness
VERSION=0.1.0-beta.2 python3 experiments/admission/reserved_probe.py --variants unbounded --cases fairness --bulk-copies 4
VERSION=0.1.0-beta.2 python3 experiments/admission/reserved_probe.py --variants bulk_only --cases locks scale fairness --bulk-copies 4
```

The default engine remains 2 GB with one query thread and 64 PostgreSQL worker
slots. The lock checks establish cross-database exclusion, streaming capacity
while bulk holds its lane, independent control-table progress, cancellation,
rollback and backend-death cleanup. The restart case retains four databases and
sixteen subscriptions, exact Iceberg contents, live updates, restart recovery,
disable/drop and slot cleanup.

The coexistence case retains the original two-million-row, 128-byte-payload copy
while three subscriptions each receive 120 transactions of twenty rows at
500 ms intervals. Retain the original limits: publication p95 below two seconds,
maximum publication gap and final catch-up within five seconds, at least five
commits per stream during the copy, copy completion within three minutes and
source scheduling within one offered interval. Verify exact rows/IDs/payloads,
observe permit contention and reject clock discontinuities. Sampled occupancy
must never exceed one writer per lane or two total. Engine OOM and unexpected
PostgreSQL restarts are findings even if eventual rows match.

Results, input hashes, archive provenance, image IDs and logs are written under
`.qualification/*_reserved_admission/`. A diagnostic pass is not a production
promotion: repeat the matched steady/catch-up cohorts with five complete pairs,
check single-copy overhead, then qualify the packaged policy on both
architectures and in the release soak. The operational follow-up covers the
tested concurrent bulk jobs and cancellation paths; broader concurrency and
sustained overload remain release qualification work.

The [paired performance follow-up](bulk-admission-performance-results.md) now
passes five complete pairs for steady streaming, catch-up and single-copy
overhead. Production migration, native package qualification and the soak remain
the next gates.

# pg_extension_base adoption evaluation

Status: evaluation in progress; production adoption remains gated on the work below.
Progress and measurements are tracked in the
[evaluation results](pg-extension-base-evaluation-results.md). The original
85-check result below is the starting point, not completion of these gates.

The hardened prototype passed 85 functional checks on PostgreSQL 18.6/Linux
arm64. That establishes feasibility, not production readiness or performance.
See the [investigation](pg-extension-base-prototype.md) for evidence.

Decide whether per-database lifecycle management through pg_extension_base
justifies its dependency, migration, and process-management complexity. Keep
SQL publication through pg_lake unchanged during this evaluation.

## 1. Establish a fair baseline

Preserve existing evidence. Extract the shared correctness fixes into reviewable
changes: durable worker handles, handle release, exit notifications, and explicit
descendant cleanup. Build two comparable variants from the same sources:

- Static launcher with those fixes.
- pg_extension_base launcher with the same fixes.

Keep the original shipped variant as a diagnostic reference only. Keep the
adapter isolated until the adoption decision. Record source commits, image
digests, architecture, PostgreSQL settings, and resource limits for every run.

Gate: both variants pass applicable replication and lifecycle checks. Existing
crashes or leaks must not distort the framework comparison.

## 2. Remove prototype overhead

Replace steady-state 10 ms child polling with an interruptible event-driven
wait, preserving bounded shutdown and reaping. Write binding errors only when
state changes; measure remaining periodic SQL/lock costs. Release handles when
tracked workers disappear. Namespace worker names and logs by database and
subscription. Document cleanup ownership, including PostgreSQL as PID 1 and
SIGKILL of the cleanup owner.

Gate: all 85 hardened checks still pass; repeated enable/disable and launcher
replacement leave no owned processes or retained handle growth.

## 3. Define identity and upgrade behavior

Specify these behaviors before extending the implementation:

| Event | Required outcome |
| --- | --- |
| Database rename | Explicit supported behavior beyond comparing names. |
| Template clone / restore elsewhere | No writes into the original destination or automatic reuse of its Iceberg bindings. |
| Restore into a new cluster | Safe stopped state or documented explicit rebind. |
| Upgrade with old preload settings | Exactly one launcher owner per database throughout the supported transition. |
| Failed or rolled-back upgrade | Recoverable state without duplicate supervisors. |
| Extension removal / database drop | Owned workers and descendants stop within a documented bound. |

A copied identifier, database OID, or name alone may not distinguish all clone
and restore cases. Specify when an explicit administrative rebind is required.
Test a real upgrade from the packaged static-launcher release, including any
required restart, and its documented rollback/recovery procedure.

Gate: executable regression cases cover this table with no duplicate owners,
cross-database publication, or silent reuse of copied bindings.

## 4. Run controlled performance and capacity comparisons

Extend the disposable qualification fixture to run identical workloads against
both variants. The existing performance probe separates staging and publication;
add sustained traffic with receipt, staging, and publication running together.

Start with 1 and 4 destination databases and 1, 4, and 16 total subscriptions,
distributed where possible. Include databases without subscriptions and idle
enabled subscriptions. Approach the worker-slot limit only after smaller cases
pass. Hold total offered load constant for A/B runs; test saturation separately.
The existing static launcher supports one configured database. Restrict direct
A/B claims to matching supported topologies; treat additional base-worker
databases as a separate capacity/operational evaluation unless a comparable
static multi-database implementation is explicitly built and qualified.

Cover idle, initial copy, sustained inserts, mixed inserts/updates/deletes,
bursts/catch-up, and source outages, with small and larger transactions. Use
identical data, batching, storage, and PostgreSQL settings. Measure:

- Sustained throughput, visible commit lag p50/p95/p99, backlog and catch-up time.
- CPU, process counts, wakeups, process memory and container memory separately.
- Idle and loaded WAL, connections, worker slots, and relevant lock waits.
- Enable/disable, recovery, and extension removal latency.

Document commit/visibility timestamping and avoid double-counting PostgreSQL
shared memory. Warm up, alternate variant order, and collect at least five
measured repetitions on the same otherwise idle host, running variants
sequentially. Retain raw results and report variability. Use initial baseline
runs to select duration and an absolute latency budget, then freeze the gate.

Proposed screening thresholds: no repeatable throughput regression over 5% or
p95 lag regression over 10% above measurement resolution. Treat differences as
inconclusive if run variability is comparable. No growing idle WAL from repeated
unchanged-state writes. Report fixed per-database/subscription overhead and
accept it only within a documented deployment resource budget. These thresholds
are proposals, not existing guarantees.

Gate: reproducible evidence establishes performance and practical capacity;
throughput alone is insufficient.

## 5. Qualify faults, longevity, and packaging

Inject SIGTERM/SIGKILL into launchers, wrappers, supervisors, and busy or paused
descendants. Check cleanup, source slot ownership, restart behavior, data
correctness, and postmaster stability. Test abrupt server/container termination,
source outages, and worker-slot exhaustion, requiring clear diagnostics and
bounded retries. Distinguish expected PostgreSQL crash semantics from avoidable
crashes caused by incorrect child ownership.

After short tests pass, run a 24-hour replication/churn soak with repeated
enable/disable and launcher replacement. Track row correctness, processes,
handles, memory trends, and WAL. Repeat clean package installation, real upgrade,
and critical failure cases on Linux amd64 and arm64. Verify dependency/preload
requirements and operator instructions.

Gate: no unexplained data loss/duplication, cross-database writes, leaked owned
processes, or unbounded resource growth.

## 6. Make and record the decision

Write one decision record with comparative results, deployment matrix, resource
limits, upgrade/recovery procedure, and remaining risks:

- **Adopt:** correctness, upgrade, performance, and operational gates pass;
  per-database installation removes meaningful manual configuration.
- **Revise and retest:** failures are localized and fixable without substantial
  expansion of the supported contract.
- **Retain the static launcher:** framework constraints, migration risk, or
  recurring overhead outweigh benefits. Keep shared correctness fixes.

After adoption, switch production code, dependencies, migrations, packaging,
and documentation together. Prototype success does not qualify shipped code
or clear its release blockers.

The [extended evaluation](pg-extension-base-extended-results.md) now records
matched steady/catch-up cohorts and a 42-check stopped-state upgrade experiment.
Immediate next work: forced-crash cleanup/ownership, clone/restore identity,
namespaced logs/worker names, and scale/architecture/soak qualification. Keep the
steady-load relative p95 comparison open: its interval narrowly crosses the
frozen margin. The prototype results do not qualify a shipped migration.

The [forced-crash evaluation](pg-extension-base-fault-results.md) found a concrete
recovery blocker: with a paused external receiver, killing the launcher or
wrapper can leave the replacement permanently paused after transient source-slot
contention. Prioritize bounded fail-closed contention recovery and cleanup-owner
qualification before scaling or soak tests.

The [bounded-recovery revision](pg-extension-base-recovery-results.md) now passes
the failed paused-owner cases under Docker init and refuses an independently
held source slot after a bounded retry. Continue with clone/restore/rename
bindings and explicit recovery/rebind; ownership outside that tested init
topology, performance of the revised binary, and the remaining fault matrix
are still open.

The [identity evaluation](pg-extension-base-identity-results.md) now qualifies
scoped origin dispatch/cleanup checks in an isolated image. Next define and test
full Iceberg restore/import/rebuild plus explicit rebind and publication-side
ownership; the ordinary full dump currently fails at non-empty Iceberg locations.
A copied physical tuple is still not an exclusivity proof.

The [fresh rebuild procedure](pg-extension-base-rebuild-procedure.md) exercises
new allocations and retained-slot rollback on the combined prototype. Existing
Iceberg-object takeover/rebind and application cutover remain outside that
contract. Continue with publication-side ownership and physical-copy exclusion,
then remaining faults, log/worker namespacing, scale/architecture/soak and
performance/upgrade requalification of the combined implementation.

The [publication/deployment cohort](pg-extension-base-publication-results.md)
passes transactional revocation tests and rejects actual physical copies with
different/empty external identities. Next qualify a manual exclusive-writer
transition or storage-fenced lease; duplicated profiles remain a counterexample
to universal exclusion. Requalify the combined regression/performance/upgrade
matrix before adoption, then complete scale/architecture/soak gates.

The [controlled cold-transfer cohort](pg-extension-base-cold-transfer.md) passes
40 checks for drained backup, quarantined validation, complete tracked-writer
shutdown, retained-slot resume and stale-failback refusal. Treat this as a manual
private-inventory contract, not automatic failover. Next requalify complete
multi-database diagnostics/performance and packaging; worker limits, amd64 and
soak gates remain open before an adoption decision.

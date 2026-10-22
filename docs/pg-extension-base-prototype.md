# pg_extension_base launcher investigation

> Historical prototype record. Commands and source paths below refer to the
> [preserved implementation](https://github.com/proddata/lake_replication/tree/7ef18381810075dddd26137ee959770591b95863/experiments/base_workers) before cleanup.
> Current lifecycle tests live in `tests/lifecycle`; retained admission helpers
> are described in [experiments/admission](../experiments/admission/README.md).

Decision: use `pg_extension_base` as the proposed foundation for per-database
launcher lifecycle, while retaining SQL publication through pg_lake. The
experiment demonstrates that this is viable; it is not a production migration.

Next steps and adoption gates are in the
[evaluation plan](pg-extension-base-evaluation-plan.md).
The [first evaluation results](pg-extension-base-evaluation-results.md) cover
shared hardening, event-driven waits, and comparative performance screening.
The [extended evaluation](pg-extension-base-extended-results.md) adds calibrated
commit measurements, longer matched cohorts and a stopped-state upgrade test.

The pinned upstream is pg_lake `8180647d4535fdb1ed223f92f301ba6094a8ec3e`,
including pg_extension_base 3.6, on PostgreSQL 18.6/Linux arm64. The release
worker/migration/extension sources match the clean archive from `85a32b4`.
The experimental image overlays an adapter for the actual C launcher; it runs
the actual Rust supervisors, initial copy, receiver, publisher, and Iceberg
tables. No production extension or worker source was edited.

## Results

| Variant | Result | Meaning |
| --- | --- | --- |
| Minimal lifecycle adapter | 81 passed, 3 failed | Automatic loading and per-database replication work; inherited bindings and explicit child cleanup need additional handling. |
| Adapter with binding fences and process ownership fixes | 85 passed, 0 failed | The tested lifecycle and fault cases pass with prototype-only fixes. |

Evidence: [minimal results](evidence/2026-10-18-base-workers-minimal-results.json),
[hardened results](evidence/2026-10-18-base-workers-hardened-results.json), and
[source/prototype provenance](evidence/2026-10-18-base-workers-provenance.json).
Run instructions and implementation are in
[`experiments/base_workers`](https://github.com/proddata/lake_replication/tree/7ef18381810075dddd26137ee959770591b95863/experiments/base_workers/README.md).

The destination explicitly preloads only `pg_extension_base`. It needs neither
`lake_replication.database_name` nor `session_preload_libraries`. Each database
gets its own launcher after extension installation. The real replication tests
cover credentials and rotation, initial/live changes, additive schema changes,
disable/enable, source outage, restart, existing slots, and interrupted-copy
recovery. Additional tests cover:

- committed and rolled-back extension creation/removal;
- two simultaneous destinations, launcher replacement, and postmaster stability;
- templates installed as templates before cloning;
- a failed DROP DATABASE with an ordinary client connected, followed by recovery;
- copied stopped subscriptions and an attempted enable of their inherited binding;
- DROP EXTENSION with an external supervisor deliberately paused by SIGSTOP.

The template test does not assume that converting a live database into a
template automatically stops workers. Upstream skips launching workers into
template databases; existing application state needs its own policy.

## What the minimal adapter exposed

**Copied configuration still names the old destination.** PostgreSQL copies our
`worker_launch_config.destination_database` field along with the database. A
launcher discovering that copy must not dispatch into the old database or
silently reuse a cloned Iceberg catalog. The hardened prototype stops mismatched
configurations and records a durable error requiring an explicit rebuild.
It deliberately does not rewrite bindings or automatically resume them.

**Launcher exit does not by itself stop external processes.** With a supervisor
paused, dropping its extension stopped the base launcher but left a replication
wrapper and the external supervisor alive. Ordinary idle workers can hide this
gap by exiting after a subsequent SQL error.

The hardened adapter drains tracked workers, uses interruptible child waits,
and explicitly cancels and reaps the supervisor process group. A Linux child
subreaper collects orphaned receiver/publisher descendants. An intermediate
prototype that only signalled process groups caused PostgreSQL running as PID 1
to adopt untracked descendants and restart after those descendants died. The
final adapter passes explicit postmaster-start-time checks around termination
and extension removal.

**Worker handles need durable memory and exit notifications.** The current
launcher obtains `BackgroundWorkerHandle` pointers while connected to SPI and
stores them beyond `SPI_finish()`. PostgreSQL allocates those handles with
`palloc` in the current memory context. The prototype explicitly allocates them
in `TopMemoryContext` and sets `bgw_notify_pid` before waiting for shutdown.
These are requirements of the [PostgreSQL implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/postmaster/bgworker.c),
not facilities supplied automatically by moving the launcher into a base worker.
The production handle lifetime is a follow-up correctness fix even if the
static launcher is retained.

## Production work still required

1. Fix worker-handle ownership and explicit child shutdown in the shipped code,
   with regression coverage for paused and busy descendants. Release handles
   when their tracked entries are removed; the prototype is not a long-running
   memory-retention qualification.
2. Define database identity and clone/restore/rename behavior. The prototype's
   name comparison conservatively blocks renames as well as copied bindings.
3. Implement a versioned migration with exactly one owner per database, preventing
   the old static launcher and new base workers from overlapping during upgrade.
4. Namespace logs and worker names by database as well as subscription ID; local
   subscription sequences can produce identical IDs in different databases.
5. Qualify abrupt launcher/worker death, postmaster crashes, worker-slot limits,
   and both supported architectures. SIGTERM and orderly server restart do not
   establish SIGKILL or power-loss safety.

No direct pg_lake_engine C API is needed for the demonstrated integration.

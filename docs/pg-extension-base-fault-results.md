# Forced-crash evaluation

Status: **revise and retest**. The first forced-crash cohort found a recovery
failure in the hardened prototype. Production adoption remains blocked.
A subsequent [bounded-recovery experiment](pg-extension-base-recovery-results.md)
fixes the transient contention regression under Docker init; the original
failures below remain as baseline evidence.

`experiments/base_workers/faults.py` installs the verified arm64 release archive,
then overlays the same hardened experimental base-worker adapter used for the
performance cohorts. Each case uses a fresh private source, destination,
object store and volumes. Runtime production sources remain unchanged.

## First cohort

[Retained results](evidence/2026-10-19-base-forced-crash-results.json): six
injections completed; 66 of 74 assertions passed. The eight failures are
reported below, rather than treated as a successful qualification.

The six-case cohort retains PID/start-time identities, process groups,
PostgreSQL logs and row/source-slot assertions. It sends SIGKILL to the Rust
supervisor, base launcher or PostgreSQL subscription wrapper, both normally
and with the external supervisor/receiver group stopped first.

| Kill target | Outcome |
| --- | --- |
| Rust supervisor, running | Children reaped; correct rows and one replacement; no PostgreSQL crash recovery. |
| Rust supervisor, stopped group | Same successful outcome, including a stopped receiver descendant. |
| Base launcher, running | One expected PostgreSQL crash recovery; rows recover; an old Rust process reaches PostgreSQL PID 1 as an untracked child. |
| Subscription wrapper, running | Same ownership gap, with correct replication recovery. |
| Base launcher, stopped group | PostgreSQL recovers, but replication stays in error and the new row remains absent after the recovery wait. |
| Subscription wrapper, stopped group | Same replication recovery failure. |

Every old process recorded before injection disappeared within the five-second
endpoint observation, and final disable left no recorded external worker.
That does not establish continuous ownership or successful automatic recovery.
Killing a shared-memory PostgreSQL worker deliberately causes PostgreSQL crash
recovery. The test counts log events; `pg_postmaster_start_time()` alone would
miss recovery within the surviving postmaster.

## Diagnosis

An [instrumented repeat](evidence/2026-10-19-base-forced-crash-diagnostic-results.json) verified a genuinely stopped supervisor and receiver
before killing the launcher. The replacement encountered:

> replication slot is already active outside this worker cycle

`worker/src/supervisor.rs` treats that source-preflight condition as fatal.
The old receiver still held the source slot briefly while the replacement
started. The old generation subsequently exited, but the durable subscription
state remained `error`; the next supervisor stayed paused. Destination rows
remained `1:1:one` instead of `2:2:one,two`.

The instrumented wrapper repeat did not reach injection because no receiver
was present at the pause instant. It is a fixture precondition failure, not
another runtime result. The harness now resumes and retries pausing until it
has verified a stopped receiver. Source snapshots preserve the exact runner
used for each cohort.

## Init/reaper control

The [Docker-init control](evidence/2026-10-19-base-forced-crash-init-control-results.json)
repeated both paused launcher/wrapper kills with a verified stopped receiver
and the same C runtime. Both had exactly one expected PostgreSQL crash recovery,
no untracked-child warnings, and no recorded old processes after five seconds.
Both still entered the same source-slot error and failed to publish the new row.
Explicit DISABLE/ENABLE also failed to recover in both cases: ENABLE changes
launch intent but does not clear the fatal subscription state.

This isolates the transient slot-contention failure from PostgreSQL's role as
container PID 1. It does not qualify init as a complete process-ownership fix,
and it does not show that pg_extension_base caused the shared Rust failure.
The static launcher has not been subjected to these exact crash injections.

## Verification and remaining scope

`make check` passed, including six measurement/analysis tests, Rust workspace
tests, generated SQL and Compose validation. Production worker/migration/extension
sources still match the tested archive source revision `85a32b4`. Per-run saved
source hashes were verified, and all private crash-test containers and volumes
were removed. New work remains local and uncommitted.

Retained JSON includes image IDs, archive and source fingerprints, process
identities, checks and diagnostic errors. Full logs and exact source snapshots
remain under `.qualification/<run>_faults/`; log hashes are retained in the JSON.
Successful endpoint observations do not establish zero transient overlap.

## Required revision

Separate process reaping from publication ownership. Test an init/reaper when
PostgreSQL would otherwise be container PID 1, and define who drains external
processes when a PostgreSQL worker exits without callbacks. An init process
alone does not prove termination or slot exclusivity.

Make source-slot contention during recovery a bounded, observable wait while
preserving refusal to consume an active slot. Retain fatal checks for wrong
source identity, invalidated/lost WAL, and checkpoint divergence. Bound retries
and surface persistent contention; never kill an unrelated slot holder or
silently seize its slot. Qualify this together with cleanup and generation
ownership before claiming automatic crash recovery.

Do not broaden performance/soak claims until these regression cases pass.
Receiver/publisher-targeted kills, abrupt container termination, scaling,
clone/restore takeover, amd64 and the 24-hour soak remain unqualified.

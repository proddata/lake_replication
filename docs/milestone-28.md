# Milestone 28: PostgreSQL-native process supervision for lake_subscription_sugar

Status: implemented and verified against both the milestone lab
(`tests/subscription_sugar/probe.py`, all 24 checks) and `deploy/poc/`.
Closes the "process supervision" gap
[Milestone 27](milestone-27.md) explicitly left open, using Option C from
that discussion: PostgreSQL's own background-worker infrastructure (the
mechanism `pg_cron` is built on), not systemd.

## What changed

Previously, `CREATE SUBSCRIPTION` spawned `lake-replication-supervisor` (and
`lake-replication-initial-sync`) as a raw, doubly-forked, detached OS
process directly from the calling backend -- no restart on crash, no
standard tooling, discoverable only via a PID column. Now:

- `CREATE SUBSCRIPTION` still spawns `lake-replication-initial-sync`
  directly (one-shot, unchanged), but no longer spawns the supervisor at
  all. It only records the subscription's launch configuration into a new
  `lake_subscription_sugar.worker_launch_config` table.
- A single, permanently-running **launcher** background worker
  (`lake_subscription_sugar_launcher_main`, registered via
  `shared_preload_libraries`) polls that table (every
  `lake_subscription_sugar.launcher_naptime_ms`, default 1000) and starts a
  genuine PostgreSQL background worker per subscription that should be
  running, using `RegisterDynamicBackgroundWorker`.
- `ALTER SUBSCRIPTION ... DISABLE/ENABLE` and `DROP SUBSCRIPTION` now just
  flip `desired_state` (or delete the row); the launcher notices and stops
  the worker via `TerminateBackgroundWorker`. `DROP SUBSCRIPTION` no
  longer blocks for up to 6 seconds polling for the old PID to exit -- it
  returns immediately, and the launcher terminates the worker
  asynchronously.
- `ALTER SUBSCRIPTION ... ENABLE` no longer needs a `dblink` round trip to
  rediscover the source: everything the supervisor needs was already
  recorded durably in `worker_launch_config` at `CREATE SUBSCRIPTION` time.

## Why a launcher, not just "call RegisterDynamicBackgroundWorker from CREATE SUBSCRIPTION"

A `BackgroundWorkerHandle` returned by `RegisterDynamicBackgroundWorker` is
only valid inside the backend process that obtained it -- there is no
supported way to reconstruct one later, in a different session, to stop a
worker that session did not itself start. `CREATE SUBSCRIPTION` and
`DROP SUBSCRIPTION` are almost always different sessions, often long
apart. The only way to manage a worker across that gap is one process that
outlives both and keeps every handle in its own memory for as long as it
might need one -- hence the launcher, and hence it must be a *static*
background worker (`RegisterBackgroundWorker`, legal only while
`shared_preload_libraries` is processed at postmaster startup), unlike the
per-subscription supervisor workers it manages dynamically.

This was verified directly against the actual PostgreSQL 18 source
(`postmaster/bgworker.c`, `postmaster/postmaster.c`) before relying on it:
the generation counter inside a `BackgroundWorkerHandle`'s slot is only
bumped when a slot is freed and reused for an unrelated registration, not
on an automatic restart -- so a handle obtained once by the launcher stays
valid across the supervisor's entire lifetime, including any restarts, for
as long as the launcher process itself lives. And `TerminateBackgroundWorker`
sets a `terminate` flag the postmaster checks ahead of `bgw_restart_time`,
so it permanently cancels future auto-restarts rather than just killing the
current instance.

## Two real bugs found and fixed while building this

### 1. Exec'ing the supervisor binary directly into the bgworker crashed the whole cluster

The first working version had the bgworker process `execve()` straight
into `lake-replication-supervisor`, replacing its own process image. This
is fast, simple, and **wrong**: PostgreSQL treats *any* non-zero exit
status from a background worker as a potential shared-memory-corrupting
crash, and reacts exactly like a real segfault -- terminating every other
backend and reinitializing shared memory. `lake-replication-supervisor`
calls `std::process::exit(1)` on perfectly ordinary conditions (e.g. losing
a duplicate-supervisor race right after its predecessor was killed -- see
Milestone 13's fencing). Verified live: `kill -9` on the running
supervisor produced

```
WARNING: lake_subscription_sugar launcher: no free background worker slot ...
LOG: background worker "lake_subscription_sugar supervisor" (PID ...) exited with exit code 1
LOG: terminating any other active server processes
LOG: all server processes terminated; reinitializing
```

-- i.e. the exact crash-and-recover cycle a real segfault would trigger,
from killing one subscription's worker.

**Fix:** the bgworker process now `fork()`s a child to `execve()` the real
supervisor binary into, `waitpid()`s for it, logs the real outcome (signal
or exit code), and **always** reports a clean `exit(0)` to PostgreSQL
itself regardless of what actually happened. This also means PostgreSQL's
own `bgw_restart_time` can no longer be what restarts a crashed supervisor
(a clean exit tells the postmaster "don't restart, this was intentional"),
so restart-on-death moved entirely into the launcher's own liveness check
(`GetBackgroundWorkerPid` each cycle; `BGWH_STOPPED` triggers a fresh
`RegisterDynamicBackgroundWorker`), and the per-subscription registration
now uses `BGW_NEVER_RESTART` explicitly to make that ownership unambiguous.

### 2. Removing the wait-for-exit broke the source-slot drop

The old `DROP SUBSCRIPTION` polled for up to 6 seconds for the supervisor's
PID to disappear *before* attempting to drop the source-side replication
slot -- the delay existed precisely to let the receiver disconnect first.
Making `DROP SUBSCRIPTION` fast and non-blocking (a deliberate goal of this
milestone) removed that wait, and the very next test run reproduced the
consequence immediately:

```
ERROR: replication slot "orders_sub_slot" is active for PID 979
```

**Fix:** `drop_source_slot` now retries the drop itself, for up to ~6
seconds, wrapped in a subtransaction (the standard PL/pgSQL-`EXCEPTION`
pattern) since `pg_drop_replication_slot` raises a real `ERROR` while the
slot is active, not a warning. The receiver typically releases the slot
within its own poll interval (well under a second in practice), so this
rarely retries more than once.

## Real deployment cost -- confirmed, not just estimated

This genuinely requires `shared_preload_libraries` and a new
`lake_subscription_sugar.database_name` GUC naming the database
`lake_subscription_sugar`/`lake_replication` are installed in -- both
requiring a PostgreSQL restart to take effect, exactly as flagged when this
option was chosen. Bringing the milestone lab in line with this surfaced
one more real wrinkle: a value set via `ALTER SYSTEM` *before* the library
has ever loaded is held as an untyped placeholder, and PostgreSQL refuses
to promote a placeholder into a `PGC_POSTMASTER` variable after startup
begins ("cannot create PGC_POSTMASTER variables after startup") -- only
into `PGC_SIGHUP` or lower. `lake_subscription_sugar.database_name` is
therefore `PGC_SIGHUP` even though, functionally, it only ever takes effect
at the launcher's own startup (reloading does not make an already-running
launcher re-read it) -- the context is a workaround for the placeholder
rule, not a claim that reload alone updates behavior.

`deploy/docker-compose/` (the milestone lab) and `deploy/poc/` (the demo)
both now bake `lake_subscription_sugar` into their images and set
`shared_preload_libraries`, `lake_subscription_sugar.database_name`, and a
raised `max_worker_processes` in their Compose `command:`. The lab's
Dockerfile build context moved from `deploy/docker-compose/` to the repo
root (matching the PoC) so it can `COPY extensions/lake_subscription_sugar`
directly; `tests/subscription_sugar/probe.py` still rebuilds and installs
the extension from the current working tree on every run for fast
iteration, but now also restarts `postgres` afterward so the freshly built
code actually loads -- a bare `CREATE EXTENSION`/reinstall no longer
suffices once the library is preloaded.

## Evidence

`make subscription-sugar-test` (all 24 checks, including "the supervisor is
a real, running OS process inside the container" and "DROP SUBSCRIPTION
did not crash the cluster (postmaster start time is unchanged)" --
precisely the property bug #1 above violated). Manually verified against
`deploy/poc/`, end to end, in this order: `CREATE SUBSCRIPTION` reaches
`streaming`; a live `INSERT` replicates; `kill -9` on the supervisor
process is followed by a new supervisor PID within about a second with no
disruption to the destination database (`pg_isready` stays healthy
throughout, no other backend is affected); replication continues
correctly with the new process; `ALTER SUBSCRIPTION ... DISABLE` stops the
worker and `ENABLE` starts a genuinely new one; `DROP SUBSCRIPTION`
returns immediately, permanently stops the worker, and drops the
source-side slot for real.

## What is still not covered

`lake-replication-initial-sync` remains on the old detached-fork path,
unmanaged by the launcher -- deliberately: it is one-shot and not safe to
blindly restart against a table that may already be mid-copy, so postmaster-
level crash-restart semantics are the wrong tool for it regardless. If it
dies mid-copy today, recovery is still whatever Milestone 7's rebuild-
promotion path already provides, not anything this milestone changed.

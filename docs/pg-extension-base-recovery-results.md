# Bounded source-slot recovery experiment

> Historical prototype record. Commands and source paths below refer to the
> [preserved implementation](https://github.com/proddata/lake_replication/tree/7ef18381810075dddd26137ee959770591b95863/experiments/base_workers) before cleanup.
> Current lifecycle tests live in `tests/lifecycle`; retained admission helpers
> are described in [experiments/admission](../experiments/admission/README.md).

Status: the transient slot-contention regression is fixed in the isolated
prototype under Docker init. Production adoption remains unqualified.
The [original failures](pg-extension-base-fault-results.md) remain retained.

## Change

`experiments/base_workers/recovery.py` patches a disposable copy of the Rust
supervisor. `Dockerfile.recovery` runs its tests and rebuilds only that binary;
the release archive and production sources remain unchanged. The C adapter,
receiver, publisher and initial-sync implementations are unchanged.

Source preflight validates system/database identity, slot identity, WAL safety,
checkpoint continuity and timeline ancestry before classifying an active slot.
Active slots are refused with an observable retry and a per-endpoint monotonic
five-second contention budget. Retry backoff is capped by the remaining budget.
A validated inactive endpoint clears the budgets; persistent contention becomes
a durable source error. Query/connection execution can add time beyond five
seconds, so this budget is not an absolute wall-clock process timeout.

No slot holder is killed, no active slot is consumed, and no arbitrary fatal
subscription error is cleared by ENABLE. Persistent contention still requires
an explicit validated recovery procedure, which remains to be implemented.

## Evidence

The [first revised regression run](evidence/2026-10-19-base-recovery-regression-results.json) passed both previously failing paused
launcher/wrapper kills (30/30 assertions). Worker logs show an actual contention
retry before recovery, rather than only a fortuitous absence of the race.

The [complete revised cohort](evidence/2026-10-19-base-recovery-full-results.json) then passed **91/91 assertions** across six kill
cases and an independent slot-holder test. Each case used fresh private data
volumes on Linux arm64/PostgreSQL 18.6 with Docker init. PostgreSQL launcher or
wrapper kills each caused exactly one expected crash recovery; Rust supervisor
kills caused none. Old recorded external processes were gone at five seconds,
replication resumed with unique correct rows, initial copy was not repeated,
and final disable left no recorded external worker.

An independent `pg_recvlogical` client held the slot while the subscription was
enabled. The contention budget produced a durable source error after 5.72 seconds
including fixture/client observation overhead. The same independent backend
still owned the active slot, destination contents remained unchanged, and the
replacement could be disabled cleanly. This demonstrates refusal and bounded
retry, rather than slot takeover.

After this cohort, the final refinement clears stale budgets for alternative
endpoints when a healthy endpoint is selected. Its virtual-time unit test also
covers that reset. The [final image retest](evidence/2026-10-19-base-recovery-final-results.json)
passed all 41 assertions across both paused crash regressions and the
persistent-holder case; exact sources are retained per run.
The full six-kill cohort predates that small refinement. Multi-endpoint live
failover under contention remains unqualified.

## Limits and next steps

The independent init/reaper is part of the tested deployment. Running PostgreSQL
as container PID 1 still has the previously observed untracked-child ownership
gap; bounded retries alone do not qualify that topology. Endpoint process counts
do not prove absence of every transient overlap. The existing supervisor advisory
lock and slot refusal remain in effect, but destination-scoped physical-copy
exclusivity has not been established.

`make check` passed after clearing stale package build artifacts from an initial
disposable-copy compile. Production source files remained unchanged. All private
crash fixtures were removed after the runs. New work remains uncommitted and
unpushed.

Ten Rust supervisor tests run in the experimental image: seven existing tests
and three recovery tests covering transient/reset behavior, persistent expiry,
and fatal identity/WAL/checkpoint mismatches even when the slot is active.
The production source tree and release archive are not changed by those tests.

Next qualify clone/logical-restore/rename bindings and explicit recovery/rebind,
then receiver/publisher-targeted failures, abrupt server death and supported
process ownership. Scaling, amd64, a 24-hour soak and a fresh performance
comparison of the revised supervisor remain open. Earlier performance cohorts
used the original supervisor and do not qualify this revised binary.

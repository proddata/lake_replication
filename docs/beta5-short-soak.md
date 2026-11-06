# Ten-minute packaged soak validation

The short soak passes against the ARM64 CI archive at `b0a703a` and the supported
`lake-replication-manifest-cleanup-v1` prerequisite. It installs unchanged
archived SQL and workers in disposable containers, with the committed original
prerequisite recipe. The existing local Dockerfile build experiment is excluded.
This validates the runner and recovery scenarios; it does not satisfy the
24-hour release soak.

The measured workload runs for 600 seconds, with 603.47 seconds including final
capture/cleanup. It completes 1,166 source transactions and 45 assertions.
Each transaction upserts 16 bounded keys, updates eight values, deletes one row
and advances a progress row in the same transaction. The final dataset has
960 rows, compared in full with the source after reaching its exact committed
sequence. Periodic and post-fault checks also match every row and value.

| Fault | Result |
|---|---|
| Native Rust supervisor SIGKILL | Replacement observed; exact recovery; PostgreSQL unchanged |
| Ten-second object-storage outage | Source writes continue; exact recovery; journal bounds retained |
| Destination PostgreSQL restart | Replication resumes with exact data and one supervisor wrapper |
| Ten-second disabled subscription | Destination stays stationary while source advances; backlog catches up exactly |

The run records 109 resource samples. There are no cgroup OOM reports, unexpected
PostgreSQL restarts or unexpected container deaths. Docker events match exactly
the one planned storage stop and one destination restart. Final subscription
drop removes its source slot, supervisor wrapper and native Rust children;
fixture teardown returns zero and records no cleanup errors.

Peak cgroup memory is 303.0 MiB for the destination and 1,638.1 MiB for the
engine. These include filesystem cache and are not standalone process RSS or
proof of a memory leak. The raw samples retain process RSS/PSS and CPU counters.
Observed visibility upper bounds include five-second polling and injected
faults, so they must not be compared with the native workload's two-second p95
gate. All 1,166 commits are observed; the maximum upper bound is 21.98 seconds.

## Growth finding before a full-day run

Logical rows remain bounded, but allocated object storage grows from 0.43 MiB
to 912.8 MiB. A read-only file inventory at 583 seconds, after 1,133 transactions,
finds approximately:

| Stored object representation | Apparent bytes |
|---|---:|
| Iceberg metadata JSON | 717,056,633 |
| Iceberg manifests | 50,938,587 |
| Parquet | 6,600,358 |

These are MinIO filesystem representations, including its per-object metadata,
and differ from the allocated-block totals. The JSON growth appears in both
the item table and progress table. The workload leaves journal and Iceberg
maintenance defaults unchanged. This is observed growth, not a projection or
a demonstrated leak. Review snapshot/metadata retention, cleanup and memory
behaviour, then repeat a bounded profile before committing to the full day.
The runner retains a 4 GiB object-storage budget and cannot be treated as
ready for a 24-hour endurance claim merely because this smoke profile passed.

## Reproducibility and controller checks

[`tests/soak/probe.py`](../tests/soak/probe.py) runs this profile; its
[guide](../tests/soak/README.md) describes fault timing, limits and artifacts.
The selected archive and adjacent checksum, supported runtime/library hashes,
actual measured harness sources, raw result/resource/event/log checksums and
fault checkpoints are retained in
[structured evidence](evidence/2026-11-06-beta5-short-soak.json).

An initial Compose-generation error is retained separately. It occurred before
any source workload and was corrected by anchoring service-name matches and
validating the generated Compose file before startup. The measured short run
then completes with all gates enforced.

Subsequent controller hardening bounds resource queries, preserves primary
failures while still attempting teardown, rejects late recovery responses and
invalid sampling arguments, and evaluates day-long eligibility after cleanup.
Ten unit tests cover these constraints, progress accounting, planned fault
scheduling, OOM/unexpected-death detection and bounded observer state. The
measured source snapshots remain retained rather than being replaced by the
later hardened controller source. All required project checks pass.

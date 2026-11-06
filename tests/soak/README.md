# Packaged soak runner

For a temporary native Linux host and SSH-independent smoke/two-hour pilot, use
the [remote-host runbook](../../deploy/soak/README.md). Its launcher checks native
archive identity and capacity and cannot start a 24-hour run. The optional
`--observe-maintenance` flag records metadata and deletion-queue state every
minute without running maintenance or taking the publication lock. The
`--min-free-mib` flag raises the fixture disk reserve above its 1 GiB default.

Run the short profile against an explicitly selected, checksum-verified native
archive and the supported pinned pg_lake prerequisite:

```sh
QUALIFICATION_ARCHIVE=/absolute/path/lake_replication-0.1.0-beta.5-linux-arm64.tar.gz \
  python3 tests/soak/probe.py --duration-seconds 600
```

Use an isolated checkout with the committed prerequisite recipe. The archive's
adjacent `.sha256` file is required. The runner installs only archive files;
it does not overlay SQL or worker binaries. It reuses the verified upstream
cache and builds/verifies the separate supported runtime layer. Candidate,
prerequisite and runner input identities are retained.

The short profile measures ten minutes after initial-copy setup. It offers two
source transactions per second. Every transaction inserts/upserts 16 bounded
keys, updates eight values, deletes one row and advances a progress row in the
same transaction. The key space is 1,024 keys, so the logical dataset remains
bounded while data and metadata continue changing. Source failures fail the
run; ambiguous commits are not blindly retried.

Four events are scheduled at 120, 240, 360 and 480 seconds:

1. SIGKILL the native Rust supervisor; observe a replacement and continued data.
2. Stop object storage for ten seconds while source writes continue, then restore it.
3. Restart destination PostgreSQL while source writes continue.
4. Disable the subscription for ten seconds, demonstrate a stationary destination
   and continued source writes, then enable it and drain the backlog.

The runner briefly pauses the producer at periodic and post-fault barriers. It
waits for the destination's committed progress row, then compares every row and
value with the quiescent source. Successful checkpoints save row counts and
data fingerprints; mismatches save the complete source and destination states.
Each checkpoint also requires exactly one supervisor wrapper. Final subscription
drop must remove its slot, wrapper and native Rust children.

Every five seconds it records checkpoint ordering, pending journal bounds,
service state and counters, database size, cgroup CPU/memory/OOM counters,
process RSS/PSS, filesystem space and object-storage usage. Docker events
independently detect unexpected container exits and OOMs. Planned container
exits must match the observed destination/storage exit counts exactly. Any
unexpected PostgreSQL restart fails. During injected outages, resource sampling
continues without issuing destination data queries that depend on unavailable
storage.

Defaults share CPUs `0-1` across services, cap destination and engine at 2 GiB
each, source at 1 GiB and storage at 512 MiB. The run stops if object storage
exceeds 4 GiB, available fixture filesystem space falls below 1 GiB, a cgroup
reports OOM, or steady backlog age exceeds 60 seconds. Recovery barriers have a
60-second catch-up budget. These explicit soak-profile limits do not change
the existing native four-copy latency gates.

Resources stream to `resources.jsonl`; Docker events and worker/container logs
are retained beside an incrementally written `results.json` under
`.qualification/*_soak/`. Pending-commit accounting is bounded. Visibility
measurements are upper bounds including observer polling and injected faults;
they are not the native workload's two-second p95 benchmark. Monotonic elapsed
time is checked against the wall clock. On macOS an idle-sleep assertion is
held only while the runner is active. Containers and volumes are removed on
completion or failure; the persistent development lab is untouched.

The controller also accepts `--duration-seconds 86400`; it repeats the four
faults each hour. That mode needs a separate reviewed run and resource budget.
A successful short profile is always `qualifies_24_hour_soak: false`. A failed
run, failed cleanup or an incomplete day cannot satisfy the day-long gate.
The ten-minute run validates the harness and recovery scenarios; it cannot
establish long-term memory, metadata-growth or latency stability. The current
profile leaves journal and Iceberg maintenance defaults unchanged, so growth
and maintenance requirements must be assessed before starting the full day.

## Explicit maintenance experiments

Two opt-in profiles investigate the two retention layers:

| Profile | Snapshot history | Orphaned-file retention during cleanup |
|---|---:|---:|
| `none` (default) | Upstream default: 1,800 seconds | Upstream default: ten days |
| `snapshot-only` | 60 seconds on the two replicated fixture tables | Ten days |
| `bounded-test` | 60 seconds on the two replicated fixture tables | 120 seconds, disposable experiment only |

```sh
QUALIFICATION_ARCHIVE=/absolute/path/to/the-qualified-native-archive.tar.gz \
  python3 tests/soak/probe.py --duration-seconds 600 \
    --maintenance-profile snapshot-only --max-storage-mib 512
```

Use `bounded-test` only on this disposable fixture. It shortens the physical
file-recovery horizon; it is not a production PITR recommendation. Both profiles
are experiments and always report `qualifies_24_hour_soak: false`, regardless of
duration. They do not change the archive, runtime libraries or global settings.

The profiles disable autovacuum on their fixture tables and explicitly run
targeted `VACUUM (VERBOSE) table` passes about every 30 seconds, with at most two
compactions and 1,000 removals per table/pass. A control transaction holds the
private subscription's publication-checkpoint row lock during maintenance.
Ingress is separate, so source writing and bounded staging continue; publication
resumes when that control transaction rolls back. The native supervisor must
stay alive. This uses an internal checkpoint table for an experiment, not a
new public operator API. Every pass ends with an exact-data barrier.

Acquiring all stream-pool permits is not a supported substitute for this
barrier: the pool fallback can wait on a permit held by the same maintenance
group. No production admission algorithm is changed here. Likewise,
`VACUUM (ICEBERG)` is a whole-catalog command in the pinned version and rejects
an explicit relation list; these profiles use the targeted command above.

Preflight holds a repeatable-read reader across a vacuum, checks that its data
stays unchanged, and independently verifies a protected metadata object in
MinIO. Later passes require the short-retention witness to be actually removed
after becoming eligible; the ten-day control must keep it. Vacuum warnings
fail the run because pg_lake may downgrade a failed phase to a warning. Current
metadata sizes, snapshot ages/counts and deletion-queue state stream to
`maintenance.jsonl`; full maintenance output is retained separately.

Storage sampling excludes MinIO's ephemeral staging directory. Disappearing-file
errors elsewhere are retried at most three times with diagnostics retained;
permission errors, I/O failures and exhausted retries fail. The disk-free
guard still covers temporary storage. Earlier storage figures that included
temporary files are not identical measurements of this retained-object scope.

For an independent check while a fixture is live:

```sh
python3 tests/soak/observe_references.py /absolute/path/to/.qualification/RUN_soak
```

It enumerates every reference from both current Iceberg metadata states and
checks the corresponding object representation directly in the pinned,
unversioned MinIO fixture. This supplements query-based checks when a cache
could otherwise hide a missing object. It is scoped to this fixture's bucket
and is not a general object-store inspection tool.

# Packaged soak runner

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

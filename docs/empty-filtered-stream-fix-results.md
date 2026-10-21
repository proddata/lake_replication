# Empty filtered commits: SQL fix and remaining native timeout

The empty-commit SQL fix passes 75 checks on PostgreSQL 18/arm64 with real
pg_lake/Iceberg. The original native two-million-row scenario then failed for a
separate receive-timeout problem; the subsequent [receiver lifetime fix](receiver-progress-fix-results.md)
now passes 96 checks, including that native case. Neither result qualifies a new release archive
or changes the admission performance assessment.

## Implementation

`migrations/0022_empty_filtered_transactions.sql` replaces three functions:

- `stage_transaction` accepts an empty JSON array, preserving checkpoint locking,
  metadata/payload replay checks, monotonic staging and the existing change
  validation. Empty commits create a transaction row and no change rows.
- `commit_stream` converts an empty aggregate to `[]`, journals the source commit,
  then deletes its durable stream fragments in the same SQL transaction.
- `next_batch` uses a left join and counts non-null change transaction IDs. Empty
  commits remain in source order, count against transaction/byte budgets, and
  contribute zero rows. Trailing empty-only batches are selected and published.

The receiver still sends feedback only after durable staging. Publication still
consumes the journal through the existing atomic batch path. Receiving a later
empty commit does not advance the publication checkpoint past pending earlier
work. No synthetic row, direct feedback shortcut or native binary change is used.
The migration is included in fresh current installs and the explicit `0.1.0` to
`0.1.0-beta.1` upgrade; baseline SQL remains unchanged.

## Regression evidence

Run the private fixture with:

```sh
make empty-filtered-test
```

The [75-check passing evidence](evidence/2026-10-21-empty-filtered-stream-fix-results.json)
records the source SQL hashes, generated install/upgrade SQL hashes, test driver
hash, immutable test image ID and unchanged native archive fingerprint. Raw
results and worker logs remain in
`.qualification/qualification_20261021_062030_empty_filtered/`.

Coverage includes fresh/upgrade installs, exact replay, conflicting metadata,
empty replay against real changes, staged-commit rollback, acknowledgement bounds,
transaction limits, preceding unpublished data, empty-only publication rollback
and replay, abort, older commits, and ordinary empty staging. Real pgoutput tests
force streaming with 64 kB logical decoding memory and the debug immediate-stream
option: an excluded transaction commits with zero relevant changes, survives a
destination restart before publication, and is followed by real changes. A
receiver killed during an uncommitted empty stream retains fragments and replays
that stream exactly once. A mixed streamed transaction retains both relevant
changes. Source feedback never exceeds durable staging.

This fixture installs current SQL over the previously qualified archive's native
binaries. It is SQL regression evidence, not rebuilt artifact qualification.
Existing installations already at `0.1.0-beta.1` are not updated by that baseline
upgrade path; a versioned package upgrade remains release work. No release gate
was promoted. `make check` also passes.

## Historical native bulk failure before the receiver fix

The optional broader diagnostic is:

```sh
make -C extensions/lake_replication generate
python3 tests/streaming/empty_filtered.py --native-bulk
```

The [retained failure](evidence/2026-10-21-native-bulk-timeout-diagnostic-results.json)
passes the 75 SQL/wire checks and native initial copy, then fails to receive a
real change following an excluded two-million-row source transaction within
90 seconds. The subscription continues to report `streaming` without an error.
Interactive inspection found successive receiver `idle_timeout` exits at 200 ms,
reconnections at `0/1A2DFE38`, and source WAL at `0/1A5926E0`; the target retained
only its initial row. The full child log was not retained before cleanup; the
inspection excerpts and provenance are explicitly identified in the evidence.

The native C launcher sets `LAKE_RECEIVER_IDLE_TIMEOUT_MS=200`. The supervisor
runs one receiver synchronously; a socket read timeout makes that receiver exit
successfully and the next cycle reconnects. These observations indicate repeated
short receive lifetimes prevent decoding from reaching the source commit in
this fixture. They do not establish a universal decoding-time threshold.

Next, separate the short supervision/stop polling interval from the receiver's
connection lifetime. Preserve prompt disable/drop and publisher progress while
allowing decoding to finish. Test the native bulk case without the debug stream
option, source outages, stop latency and interrupted network frames. Simply
continuing after a socket timeout is unsafe with the current `read_exact` message
reader because a partial frame may already have been consumed. Increasing one
fixed timeout alone does not establish progress for arbitrary transaction sizes.
The subsequent [receiver lifetime fix](receiver-progress-fix-results.md) implements
and validates this separation; the native failure below is retained as history.

All private containers and volumes from these runs were removed. The validation run itself did not commit or push. The admission overlay remains an experiment; ownership,
atomic copy publication, bounded memory and reserved streaming capacity remain
requirements for its next iteration.

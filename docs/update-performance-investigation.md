# Update performance investigation

This records the original fixture-only experiment. The subsequent
[packaged key-filter integration](update-key-filter-integration.md) includes
the generalized implementation and its separate validation results.

The local ARM64 investigation reproduces the update slowdown and identifies a
specific cause: the current `UPDATE ... FROM` plan pulls the entire Iceberg
table into PostgreSQL and sorts it to match a small change run. A redundant
target-key predicate lets pg_lake filter rows in its vectorized engine before
returning them to PostgreSQL.

The fixture-only candidate improves both destination application and a native
worker follow-up. It is not installed in production SQL, and the production
500-row/4-MiB run limits remain unchanged. This is a diagnostic working point,
not release qualification or an established deployment capacity.

## Inputs and controls

The tests use a locally built native archive from frozen committed source
`bd985d81620491a10fb3cf247f940597f407597e` and the supported
`lake-replication-manifest-cleanup-v1` runtime at pg_lake `8180647d4535`.
Concurrent demo/Dockerfile/Makefile edits are excluded from the build and
measured harnesses. The archive is not substituted for an accepted CI release
candidate. Exact hashes and measured source snapshots are retained in
[structured evidence](evidence/2026-11-11-update-performance.json).

The disposable source, destination, engine and storage share CPU affinity `2-3`.
Destination/engine container caps are 1.5/2 GiB; pgduck retains its 1-GB engine
limit. A separate demo remained running on the host, so these are not dedicated
hardware observations. No shared containers, volumes or caches were pruned.

## Destination-only comparison

Each trial freshly seeds the same one million logical rows into ten Iceberg
data files (approximately 44.7 MB compressed), with deterministic 128-byte
payloads. Ten thousand distinct keys are updated in one outer transaction;
only SQL run size and the redundant key predicate vary. Clustered keys cover
the first 10,000 IDs; random keys use a fixed reproducible sample across the
table. Compressed byte sizes vary slightly between seed writes; all trials
retain the same logical data, file count and row count.

There are three trials per configuration, with the second repetition reversing
configuration order. All 36 trials pass complete row/value checks and the
exactly-one-match-per-change contract. Timings use a persistent SQL connection,
exclude fixture seeding/input construction, and include final commit. Source
WAL receipt, journal staging and worker startup are excluded.

| SQL run size | Clustered baseline | Clustered key filter | Random baseline | Random key filter |
|---:|---:|---:|---:|---:|
| 500 | 346 updates/s | 10,991 updates/s | 312 updates/s | 2,582 updates/s |
| 2,000 | 1,461 updates/s | 26,062 updates/s | 1,338 updates/s | 10,109 updates/s |
| 5,000 | 3,262 updates/s | 45,087 updates/s | 3,314 updates/s | 20,824 updates/s |

Values are medians of the three local trials, not confidence bounds. The
same-size 500-row filter gain is about 32x for clustered keys and 8.3x for random
keys. Larger runs separately amortize repeated scans. These dense batches do
not represent an application's single-row transaction rate.

Server commit durations are retained from uniquely marked COMMIT log entries.
All 36 phase-duration sums agree with client monotonic totals within one second;
no negative statement spans or missing commit records occur. The dominant
baseline cost is statement execution, not commit. Resource samples report
cgroup memory, including filesystem cache, rather than standalone process RSS.
No OOM or unexpected destination PostgreSQL restart is observed.

## Execution-profile confirmation

A separate four-trial profile executes and rolls back `EXPLAIN (ANALYZE,
VERBOSE, BUFFERS)` before its warmed diagnostic timing. These timings are not
pooled with the 36 comparison trials. The rollback is checked before proceeding.

For a 500-key update against one million rows:

| Observation | Baseline | Key filter |
|---|---:|---:|
| Rows returned by the foreign scan to PostgreSQL | 1,000,000 | 500 |
| Destination-side sort | External sort | In-memory quicksort |
| Destination-side sort space | 193,800 KiB on disk | 122 KiB in memory |
| Data files listed by the plan | 10 | 10 |

Both clustered and random profiles show this row reduction. The pushed-down
predicate appears in `Vectorized SQL`. The evidence supports reducing row
transfer and PostgreSQL sorting; it does not establish that pg_lake's own file
pruner skips additional files. Work performed inside Parquet readers is not
separately attributed by this profile.

## Native worker follow-up

The follow-up uses ordinary `CREATE SUBSCRIPTION`, a fresh native initial copy
of a million-row source table plus a progress row, and the archived workers.
It runs baseline/filter/baseline. For each pass, the source commits 200 distinct
random single-row updates in separate transactions, then commits the progress
marker. Timing starts at the first source update and ends when that marker is
observed in Iceberg; the final dataset is then compared in full.

| Pass | Data-update burst completion rate | Elapsed through marker observation |
|---|---:|---:|
| Baseline | 4.69 updates/s | 42.64 s |
| Key filter | 26.59 updates/s | 7.52 s |
| Baseline | 5.17 updates/s | 38.70 s |

Source writes finish in 0.08–0.15 seconds; most elapsed time is backlog catch-up.
The candidate is about 5.1–5.7x faster than the surrounding controls in this
one bracket. This is not a sustained application-capacity or p95 latency claim.
Journal admission, worker polling, publication batching and resource defaults
are unchanged; the larger SQL run-size experiment is not applied here.

The candidate also passes native supervisor SIGKILL/replacement, a distinct
source write after the replacement is observed, and another full exact-data
comparison. Replacement plus resumed write is observed in about 2.09 seconds.
All three subscriptions remove their native source slots. There are 26 passing
archive/correctness/native checks, no observed OOM or destination postmaster
restart, and successful fixture cleanup. The installed update function is
restored to its original definition before teardown. This does not replace
the full ambiguous-commit, storage-outage and multi-subscription suites.

## Next implementation decision

The first production candidate should add a safely typed target-key filter,
generalized beyond the diagnostic's literal `id` column, while retaining
nullable/composite fallback and exactly-one-row checks. It should keep the
500-row default initially to isolate the improvement. Key changes, repeated-key
ordering, rollback and supervisor recovery already pass in the narrow fixture;
generalized SQL and installed upgrade paths need their own tests.

Treat 2,000/5,000-row runs as a separate change requiring memory, concurrency,
latency and recovery qualification. The remaining small-transaction overhead
should be profiled across publication batches, polling, journal admission and
commit; the dense destination-only rates cannot be used to predict it.
Freeze the chosen implementation, export new native archives, and requalify
them before updating release claims. The deferred 24-hour soak remains open.

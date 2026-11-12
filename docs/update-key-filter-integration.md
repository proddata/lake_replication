# Packaged update-key filter

The follow-up to the [original experiment](update-performance-investigation.md)
adds a typed target-key membership predicate to `apply_change_run`. It reduces
rows transferred from Iceberg into PostgreSQL for small UPDATE runs. The
publisher's ordering and 500-row/4-MiB limits remain unchanged.

The fast path supports a single `NOT NULL` boolean, smallint, integer, bigint,
text, varchar or UUID identity, with arbitrary quoted column and relation names.
Keys travel as typed SQL parameters. Nullable/composite identities, domains and
other types retain the original predicate. Both paths retain exactly-one-match
checks. INSERT and DELETE behavior is unchanged.

Migration `0030_update_key_filter.sql` is included in fresh beta.5 installs and
beta.4-to-beta.5 upgrades. Existing development beta.5 installations need the
packaged `DEVELOPMENT_UPDATES/0030_update_key_filter.sql` refresh; same-version
`ALTER EXTENSION UPDATE` does not replay it. See the
[upgrade procedure](upgrade-and-rollback.md). The type/upgrade probe is wired
into full packaged lifecycle CI.

## Local validation

The native ARM64 archive was built from frozen commit
`d50223a8bde09d13c15369f72bbf2bcaaec72b43` plus the filter and packaging changes.
Concurrent demo and later security edits were excluded. This is local development
evidence, not final-commit or release qualification. Exact inputs, archive hashes,
raw evidence locations and exclusions are recorded in
[structured evidence](evidence/2026-11-12-update-key-filter.json).

The packaged SQL passed 94 checks: two reversed passes through 15 scalar types
in one backend, quoted identifiers and payloads, large integers, ordered key
changes, rollback, missing/repeated identities, duplicate destination matches,
nullable/composite identities, enum fallback, INSERT/DELETE, transactional
beta.4 upgrade and same-version refresh. Upgrade/refresh checks preserve rows,
function grants and extension membership, including rollback.

The final archive differs from the archive used for the 94-check run and
throughput trials only in README.md, UPGRADE.md and their checksums. All installed
SQL and binaries are byte-identical.
Repository `make check` also passes on the current working tree, including
generated SQL, the Rust suite and Compose configuration checks.

## Destination throughput

The unchanged 500-row runs were compared against the historical SQL control:
one million rows, ten seed files, deterministic 128-byte payloads, 10,000 distinct
updates in one outer transaction, three trials per configuration. Every trial
checks the complete final dataset. Timings include commit and exclude seeding,
source replication and worker startup.

| Key pattern | Historical SQL | Installed filter | Ratio |
|---|---:|---:|---:|
| Clustered | 353 updates/s | 11,685 updates/s | 33.1x |
| Random | 336 updates/s | 2,938 updates/s | 8.8x |

These are medians from 12 local trials on shared ARM64 hardware with private
fixtures pinned to CPUs 2–3, destination/engine caps of 1.5/2 GiB and a 1-GB
engine limit. No fixture OOM or unexpected destination restart was observed.
The original profile supports reduced foreign-scan row transfer and sorting;
it does not establish additional Iceberg file pruning. These rates do not
predict sustained single-transaction throughput or p95 latency.

## Native validation and remaining work

The native follow-up uses ordinary subscription creation and initial copy of
one million rows. Each pass commits 200 distinct random single-row source
updates, then a progress marker. Timing ends when the marker reaches Iceberg;
the complete final dataset is checked separately.

| Pass | Burst completion rate | Elapsed through marker observation |
|---|---:|---:|
| Historical control | 5.50 updates/s | 36.34 s |
| Installed filter | 23.61 updates/s | 8.47 s |
| Historical control | 5.54 updates/s | 36.10 s |

The installed filter is about 4.3x faster than both controls in this bracket.
It also passes supervisor SIGKILL/replacement, a distinct committed source
write after replacement and another full exact-data check. Replacement plus
resumed write took 2.16 seconds. All 32 native/correctness checks passed, with
no observed OOM or unexpected destination restart and successful fixture cleanup.
This is burst catch-up evidence, not sustained capacity or a latency percentile.

An initial historical-control pass applied all source updates correctly but
failed active-subscription DROP cleanup: the source slot remained after DROP.
This happened before enabling the filter. The failed run is retained, and a
passing follow-up explicitly disables each subscription and waits for its slot
to be inactive before DROP. It cannot qualify active-subscription DROP.
The later [source-slot cleanup regression](source-slot-cleanup-results.md)
verifies the committed C retry fix against the preserved failing archive and
adds a forced busy-slot test. That evidence separately covers active DROP.

The new SQL requires fresh final-commit archives and native amd64/arm64 CI
qualification. The deferred 24-hour soak remains open. Larger run sizes and
further batching/polling changes should be separate experiments.

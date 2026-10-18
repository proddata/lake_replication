# Basic replication performance probe

Start the development lab, then run `make performance-test`. The probe builds
release worker binaries, creates a temporary destination database, replicates
1,000 inserts and 200 updates by default, checks destination counts, and drops
its tables, slot, subscription, and database. It writes JSON evidence under
`.lab/perf_*/results.json`. Adjust the workload with, for example:

```sh
python3 tests/performance/probe.py --insert-rows 5000 --update-rows 500 \
  --rows-per-transaction 250 --batch-rows 1000 --payload-bytes 128
```

For a longer insert run, set `--update-rows 0`; for example:

```sh
python3 tests/performance/probe.py --insert-rows 1000000 --update-rows 0 \
  --rows-per-transaction 5000 --batch-rows 5000
```

Each phase reports source write rate, elapsed rate from the first source write
until the receiver stages all transactions, and Iceberg publication rate.
Source writes and staging run concurrently; publication starts after staging
finishes. The latter is the destination's catch-up capacity for this workload,
not a sustained end-to-end rate with all stages running at once. Source write
timing includes one `docker compose exec` and one `psql` process per phase.
The source writes use `generate_series` and bulk SQL, so their rate does not
represent typical application writes.

Memory evidence includes sampled peak RSS for the host-side Rust worker and
sampled peak memory for the PostgreSQL and pgduck containers, plus baselines.
The container figures include shared memory, caches, and other sessions.
Docker stats samples every few seconds, so short peaks can be missed. For a
memory stress run, increase `--rows-per-transaction` and `--payload-bytes`.
Use `--skip-memory-sampling` to check whether sampling affects throughput on
your host; memory fields will then be null.

The set-based publisher is installed by migration `0020_set_based_apply.sql`.
It groups consecutive inserts and independent updates to the same destination,
up to 500 rows or 4 MiB per run. Deletes remain row-wise on the pinned pg_lake
revision. Source-order dependencies, including repeated updates to one key,
start a new run. Updates verify one affected destination row per source change.

This is a diagnostic workload, not a performance gate or a claim about all
PostgreSQL deployments. Run it on target hardware with realistic row sizes,
transaction sizes, concurrent writers, and Iceberg storage before setting
capacity targets. Updates use a set-based statement, but their cost may rise
sharply as the Iceberg table grows.

## Local Docker observations (2026-09-23)

With 128-byte values and release workers, both the source and destination row
counts matched:

| Workload | Publisher rate | Publisher elapsed | Sampled container peaks |
| --- | ---: | ---: | --- |
| 100,000 inserts, 1,000 rows/transaction and batch | 8,126 rows/s | 12.3 s | PostgreSQL 238 MiB; pgduck 156 MiB |
| 10,000 updates on that 100,000-row table | 2,717 rows/s | 3.7 s | PostgreSQL 245 MiB; pgduck 141 MiB |
| 1,000,000 inserts, 5,000 rows/transaction and batch | 11,199 rows/s | 89.3 s | PostgreSQL 243 MiB; pgduck 153 MiB |
| 1,000 updates on the 1,000,000-row table | 316 rows/s | 3.2 s | PostgreSQL 251 MiB; pgduck 130 MiB |

The insert workloads use different transaction and batch sizes, so their rates
are not a controlled scaling comparison. Before the non-null identity join fix,
the same update workloads took 136.5 s and 131.8 s respectively. A direct
500-row update of a million-row Iceberg table took 1.1 s using ordinary equality,
but 68.7 s using the publisher's old `IS NOT DISTINCT FROM` predicate. The
publisher now uses equality when every value of an identity key in a run is
non-null, while retaining null-safe matching for nullable identities. Update
throughput still falls as the table grows and needs evaluation with realistic
write patterns and file layouts.

## Destination insert path comparison (2026-09-24)

`write_path_compare.py` compares two destination-only insert paths on the same
local pg_lake build: the publisher's journal-shaped `jsonb_populate_record`
`INSERT` and pg_lake's `COPY FROM STDIN`. It runs the same number of statements
and verifies both destination row counts. It does not include WAL receipt,
journal staging, updates, deletes, or a direct call to pg_lake's internal C
file-modification API. The two inputs have different serialization costs, so
the result is a screening test rather than an end-to-end publisher A/B test.

```sh
python3 tests/performance/write_path_compare.py --rows 100000 --batch-rows 1000
python3 tests/performance/write_path_compare.py --rows 100000 --batch-rows 1000 --copy-first
```

With 128-byte values and 100,000 rows in 100 writes, SQL took 8.151 seconds
and COPY 7.983 seconds when SQL ran first. With order reversed, SQL took 7.897
seconds and COPY 8.070 seconds. The difference is smaller than the variation
between runs; this probe provides no evidence that switching insert publication
to COPY would materially improve throughput. Raw evidence is in
`.lab/write_paths_20260924_061723/results.json` and
`.lab/write_paths_20260924_061817/results.json`.

The remaining deeper-path candidate is a keyed bulk mutation API in pg_lake.
The current exported C helpers accept file/row-position modifications; they
do not provide a stable entry point that accepts source replica identities.
Implementing and benchmarking such an API requires a pg_lake change or a
version-pinned experimental C shim, followed by crash and compatibility tests.

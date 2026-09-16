# Milestone 25: Iceberg storage maintenance validation

Status: this project does not implement Iceberg-side compaction, snapshot
expiration, or orphan-file removal itself. The pinned `pg_lake` revision
already provides those as native `VACUUM` support plus a background
autovacuum worker, both enabled by default for every Iceberg table this
project creates. This milestone validates that coverage against destination
tables and documents the primitives so the boundary noted at the end of
[Milestone 24](docs/milestone-24.md) is closed by evidence, not assumption.

## What pg_lake provides

Read directly from the pinned `pg_lake` source
(`8180647d4535fdb1ed223f92f301ba6094a8ec3e`, `pg_lake_table/src/ddl/vacuum.c`
and `docs/iceberg-tables.md`):

- `VACUUM <table>` on an Iceberg table, or `VACUUM (ICEBERG)` for every
  Iceberg table on the server, runs four stages per table: compact/rewrite
  data files, expire snapshots older than the retention policy and merge
  manifests, drain the deletion queue of superseded files, and remove
  abandoned in-progress files.
- A background worker runs the same cycle automatically every
  `pg_lake_iceberg.autovacuum_naptime` (default 10 minutes), governed by
  `pg_lake_iceberg.autovacuum_enabled` (server-wide) and the per-table foreign
  table options `autovacuum_enabled` and `autovacuum_compact_data_files`
  (both default `true`).
- Snapshot retention is governed by the `pg_lake_iceberg.max_snapshot_age` GUC
  (default 1800 seconds), overridable per table with the `max_snapshot_age`
  option; setting it to `0` expires old snapshots immediately on every write,
  without a separate `VACUUM`.
- Dropping a table (or superseding its files through normal writes) queues
  the old files in a deletion queue rather than deleting them immediately.
  `VACUUM (ICEBERG)` only removes queue entries older than a fixed 10-day
  retention window, preserving the ability to reconstruct a prior table state
  from an old, dereferenced `metadata.json` for that long.

None of this is configured by this project's destination-table creation path
(`lake_replication.ensure_iceberg_destination`); it applies automatically
because the defaults are already on.

## Evidence

Run `make iceberg-maintenance-test` against the main laboratory. The probe
covers, entirely through `VACUUM` and the `iceberg_tables` /
`lake_iceberg.metadata()` / `lake_iceberg.files()` / `lake_engine.deletion_queue`
introspection also used by the Milestone 0 and crash-window probes:

- a destination table created with no explicit options amplifies five
  separate single-row commits into five data files, matching the Milestone 0
  file-amplification finding;
- manual `VACUUM` compacts those five files into one; the compaction rewrite
  itself commits new snapshots (observed 5 to 7), so a table well within its
  `max_snapshot_age` window grows its snapshot count from vacuuming even as
  its file count shrinks — only later expiry (via the age policy or a
  `max_snapshot_age = 0` override) prunes that history;
- a table created with the per-table `max_snapshot_age = 0` option keeps only
  its latest snapshot after each write, with no separate `VACUUM`;
- a table created with `autovacuum_enabled = false` records that override on
  the foreign table, so the background worker will skip it while a manual
  `VACUUM` still compacts it; and
- a dropped table's files enter the deletion queue and survive
  `VACUUM (ICEBERG)`, since they are younger than the 10-day retention
  window.

`lake_engine.deletion_queue.table_name` is a `regclass` column that
`DROP TABLE` clears to `NULL`, since the relation it would reference is
already gone in the same transaction. A dropped table's queue entries are
only identifiable by object-store `path`, not by the table's former oid or
name; any future per-table storage observability view needs to account for
this rather than joining on `table_name`.

Run `icebergvac_20260916_190130` passed all 8 assertions; its retained result
is [recorded here](evidence/2026-09-16-iceberg-maintenance-results.json).

## Remaining boundary

This milestone exercises `pg_lake`'s maintenance primitives directly with
manual `VACUUM`; it does not wait out the real autovacuum naptime or GUC
default retention windows end-to-end, since doing so would make the lab
prohibitively slow. It also does not evaluate interactions with the
destination-side journal retention added in Milestone 24 under sustained,
concurrent production load, or verify behavior against a `pg_lake` revision
other than the one currently pinned.

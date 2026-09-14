# Milestone 9: publication discovery and coordinated table cutover

Status: one initial-sync worker can discover every effective table in a
publication, honor its column list and row filter, and atomically transition all
tables from their prior Iceberg state to one shared source snapshot.

## Discovery

With `LAKE_DISCOVER_PUBLICATION=1`, the worker reads
`pg_catalog.pg_publication_tables` inside the imported exported-snapshot
transaction. This view is PostgreSQL's effective publication surface: it
expands `FOR ALL TABLES` and `FOR TABLES IN SCHEMA` publications and supplies
the selected `attnames` and `rowfilter` for each table.

Catalog column metadata is restricted to the published columns and ordered by
source attribute number, matching pgoutput relation messages. Initial COPY
constructs each JSON row from those selected names and applies the catalog row
filter. Destinations are resolved as tables with the same relation name under
`LAKE_DESTINATION_SCHEMA`.

## Coordinated cutover

All discovered tables import the same exported snapshot and record the same
slot `consistent_point`. `finish_initial_sync_group` locks the subscription
checkpoint and validates that:

- the expected number of tables is present and still copying;
- every table uses the same consistent LSN;
- every table has a distinct destination;
- all staged row ordinals are complete; and
- non-empty destinations have explicit overwrite permission.

It then replaces every Iceberg destination, marks every relation and copy
ready, and initializes the subscription checkpoint in one destination
transaction. A validation or write failure rolls back the entire group.

## Evidence

Run `multi_20260914_163430` passed all 11 assertions. The retained
[results](evidence/2026-09-14-multi-table-results.json) cover two tables under
one snapshot at `0/5CB9858`:

- an accounts table publishing only `id` and `note`, filtered by
  `tenant_id = 1`;
- an unfiltered events table;
- both previous Iceberg tables remaining readable before cutover;
- two ready copies and one initialized checkpoint boundary;
- no unpublished `tenant_id` or `secret` metadata at the destination;
- a concurrent source transaction containing three inserts, reduced by
  pgoutput to two eligible changes while retaining one journal transaction;
- final Iceberg rows exactly matching the publication's effective filter and
  projection.

This follows PostgreSQL's documented
[effective publication table view](https://www.postgresql.org/docs/18/view-pg-publication-tables.html),
[row-filter initial-copy semantics](https://www.postgresql.org/docs/18/logical-replication-row-filter.html#LOGICAL-REPLICATION-ROW-FILTER-INITIAL-DATA-SYNC),
and [column-list initial-copy semantics](https://www.postgresql.org/docs/18/logical-replication-col-lists.html).

## Remaining boundaries

- Discovery accepts one publication name. Combining several publications,
  including OR-combined filters and column-list compatibility checks, remains
  pending.
- Destination tables must already exist with exactly the published column names.
  Automatic Iceberg DDL and versioned cross-cluster type validation are pending.
- Tables are copied sequentially under one snapshot. Parallel COPY workers would
  require safely sharing the exported snapshot while retaining bounded staging.
- Publication DDL changes after discovery are detected later as a new pgoutput
  relation version; an explicit refresh/rebuild policy is not implemented.
- Partition-root behavior is exposed by PostgreSQL's effective view but has not
  yet been exercised in the integration lab.

# Spike: could native `CREATE SUBSCRIPTION` ever target Iceberg?

Status: this is an architecture spike, not part of the shipped replication
path. It investigates why PostgreSQL's own logical replication cannot target
`pg_lake` Iceberg tables today, and whether that is fixable without patching
PostgreSQL itself. It directly informs, but does not decide, whether a
future rewrite of this project onto a native `CREATE SUBSCRIPTION` design is
viable.

## The question

The [README](../README.md) records that "PostgreSQL 18 rejects native
Iceberg subscription targets with SQLSTATE `42809`" -- the original reason
this project builds its own external receiver, journal, and publisher
instead of using `CREATE SUBSCRIPTION`. Crunchy Data Warehouse (the
predecessor of the `pg_lake` project this repository depends on, before
Snowflake acquired Crunchy and open-sourced it) publicly ships exactly this:
`CREATE SUBSCRIPTION sub CONNECTION '...' PUBLICATION pub WITH
(create_tables_using = 'iceberg')`. Does closing that gap require a patched
PostgreSQL core, or is it reachable as an ordinary extension?

## What the source shows

Two independent PostgreSQL mechanisms are relevant, read directly from
`postgres/postgres` and the pinned `pg_lake` commit
(`8180647d4535fdb1ed223f92f301ba6094a8ec3e`):

- `CREATE SUBSCRIPTION ... WITH (...)` parses its options through the same
  generic, unrestricted `opt_definition` grammar production as every other
  PostgreSQL DDL `WITH` clause (`gram.y:11045`). Option *names* are validated
  later, in plain C code (`parse_subscription_options`), which a
  `ProcessUtility_hook` runs before -- the exact trick `pg_lake` already
  uses for `VACUUM (ICEBERG)` (`ProcessVacuumPgLakeIcebergFlag` in
  `pg_lake_table/src/ddl/vacuum.c`). Adding a custom `create_tables_using`
  option needs no core patch.
- The actual blocker is `CheckSubscriptionRelkind`
  (`src/backend/executor/execReplication.c:1140`), called from the apply and
  tablesync workers on every relation open:
  ```c
  if (localrelkind != RELKIND_RELATION &&
      localrelkind != RELKIND_PARTITIONED_TABLE &&
      localrelkind != RELKIND_SEQUENCE)
      ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), ...));
  ```
  This is a plain internal function call, not a hook -- unpatchable by any
  extension. It allows any `relkind='r'` table regardless of which access
  method it uses.
- `pg_lake` registers a real Table Access Method named `iceberg`
  (`CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER
  lake_iceberg.pg_lake_iceberg_am_handler;`), but its own comment calls it
  "a placeholder for internal usage." A DDL hook silently builds a real
  **foreign table** (`relkind='f'`) underneath instead of implementing real
  TAM callbacks. That `relkind='f'` is what `CheckSubscriptionRelkind`
  rejects -- confirmed independently by this project's own probes
  (`tests/auto_schema/probe.py` reads back `relkind='f'`,
  `srvname='pg_lake_iceberg'` for a `USING iceberg` table).

So the rejection is specific to `pg_lake`'s placeholder implementation, not
to Iceberg storage or to logical replication in general. `create_tables_using`
does not appear anywhere in the pinned `pg_lake` commit or the project's
current `main` branch (checked by code search); whatever Crunchy's hosted
product does was not included in what Snowflake open-sourced.

## The experiment

`make native-subscription-spike-test` proves the hypothesis directly,
without writing or compiling any C code: PostgreSQL's own `create_am`
regression test registers the **built-in, already-compiled** heap handler
under a second access method name --
`CREATE ACCESS METHOD heap2 TYPE TABLE HANDLER heap_tableam_handler;` -- and
this probe reuses exactly that trick. A table `USING heap2` is a genuine
`relkind='r'` table using a non-default, explicitly-named access method,
which is the narrowest real stand-in for "a complete Iceberg access method,"
without building one.

The probe:

1. creates an ordinary heap source table and publication;
2. creates a `heap2`-backed destination table;
3. creates the replication slot as its own step (see below), then
   `CREATE SUBSCRIPTION ... WITH (create_slot = false, slot_name = '...')`
   targeting the `heap2` table;
4. confirms the subscription reaches `srsubstate = 'r'` (streaming) with no
   `42809`;
5. inserts, then updates and deletes, rows on the source and confirms the
   real logical-replication apply worker streams all three into the
   `heap2`-backed table correctly; and
6. confirms the destination relation is still `relkind='r'`/`amname='heap2'`
   throughout -- nothing silently substituted a foreign table.

All 5 checks passed. Run `natsub_20260917_041052` is
[recorded here](evidence/2026-09-17-native-subscription-spike-results.json).

### An unrelated deadlock found along the way

`CREATE SUBSCRIPTION`'s default `create_slot = true` **deadlocks** when the
publisher and subscriber are two databases on the same PostgreSQL cluster (as
this lab's `application`/`analytics` databases are): the calling backend
holds an open transaction while it opens a second connection back into the
same cluster to negotiate the new slot's synchronized snapshot, which then
blocks on that same transaction's own `transactionid`. The workaround --
create the slot as an independent step with `pg_create_logical_replication_slot`,
then `CREATE SUBSCRIPTION ... WITH (create_slot = false, slot_name = '...')`
-- is the same two-step pattern this project's own Rust receiver already
uses, for what looks like a related reason. This is a same-cluster lab
artifact, not something a production deployment (separate source and
destination clusters) would hit, but it is worth remembering if this lab
topology is ever reused for subscription-based experiments.

## What this settles, and what it does not

Settled: a *complete* table access method -- unlike `pg_lake`'s placeholder
-- would satisfy every check native `CREATE SUBSCRIPTION` performs, with no
PostgreSQL core changes. The `create_tables_using`-style convenience is also
implementable purely as an extension, via the same `ProcessUtility_hook`
pattern `pg_lake` already uses for `VACUUM (ICEBERG)`.

Not settled: whether implementing a real Iceberg table access method --
correct MVCC visibility, all the TAM callback paths, concurrent access, not
just routing single-row inserts into Parquet files -- is a smaller or larger
effort than this project's current external-worker design, which already
has 25 milestones of correctness and HA validation behind it. That is the
next question, not answered here.

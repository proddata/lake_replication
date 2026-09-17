# Spike: could native `CREATE SUBSCRIPTION` ever target Iceberg?

Status: open, redirected. This is an architecture spike, not part of the
shipped replication path. It investigates why PostgreSQL's own logical
replication cannot target `pg_lake` Iceberg tables today, whether that is
fixable without patching PostgreSQL itself, and what is actually worth
building toward. A from-scratch Iceberg table access method is rejected (see
"The real obstacle a TAM would face" and "Is a TAM actually needed?" below).
But public evidence about how Crunchy Data Warehouse's own
`CREATE SUBSCRIPTION ... WITH (create_tables_using = 'iceberg')` behaves
(see "What Crunchy's own public docs suggest they actually built") points at
a different, much cheaper design -- a `CREATE SUBSCRIPTION`-shaped syntax
wrapper over a custom batching writer, structurally close to what this
project already has -- which this project has since prototyped successfully
against the live lab (see "A working prototype of the syntax-sugar route").
That is the live direction now.

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

Not settled: whether implementing a real Iceberg table access method is a
smaller or larger effort than this project's current external-worker
design, which already has 25 milestones of correctness and HA validation
behind it.

## The real obstacle a TAM would face: row identity vs. compaction

Reading the full `TableAmRoutine` struct (`src/include/access/tableam.h`)
narrows the *number* of callbacks a replication-only Iceberg TAM would need,
but surfaces a harder problem than "how many callbacks."

The apply worker can locate the row to update/delete in two ways: through a
unique index (needing real btree integration -- `index_scan_*`,
`index_build_range_scan`), or, with `REPLICA IDENTITY FULL`, through a plain
sequential scan comparing whole rows (`scan_begin`/`scan_getnextslot` only).
Since a destination Iceberg table has no natural unique-index concept
anyway, `REPLICA IDENTITY FULL` on both ends avoids the entire index-callback
group. That leaves a real but bounded set: `slot_callbacks`, the sequential
scan callbacks, `tuple_insert`/`multi_insert`, `tuple_update`, `tuple_delete`,
`tuple_lock`, `tuple_fetch_row_version`, `relation_set_new_filelocator`, plus
TOAST support (already solved in the current design, Milestone 6) and
`relation_nontransactional_truncate` (already solved, Milestone 4).

But every one of those write/lock/fetch callbacks identifies its target row
by `ItemPointer tid` -- PostgreSQL's physical row address. The executor's
contract assumes a `tid` obtained from a scan stays valid long enough to act
on it. Iceberg has no equivalent physical identity: a row lives in some
Parquet file at some row offset, and Milestone 25 already proved, against
this exact pinned `pg_lake` revision, that its autovacuum **rewrites and
compacts those files automatically every 10 minutes by default**. A naive
TAM's `tid`s would routinely point at locations compaction has since
rewritten.

This is not a proof of infeasibility -- Crunchy Data Warehouse is a real,
shipped product that does exactly this in production, so it has been solved
by someone. The plausible shape of a solution: make the TAM's `tid` an
opaque logical row identifier the AM controls (backed by its own internal
mapping to current file/offset), and make the AM's own compaction
(`relation_vacuum`) never invalidate a `tid` a live transaction might still
hold -- the same discipline ordinary heap tables already follow (ordinary
autovacuum never relocates a live tuple's `tid`; only `VACUUM FULL`/`CLUSTER`
do, and both already get special-cased in the TAM interface). That is a
real, scoped, *additional* subsystem -- not a blocker, but a materially
bigger prototype than "implement the write callbacks."

The current design never has to solve this, because it never touches
physical row identity: the publisher writes through `pg_lake`'s own SQL/FDW
surface (`INSERT`/`COPY`/predicate-based `DELETE`), and `pg_lake` resolves
row identity internally via Iceberg's own merge-on-read equality-deletes,
not PostgreSQL TIDs.

## Is a TAM actually needed?

No. Nothing in this section is required for this project's stated purpose
-- replicating PostgreSQL tables into Iceberg reliably. The current
external-worker design already achieves that outcome, with 25 milestones of
evidence behind its correctness, durability, and HA. A native-subscription
TAM would not add a capability the project is missing; it would only change
two things: the operator-facing syntax (`CREATE SUBSCRIPTION ... WITH
(create_tables_using = 'iceberg')` instead of this project's own SQL
functions plus deployed Rust binaries), and the deployment footprint (one
fewer external process).

Against that limited upside, a TAM rewrite would need to re-solve, in C,
inside a PostgreSQL backend, several problems this project has already
solved once in Rust/SQL: schema evolution (Milestone 11 -- and note native
logical replication does not replicate DDL at all, so this problem does not
even go away under a TAM design, it just moves), TOAST handling (Milestone
6), mutation semantics (Milestone 4), and HA/failover behavior (Milestones
17-21). On top of re-solving those, it would need to solve the row-identity-
vs-compaction problem above, which the current design was never exposed to.
That is a substantially larger and riskier undertaking than the ergonomic
gain justifies. Recommendation: do not build a real Iceberg table access
method for this.

That recommendation turned out to rest on an assumption worth checking:
that Crunchy Data Warehouse's own `CREATE SUBSCRIPTION` support is backed by
a real TAM in the first place. The next two sections check that assumption
against Crunchy's own public documentation, and find it's probably wrong.

## What Crunchy's own public docs suggest they actually built

Their [replication docs](https://docs.crunchybridge.com/warehouse/replication)
describe monitoring through two custom views, not the standard PostgreSQL
ones:

- `crunchy_iceberg.stat_subscription` -- `last_flush_time`,
  `last_flush_duration`, `last_flush_lsn`
- `crunchy_iceberg.stat_subscription_rel` -- `unflushed_inserts`,
  `unflushed_deletes`, `rows_inserted`, `rows_deleted`

If the destination were a real TAM being written by PostgreSQL's actual
built-in apply worker, the standard `pg_stat_subscription`/
`pg_stat_subscription_rel` views (which already report receive/flush LSNs
per subscription and per relation) would already cover this -- there would
be no reason to invent a parallel `crunchy_iceberg` schema. The vocabulary
is "flush," not "apply," and `unflushed_inserts`/`unflushed_deletes` are
counts of buffered work, not transaction-apply state. That is durable-
journal-plus-batch-flush language, not per-tuple-apply-worker language.

The documented apply cadence confirms it: "The apply worker applies the
changes... to the Iceberg tables in **batches every 30 seconds**, which
leads to an end-to-end lag which is typically under 60 seconds." PostgreSQL's
real logical replication apply worker applies each transaction as it
streams in; there is no 30-second batch timer in the native path. A fixed
flush cadence is a custom micro-batcher.

And their own docs recommend the *exact* same-cluster workaround this
project's spike independently hit above: creating the slot manually with
`pg_create_logical_replication_slot('source', 'pgoutput')` and passing
`create_slot = false` to `CREATE SUBSCRIPTION`, "for same-server
replication." That is not a coincidence -- it implies their subscription
path opens the same kind of nested connection back into the same cluster
ours did, consistent with a real slot and a real streaming connection for
*receiving* WAL, decoupled from a custom worker that *writes* into Iceberg
on its own schedule.

The best-supported read: `CREATE SUBSCRIPTION ... WITH
(create_tables_using = 'iceberg')` likely does create a real `pg_subscription`
row and a real slot receiving real WAL (so `ALTER`/`DROP SUBSCRIPTION` work
as documented), but the actual Iceberg-writing side is a custom batching
worker with its own tracking schema -- not PostgreSQL's built-in per-tuple
apply worker hitting a real table access method. That is structurally much
closer to this project's own receiver-to-journal-to-micro-batch-publisher
split than to a from-scratch Iceberg TAM. This cannot be fully confirmed
without Crunchy's source (not public); it is the best-supported reading of
their own documentation, not a proven fact.

## A working prototype of the syntax-sugar route

If the above reading is right, the achievable target is not a TAM -- it is
`CREATE SUBSCRIPTION`-shaped syntax as a thin wrapper over this project's
existing control plane, using the same `ProcessUtility_hook` trick `pg_lake`
already uses for `VACUUM (ICEBERG)` (see "What the source shows" above).
This was built and run against the live lab as
[`extensions/lake_subscription_sugar/lake_subscription_sugar.c`](../extensions/lake_subscription_sugar/lake_subscription_sugar.c)
(compiled and `LOAD`ed with no image rebuild) plus
[`extensions/lake_subscription_sugar/monitoring_views.sql`](../extensions/lake_subscription_sugar/monitoring_views.sql).
Run `make subscription-sugar-test` to reproduce it; run
`subsug_20260917_050818` passed all 11 assertions, recorded
[here](evidence/2026-09-17-subscription-sugar-results.json).

```sql
LOAD 'lake_subscription_sugar';
CREATE SUBSCRIPTION sugar_sub
  CONNECTION 'host=127.0.0.1 port=5432 dbname=application user=postgres'
  PUBLICATION sugar_pub
  WITH (lake_target = 'iceberg');
-- CREATE SUBSCRIPTION
```

The hook detects the `lake_target = 'iceberg'` option on the parsed
`CreateSubscriptionStmt` and, if present, never calls
`standard_ProcessUtility`. `ALTER`/`DROP SUBSCRIPTION` are intercepted the
same way, but only when `subname` matches a subscription this hook already
created (looked up by name in `lake_replication.subscriptions`); anything
else falls through untouched, so ordinary native subscriptions keep
working alongside these. Checks against the running lab confirmed:

- `lake_replication.subscriptions` gained a real row, routed through this
  project's own control plane, with **the real source identity** -- this
  is no longer a stub. The hook connects to the statement's own `conninfo`
  via `dblink` and reads `system_identifier`/`database_oid`/`timeline` from
  `pg_control_system()`/`pg_control_checkpoint()` on the actual source,
  exactly like every other probe in this project. The discovered values
  matched the source's real identity exactly in the probe run.
- `pg_subscription` and `pg_replication_slots` stayed empty throughout --
  PostgreSQL's real subscription/slot/apply-worker machinery never ran.
- `ALTER SUBSCRIPTION ... DISABLE` is routed to
  `lake_replication.request_service_stop()`; other `ALTER` kinds are
  accepted and logged as an intentional no-op rather than erroring, since
  there is no real `pg_subscription` row for core to act on.
  `ALTER SUBSCRIPTION ... ENABLE` cannot actually start anything -- there
  is no running process to hand off to from inside a DDL hook; an operator
  still has to run `lake-replication-supervisor` pointed at the
  subscription id.
- `DROP SUBSCRIPTION` tears down this project's own control-plane rows
  (transactions, changes, batches, relations, checkpoints, the
  subscription itself) instead of erroring for lack of a real
  `pg_subscription` row to drop.
- The `crunchy_iceberg.stat_subscription`/`stat_subscription_rel` views
  (mirroring the field names from Crunchy's own docs) correctly tracked a
  staged-but-unpublished insert as `unflushed_inserts = 1`, then reported
  it as flushed with a real `last_flush_time`/`last_flush_duration`/
  `last_flush_lsn` once the batch was published -- built entirely over
  existing `lake_replication.batches`/`transactions`/`changes` data, no new
  instrumentation. Deliberately not implemented: Crunchy's cumulative
  `rows_inserted`/`rows_deleted` counters, since this project does not keep
  a running total once Milestone 24 maintenance reclaims journal rows.

Decision made along the way, worth recording: rather than trying to fake a
`pg_subscription` catalog row for `ALTER`/`DROP` to act on (risky --
hand-inserting into a system catalog bypasses cache invalidation and other
invariants core relies on), `ALTER`/`DROP SUBSCRIPTION` are intercepted the
same way `CREATE` is, matched by name against this project's own metadata
instead of a real catalog row.

## Where this leaves the project

Do not build a real Iceberg table access method (see "Is a TAM actually
needed?"). The `CREATE SUBSCRIPTION`-shaped syntax-wrapper route is no
longer just a cheap idea -- it is a working prototype covering create,
disable, drop, and Crunchy-shaped monitoring, all proven against the live
lab and all still just a `ProcessUtility_hook` extension on stock
PostgreSQL, no core patch, no TAM.

What is still deliberately unfinished, since this remains a prototype, not
shipped code:

- the extension only claims a statement when its `lake_target`/matching-
  name conditions are met; it has not been reviewed for every
  `CreateSubscriptionStmt`/`AlterSubscriptionStmt`/`DropSubscriptionStmt`
  shape PostgreSQL's grammar allows (multiple publications, `SERVER`-form
  `CREATE SUBSCRIPTION`, etc.);
- `ALTER SUBSCRIPTION ... ENABLE` only records intent; nothing here starts
  `lake-replication-supervisor` for the operator;
- the monitoring views omit cumulative row counters, as noted above; and
- none of this has been connected to the real receiver/publisher binaries
  -- creating a subscription this way registers control-plane state, but
  an operator still has to run the supervisor against that subscription id
  exactly as before.

Promoting this from prototype to something operators could actually rely
on would mean deciding how far to take it: keep it as an optional,
separately-installed convenience layer over the existing SQL-function
control plane (low risk, purely additive), or make it the documented,
primary way to configure a subscription (bigger commitment: it would need
the grammar-shape review above, and a real answer for `ENABLE`).

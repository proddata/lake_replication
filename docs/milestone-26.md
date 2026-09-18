# Milestone 26: REST-catalog commit-ambiguity root cause

Status: root cause confirmed by reading the pinned `pg_lake` source and
reproducing the failure live against a real, self-hosted Iceberg REST
catalog. This is not a crash-injection lab with durable evidence artifacts
like Milestones 0/1 -- it is the targeted spike [the beta readiness
plan](beta-readiness.md) called for to decide whether REST-catalog support
belongs in beta at all. Decision: it does not. REST-catalog destinations are
tracked there as a known, deliberately deferred defect, not scheduled for
beta; the read-back-verification fix outlined below is the known shape of
the eventual fix, to pick up later, not near-term work.

## Background

The feasibility report flagged, without confirming, that REST-catalog
commits might not share the local catalog's atomicity: "Optional REST
catalog requests run in the commit callback, and HTTP failures can be
reported as warnings... A local heap checkpoint therefore cannot by itself
prove successful REST-catalog publication." Milestones 1 and 3 built this
project's recovery protocol against `pg_lake`'s **local** catalog
specifically, where catalog changes are part of the same PostgreSQL
transaction as the write. Nothing in this project has yet exercised a real
REST catalog.

## What a REST-catalog destination requires

Reading `pg_lake_iceberg/pg_lake_iceberg--3.3--3.4.sql` (revision
`8180647d4535fdb1ed223f92f301ba6094a8ec3e`, same pin the lab uses):
`CREATE SERVER <name> TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog
OPTIONS (rest_endpoint '...')`, then `CREATE USER MAPPING ... OPTIONS
(client_id '...', client_secret '...')`, then `CREATE TABLE ... USING
iceberg WITH (catalog = '<name>')`. Credentials are OAuth2 client-credentials
only for a user-created server (`rest_auth_type = 'horizon'` is refused
outside the built-in catalog); `rest_catalog_auth.c` hard-requires an
`expires_in` field in the token response
(`rest_catalog_auth.c:455-458`) or every catalog call fails before it
starts. For a table `pg_lake` creates (as opposed to attaching read-only),
`GetRestCatalogName()` always uses the database name as the REST API
catalog/prefix segment (`rest_catalog_ops.c:1480`, "return
get_database_name(MyDatabaseId)") -- not configurable, so the REST catalog
must support that name as a routable prefix.

## Reproducing a real REST catalog

The Apache Iceberg project's own reference REST catalog server
(`tabulario/iceberg-rest`, JDBC/SQLite-backed, `S3FileIO` pointed at this
lab's MinIO) implements the protocol but not those two specifics: its
`/v1/oauth/tokens` stub omits `expires_in`, and it is single-tenant (no
catalog-name path prefix). Both were worked around with a small,
lab-only Node.js reverse-proxy shim (not part of this project's shipped
path) that (1) injects a default `expires_in` into token responses and (2)
strips the leading `/v1/<name>/...` prefix segment before forwarding, since
the fixture's own routes are unprefixed. With that shim in front of the
fixture, `CREATE SERVER` / `CREATE USER MAPPING` / `CREATE TABLE ... USING
iceberg WITH (catalog = ...)` / `INSERT` / `SELECT` all worked end to end
against a real, independent REST catalog process over real HTTP.

A production REST catalog (Polaris, Unity Catalog OSS, a managed Glue/Nessie
endpoint, etc.) is expected to need neither workaround: multi-tenant catalog
prefixes and spec-complete OAuth2 responses are exactly what those exist to
provide. The workarounds are an artifact of using the lightweight
single-tenant reference fixture as a lab stand-in, not a finding about REST
catalogs generally.

## Root cause: the commit-callback POST is unconditionally best-effort, with no failure record

Confirmed by reading `pg_lake_table/src/transaction/transaction_hooks.c` and
`pg_lake_table/src/transaction/track_iceberg_metadata_changes.c`:

1. `IcebergXactCallback` calls `PostAllRestCatalogRequests()` on
   `XACT_EVENT_COMMIT` / `XACT_EVENT_PARALLEL_COMMIT`
   (`transaction_hooks.c:81-85`) -- PostgreSQL's commit-callback phase, which
   runs **after** the local transaction's commit record is already durable.
   This is the point of no return: nothing here can abort the local commit
   that already happened.
2. Inside `PostAllRestCatalogRequests()`, every REST call -- the per-table
   create/drop request expecting HTTP 200/204, and the single batched
   "transaction commit" request expecting HTTP 204
   (`track_iceberg_metadata_changes.c:303-317`, `:325-333`, `:454-458`) --
   is checked only well enough to log a warning:
   ```c
   if (httpResult.status != 204)
   {
       ReportHTTPError(httpResult, WARNING);
   }
   ```
   `ReportHTTPError` at `WARNING` severity never raises, aborts, or retries
   anything (`rest_catalog_http.c:248-268`). One call site's own comment
   admits the gap is unresolved: `/* Ouch, something failed. Should we stop
   sending the requests? */` (`track_iceberg_metadata_changes.c:314-316`).
3. Immediately afterward, `IcebergXactCallback` calls
   `ResetRestCatalogRequests()` (`transaction_hooks.c:84`) unconditionally --
   success or failure, the tracked request set for this transaction is
   discarded. There is no retry queue, no dead-letter table, no durable
   record anywhere in `pg_lake` that this catalog update never landed.

**Net effect:** on any transient HTTP failure (timeout, 5xx, connection
reset, or a response lost in transit) while posting to a REST catalog,
`pg_lake`'s local PostgreSQL transaction has already committed -- data files
are already written to storage, and the table's local state says success --
but the REST catalog's `metadata.json` pointer is never updated to reflect
it, and nothing durable anywhere records that the update was attempted, let
alone that it failed. This is not "ambiguous, needs reconciliation logic on
our side to figure out which case happened" (the local-catalog framing).
It is silent, undetectable-by-us-after-the-fact divergence between what
PostgreSQL believes committed and what the REST catalog actually has,
produced entirely inside `pg_lake`, before this project's own worker ever
sees a WAL record for the change.

Reproduced live: a plain `INSERT` into a REST-catalog-backed table (through
the shim, not injecting any deliberate failure) printed `WARNING: HTTP
request failed (HTTP 200)` from exactly this code path -- meaning even
routine traffic through this stack hits a case the batched-commit call
doesn't accept as success, and the transaction commits anyway with no
further signal beyond that log line.

## Consequence for this project's recovery protocol

Milestone 1's guarantee -- "staging must become durable before replication
feedback acknowledges its WAL... published progress should be committed in
the same destination PostgreSQL transaction as the corresponding Iceberg
catalog changes" -- holds for the local catalog specifically because local
catalog changes really are part of that same PostgreSQL transaction. It does
**not** hold for a REST-catalog destination table: `pg_lake`'s own commit
already happened before the REST call is even attempted, and `pg_lake`
gives this project no durable signal, in any table or view, to detect a
failed catalog post after the fact. A checkpoint this project advances past
a REST-catalog-backed micro-batch cannot be trusted to mean the REST catalog
agrees, and nothing in `pg_lake` will ever tell us otherwise later.

Supporting REST-catalog destinations safely therefore needs a verification
step this project owns, not one `pg_lake` provides: after each publish to a
REST-catalog-backed table, read back the REST catalog's current
`metadata_location` for that table (a `GET
.../namespaces/<ns>/tables/<table>` call) and confirm it reflects the write
just made, before treating that batch as durably published. A mismatch
means retrying the write (idempotently, by re-deriving the same Iceberg
commit) or surfacing the subscription as needing manual reconciliation --
neither of which exists today. This is materially more worker-side logic
than the local-catalog path needed, not a config flag.

## Status against the beta readiness plan

This spike answered the question it was meant to answer: the risk the
feasibility report flagged is real, and the shape of the fix is known
(read-back verification owned by this project, not reconciliation logic
reacting to an ambiguous-but-detectable `pg_lake` signal, since no such
signal exists). Given that, [the beta readiness
plan](beta-readiness.md#known-defect-deferred-rest-catalog-destinations)
tracks REST-catalog destinations as a **known, deliberately deferred
defect**: beta ships supporting `pg_lake`'s local catalog only (which,
recall, is already Iceberg-JDBC-catalog-compatible for external readers),
and does not build the crash-injection lab or the read-back-verification
logic this milestone scoped. Both remain accurately described above for
whenever REST-catalog support is picked back up -- a durable crash-injection
lab (`deploy/rest-catalog/` + a probe, matching the Milestone 0 pattern)
exercising the real failure windows against a real REST catalog, on a
self-hosted implementation better suited to it than the reference fixture
used here (Polaris or Lakekeeper, not needing this spike's two lab-only
shims) -- just not before beta.

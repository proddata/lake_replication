# Local PoC / demo stack

A single `docker compose up` that gives you a working end-to-end demo of
this project: a source PostgreSQL database, a pg_lake/Iceberg destination
database, this project's replication workers running continuously, and a
live dashboard and two-pane SQL console to drive both sides.

This is a demo convenience, not a production deployment: trust
authentication, a loopback-only source/destination, no TLS, and the same
development-toolchain PostgreSQL image the milestone lab uses (built from
source, several GB, first build is slow). It does **not** include the
lab's crash-injection patch, so it is safe to keep data in.

## What's running

| Service | Role |
|---|---|
| `postgres` | One PostgreSQL 18 + pg_lake instance with two databases: `application` (source, plain heap tables) and `analytics` (destination, Iceberg via pg_lake) |
| `pgduck` | pg_lake's DuckDB execution engine |
| `storage` / `storage-init` | MinIO object storage for the Iceberg tables, plus its bucket |
| `webui` | Node.js/Express + `pg`, serving the live dashboard and SQL console |

`analytics` also has this project's `lake_replication` extension
loaded, so plain `CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg')`
statements are what starts replication -- they spawn this project's own
`lake-replication-supervisor`/`initial-sync` binaries as real OS processes
inside the `postgres` container and route to its `lake_replication.*`
control plane, instead of native PostgreSQL logical replication (which
cannot target an Iceberg foreign table at all -- see
[`docs/archive/investigations/native-subscription-spike.md`](../../docs/archive/investigations/native-subscription-spike.md)).

A demo table (`application.public.orders`, three seed rows) and its
publication (`orders_pub`) are created automatically. No subscription is
created automatically -- creating it live is the point of the demo.

## Running it

```sh
make poc-build   # first build compiles pg_lake from source; slow, do it once
make poc-up
```

Then open <http://127.0.0.1:8080>.

The default **Demo Mode** tells the replication story in three steps:

1. Click **Connect replication** if no subscription exists. This runs the same
   `CREATE SUBSCRIPTION` statement as the destination SQL preset.
2. Click **Insert order**, then **Update order** or **Delete order**. Both tables
   follow the same tracked IDs: **PostgreSQL 1** (`application`, heap tables) on
   the left and **PostgreSQL 2 (Iceberg)** (`analytics`, pg_lake) on the right.
3. Watch row/cell highlights, pending differences, and the query-visible result.
   Select another source row in the dropdown or click either table to track it.
   Deleted orders remain as crossed-out rows so their disappearance stays visible.

The compact pipeline animates only after observed WAL/checkpoint progress,
verified destination changes, or actual Iceberg snapshot changes. The overview
shows received-to-published WAL distance in bytes, verified changes for this
console session, observed throughput, and the latest Iceberg commit. The snapshot
timeline uses real metadata; unavailable file/record counts appear as dashes.
Iceberg file record counts are not CDC event counts.

Each playground action uses a parameterized transaction against the source and
shows the executed SQL and parameters. Simulator writes feed the same tracker.
Verification compares the returned row version with a successful destination
query. Source/destination reads use one shared window of up to 30 IDs, not an
atomic whole-table consistency check. Intermediate versions superseded before
observation, and versions outside the tracked window, are not counted as verified.

**Timing:** Query visibility is measured from source commit acknowledgement to
its first matching Iceberg query. It includes polling and query time and is an
observed upper bound, not an exact engine latency. WAL staging is timed separately
using the first journal observation; batch publication duration uses journal
timestamps. Retained transaction IDs can link an action to a batch. Exact
batch-to-snapshot links and per-snapshot CDC change counts are not recorded, so
none are inferred. Session counters/history reset when the web UI server restarts.

Use **Inspect Mode** for worker health, heartbeats, all four WAL checkpoints,
replication slots, batch journal, unflushed changes, Iceberg metadata, and the
source/destination SQL consoles. Diagnostics remain independently expandable.
The optional PoC monitoring views supply unflushed-change counts.

**Live** pauses observation; **Refresh** samples once. Polling is faster while
changes are pending or the simulator runs, and pauses when the tab is hidden.
Panel errors stay independent; missing destination tables are expected before
initial sync. The simulator keeps running when observation is paused.

### Simulating continuous changes

After creating the subscription, use **Workload simulator → Start simulation**.
Choose a target rate of 1, 2, 5, or 10 operations per second and a duration of
30 seconds to five minutes. Each five-operation cycle inserts three orders,
updates one, and deletes one; each operation commits separately so replication
can be observed in the live previews, batch history, and snapshot metadata.

The simulator only updates/deletes orders it inserted during the current run.
Seed orders and manually inserted orders remain untouched. Surviving generated
orders remain after stopping, so the resulting replicated data can be inspected.
The panel shows successful row counts and the latest 12 changes; the playground
tracks the latest row versions and measures their destination visibility.

One server-side run is shared across browser tabs. **Stop** prevents new work;
an operation already in progress may finish. Closing the page or pausing **Live**
does not stop writes. Every run stops at its deadline (at most five minutes),
on a database error, or when the web console server exits. Slow operations run
at a lower rate rather than overlapping or accumulating a queue.

To rebuild just the console after UI changes (with the stack already running):

```sh
make poc-ui
```

Console checks: `cd deploy/poc/webui && npm test` (Node.js 20+). Direct Node runs
bind to `127.0.0.1` by default. The container listens internally on all interfaces;
Compose publishes its web port on loopback only.

Direct `psql` access, if you want it:

```sh
psql -h 127.0.0.1 -p 55435 -U postgres -d application
psql -h 127.0.0.1 -p 55435 -U postgres -d analytics
```

## Stopping / cleaning up

```sh
make poc-stop   # stop containers, keep data (pgdata/objects volumes)
make poc-down   # stop and remove containers *and* volumes
```

## Known limitations of this demo

- One publication per `CREATE SUBSCRIPTION`, matching the worker binaries'
  current scope (see `lake_replication.c`).
- `ALTER SUBSCRIPTION ... DISABLE/ENABLE` and `DROP SUBSCRIPTION` work (they
  route to the same control plane), but there is no UI shortcut for them
  yet -- run them as plain SQL on the destination pane.
- The web console runs whatever SQL you type against real superuser
  connections; it is meant to stay bound to `127.0.0.1` only (see
  `compose.yml`) -- do not change that binding or put this behind any kind
  of public access.

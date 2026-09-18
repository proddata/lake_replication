# Local PoC / demo stack

A single `docker compose up` that gives you a working end-to-end demo of
this project: a source PostgreSQL database, a pg_lake/Iceberg destination
database, this project's replication workers running continuously, and a
small two-pane web console to drive both sides.

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
| `webui` | Node.js/Express + `pg`, serving the two-pane SQL console |

`analytics` also has this project's `lake_subscription_sugar` extension
loaded, so plain `CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg')`
statements are what starts replication -- they spawn this project's own
`lake-replication-supervisor`/`initial-sync` binaries as real OS processes
inside the `postgres` container and route to its `lake_replication.*`
control plane, instead of native PostgreSQL logical replication (which
cannot target an Iceberg foreign table at all -- see
[`docs/native-subscription-spike.md`](../../docs/native-subscription-spike.md)).

A demo table (`application.public.orders`, three seed rows) and its
publication (`orders_pub`) are created automatically. No subscription is
created automatically -- creating it live is the point of the demo.

## Running it

```sh
make poc-build   # first build compiles pg_lake from source; slow, do it once
make poc-up
```

Then open <http://127.0.0.1:8080>.

On the **destination** pane, pick "Create the demo subscription" from the
preset dropdown and hit Run. This starts real replication: initial copy of
the three seed rows, then live streaming. Watch the "Replication status"
panel at the bottom pick up the subscription and its state.

On the **source** pane, run the "Insert a new order" or "Update an order"
presets a few times, then switch to the **destination** pane, tick
"auto-run every 2s", and run "View replicated table" -- new/changed rows
appear within a couple of seconds without any manual step.

Other useful presets: "Iceberg files for orders" (destination) shows the
underlying Iceberg snapshot/file inventory pg_lake maintains.

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
  current scope (see `lake_subscription_sugar.c`).
- `ALTER SUBSCRIPTION ... DISABLE/ENABLE` and `DROP SUBSCRIPTION` work (they
  route to the same control plane), but there is no UI shortcut for them
  yet -- run them as plain SQL on the destination pane.
- The web console runs whatever SQL you type against real superuser
  connections; it is meant to stay bound to `127.0.0.1` only (see
  `compose.yml`) -- do not change that binding or put this behind any kind
  of public access.

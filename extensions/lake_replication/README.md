# lake_replication PostgreSQL extension

This extension is the single PostgreSQL integration point for lake replication.
It packages the destination control schema from `migrations/`, the subscription
management hooks, and the background-worker launcher for installation next to
an existing PostgreSQL 18 and pg_lake installation.

Build and install it with the target PostgreSQL's PGXS toolchain:

```sh
make
sudo make install
```

Add `lake_replication` to `shared_preload_libraries`, restart PostgreSQL, and
then install it in the pg_lake-enabled destination database:

```sql
CREATE EXTENSION dblink;
CREATE EXTENSION lake_replication;
```

The generated SQL files are build output. The `0.1.0` development baseline is
frozen at migrations 0001-0015, the `0.1.0-beta.1` base contains migrations
0001-0019, and the explicit `0.1.0--0.1.0-beta.1` path contains migrations
0016-0019. Each numbered migration keeps its transaction wrapper for the
laboratory's legacy installer; the generator validates and removes only that
outer wrapper because PostgreSQL runs extension installation and upgrades in
their own transaction.

Every future release must add a new base version and explicit forward upgrade
scripts from supported versions. Released migration contents and generated
version boundaries must never be edited in place.

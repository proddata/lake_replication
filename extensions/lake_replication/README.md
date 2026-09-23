# lake_replication PostgreSQL extension

This SQL-only extension packages the destination control schema from
`migrations/` for installation next to an existing PostgreSQL 18 and pg_lake
installation.

Build and install it with the target PostgreSQL's PGXS toolchain:

```sh
make
sudo make install
```

Then, in the pg_lake-enabled destination database:

```sql
CREATE EXTENSION lake_replication;
```

The generated SQL files are build output. The `0.1.0` development baseline is
frozen at migrations 0001-0015, the `0.1.0-beta.1` base contains migrations
0001-0017, and the explicit `0.1.0--0.1.0-beta.1` path contains migrations
0016-0017. Each numbered migration keeps its transaction wrapper for the
laboratory's legacy installer; the generator validates and removes only that
outer wrapper because PostgreSQL runs extension installation and upgrades in
their own transaction.

Every future release must add a new base version and explicit forward upgrade
scripts from supported versions. Released migration contents and generated
version boundaries must never be edited in place.

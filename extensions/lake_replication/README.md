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

The generated `lake_replication--0.1.0.sql` is build output. Each numbered
migration keeps its transaction wrapper for the laboratory's legacy installer;
the generator validates and removes only that outer wrapper because PostgreSQL
runs an extension installation in its own transaction.

Future releases must add a new base version and an explicit
`lake_replication--OLD--NEW.sql` upgrade script. Released migration contents
must never be edited in place.

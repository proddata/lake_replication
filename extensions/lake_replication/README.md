# lake_replication PostgreSQL extension

This extension is the single PostgreSQL integration point for lake replication.
It packages the destination control schema from `migrations/`, the subscription
management hooks, and the background-worker launcher for installation next to
an existing Linux PostgreSQL 18 and pg_lake installation.

Build and install it with the target PostgreSQL's PGXS toolchain:

```sh
make
sudo make install
```

Add `pg_extension_base` to `shared_preload_libraries`, restart PostgreSQL, and
then install it in the pg_lake-enabled destination database:

```sql
CREATE EXTENSION dblink;
CREATE EXTENSION lake_replication;
```

The generated SQL files are build output. Version boundaries are explicit:

- `0.1.0`: migrations 0001–0015.
- Frozen `0.1.0-beta.1`: migrations 0001–0021 plus config-dump registration.
- `0.1.0-beta.2`: migrations 0001–0023 plus config-dump registration.
- `beta.1` to `beta.2`: 0022 (empty commits) and 0023 (base lifecycle).

Both development-baseline upgrade paths are shipped. The beta.1 scripts match
the previously qualified archive; an already-fixed beta.1 installation can also
upgrade because migration 0022 replaces functions without changing table shape.
Follow the stopped-server procedure in `docs/upgrade-and-rollback.md`; the new
binary never starts a legacy static launcher. No online handoff or SQL downgrade
is supported. Existing destination bindings require explicit verified adoption;
copied metadata cannot authorize dispatch or deletion of the original source slot.

The migration generator validates/removes each outer transaction wrapper because
PostgreSQL executes extension scripts inside its own transaction. Released version
boundaries must not be edited in place.

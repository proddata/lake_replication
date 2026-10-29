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
- Frozen `0.1.0-beta.2`: migrations 0001–0023 plus config-dump registration.
- `beta.1` to `beta.2`: 0022 (empty commits) and 0023 (base lifecycle).
- Frozen `0.1.0-beta.3`: migrations 0001–0024 plus config-dump registration.
- `beta.2` to `beta.3`: 0024 (one bulk transaction and four streaming transactions). Earlier versions
  upgrade through the preserved beta.2 paths.
- `0.1.0-beta.4`: migrations 0001–0025 plus config-dump registration.
- `beta.3` to `beta.4`: 0025 (staging admission, journal budgets and managed worker fencing).

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

Bulk destination creation, schema changes, initial-copy publication and rebuild
promotion share one instance-wide transaction permit. Streaming publication has
a separate pool of four transaction slots. Initial-copy PostgreSQL staging has a separate instance-wide permit acquired before JSONB conversion. Permits last through commit and use native
PostgreSQL cancellation/rollback cleanup. Administrators can inspect
`lake_replication.bulk_admission` and `lake_replication.stream_admission` for
holders and waiters. `lake_replication.staging_admission` exposes the staging permit,
and `lake_replication.journal_admission` exposes per-subscription journal budgets.
Streaming slots use separate native queues rather than one
global FIFO. This policy does not
bound total memory or arbitrary application engine queries.

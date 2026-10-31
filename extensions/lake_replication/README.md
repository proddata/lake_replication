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
- Frozen `0.1.0-beta.4`: migrations 0001–0025 plus config-dump registration.
- `beta.3` to `beta.4`: 0025 (staging admission, journal budgets and managed worker fencing).
- `0.1.0-beta.5`: migrations 0001–0028 plus the new physical-store dump registration.
- `beta.4` to `beta.5`: 0026–0028 (split checkpoints, chunk staging, initial-copy session admission and privileged search-path hardening).

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

Native initial copies additionally hold one of four instance-wide session leases
on their staging connection before opening source snapshots. Inspect
`lake_replication.initial_copy_admission`; waiters have no assigned slot and can
use any released lease. The default `lake_replication.initial_copy_pause_ms=40`
is captured at copy startup and pauses after staging COMMIT. These settings
bound admitted sessions, not total CPU, RSS or remote snapshot retention.

The logical checkpoint and row interfaces remain writable, while their physical
storage changes. Read [migration contracts](../../docs/checkpoint-chunk-migration-results.md)
before upgrade or logical restore; validate restored runtime state before
resuming subscriptions.

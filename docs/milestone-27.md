# Milestone 27: harden the subscription control surface

Status: the three concrete gaps [the beta readiness plan](beta-readiness.md)
listed under "harden the subscription control surface" -- authorization,
credential handling, and hardcoded worker log paths -- are addressed for
`lake_subscription_sugar`. Process supervision (running workers under a
real supervisor like systemd instead of a raw double-fork) is intentionally
not part of this milestone; see the note at the end.

## What changed

`extensions/lake_subscription_sugar/lake_subscription_sugar.c` and
`lake_subscription_sugar--1.0.sql`:

- **Authorization.** `RequireAuthorized()` is called at the top of every
  `handle_create_subscription` / `handle_alter_subscription` /
  `handle_drop_subscription` this hook claims, before any dblink
  connection, SPI write, or process spawn happens. It raises
  `ERRCODE_INSUFFICIENT_PRIVILEGE` unless the calling role is superuser or
  a member of a new `lake_replication_admin` role, which the extension's
  install script creates (`NOLOGIN`, meant to be `GRANT`ed to real login
  roles). Previously, any role that could load the extension and issue the
  statement got full dblink-based source access and the ability to spawn
  OS processes as this backend's user -- there was no gate at all.
- **`pending_cleanup` lockdown.** Added an explicit `REVOKE ALL ON
  pending_cleanup FROM PUBLIC` and a comment explaining the table may hold
  a cleartext password (from an embedded `CONNECTION` password), matching
  how PostgreSQL protects `pg_subscription.subconninfo`. This was already
  the default (PostgreSQL grants no table privileges to `PUBLIC`
  automatically), so the REVOKE is defensive, not a behavior change --  but
  it is now explicit and documented instead of relying on an unstated
  default.
- **Configurable worker log directory.** Worker stdout/stderr previously
  always went to hardcoded `/tmp/lake_sugar_*.log` paths. A new
  `lake_subscription_sugar.log_directory` GUC (default `/tmp`, `PGC_SUSET`
  so an unprivileged session cannot redirect it before calling `CREATE
  SUBSCRIPTION`) controls this now.

## Evidence

Verified by hand against the `deploy/poc/` stack (rebuilt image; only the
extension/worker-binary layers needed to recompile, thanks to Docker layer
caching over the unchanged `pg_lake` build):

- An unprivileged role (`unprivileged_tester`, no `lake_replication_admin`
  membership, not superuser) attempting `CREATE SUBSCRIPTION ... WITH
  (lake_target = 'iceberg')` got `ERROR: must be superuser or a member of
  "lake_replication_admin" to CREATE SUBSCRIPTION ...`, with no control-plane
  row left behind.
- After `GRANT lake_replication_admin TO unprivileged_tester`, the same
  statement passed the authorization check (reached the dblink call next).
- Setting `lake_subscription_sugar.log_directory` and creating a
  subscription as superuser produced logs at the configured path instead of
  the hardcoded default, confirmed by file listing and content.
- The superuser demo path (`deploy/poc/webui`'s existing presets) still
  works unchanged: `CREATE SUBSCRIPTION`, initial copy, live streaming,
  `DROP SUBSCRIPTION` all verified again end to end after the rebuild.

## An unplanned finding: dblink blocks non-superuser use over `trust` auth, with or without a password string

Testing the granted, non-superuser role against this lab's `trust`-only
Postgres produced a different error than expected:

```
ERROR:  password or GSSAPI delegated credentials required
DETAIL:  Non-superusers may only connect using credentials they provide,
eg: password in connection string or delegated GSSAPI credentials
```

This is `dblink`'s own `dblink_security_check`, not something this
project's authorization gate controls. It checks
`PQconnectionUsedPassword()` after connecting: a password merely present in
the `CONNECTION` string is not sufficient -- the source's authentication
method must have actually verified one during the handshake. `trust` auth
(what every source in this project's labs and the PoC uses) never satisfies
this, no matter what the connection string contains, because no password
exchange happens at the protocol level under `trust`.

**Consequence:** a non-superuser `lake_replication_admin` member can only
successfully run `CREATE SUBSCRIPTION` against a source that requires real
password authentication (SCRAM, per Milestone 22) -- not against any of
this project's current `trust`-authenticated lab/demo sources. The header
comment in `lake_subscription_sugar.c` now documents this distinction
explicitly (superuser callers may omit the password and rely on `.pgpass`;
non-superuser callers cannot, and the password they must supply ends up
stored in `pending_cleanup`). This is a real, useful constraint for beta
documentation -- it means the beta install docs should recommend SCRAM on
the source whenever `CREATE SUBSCRIPTION` will be run by anything other
than a superuser -- not a bug introduced by this milestone's authorization
change.

## What this milestone deliberately does not do

Process supervision (workers spawned via a raw double-fork from inside a
Postgres backend, versus running under a real supervisor like systemd) is
unchanged. The beta readiness plan's framing of that gap still holds: it is
a real operability gap (no `systemctl status`, no log rotation, discovery
only via `lake_replication.service_status.supervisor_pid`), but fixing it
means either shelling out to a process manager from inside a Postgres
backend (its own new security surface) or decoupling supervision from
`CREATE SUBSCRIPTION` entirely (call `lake_replication.create_subscription()`
directly and let an operator start a systemd-managed supervisor themselves).
That decision needs to be made deliberately, not folded into this
milestone's authorization/credential/logging fixes.

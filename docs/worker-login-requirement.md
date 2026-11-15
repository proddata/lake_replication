# Explicit launcher-managed worker login

Launcher-managed workers now require a non-empty `LAKE_DESTINATION_USER`.
The C launcher no longer injects `LAKE_USER=postgres`. The receiver, publisher,
initial-sync and supervisor share one managed-identity check. Missing, empty or
whitespace-only identities fail before a destination connection or child-worker
startup; an explicit blank environment value does not fall back to a valid file.
The standalone CLI's legacy username fallback remains unchanged.

Configure a private regular file owned by the PostgreSQL operating-system user
with mode `0600`, for example:

```text
LAKE_DESTINATION_USER=your_existing_worker_login
LAKE_DESTINATION_PASSWORD=replace-if-needed
LAKE_DESTINATION_TLS_MODE=disable
```

`disable` is for a trusted local/plaintext destination; use the documented
verified TLS settings for remote destinations. Passwordless authentication still
requires the username: omit the password entry when it is not needed. Select
the file globally with `lake_replication.credential_file` or per subscription
with `lake_credential_file`.

Before replacing existing worker binaries, disable subscriptions, configure an
explicit destination login, then follow the coordinated backup and stopped-server
[upgrade procedure](upgrade-and-rollback.md). This is a binary/configuration
change with no new SQL migration. Correct an invalid file and reapply the
subscription's credential-file setting or disable/enable it to reload it.

Selecting a username does not create a role or grant permissions.
`lake_replication_worker` alone grants access to protected credential getters;
it is not a qualified complete runtime-grant recipe. A subsequent [runtime grant recipe](worker-runtime-grants.md) qualifies a fresh
non-superuser login in a dedicated schema. Its separate evidence does not alter
this login-selection test. Per-subscription authorization, staging/journal
access, retention and aggregate resource controls remain open in the
[security follow-up](security-todos.md).

## Validation

[Structured evidence](evidence/2026-11-13-worker-login-requirement.json) records
the exact locally built ARM64 archive, runtime source hashes, raw result paths,
control archive and excluded harness attempts. Concurrent demo edits are excluded.

- Two host-native Rust integration tests exercise 32 cases through the four
  real executable entry points. Negative cases cover no username/file, legacy
  user alone, missing file entry, blank/whitespace values and a blank environment
  override. Positive cases inspect the PostgreSQL startup username, including
  Unicode and quotes, from the file and from an explicit environment override.
  Private loopback listeners prove the negative cases never connect.
- Nineteen native archive checks cover actual launcher failures, no initial
  copy or source slot for missing/blank identities, no password in those error
  logs, configured destination-session identity, no injected legacy user,
  copy, streaming, disable/enable, source-slot cleanup and postmaster stability.
- The standard native packaged integration suite passes 57 checks, including
  source outage, restart, interrupted initial copy, upgrade contracts and uninstall.
- The preserved pre-change archive fails the new requirement check and starts
  source replication for the missing-identity subscription, confirming the
  regression distinguishes the previous fallback behavior.
- Repository `make check` passes. Private fixtures are removed.

The positive native login is deliberately privileged to isolate identity
selection. These tests do not establish minimum grants or subscription isolation.
The earlier [790-check native result](beta5-d5e05d2-native-qualification.md) belongs
to committed `d5e05d2`; it does not qualify this subsequent login change.
Final-commit native AMD64/ARM64 qualification and the deferred 24-hour soak
remain outstanding. No release is published by this work.

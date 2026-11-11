# Security regressions

`make check` runs `test_secure_log.py`, compiling and exercising the launcher's
actual POSIX log opener. It checks private creation and append, unsafe file and
directory permissions, symlinks, hardlinks, FIFOs, and nonregular files. Foreign
ownership checks require root and also run in the Linux build container.

Run the SQL regression against an existing locally cached PostgreSQL 18 image:

```sh
python3 tests/security/probe_sql.py --image YOUR_CACHED_PG18_IMAGE
```

The probe creates a disposable cluster, bypassing image initialization scripts,
with `standard_conforming_strings=off` and `client_encoding=LATIN1`. It publishes
a random port on loopback and removes the container afterward. The image must
provide `bash`, PostgreSQL tools on PATH, and a `postgres` OS user.

The worker test verifies UTF-8 and standard-string startup settings, exact
round-trips of quotes, backslashes, Unicode, JSON, and an injection payload under
both string modes, and replication-protocol startup. A positive control executes
the old escaping against a temporary table to prove the regression is detectable.
The fix uses explicit escape literals shared by all four workers; this is not a
conversion of the simple-query client to the extended parameterized protocol.

Launcher log configuration and upgrade permissions are documented in
[the operations guide](../../docs/operations.md#structured-worker-logs).

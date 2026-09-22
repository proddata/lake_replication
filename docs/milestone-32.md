# Milestone 32: launcher startup crash fix

Status: the preloaded launcher no longer crashes PostgreSQL when its configured
database exists but the extension schema has not been created yet.

Restarting the retained lab after recreating its container exposed a production
startup defect. The launcher correctly caught `worker_launch_config` being
absent, but `CopyErrorData()` allocated the diagnostic in a context subsequently
destroyed by `FlushErrorState()`/transaction abort. Logging `edata->message`
then dereferenced freed memory, segfaulting the background worker. PostgreSQL
treated that as a shared-memory worker crash, terminated every backend, and
entered an endless recovery/restart loop.

The catch handler now copies `ErrorData` into `TopMemoryContext` before error
and transaction cleanup, then frees the copy after logging. With the same
retained database and missing table, the launcher repeatedly reported the
configuration problem while PostgreSQL remained healthy. No volume reset was
used.

The exact exported bundle was then rebuilt and tested against the live pg_lake
lab. All eight artifact assertions passed, including the compatibility matrix,
checksums, PostgreSQL 18 selection, validated pg_lake notice, extension loading,
and native linkage. The result is
[retained here](evidence/2026-09-22-install-bundle-results.json).

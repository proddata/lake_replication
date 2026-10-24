# Production bulk admission migration

Historical bulk-only candidate evidence. The four-slot streaming policy is now
adopted and locally package-qualified in the [production beta.3 candidate](stream-pool-migration-results.md).

Status: implemented as the beta.3 candidate, but qualification is blocked by a
reproduced high-fan-out streaming OOM. Bulk-only admission does not close the
resource gate. See the [finding and streaming-pool diagnostic](streaming-admission-results.md).
Native amd64 CI qualification and the release soak remain pending.

The production extension now uses the [qualified bulk-only policy](bulk-admission-performance-results.md).
`pg_extension_base` still owns worker lifetime. One transaction at a time may
perform managed bulk engine work across the PostgreSQL instance; streaming
publication and PostgreSQL staging remain unqueued.

## Implementation

Migration `0024_bulk_admission.sql` installs `lake_replication.admit_bulk()` and
explicit replacements for five bulk entry points: destination creation, additive
schema reconciliation, single/group initial-copy publication and rebuild
promotion. Production does not rewrite function definitions dynamically and
installs no experimental functions or views.

The C helper acquires a transaction-scoped PostgreSQL advisory lock in the
instance-wide namespace (database 0, class 1280460112, object 2, sub-ID 1).
It is reentrant within a transaction and held through engine commit. Native lock
handling supplies interruptible waits, savepoint rollback and backend-death
cleanup. Public privileges are revoked on the helper and
`lake_replication.bulk_admission` view; the administration role receives execute
and select privileges. The view exposes holders, waiters,
database names and wait details.

Ready-schema and existing-table checks bypass admission. Schema DDL acquires
its permit before entering the DDL exception handler. Deadlock, lock-unavailable
and serialization errors (`40P01`, `55P03`, `40001`) propagate rather than
committing a permanent subscription pause. Genuine incompatible schemas still
pause replication. The policy handles detected lock-order cycles through abort
and retry; it does not prevent every deadlock.

## Version and package boundary

The default version and worker package are `0.1.0-beta.3`. Fresh installation
includes migrations 0001–0024. The beta.2-to-beta.3 upgrade applies only 0024;
earlier versions chain through the preserved beta.2 paths. All six earlier SQL
files remain byte-identical to the qualified beta.2 archive, enforced by a
regression check.

The bundle includes the new fresh/upgrade scripts; the uninstaller removes them.
The documented procedure drains subscriptions and stops PostgreSQL before binary
replacement. Beta.1 origin adoption remains explicit. Beta.2 bindings already
record their origin and need no legacy adoption override. No SQL downgrade,
online binary replacement or physical takeover is introduced.

## Qualification

The production probe installs only exported packages; it applies no experimental
SQL or binary overlay. Shared operational and measurement helpers are reused
from the qualification libraries. [Retained evidence](evidence/2026-10-23-bulk-admission-migration-results.json)
records exact input fingerprints, payload/archive hashes, checks, source snapshots
and raw-log hashes. The final local arm64 archive is
`dist/bulk-beta3-aligned/lake_replication-0.1.0-beta.3.tar.gz`, SHA256
`ea10f47b5cedf7084fa34dd682c2df14d23f21d5c9d5e3bf2a312411e4a7ec37`.
It is an uncommitted development build, not a signed release or CI artifact.

| Final archive lane | Checks | Result |
| --- | ---: | --- |
| Fresh lifecycle, install/restore/uninstall, native management | 119 | Pass |
| Real stopped-server beta.1-to-beta.3 handoff | 47 | Pass |
| Forced Rust child-crash recovery | 17 | Pass |
| Receiver regressions and native bulk/recovery | 96 | Pass |
| New admission suite | 100 | 99 pass; sixteen-stream resource check fails |

Check counts include each lane's archive checks. The admission suite passes lock,
schema, rebuild, active-copy cancellation, beta.2 handoff and four-copy cases.
The sixteen-subscription resource case fails; a separate repeat retains a fatal
engine OOM. Passing 279 existing package checks does not override that finding.
See the [streaming investigation](streaming-admission-results.md).

Earlier development runs, the corrected upgrade-harness image-tag collision and
the superseded worker-version build are retained but not counted as final
qualification. One failed scale log was overwritten after cleanup; the corrected
harness preserves logs and captures worker/container state before removal. The
repeat retains the OOM evidence. All private fixtures were removed. `make check`
passes, including the six frozen SQL fingerprints.

Run the package lanes with an explicit exported archive:

```sh
QUALIFICATION_ARCHIVE=/path/to/lake_replication-0.1.0-beta.3-linux-arm64.tar.gz make admission-test
QUALIFICATION_ARCHIVE=/path/to/lake_replication-0.1.0-beta.3-linux-arm64.tar.gz make lifecycle-test
QUALIFICATION_ARCHIVE=/path/to/lake_replication-0.1.0-beta.3-linux-arm64.tar.gz make lifecycle-upgrade-test
```

The packaged CI workflow includes admission and beta.2 upgrade qualification on
native arm64 and amd64 runners, alongside lifecycle, beta.1 handoff, receiver and
child-crash lanes. Those remote results are still required for the final commit.

The policy does not bound staging memory, wrapper/connection growth, retained
source WAL or arbitrary application engine SQL. Neither the local tests nor the
previous paired prototype cohort establishes a total memory bound, a universal
capacity ceiling or release readiness.

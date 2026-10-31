# Checkpoint, staging and initial-copy migration candidate

Beta.5 moves the split checkpoint, chunk staging and paced initial-copy route
from diagnostic overlays into the extension's install/upgrade SQL and worker
binaries. The previous [paced native observation](staging-pacing-prototype.md#prospective-native-result)
passes all absolute gates, but its full comparison remains inconclusive. That
result motivates this implementation; it does not qualify the new archive.

The new version adds migrations 0026–0028 and a beta.4-to-beta.5 upgrade. All ten
historical SQL payloads remain byte-identical, enforced by a checked-in digest
manifest. The archive contains the complete earlier upgrade chain. The separate
DuckDB build optimization remains excluded.

## Checkpoint contract

`checkpoints` retains its six logical columns and INSERT/UPDATE/DELETE interface.
Physical `checkpoint_ingress` owns received/staged/acknowledged frontiers;
`checkpoint_publication` owns published progress. No stale publication column
remains in ingress. Monotonicity and cross-frontier bounds are enforced on both
physical write paths. Publication validates committed staging without holding
its row lock; staging and acknowledgement can proceed while publication owns
its own row. Journal admission remains serialized on ingress and does not free
capacity until publishing commits. Destination data, journal states and the
published frontier still commit in one transaction.

A foreign key in each direction prevents a committed half-pair. Pair deletion
cascades from ingress; creation through the logical view is atomic. Missing
publication state causes publication and managed-worker admission to fail closed.
Maintenance uses conservative monotonic-frontier reads instead of holding the
shared ingress row during cleanup. Built-in health views bind to the logical
frontiers, rather than to a renamed table's obsolete fields.

The logical route preserves defaults, rejects explicit NULLs and identity
changes, and applies only changed fields to locked current state. It does not
silently overwrite an acknowledgement which advanced after the caller's
snapshot. Regressions and publication beyond durable staging are rejected.

## Staging contract

`initial_copy_rows` becomes a writable logical view over preserved
`initial_copy_legacy_rows` and new `initial_copy_chunks`. Existing staged rows
are not rewritten during upgrade. New native appends persist one JSON array
per transaction, retaining parent/cascade foreign keys, object/ordinal checks
and range exclusion. Configured chunk sizes above 1,000 remain supported; the
native defaults remain 1,000 rows and 8 MiB of row JSON text with the existing
single-oversized-row exception. This is not a hard RSS bound.

Cross-store guards serialize additions and moves using a real parent version
change, preventing repeatable-read write skew as well as read-committed races.
Deletion cannot create an overlap, so FK cascade deletion does not run an
extra parent update for every removed row. Legacy row edits materialize only
the affected chunk into the legacy store, atomically preserving peers. Stale
row edits fail with SQLSTATE 40001 rather than silently losing a concurrent
change. As before, direct row-count-changing repairs must maintain progress or
restart the incomplete copy; the append API maintains its own progress.

The stable interface here is logical SELECT/INSERT/UPDATE/DELETE. Raw table
operations such as TRUNCATE, system-column access and SELECT FOR UPDATE on the
union row view are not supported. Physical stores are available for privileged
inspection. User views/functions depending on the old physical relations,
custom row security/triggers, changed runtime-table ownership, and non-owner
grant chains require a separately rehearsed migration; the upgrade refuses
them atomically instead of silently changing their semantics.

## Privileges and logical restore

Ordinary owner-issued table/column grants and grant options transfer to the new
logical and physical relations without implicitly giving the management role
checkpoint mutation rights. Trigger helpers explicitly place `pg_temp` last in fixed search paths and use private
execution privileges; logical view ACLs govern their callers. Adversarial tests
verify that temporary staging and control tables cannot redirect privileged
writes or spoof the generation fence. The earlier development archive failed
the staging-shadow regression and is retained as a failure, not qualified.

Copying an operator GRANT inside extension DDL normally records it as an initial
extension privilege, causing pg_dump to omit it on restore. The owner-only
`migration_grant` bridge accepts one GRANT only during this extension's own DDL.
It temporarily suspends that initial-privilege recording, executes normal core
GRANT authorization/dependency handling, and restores the extension context on
ERROR. It neither writes system catalogs directly nor exposes a general SQL
execution API. See PostgreSQL's [initial-privilege documentation](https://www.postgresql.org/docs/18/catalog-pg-init-privs.html)
and [grant implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/catalog/aclchk.c).

The split checkpoint tables, legacy row store and chunk store are registered
with pg_dump. Use the existing stopped, coordinated PostgreSQL/object-store
backup procedure. For logical rehearsal: restore pre-data, restore data with
`--disable-triggers`, then restore post-data. The paired foreign keys require
that documented disabled-trigger data pass. Before enabling subscriptions run:

```sql
SELECT lake_replication.validate_runtime_state();
```

It rejects incomplete checkpoint pairs, publication beyond staging, orphaned
staging and cross-store overlaps. Logical data restore also preserves ordinary
column grants and grant options. Restore never permits a copied destination to
reuse the original source slot without the existing destination-binding checks.

## Pacing and concurrency

Four session leases admit native initial-copy pipelines across the PostgreSQL
instance, including all installed databases. Acquisition happens before opening
a source snapshot. A fifth pipeline waits interruptibly before its generation
fence; stop cancels it. A transaction-scoped admission gate serializes
contenders; its head checks every slot at most 100 ms apart, so a waiter can
use any released lease instead of waiting on a particular occupied slot.
Cancellation releases the gate automatically. No strict FIFO promise is made. The lease belongs to the actual staging connection and
survives its short SQL transactions. Connection replacement must reacquire it.
A failed fence after session admission closes the connection, because SQL
ROLLBACK alone would retain a session lease. Managed appends require that lease,
so old binaries cannot silently bypass the new admission contract after upgrade.

The `initial_copy_admission` view shows PID, database, slot, state and wait event.
Queued contenders have a NULL slot, including the gate holder awaiting capacity.
Disconnect/backend death releases capacity. This bounds admitted sessions, not
instantaneous CPU, total RSS or remote snapshot retention after a connection
failure. Buffered source work can remain until the worker detects its failed
staging connection. Existing staging, engine and journal admission remains.

`lake_replication.initial_copy_pause_ms` defaults to 40, accepts 0–1,000 ms, and
is captured when a copy starts. Set it as a superuser using the normal PostgreSQL
configuration mechanisms, then restart affected subscriptions to apply it to new
copies. A direct worker's explicit `LAKE_INITIAL_COPY_CHUNK_PAUSE_MS` environment
value takes precedence. Zero explicitly opts out of pacing. Sleep follows COMMIT
and holds neither the staging permit nor a transaction generation fence. The
four-lease cap prevents a per-worker pause from scaling into arbitrarily many
simultaneous admitted native pipelines; it is still not an aggregate byte-rate
or CPU guarantee for arbitrary data, worker configuration or host capacity.

## Qualification boundary

Qualification covers fresh installation, populated stopped upgrade and
rollback, mixed-store writes, repeatable-read races, logical restore including
ACLs, cross-database lease capacity/cancellation, native stop/crash cleanup,
maintenance/rebuild and constrained performance using only the exported archive. The local results below pass those checks;
clean-commit native and release qualification remain separate.
Development failures are retained separately from final frozen-input evidence.
Native qualification of an unmodified clean CI archive on both architectures,
compatibility, recovery, security review and soak remain release requirements.

## Local packaged results (2026-10-31)

The hardened arm64 archive passes all 231 production admission checks, including
populated beta.4 upgrade/rollback, mixed-store writes, repeatable-read conflict,
ordinary grants/grant options through logical restore, temporary-table attacks,
cross-database copy admission/cancellation, native chunk row/byte limits,
stop/crash cleanup, schema/rebuild lock ordering, and cancellation during a real
engine write followed by exact-data recovery. Fresh lifecycle adds 119 passing
checks; a real stopped-server beta.4 archive handoff adds 24 passing checks.

The unmodified production archive also passes the sixteen-stream live/restart
regression and the shared-two-CPU four-copy test: 104.40 seconds, streaming p95
304.65/307.54/305.40 ms, four overlapping publications, valid source/observer
clocks, exact data, bounded catch-up, no engine OOM and no PostgreSQL crash. The
source, destination, engine and storage share CPUs 0–1. This is one absolute
working point; it is not a paired causal comparison or native-runner result.

The isolated one-CPU sensitivity check also passes all gates: 118.03-second
copy completion and streaming p95 958.21/985.77/969.80 ms, valid source/observer
clocks, four overlapping publications, exact data, no OOM and no crash. Both
performance observations use the same unmodified production runtime. Detailed
archive hashes, unchanged runtime payloads, frozen historical SQL, input hashes,
results and retained development failures are recorded in the
[machine-readable evidence](evidence/2026-10-31-checkpoint-chunk-migration-results.json).
The cached pg_lake prerequisite is pinned at `8180647d4535`; the separately
uncommitted DuckDB build optimization is not cold-build qualified here.

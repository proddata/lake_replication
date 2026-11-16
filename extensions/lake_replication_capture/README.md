# Optional source capture prototype

This companion is separately installed on a **source** PostgreSQL 18 server.
Normal lake_replication destination-pull deployments do not require it. This
prototype supports one source database, one fixed two-table group, permanent
heap tables without RLS, with non-null primary keys, and bigint/integer/text/boolean columns.
Activation supports empty tables or a bounded coordinated `paused_copy` or `online_copy` baseline. Installation requires a source administrator. Routine operation supports a
dedicated LOGIN REPLICATION role with the database-local grants in
[`source-grants.sql`](../../deploy/push/source-grants.sql) and explicit SELECT,
MAINTAIN on the selected tables. MAINTAIN permits the SHARE lock used to freeze
registration and paused activation; it does not grant application DML.

The native output plugin uses PostgreSQL logical decoding and historic relation
descriptors. Callbacks only encode committed events. A separate Rust child reads
bounded transaction envelopes, appends private pg_lake Iceberg logs and a durable
receipt/checkpoint in one source transaction, then advances the owned slot.
An ambiguous publication commit is reconciled by raw transaction identity before
retrying. INSERT supports PostgreSQL-reconstructed TOAST. Aborted transactions
and subtransactions are discarded by PostgreSQL's decoding machinery.

Optional `transactions_per_read` (1–32, default 1) and
`publication_interval_ms` (0–1000, default 0) coalesce complete source commits.
Every source transaction keeps an independent manifest and receipt; one bounded
publication transaction commits the group atomically. The decoder caps each
read at 64 MiB/100,000 events and retries smaller windows before any upload
reservation when a group exceeds that cap. An individually oversized source
transaction still stops capture. Larger groups/intervals trade capture latency
for fewer publication commits; qualify resource use for the intended workload.

Registration pins the actual four private table roots in `log_locations`; the
common `object_prefix` comes from those roots. Changing a session's default
prefix does not move existing Iceberg logs. Capture and maintenance stop before
new reservations if a private table root changes. Repair the qualified location
or perform a controlled rebuild; automatic relocation is not supported.
Private table/schema-name DDL is rejected before lock acquisition while a stream
exists, including while stopped. Native storage pins span maintenance commits
and are released on backend exit. Reset/drop also wait for manually launched
capture or maintenance actors to release their lease, even with an idle slot.
An administrator must repair a changed catalog root to its recorded original
value before cleanup can remove the protected slot.

UPDATE, DELETE, key changes and ordered TRUNCATE normalize to the existing
publisher operations. Unchanged external TOAST is carried as an unchanged marker;
no current source-row lookup reconstructs old values. Registration requires
default primary-key identity or an explicitly selected primary-key index;
distinct identity indexes and full-row identity are rejected. Selected-table
DDL is rejected while registered, including while capture is stopped. Other
tables remain outside this guard. Capture log/state writes are excluded from
selected-table events. Decoded commits with no selected changes may advance the
owned slot without producing recursive progress writes or empty manifests.

## Build and isolated qualification

Use the pinned pg_lake runtime from [the prerequisite build](../../deploy/pg_lake/README.md).
For example, with the local native ARM64 cache:

```sh
docker build --pull=false \
  --build-arg BASE_IMAGE=pg-lake-runtime:arm64-b6794e8209f18b90cf80 \
  -f deploy/push/Dockerfile.capture -t lake-push-capture:prototype-v1 .
```

This installs the companion `.so`/control/SQL and the feature-gated
`lake-replication-capture` executable in a test image. It does not add them to
ordinary lake_replication release archives. On a source build, install the
extension through PGXS (`make install`) and build the executable with
`cargo build --release --features push-prototype --bin lake-replication-capture`.
The lifecycle manager expects the executable at
`/usr/local/bin/lake-replication-capture`.

The qualified source fixture uses PostgreSQL 18.6, UTF8, `wal_level=logical`,
`shared_preload_libraries=pg_extension_base`, a source pgduck server and configured
object-store credentials, plus a finite `max_slot_wal_keep_size`. Explicitly add
`lake_replication_capture` to `output_plugin_libraries` alongside `pgoutput`.
PostgreSQL checks this allowlist before loading an output plugin; see the
[PostgreSQL 18 implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/logical.c).

See [the live/fault harness](../../tests/push_capture/README.md) and
[implementation status](../../docs/design/push-replication.md) for evidence and limits.
The first prototype is not a production mirroring release.

## Lifecycle

```sql
CREATE EXTENSION lake_replication_capture;
SELECT lake_capture.register_stream('public.a', 'public.b'); -- returns stream UUID
SELECT lake_capture.start_stream('STREAM_UUID');
SELECT * FROM lake_capture.status;
SELECT lake_capture.stop_stream('STREAM_UUID');
```

Once the baseline is complete, the registered owner can request an explicit
control-only progress barrier while capture is running:

```sql
SELECT lake_capture.request_progress('STREAM_UUID'); -- returns a request UUID
SELECT request_id,captured_sequence,captured_end_lsn
FROM lake_capture.progress_requests;
```

The request is transactional. Its captured fields become non-NULL atomically
with the complete manifest and source receipt. Slot advancement follows durable
capture; the returned UUID itself is not a safe WAL position. The importer and
existing publisher can publish this zero-row transaction, so actual publication
acknowledgement can release its control log. A request in a transaction containing
selected DML keeps that transaction's ordering and atomicity. Rollback consumes
no request budget. Requests require the owner and a running, initialized stream;
ordinary source logins have no request-table access.

`max_progress_requests` defaults to 1024 and permits 1–10000 requests per capture
generation. Request rows remain as bounded completion receipts until controlled
reset/drop. Normal log/upload/generation quotas also apply. Requests are explicit;
capture's metadata writes and completed request updates never request another
barrier. Automatic idle heartbeats are outside this implementation.

Registration and desired-state changes are transactional. The pg_extension_base
manager sees committed rows, owns/reaps the Rust child and fences stale config
generations. The child takes a session advisory lease, independently preventing
duplicate capture. Credential file contents stay out of process arguments and
log payloads; the optional third registration argument is an absolute owner-only
credential file supported by the existing Settings reader. Local source TCP uses
the explicit registered owner and its authentication policy.

Empty activation holds SHARE locks on both tables through checking emptiness and
creating the persistent logical slot. Subsequent writes are protected by that
slot. Lost slot/history stops capture; restarting does not silently create a
replacement slot past existing progress. A full postmaster restart starts capture
stopped until explicitly verified/restarted. Logical restores cannot adopt the
original source binding or clean up its source slot.

`reset_stream(UUID)` requests controlled cleanup: stop/reap the child, drop only
the verified owned slot, clear private logs/receipts, increment capture generation,
and leave the stream stopped with no baseline. Wait for `desired_state='stopped'`
and the new generation before explicitly activating a new empty baseline.
Consumers bound to the old generation must rebuild/rebind; they never switch
implicitly. `drop_stream(UUID)` requests the same owned cleanup and removes the
registration. Wait for its status row to disappear before dropping the extension.
These operations discard source logs and are intended for controlled rebuilds.

The bounded prototype rejects events over 8 MiB and complete transactions over
64 MiB/100,000 events. Native decoding envelopes are transferred in 4 MiB chunks;
a chunk never authorizes slot advancement. WAL high-water failure stops capture;
server slot-retention configuration remains necessary. Capture, imported and
published frontiers have separate meanings; a running process does not establish
mirror freshness.

## Bounded baseline and resource policy

The optional fourth registration argument `paused_copy` holds SHARE locks through
slot creation and complete baseline publication. It copies existing rows as one
bounded manifest with explicit snapshot lifecycle controls. Incomplete copies
require controlled reset and recopy; old consumers never adopt new generations.
The baseline shares the 64 MiB/100,000-event cap, including controls.

Finite retained-log, cumulative generation, and pre-upload attempt budgets stop
capture while preserving the unacknowledged source transaction in the slot.
`register_consumer(UUID)` registers a retention consumer before pruning begins.
`acknowledge_publication(stream,consumer,generation,sequence,end_lsn,digest)` must
use the actual destination published checkpoint, not journal admission.
`prune_logs(UUID)` uses the slowest registered publication and the source slot's
confirmed frontier. It removes logical rows only. Receipts/reservations persist
for the bounded generation; reset clears them. See the implementation status for
qualified limits and physical-maintenance evidence.

A failed reservation starts no upload. Fatal child failures require explicit
start after repair, including when the failure prevents writing `last_error`.
Look at the source server logs if desired state remains running with no child.

`online_copy` uses PostgreSQL's slot-exported snapshot and imports it into a
repeatable-read transaction before releasing the exporter. Application DML can
continue during the bounded copy; the slot retains changes after its exact
consistency boundary. The baseline commits before those changes are replayed.
An interrupted online copy follows the same reset/recopy rule as paused copy.
This does not add unbounded or independently resumable snapshot chunks.

## Dedicated source credentials

Create a dedicated `LOGIN REPLICATION INHERIT` role as administrator, apply
`psql -v capture_login=ROLE -f deploy/push/source-grants.sql`, then explicitly
grant SELECT, MAINTAIN on the two selected source tables. Register/start as that
role. The manager connects the Rust child as the persisted owner. No superuser
attribute or application INSERT/UPDATE/DELETE grant is required. PostgreSQL
REPLICATION remains a server-level capability; the recipe trusts one capture
operator with this database's control plane and pg_lake `lake_read_write` access.
It does not isolate untrusted operators or object-store credentials.

A separate importer source login needs only USAGE on `lake_capture_log`, SELECT
on its tables/views and pg_lake `lake_read` membership. It needs no capture API,
application-table access or REPLICATION attribute. Its transport authentication,
TLS and object-store credential policy must be configured explicitly.

## Private-log maintenance

The four log tables use `autovacuum_enabled=false,max_snapshot_age=0`. All
importer reads acquire shared advisory gate `(1280593921,1)` before BEGIN and
release it after COMMIT/ROLLBACK. Other supported log readers must do the same.
The capture worker tries the exclusive gate every five minutes and performs
scoped VACUUM only after a durable generation maintenance-attempt reservation.
A pinned reader causes a skipped cycle. Default pg_lake file-retention delays
remain in force; logical row pruning and snapshot expiry do not delete physical
objects immediately. The finite maintenance budget is independent of capture
upload-attempt budgets. Exhaustion needs explicit configuration or rebuild;
failed maintenance requires an explicit repaired restart.

The isolated deployment uses different source/destination object users scoped to
separate prefixes; see `deploy/push/policies/`. Supply equivalently scoped real
credentials to the separate pgduck engines. Fixture credentials are disposable
and must not be reused. The source operator remains trusted within its source
engine's object prefix.

Rotate a source SQL password by stopping capture, waiting for its connection to
exit, atomically replacing the owner-only KEY=value credential file, and starting
a new configuration generation. Retained WAL protects application writes during
the pause. If authentication fails before the child can report an error in SQL,
the manager's fatal retry fence requires an explicit repaired start; inspect the
server log and capture connection count.

## Native decoder spill limits

`decode_work_mem_kb` (default 4096) bounds the native reorder-buffer working
allowance, separate from the 64 MiB complete encoded event buffer.
`max_decode_spill_bytes` (default 256 MiB) limits native spill accounting during
one SQL decoding read, including unfinished and unselected transactions. The
plugin reads the live shared slot counter before queued WAL changes and rejects
a statistics reset during a read. Checks occur at WAL-record boundaries; use
server filesystem quotas/monitoring for physical disk limits. These values do
not describe total process RSS or all SQL/transport memory copies.

A spill-limit error leaves capture/slot progress behind the complete pending
transaction. Repair the budget or workload and explicitly restart the source
configuration. Protected WAL recreates provisional native state. Source status
includes `total_decoder_spill_bytes` and `decoder_spill_count`; counters are
telemetry and do not authorize capture or retention advancement. Source-role
grants permit operational progress/budgets but deny direct rewriting of selected
tables, slot name, capture generation and privileged child identity; controlled
reset/drop remain the lifecycle path.

## Initial schema history

Registration requires READ COMMITTED isolation and records eligible strict
version-one definitions while both selected tables are locked. Native validation
uses current relation metadata after lock acquisition. Frozen capture retains its
existing default/identity compatibility; those columns are outside the stricter
evolution contract and receive no inferred strict history.

`lake_capture.verify_schema_history(stream_id)` checks stored definitions against
the locked tables. It requires a stopped, initialized, correctly bound stream and
an available capture lease. Missing or conflicting history requires a new
registration; later catalogs cannot reconstruct version one. Frozen reset carries
the original bytes into the next generation. Frozen mode remains the default,
including for repeatable-read transactions begun before registration.

Call `enable_schema_auditing(stream_id)` on a stopped, initialized source before
registering consumers to opt into nullable additions and column renames. The
owning replication LOGIN must have the capture lease and retained eligible
definitions. Register ordered consumers with `register_consumer(stream_id,true)`.
See [ordered schema activation and bounds](../../docs/design/push-schema-evolution.md).

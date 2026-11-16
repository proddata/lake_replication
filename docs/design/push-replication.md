# Optional source-push replication: implementation status

The primary deployment goal was clarified on 15 November 2026: mirror tables
from a single PostgreSQL instance directly into Iceberg. Use the
[single-instance path](../guides/local-iceberg.md), which does not require this document's
intermediate Iceberg transport or a separate destination PostgreSQL. The material
below records the optional two-instance transport implementation and its evidence.
Its benchmark ratios do not describe the single-instance topology.


The [historical implementation checklist](../archive/source-push-implementation-checklist.md) records this transport experiment. Current priorities live in the [replication roadmap](../roadmap/replication.md).
The optional native source extension now captures committed row mutations into
Iceberg logs, and the feature-gated importer feeds the existing journal and
publisher. Empty activation, bounded paused/online baselines, and managed destination lifecycle are qualified. The
source companion and importer require the `push-prototype` Cargo feature;
destination metadata/API is a separately installed `lake_replication_push`
companion. Default destination SQL and subscription syntax stay compatible, and
the existing publisher still performs every final mirror write.

## Compatibility contract

| Property | Existing logical pull | Proposed Iceberg push |
| --- | --- | --- |
| Mode name | `logical_pull`, existing default | `iceberg_push`, explicit optional API |
| Source capture | Existing pgoutput receiver | Optional source extension using logical decoding |
| Source installation | Existing PostgreSQL/publication/slot requirements | Source installation privileges, pg_lake, object-store access and resource budget |
| Destination transport | Existing receiver and journal | Prototype importer into the same journal |
| Final mirror writes | Existing publisher | Same existing publisher |
| Slot ownership | Existing pull lifecycle | Optional source capture extension; push destination must not manage these slots |
| First supported topology | Existing qualified deployments | PostgreSQL 18, one source DB, fixed two-table group, non-null supported primary keys, frozen schema by default, bounded opt-in ordered DDL, local PostgreSQL pg_lake catalog |
| Availability | Existing behavior | Prototype still requires the source PostgreSQL catalog for discovery and consistent reads |

Source mode remains separate from `lake_target = 'iceberg'`. Existing subscription
syntax, permissions, installation, checkpoints and logical slot behavior stay
compatible. Existing subscriptions will not switch modes automatically.
Physical WAL decoding, REST-catalog guarantees, live views, automatic discovery,
online mode switching and automatic failover are outside the first prototype.
The optional artifacts remain a bounded prototype, not a general mirroring release.

## First implementation slice

The [progress-barrier qualification](../evidence/2026-11-14-push-progress-qualification.json)
retains 394 passing normal checks across 26 selected fixtures, 111 native source
crash checks and 38 packaging checks. Progress requests include an older unfinished
transaction case and actual upload/commit crashes. Bounded ordered schema evolution has an explicit
[activation contract](push-schema-evolution.md) covering source audit, versioned
journal admission and atomic schema/data publication.

[The disposable probe](../../tests/push_capture/README.md) runs against a source
PostgreSQL 18 / pg_lake deployment with no lake_replication extension installed.
It appends both Iceberg logs, a manifest and PostgreSQL receipt/checkpoint state
atomically. It checks rollback, failed transactions, backend death, postcommit
session loss, replay conflicts and duplicate writers. Receipts provide durable
identity when reconnecting; a row lock serializes publication. The
[bounded protocol](push-protocol.md) separately preserves absent, NULL,
unchanged, text, old-key and full-old-row cases. Its importer accepts
INSERT/UPDATE/DELETE/TRUNCATE plus complete snapshot controls and validated explicit
progress requests. Schema controls fail before admission in frozen mode; explicitly ordered bindings use versioned admission and the optional schema publisher.

The starting worktree was based on `d5e05d2780796dd9979e99a08edfdf57c2266a3d`
with pre-existing uncommitted changes. Each run records the actual worktree
status, tracked diff hash, fixture hashes, PostgreSQL build, source identity,
pg_lake revision/patchset and installed library hashes, and immutable container
image IDs. An existing destination image is not used as source-side proof.

The [retained source SQL run](../evidence/2026-11-13-push-capture-sql-feasibility.json)
passes all 27 checks on ARM64 PostgreSQL 18.6 with pg_lake revision
`8180647d4535fdb1ed223f92f301ba6094a8ec3e` and the manifest-cleanup-v1 runtime
patchset. Fixture source hashes match the retained evidence and disposable
container/volume cleanup succeeded.

The [source crash/read-view run](../evidence/2026-11-13-push-capture-crash-boundaries.json)
passes 56 checks, including SIGKILL before/after upload completion and inside the
postcommit callback before success reaches the client. Reconnect resolves the
durable receipt before retrying. A reader that pins its snapshot before capture
sees the old manifest, both logs and checkpoint even when it first opens the
Iceberg tables after capture commits.

Logical correctness/recovery passes 57 checks both
[before](../evidence/2026-11-13-push-logical-baseline.json) and
[after](../evidence/2026-11-13-push-logical-refactor.json) extracting shared types and
staging. Existing worker-crash recovery passes 17 checks
[before](../evidence/2026-11-13-push-logical-baseline-faults.json) and
[after](../evidence/2026-11-13-push-logical-refactor-faults.json). These are separate
source-pinned worktree archives, not a new release qualification. The publisher
and SQL admission functions are unchanged by the extraction.

The [synthetic importer run](../evidence/2026-11-14-push-synthetic-importer.json)
passes 33 checks: cross-language protocol encoding, two-table materialization,
replay/conflicts, invalid batches, atomic imported progress, process death
before/after admission, journal backpressure and stale generation fencing.
Source and destination have separate pgduck sockets/staging directories. The
importer creates no source slot. This fixture uses generation zero for its
unmanaged test subscription; a nonzero stale generation is rejected through the
existing ManagedClient fence.

## Native capture and bootstrap evidence

The [live native run](../evidence/2026-11-13-push-native-live.json) passes 21 checks,
including independent pgoutput comparison, abort/subabort handling, duplicate
leases, stop/start, child death, and capture while the importer is stopped.
The [native crash run](../evidence/2026-11-13-push-native-crash-reconciliation.json)
passes 25 checks at upload/commit boundaries, including durable receipt
reconciliation before slot advancement.

[Paused snapshot qualification](../evidence/2026-11-14-push-paused-snapshot.json)
passes 18 checks. A SHARE lock freezes both selected tables while a slot and a
bounded, complete baseline are created. Begin/chunk/complete/activate controls,
baseline rows, receipt and source checkpoint commit together. Concurrent writers
wait behind the baseline locks, then replay from its slot boundary. Incomplete
copy requires reset and recopy; consumers cannot silently follow a new generation.
[Scalar qualification](../evidence/2026-11-14-push-native-scalars.json) covers integer
limits, bool, NULL, Unicode, compound keys, INCLUDE attributes and quoted names.
[Lifecycle qualification](../evidence/2026-11-14-push-native-lifecycle.json) covers
full postmaster restart, restored-source fencing, controlled cleanup, frozen DDL,
oversize transactions and missing-history rejection.

## Retention and bounded upload attempts

[Retention qualification](../evidence/2026-11-14-push-retention.json) verifies two
registered consumers, acknowledgements derived from actual destination
publication, conservative pruning, lagging-reader gap errors and bounded source
capture when the importer is stopped. Imported journal admission alone never
releases logs. Every active consumer must acknowledge publication through the
batch, and the source slot must have confirmed it, before logical pruning.
New consumers after pruning require a new snapshot generation.

[Upload-budget qualification](../evidence/2026-11-14-push-upload-budget.json) verifies
durable pre-upload reservations. If reservation fails, no object upload begins.
A fatal child exit fences automatic retries for that configuration even when a
metadata failure prevents persisting the error. Explicit start after repair
creates a new configuration generation and retries protected WAL.

Retained payload bytes, cumulative generation bytes/batches, and cumulative upload
attempt bytes/counts have independent finite limits. Pruning releases retained
payload budget; it does not release generation or attempt budgets. Receipts and
reservations remain bounded for the complete generation and are cleared by
controlled reset. These are logical/attempt budgets, not a physical object-store
byte measurement. Coordinated maintenance is qualified below; this is still not an actual object-byte quota.
The source owner supplies acknowledgement coordinates from the destination's
published checkpoint; the source cannot independently authenticate a remote
publication claim in this prototype.

## Gates still open

[Mutation qualification](../evidence/2026-11-14-push-native-mutations.json) passes
13 checks for historical TOAST, key changes, repeated mutations, delete/reinsert,
NULL transitions, ordered TRUNCATE and replay. The decoder never fetches current
source rows to repair historical values. A distinct replica-identity index is
rejected at registration; default identity or the primary-key index is required.

[Online snapshot qualification](../evidence/2026-11-14-push-online-snapshot.json)
passes 22 checks, including concurrent committed UPDATE/DELETE/key changes while
the baseline is incomplete. `CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export')`
provides both the consistency boundary and the snapshot imported into the
baseline's repeatable-read transaction. The exporting connection stays alive
until import completes. Baseline reads finish before a separate publication
transaction, and protected WAL supplies all later commits. See the
[PostgreSQL replication protocol](https://www.postgresql.org/docs/18/protocol-replication.html).

Sustained resource benchmarking, general release qualification and larger resumable transactions/snapshots remain open. Bounded ordered DDL is separately qualified through the opt-in schema path. Optional destination APIs and native prototype packaging are qualified below. A [dedicated-role run](../evidence/2026-11-14-push-source-roles.json) verifies a
non-superuser LOGIN REPLICATION owner with explicit selected-table SELECT and
MAINTAIN grants, plus a separate read-only log consumer. This is a trusted
single-operator control plane, not isolation between untrusted operators sharing one source engine. The prototype uses a finite
single-generation budget and requires controlled rebuild or deliberate budget
configuration when exhausted. It is not a production mirroring release.

## Coordinated physical maintenance and outages

[Maintenance qualification](../evidence/2026-11-14-push-maintenance.json) passes
14 checks. Private logs disable automatic vacuum and expire old Iceberg snapshots
on writes. Each importer takes a database-local shared advisory gate before
starting its repeatable-read transaction. Every five minutes the capture worker
tries the exclusive gate, commits a durable maintenance-attempt reservation, then
runs VACUUM on the four private logs outside a SQL transaction. A pinned reader
causes the cycle to skip without consuming an attempt. An importer waits before
pinning a snapshot while maintenance owns the gate. Fatal maintenance failures
stop the configuration; finite generation attempt limits prevent endless
compaction/upload retries. Exhaustion is reported in the source log and status
counters and needs explicit budget configuration or controlled rebuild.

The fixture independently observes a retired MinIO object. The default ten-day
deletion delay preserves it, and an isolated test override to zero removes it.
Live unpruned rows and subsequent journal/publication remain exact. Arbitrary
external source-log readers must participate in the same gate; uncoordinated
readers are outside this guarantee. Drop/reset ends the old generation and its
reader contract. Old files still have the pg_lake deletion-retention/PITR policy;
these tests do not override deployed retention settings.

[Outage qualification](../evidence/2026-11-14-push-outages.json) passes eight checks.
Application COMMIT completes while the source object store is stopped. Capture
hits its statement budget, retains a durable incomplete attempt, and leaves the
manifest/slot frontier behind the pending source transaction. Explicit repaired
restart reconciles it once. Source-catalog loss stops discovery without moving
the imported checkpoint; restart and rebind restore exact publication.

## Larger ordering and publication recovery

[Native chunk qualification](../evidence/2026-11-14-push-native-chunks.json) checks
more than ten transport chunks and event ordinals. Transport testing uses smaller
bounded chunks, leaving the canonical batch unchanged. Numeric source columns
explicitly determine SQL order, avoiding lexical ordering of text output aliases.
The [12-batch cost experiment](../evidence/2026-11-14-push-source-impact.json) also
covers multi-digit manifest sequences and compares complete mirrors to pgoutput.

[Publisher fault qualification](../evidence/2026-11-14-push-publisher-crash-recovery.json)
passes 21 checks against a test-only instrumented destination. Actual backend
SIGKILL before upload completion, after upload and after commit exercises INSERT,
key changes and TRUNCATE respectively. Imported receipts survive every crash;
only a committed mirror transaction advances publication. Repeated import and
publisher restart preserve exactly the source rows, including non-idempotent
mutations. The existing publisher implementation is unchanged.

## Initial source-impact experiment

One local ARM64 run used 12 transactions of 20 rows per table, each with 256-byte
text, with persistent application psql sessions and separate source/destination
engines. Pull ran first; both paths drained after the writes. Observed source plus
source-engine CPU was 0.12 seconds for pull and 3.06 seconds for push. Source WAL
was 206,024 versus 534,184 bytes. End-to-end time was 0.67 versus 2.49 seconds;
push source maintenance added 0.83 seconds and 0.65 CPU seconds. Application commit
medians were about 0.49 and 0.50 milliseconds. These are a small experiment,
including observer overhead and process startup, not sustained latency or sizing
results. Physical object count is not object request count. The source upload and
metadata cost is material and does not support a low-overhead claim.

A [paced concurrent run](../evidence/2026-11-14-push-concurrent-source-impact.json)
uses 120 transactions of ten rows per table at 250 ms intervals, with concurrent
replication and two-second resource sampling. Pull completed in 30.47 seconds;
single-transaction push drained in 49.43 seconds. Source plus source-engine CPU
was 1.61 versus 59.68 seconds, and source WAL was 1.32 versus 5.17 MB. Median
application commit time was 1.42 versus 1.30 ms. Sampled source PostgreSQL PSS
peaked at about 44 versus 111 MiB; source-engine PSS was about 100 versus 123 MiB.
RSS sums also appear in the evidence but count shared pages repeatedly. This
remains one local, pull-first experiment with observer overhead, object counts
rather than request counts, and no production sizing claim.

## Optional source publication grouping

`lake_capture.streams.transactions_per_read` defaults to 1 and allows 1–32
complete native transaction envelopes. `publication_interval_ms` defaults to 0
and allows a 0–1000 ms coalescing interval before each read. Increasing these
settings trades capture latency for fewer source publication commits. Snapshot
activation remains one separate, coherent baseline transaction.

Each source transaction retains its own manifest, sequence, identity, digest and
receipt. The driver reserves upload attempts for the complete bounded group in
one durable metadata transaction, then publishes its manifests and changes in
one PostgreSQL transaction before advancing the slot. A single INSERT per
private log appends all accepted members, allowing the writer to pack their
rows together instead of opening a data file for each source transaction.
Manifests and receipts remain separate per source transaction. Any group publication
failure consumes the reserved attempts but exposes none of its manifests.
Cumulative retained/generation bytes, batch counts and attempt quotas are
checked for the entire group before reserving any member. A group that does not
fit stops before object I/O; repair the budget or reduce the group size to resume.
Reconciliation validates each historical raw envelope before slot advancement;
changing grouping after restart cannot change transaction identity.

The output plugin bounds an entire read to 64 MiB and 100,000 events, in addition
to per-transaction limits. If a multi-transaction window exceeds that budget,
the driver retries progressively smaller complete windows before reserving any
uploads. An individually oversized transaction still stops capture. The Rust
transport validates distinct message/chunk ordinals and a cumulative byte cap.
These are payload high-water limits; SQL/hex copies and engine memory add cost.

[Grouped qualification](../evidence/2026-11-14-push-grouped-publication.json) proves
twelve independent commits use one publication transaction and one data file
per populated event/manifest log, including ordered key changes and TRUNCATE.
It also forces read-window fallback and complete
historical replay. [Grouped crash qualification](../evidence/2026-11-14-push-grouped-crashes.json)
checks all-or-none visibility before/after uploads and after a lost commit
response. Grouping is opt-in; qualify its performance for the intended workload.

The private schemas are `lake_capture` for owner/control/receipts and
`lake_capture_log` for four Iceberg event/manifest/control tables and the frozen
registry. Public access is revoked; the capture owner writes, and the separate
consumer receives read-only log access. The pinned pg_lake Iceberg writer uses
Parquet with Snappy and a default 512 MiB approximate target file size. That
target is not a physical quota or a required chunk boundary. The stricter
64 MiB complete read/group payload budget applies before publication; normal
engine buffering and compression determine actual files. Reader-gated VACUUM
provides later compaction and delayed cleanup.

The [traced paced experiment](../evidence/2026-11-14-push-traced-source-impact.json)
predates physical log-file batching. It used the same 120-transaction workload with groups up to 32 and a one-second
interval. It observed 25 source publication commits, 12.51 source/engine CPU
seconds, 3.84 MB of source WAL and a 32.33-second drain. The corresponding pull
run used 1.97 CPU seconds, 1.32 MB WAL and 30.83 seconds. This is a separate local
run from the earlier single-transaction experiment, with different instrumentation
and retained exact inputs; it does not establish a production speedup or budget.

The trace observed 1,305 source-prefix requests and 870 destination-prefix
requests for push, versus 1,146 destination-prefix requests for pull. Maintenance
added 93 source-prefix requests. Bucket-wide/controller discovery is recorded
separately. Counts include observed API calls and protocol bytes by Parquet,
JSON and Avro file kind; they are not inferred from live object counts. The
collector witnesses readiness/completion and retains no headers, object paths
or bodies. Warm caches, publisher batching, compressible fixture text and trace
overhead affect these results; no billing/completeness guarantee is implied.
Source database growth and sampled frontiers/RSS/PSS are retained too. Actual
Parquet metadata confirms Snappy and the 512 MiB approximate file target.

After [packing grouped rows into one append per log](../evidence/2026-11-14-push-bulk-file-source-impact.json),
the same paced workload observed 27 publication commits and 27 data files per
event/manifest log before maintenance. Push used 9.58 source/engine CPU seconds,
2.60 MB WAL and 486 source-prefix requests, plus 99 maintenance requests; its
drain took 32.05 seconds. Pull used 1.86 CPU seconds, 1.32 MB WAL and 30.63 seconds.
The earlier unbatched-log run used 1,305 source-prefix requests. Group counts and
publisher scheduling differ between these individual runs, so the comparison
does not isolate every timing effect. Source overhead remains material.
The benchmark now retains pre-maintenance file counts and bytes separately
from the compacted file profile.

The [counterbalanced experiment](../evidence/2026-11-14-push-counterbalanced-source-impact.json)
repeats the 120-transaction paced workload in six fresh fixtures, three per
mode order. These trials preceded the explicit progress-request path and generated
no automatic barrier traffic. Median measured costs were:

| Order | Mode | Source + engine CPU seconds | Four database/engine CPU seconds | Drain seconds |
| --- | --- | ---: | ---: | ---: |
| Pull first | Pull | 1.82 | 14.19 | 30.77 |
| Pull first | Push | 9.58 | 20.03 | 31.80 |
| Push first | Pull | 1.67 | 14.50 | 30.67 |
| Push first | Push | 9.18 | 19.05 | 31.48 |

Push maintenance separately added median 1.21–1.22 database CPU seconds and
99 observed source-prefix requests. Capture itself observed 450–486 source-prefix
requests per run. Storage, host and controller CPU are excluded. Medians/ranges
are descriptive small-sample results; caches, catalog history, observer overhead
and compressible data remain part of the experiment. The results show material
source and overall database CPU cost in both mode orders.

An [earlier interrupted trial](../evidence/2026-11-14-push-counterbalanced-interruption.json)
had simultaneous source/destination log gaps of about 74 seconds and a WAL-sender
timeout while writes were still in progress, suggesting a host/VM suspension.
That matrix stopped without an aggregate. The complete six-fixture matrix ran
under an idle-sleep assertion limited to the benchmark process. Failed trials
now retain bounded writer/replication diagnostics plus wall and monotonic timings.

## Private storage identity

[Storage-location qualification](../evidence/2026-11-14-push-storage-locations.json)
checks that registration persists the four actual private foreign-table roots
in `streams.log_locations`, plus their common `object_prefix`. A session default
changed after extension installation does not describe or reroute those existing
tables. Publication and maintenance recheck the catalog roots under the source
generation fence before reserving work. A changed root stops both paths without
advancing capture or consuming new reservations. Restoring the qualified root
allows protected WAL to replay once; automatic relocation/rebind is unsupported.

The capture connection holds native access-share relation locks across SQL
transactions, including the separate VACUUM commands. Ordinary private-table
and schema-name DDL is rejected at command start while a stream exists, before
an exclusive lock can queue and block new readers. Target resolution preserves
the caller's search path; dependency queries use qualified catalogs. The native
helper reads invalidated catalog caches after waiting, so it cannot validate an
old statement snapshot against newly committed DDL. Exit cleanup releases its
session locks, and adaptive read retries reacquire locks released by an abort.
Metadata-only snapshot reservations avoid taking a second set of relation locks
behind DDL waiting on their publishing connection.

[Cleanup lease qualification](../evidence/2026-11-14-push-cleanup-leases.json) holds
a manual actor lease with an idle decoder slot. Reset/drop wait for that lease
before deleting logs or removing protected WAL. Cleanup and pruning also reject
storage-root drift. Administrative repair must restore the recorded original
catalog locations; changing defaults or repointing a running stream is not a
supported relocation workflow.

## Credential scope and rotation

[Storage credential qualification](../evidence/2026-11-14-push-storage-credentials.json)
passes 11 checks with separate source/destination MinIO users. Their IAM policies
limit object access to `iceberg/` and `destination/` respectively. Each engine can
read its own metadata; cross-prefix reads return no data and cross-prefix writes
fail. The destination importer needs no source object credential because it reads
through the source PostgreSQL catalog. These are deployment-level engine
credentials, not isolation between untrusted SQL operators within one engine.
The fixture administrator credential is used only by its storage initializer.
Cloud-provider IAM and production credential distribution remain separate gates.

[Credential rotation qualification](../evidence/2026-11-14-push-credential-rotation.json)
passes seven checks. The source child authenticates using SCRAM and an owner-only
credential file. Explicit stop, atomic file replacement and a new start generation
reload the password while retaining pending WAL. An obsolete password starts no
upload and cannot advance capture; repaired start replays once. Fixture password
values stay out of process arguments and failure diagnostics. Object credential
rotation is still unqualified.

## Optional destination companion and managed mode

[Managed-mode qualification](../evidence/2026-11-14-push-managed-mode.json) covers the
separately installed `lake_replication_push` companion. An explicit
`create_push_subscription` call requires `mode=iceberg_push`, a frozen two-table
registry, transport/binding configuration, a credential file, an explicit runtime
LOGIN and a new target schema. Creation is transactional and starts stopped.
Existing subscriptions appear as `logical_pull` in `subscription_modes`; they
are never converted automatically. The optional binding selects the log importer
in the existing supervisor; push bypasses pgoutput slot preflight and initial
copy. The importer still uses normal admission and managed generation fences.

The dedicated destination LOGIN uses the existing worker-grant recipe plus
companion grants to imported progress and new mirror objects. It cannot rewrite
stream identity. A source read-only log LOGIN supplies discovery; it does not
own a capture slot. Native ENABLE/DISABLE/DROP manage the same wrapper and child
lifecycle as pull. Destination push creation records no source-slot cleanup row,
and dropping it preserves the source capture slot and published mirror tables.
The companion cannot be removed while push subscriptions remain.

The run exercises simultaneous pull/push mirrors, separate published frontiers,
TRUNCATE and key-changing mutations, paused push with continuing pull, and pull
continuity after companion removal. Control-plane logical restore preserves the
binding but fences copied workers. As in the existing runtime, full restore of
user-created writable Iceberg tables refuses a non-empty live object location;
the control-only test uses empty placeholder relations and makes no claim to
restore mirror data or authorize rebind. Worker capacity must cover every active
database and managed subscription; the private multi-database fixture uses
`max_worker_processes=24`.

## Optional native artifacts

[Packaging qualification](../evidence/2026-11-14-push-packaging.json) verifies separate
source and destination ARM64 Linux/PostgreSQL 18 prototype archives. The source
archive contains only the source companion/capture worker. The destination one
contains the optional metadata companion, importer and push-capable supervisor;
it requires the matching qualified default destination bundle. Normal release
archives still build the default workers without enabling `push-prototype`.

`deploy/push/build-bundles.py` builds from explicitly selected cached prerequisite
images and records exact build inputs, immutable image IDs and payload hashes.
Archives include verified native Rust dependency notices, checksums and an
installer. Qualification stages installation in networkless disposable containers,
verifies executable contents/modes, and rejects corrupt payloads, an active
target server, a different PostgreSQL build and a different pg_lake runtime
marker. The native prototype installer pins the recorded PostgreSQL version
string and prerequisite runtime marker before writing files. Destination
installation also checks exact prerequisite core extension and default worker
hashes. No artifact was
installed on the host or deployed.

Installation prepares every replacement before publishing files with
same-filesystem renames. Injected preparation errors, replacement errors and
termination signals restore previous contents and modes and remove new files.
The Linux installer requires `flock` and preserves the lock across surviving
children. After SIGKILL during publication, the same extracted bundle's
`install.sh --rollback` explicitly restores the previous files while the server
stays stopped. It validates the prepared journal, bundle identity, allowed target
paths and backup checksums before any restoration. A failed repair retains its
backups for retry. Packaging qualification covers this interruption and a second
failure partway through rollback, plus active-lock, unsafe-target and corrupt-backup
refusal. After installation commits, `install.sh --finish` verifies every installed
payload byte and mode before removing remaining staging directories. A SIGKILL
after one staging directory was deleted, corrupt installed bytes, incorrect modes
and attempted rollback of a committed installation are covered by the
[cleanup recovery qualification](../evidence/2026-11-15-push-install-cleanup-recovery.json).
New journals record the complete staging plan before creating external staging
directories. After SIGKILL during planning or preparation, `install.sh --discard`
removes unpublished staging data without changing original files or modes. These
cases also refuse the wrong recovery actions. Interrupted journal deletion, power
loss and version upgrades still require inspection and separate qualification. This does not provide a
cross-filesystem atomic package transaction or automatic startup recovery.

See [the destination companion README](../../extensions/lake_replication_push/README.md)
for the opt-in API and package commands. Only the recorded native architecture
and runtime are qualified by these results.

## Native decoding and snapshot resource boundaries

[Spill qualification](../evidence/2026-11-14-push-native-spill.json) checks an actual
unfinished transaction with forced native PostgreSQL spilling. A 64 KiB spill
allowance stops decoding before commit and before any log upload reservation.
After the application commits, repaired capture replays the protected transaction
and publishes every row once. The test then stops the actual decoding backend
with native spill files present, kills it, and verifies crash recovery removes
those provisional files, aborts unfinished application work, preserves complete
capture progress and resumes exact publication.

`decode_work_mem_kb` defaults to 4096; `max_decode_spill_bytes` defaults to 256 MiB
per SQL decoding read. The output plugin checks PostgreSQL's live shared slot
spill counter before queued WAL changes. PostgreSQL resets the private reorder
buffer counter immediately after reporting each spill, so that private field is
not a usable high-water measurement. The plugin reads shared memory without SPI
or filesystem access; a counter reset during a read fails closed. This uses the
pinned PostgreSQL 18 native statistics ABI; see
[the PostgreSQL implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/reorderbuffer.c)
and [slot statistics](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/activity/pgstat_replslot.c).
The limit is checked at WAL-record granularity and measures PostgreSQL spill
accounting, not an absolute filesystem-byte quota. Native memory, serialized
output, transport/SQL copies and engine memory are distinct budgets. Source
status exposes cumulative decoder spill bytes/counts; SQL statement and lock
budgets remain in force. Configure server WAL retention and physical disk limits
with headroom; source history loss still requires controlled rebuild.

The [failure telemetry probe](../evidence/2026-11-14-push-failure-telemetry.json)
checks the dedicated nonsuperuser operator, native error-code classification,
restart history, obsolete actors and counter saturation.
`lake_capture.status` also exposes `capture_failure_count`,
`capture_conflict_count`, `last_failure_at` and `last_failure_sqlstate`. Failure
counts are cumulative for the registered stream and saturate at bigint's limit.
The conflict count covers capture failures with native SQLSTATE `23505`;
diagnostic text cannot impersonate that code. Starting a repaired configuration
clears the active `last_error` while preserving this history. Obsolete workers
cannot change the newer configuration's history. These writes are best effort
after rollback: backend/process crashes, failed connections and failures to
persist status can leave no increment. They are capture-failure counters, not
an exact count of failed uploads or object requests.

Destination `lake_replication.push_status` separates the source's
`observed_captured_sequence`, durable `imported_sequence` and derived
`published_sequence`. `observed_capture_to_import_batches` and
`imported_to_published_batches` expose the two backlogs. The view also reports
committed unpublished journal transaction/payload counts and configured journal
limits; payload bytes exclude indexes, physical storage and uncommitted streaming
segments. Limits are NULL when no journal policy row has been configured.

The importer records source observations only after a successful source read,
under the destination generation fence. Identity/generation mismatches and a
backwards observed capture frontier fail closed. These observations never admit
a transaction or acknowledge publication. Unobserved capture backlog is NULL,
not zero. `source_observed_at` is the destination time when the completed source
read was recorded; `source_observation_age_seconds` exposes stale observations.
The source snapshot may be older than that recording time, and these frontiers
are not an atomic cross-database snapshot. An unchanged frontier refreshes at
most once per five seconds while polling; journal backpressure can leave it
stale until another source read succeeds.

[Snapshot-limit qualification](../evidence/2026-11-14-push-snapshot-limits.json) checks
paused and online groups exceeding the row cap. Preflight counts only up to the
remaining allowance plus one before evaluating byte inventory. No partial
baseline, manifest or upload reservation is exposed; controlled cleanup removes
any provisional online slot. This does not add independently resumable or
unbounded baseline chunks.

[External text-key qualification](../evidence/2026-11-14-push-toast-keys.json)
uses actual out-of-line primary-key values. PostgreSQL can mark an unchanged
new key as external while supplying its historical old identity. The importer
resolves that marker from the transaction's old key or old full row, then applies
ordinary updates, key changes and deletes through the existing journal. Missing
historical identity is rejected; no current source-row lookup is used.

[Physical spill disk-full qualification](../evidence/2026-11-14-push-disk-full.json)
exhausts a 16 MiB disposable tmpfs mounted over one stopped native slot directory.
An unfinished source transaction reaches PostgreSQL's real ENOSPC error without
advancing complete capture or reserving uploads. Its application transaction can
commit because WAL and heap storage are separate. After space repair, the owned
slot replays protected history and the existing publisher applies all rows once.
This qualifies native spill storage exhaustion, not full PGDATA/WAL disk failure.
The fault fixture requires a mount capability inside its disposable container.

The refreshed [capture crash qualification](../evidence/2026-11-14-push-native-crash-reconciliation.json)
also restarts the entire source after a committed capture loses its response,
while slot confirmation still lags. The receipt survives, implicit startup is
fenced by the new postmaster binding, and explicit same-source rebind reconciles
without duplicate log events or final rows.

The [logical compatibility qualification](../evidence/2026-11-15-push-logical-compatibility.json)
runs the existing archive upgrade/restore, replication, interrupted initial-copy
and uninstaller cases on fresh volumes with the optional extension installed and
after removal: 55 checks in each state pass. Other logical test targets remain
separate release gates.

The [1,000-transaction comparison](../evidence/2026-11-15-push-sustained-1000-source-impact.json)
runs each mode for about 250 seconds at four transactions per second, with ten
rows per table per transaction, concurrent draining and observed S3 requests.
Both mirrors match. Source plus engine CPU is 118.0 seconds for push versus 14.0
for pull (8.4 times); all database/engine CPU is 207.6 versus 131.0 seconds (1.59
times). Push p99 application commit time is 4.79 ms versus 8.34 ms for pull in
this single order-dependent local run. Source maintenance adds 2.92 CPU seconds.
This extends transaction count and duration; it does not establish long-duration
stability, cold-cache behavior or production sizing. Push still has material
primary-side CPU cost, so these results do not support a low-overhead claim.

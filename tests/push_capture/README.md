# Source capture feasibility probe

Run from the repository root with Docker Compose and cached prerequisite images:

```sh
PUSH_PG_LAKE_IMAGE=pg-lake-runtime:arm64-b6794e8209f18b90cf80 \
PUSH_STORAGE_IMAGE=lake-replication-fixture-minio:0d7408fc9969-033deb905017-arm64 \
PUSH_STORAGE_INIT_IMAGE=lake-replication-fixture-mc:b00526b153a3-033deb905017-arm64 \
python3 tests/push_capture/probe.py
```

These are local ARM64 cache names, not published image references. Select the
corresponding native pinned images on other hosts. Build the supported runtime
using [the prerequisite instructions](../../deploy/pg_lake/README.md) and the
storage fixtures using [storage preflight](../qualification/storage_preflight.py).
The probe verifies the runtime revision, patch checksum and installed libraries;
it records immutable image IDs and input hashes. It does not build or pull images.

Each invocation provisions a unique Compose project with its own PostgreSQL 18
source, pgduck engine, MinIO bucket and volumes. It installs pg_lake on the source
without installing lake_replication. No ports are published. Local trust
authentication and the checked-in object-store credentials are disposable fixture
settings. It always attempts to remove only that project's containers and volumes.
If interrupted externally, remove the printed project with
`docker compose -p PROJECT -f deploy/push/compose.yml down -v`, using the same
image environment variables.

`fixture.sql` creates two private Iceberg logs, an Iceberg manifest and ordinary
PostgreSQL receipts/progress. A locked progress row serializes batch publication;
a PostgreSQL unique batch identity arbitrates replay before Iceberg appends.
Synthetic batches use JSON arrays and SHA-256 over PostgreSQL's JSONB text. This
encoding is a feasibility aid; it is not the versioned replication protocol.
The fixture appends two selected-table logs and all control state in one
transaction. A reconnect queries durable receipts before retrying. Conflicting
content or a backwards frontier fails closed. WAL-coordinate gaps do not create
manifest-sequence gaps.

Coverage includes commit, explicit rollback, SQL error after appending, backend
termination before commit, session termination immediately after commit,
identical/conflicting replay, concurrent duplicate submission, and manifest
count/digest checks. Readers use source PostgreSQL/pg_lake in a repeatable-read
transaction. The separate instrumented fault harness qualifies a pinned concurrent read view.
The postcommit test deliberately kills the session after COMMIT and ignores its
append response; it does not simulate a network failure inside COMMIT.

Results and container logs go to `.qualification/lake-push-probe-*/`. Each run
retains exact input sources, hashes, runtime identity and isolated cleanup status.
SQL-only feasibility is distinct from native source qualification.

## Native capture and importer probes

Build the source image with `deploy/push/Dockerfile.capture` as described in
[the companion README](../../extensions/lake_replication_capture/README.md).
Build the destination importer on an archive-installed destination test image:

```sh
docker build --pull=false --build-arg BASE_IMAGE=YOUR_DESTINATION_TEST_IMAGE \
  -f deploy/push/Dockerfile.importer -t lake-push-importer:prototype-v1 .
export PUSH_PG_LAKE_IMAGE=lake-push-capture:prototype-v1
export PUSH_DESTINATION_IMAGE=lake-push-importer:prototype-v1
# Keep PUSH_STORAGE_IMAGE and PUSH_STORAGE_INIT_IMAGE set as above.
python3 tests/push_capture/live.py
python3 tests/push_capture/lifecycle.py
python3 tests/push_capture/snapshot.py
python3 tests/push_capture/online_snapshot.py
python3 tests/push_capture/snapshot_limits.py
python3 tests/push_capture/scalars.py
python3 tests/push_capture/mutations.py
python3 tests/push_capture/chunks.py
python3 tests/push_capture/spill.py
python3 tests/push_capture/retention.py
python3 tests/push_capture/upload_budget.py
python3 tests/push_capture/roles.py
python3 tests/push_capture/credentials.py
python3 tests/push_capture/storage_credentials.py
python3 tests/push_capture/managed.py
python3 tests/push_capture/maintenance.py
python3 tests/push_capture/outages.py
python3 tests/push_capture/benchmark.py
python3 tests/push_replication/probe.py
```

Each script creates its own project. Source and destination use separate pgduck
instances and staging/socket volumes. The shared schema can be installed as a fixture or through the optional
`lake_replication_push` companion. Default release installation does not enable
push mode. `managed.py` uses the companion and the native lifecycle.

`live.py` compares INSERT/abort results to independent pgoutput. `lifecycle.py`
checks clone/restart/drop/reset fencing, selected-table DDL and missing history.
`snapshot.py` tests paused writers, atomic activation and interrupted recopy.
`scalars.py` covers scalar limits, quoted/compound keys and INCLUDE columns.
`toast_keys.py` covers external text primary keys, historical old-key resolution,
key changes, deletion and replay.
`reference.py` compares the native source path against independent pgoutput
for interleaved commits, aborted work, spilled historical TOAST, key changes,
TRUNCATE and complete historical replay while checking frozen DDL behind capture.
`snapshot_faults.py` runs actual upload/COMMIT backend crashes around paused and
online activation; run it through `push-native-fault-test` with the instrumented
source image.
`grouped.py` checks opt-in complete-transaction publication groups, numeric
message/chunk ordering, bounded read-window fallback and destructive replay.
The grouped probe also checks one physical append per populated log across
twelve source transactions. `grouped_faults.py` adds upload/COMMIT crashes and changed replay group sizes;
it is included in `push-native-fault-test`.
`progress.py` checks owner-authorized control-only barriers, aborted and mixed
transactions, zero-row journal/publication, replay, quotas, pruning and reset.
`progress_faults.py` crashes real source backends around its uploads/commit and
checks request completion atomically with manifests. `PUSH_NATIVE_PROBES` can
select native fault probes when rechecking a subset.

For a bounded paced resource experiment, set `PUSH_BENCH_CONCURRENT=1`,
`PUSH_BENCH_TRANSACTIONS=120`, `PUSH_BENCH_ROWS=10` and
`PUSH_BENCH_INTERVAL=.25` when running `benchmark.py`. Optional
`PUSH_BENCH_CAPTURE_GROUP` (1–32) and `PUSH_BENCH_CAPTURE_INTERVAL_MS` (0–1000)
select source grouping. Memory samples distinguish process RSS/PSS sums from
cgroup memory and its lifetime peak. Object counts do not count S3 requests.
Set `PUSH_BENCH_OBJECT_TRACE=1` to aggregate observed S3 API/byte counts by
source/destination prefix and Parquet/JSON/Avro kind, including maintenance.
The private trace container uses only the disposable fixture administrator;
request headers, paths and bodies are not retained. Witness object/list calls
are excluded, while bucket-wide/controller discovery remains separate.
The benchmark also checks actual Parquet codec metadata and the file target.
It samples CPU/RSS/PSS for both PostgreSQL servers and both engines; database CPU
totals exclude storage, the host and controller. `PUSH_BENCH_ORDER=push_first`
reverses the default order, performs source maintenance and controlled capture
cleanup before pull, and verifies that no capture slot remains. This does not
clear database or host caches.

`python3 tests/push_capture/benchmark_matrix.py --repetitions 3 --trace` runs
three fresh fixtures per order (six fixtures total), alternating the first order
between repetitions. Each fixture compares the same bounded paced workload and
retains its full evidence. The matrix records descriptive median/min/max values
by mode/order plus maintenance costs; it makes no statistical significance or
production sizing claim. Run timed matrices without competing builds/probes.
Use `--repetitions 1 --transactions 4 --rows 2 --interval .05
--capture-interval-ms 100 --trace` for a smoke check.
Failed trials retain partial samples, application commit counts, bounded source
and destination diagnostic snapshots, and the complete pull-worker transcript.
The matrix stops on a failed fixture and keeps its earlier trials without
publishing an aggregate. `benchmark_failures.py` verifies this diagnostic path
and disposable fixture cleanup. Wall-clock and monotonic elapsed values are
retained to help identify host/VM suspension during a timed run.

`locations.py` verifies actual catalog roots rather than the session default,
then injects a superuser-only foreign-catalog change in an isolated database.
It checks capture/maintenance fencing and exact repair without writing at the
altered destination. Ordinary operators cannot rewrite the pinned roots.
It also tests immediate DDL rejection, caller search-path/catalog shadows,
unrelated non-superuser extension ownership, post-wait catalog visibility and
complete lock release. Superuser-only fault steps temporarily disable the DDL
guard to exercise the lower-level fence. Snapshot probes likewise inject a
queued private lock and show budget reservation/publication can finish before
the test cancels that waiter and admits new readers.
`lease_cleanup.py` holds a manual actor lease over an idle slot and verifies
the manager completes a cleanup attempt without dropping/resetting its state
until the lease is released.
`disk_full.py` uses a 16 MiB tmpfs and a real mount over one stopped disposable
slot directory to provoke native spill ENOSPC. Its dedicated Compose override
adds `SYS_ADMIN` only to the isolated source container for that mount; host,
application and WAL storage are not filled. It checks failure, retained history,
space repair and exact publication. Do not use this override for deployments.
`mutations.py` covers historical unchanged TOAST, keys, NULLs, delete/reinsert,
TRUNCATE ordering and replay. `retention.py` tests two publication consumers,
pruning gaps and finite log/generation limits. `upload_budget.py` injects failed
reservations plus failed error-status writes to check fatal retry fencing.
`telemetry.py` checks native failure-code classification, cumulative counters,
restart history and obsolete-worker fencing.

The fault harness requires an instrumented source runtime:

```sh
docker build --pull=false --build-arg BASE_IMAGE=pg-lake-runtime:arm64-b6794e8209f18b90cf80 \
  -f deploy/push/Dockerfile.faults -t lake-push-faults:8180647-v1 .
docker build --pull=false --build-arg BASE_IMAGE=lake-push-faults:8180647-v1 \
  -f deploy/push/Dockerfile.capture -t lake-push-capture-faults:prototype-v1 .
PUSH_PG_LAKE_IMAGE=lake-push-capture-faults:prototype-v1 python3 tests/push_capture/native_faults.py
```

The instrumented image is test-only. See [implementation status](../../docs/push-replication.md)
for retained results and remaining release gates.

`maintenance.py` protects pinned readers, checks importer exclusion, and observes
actual delayed object deletion in the private MinIO volume. `outages.py` stops
storage and source services to verify bounded failure and exact recovery.
`benchmark.py` records a small sequential pull/push source CPU/WAL, commit-time
and object-count experiment including a maintenance cycle. Its methodology and
limitations are retained in results; it makes no production performance claim.

For destination publisher crash tests, instrument the archive-installed importer
image independently and select it only for the fault probe:

```sh
docker build --pull=false --build-arg BASE_IMAGE=lake-push-importer:prototype-v1 \
  -f deploy/push/Dockerfile.faults -t lake-push-importer-faults:prototype-v1 .
PUSH_DESTINATION_IMAGE=lake-push-importer-faults:prototype-v1 python3 tests/push_capture/publisher_faults.py
```

`make push-prototype-test` runs the normal native/synthetic probes with selected
cached images. The separate `push-native-fault-test`, `push-publisher-fault-test`
and `push-benchmark` targets use the image environment supplied by the caller.

The managed fixture qualifies a dedicated destination worker, simultaneous
pull/push, native lifecycle, control-only dump/restore and copied-binding fencing.
Full writable Iceberg restore at a live object location remains rejected. The
multi-database fixture sets max_worker_processes=24; eight workers can prevent a
second database's launcher from starting and therefore delay its diagnostics.

`spill.py` verifies actual native spill accounting before application COMMIT,
replays after budget repair, and kills a stopped source backend with provisional
spill files present. PostgreSQL recovery must remove them and keep unfinished
transactions outside complete batches. This is distinct from a physical
filesystem-full simulation or an absolute disk-byte quota.
After confirmed slot advancement, it checks eventual spill cleanup between
completed decoding contexts: a subsequent active SQL peek can already be spilling
again from an older restart point. PostgreSQL's
[reorder-buffer cleanup](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/reorderbuffer.c)
removes provisional files when a decoding context is freed.

## Ordered schema qualification

Frozen mode remains the default. `schema_history.py` and `ddl_snapshots.py` check
registration-time definitions and current-snapshot DDL fences. The descriptor
and native fixtures check bounded cross-language encodings, historical row widths,
large TOAST audit payloads and explicit version initialization.

`schema_audit.py`, `schema_destination_history.py`,
`schema_destination_admission.py` and `schema_destination_publish.py` cover
transactional audit, immutable target scope, late quota rollback, per-event
versions, DDL-only publication, reader exclusion and whole-batch rollback.
`schema_live.py`, `schema_managed.py` and `schema_mutations.py` run the actual
pipeline, guarded activation, ordinary application ownership, dedicated runtime
logins, aborted DDL subtransactions, renamed keys and quoted UTF8 identifiers.

Source crash images also run `schema_capture_faults.py`. Destination crash images
run `schema_publisher_faults.py`; the latter is included in
`make push-publisher-fault-test`. Both inject actual backend SIGKILL around upload
and COMMIT and retain restart/replay evidence. These opt-in test images must not
be distributed as release artifacts. See the [activation contract](../../docs/push-schema-evolution.md).

The empty ordered baseline regression is included in `make push-prototype-test`.
`make push-logical-compatibility-test` runs the existing archive upgrade, logical
replication, interrupted initial-copy and uninstaller cases twice on fresh private
volumes: with the optional extension installed, and after it has been removed.
Set `PUSH_LOGICAL_SOURCE_IMAGE` to a cached qualification source image and
`PUSH_DESTINATION_IMAGE` to the optional destination bundle image. The runner pins
both image IDs before startup and preserves each phase's container logs. This
supplements the other logical qualification targets; it does not replace them.

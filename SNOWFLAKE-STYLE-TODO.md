# TODO: optional source-push replication for lake_replication

Scope correction, 15 November 2026: the primary goal is a **single PostgreSQL
instance mirroring its own application data directly into Iceberg**. The earlier
checklist below implemented an optional two-instance Iceberg transport and is
retained as historical work; that transport is not required for the corrected
local deployment. The local path uses existing WAL decoding, a journal and the
publisher in the same database, with no intermediate Iceberg change logs or
importer. See [local deployment and qualification](docs/local-iceberg.md).

Current local gates:

- [x] Qualify consistent initial copy, streamed mutations and rollback in one database.
- [x] Qualify local stop/start, shared-instance restart, nullable-column schema evolution and cleanup.
- [x] Verify explicit application-table selection prevents replication feedback.
- [x] Measure bounded single-instance CPU/memory, WAL, observed object operations and publication-latency upper bounds with fresh application-only baselines. In the 120-transaction local experiment, opt-in 1-second coalescing reduces CPU from 19.72 to 9.33 seconds and observed S3 requests from 1,442 to 942; sustained sizing remains open.
- [x] Qualify managed initial-copy interruption with concurrent writes, local streaming disconnect and replay, mirror-reader isolation, blocked publication and object-store outage in bounded fixtures. Earlier real publication backend-crash boundaries pass 27 checks; current-core runtime probes add managed recovery and contention evidence.
- [x] Add opt-in publication coalescing with bounded delay, responsive stop/restart and early flushing under journal transaction/byte pressure.
- [x] Qualify native local mirror compaction and bounded, checkpoint-safe journal cleanup followed by continued replication. Final-runtime publication crash tests (27 checks) and default logical compatibility cases (54 checks) pass.
- [x] Extend the local comparison to 1,000 paced transactions per mode with control-plane publication observation; mirrors match and application p99 remains around 8 ms in that bounded run.
- [x] Correct JSONB WAL publication and qualify supported scalar/compound-key/TOAST values under primary-key and full-row identity.
- [x] Qualify one controlled flow under explicit CPU/RAM caps (PostgreSQL 512 MiB, engine 1 GiB), with journal admission restricted to one transaction. Smaller internal query-budget attempts fail and are retained as unsupported configurations.
- [x] Map bounded transaction-shape trade-offs and add opt-in independent INSERT grouping with native target/DDL/permission fences. Guard tests (18 checks), grouped publication crash recovery (29 checks) and default logical compatibility (54 checks) pass on the final runtime; timed comparisons are recorded separately.
- [x] Qualify a bounded local WAL-retention overrun: lost history reports a critical source error without advancing checkpoints or replacing its invalidated slot; application commits continue and an explicit fresh snapshot restores streaming while preserving the previous mirror (17 checks). Deployment-specific disk and power-loss qualification remains open.
- [x] Extend grouped local mirroring to 10,000 transactions at an offered 100 transactions/second: exact 40,000 rows per table and bounded-run recovery after a 4.61-second drain. This does not establish a production capacity maximum.
- [x] Qualify actual WAL ENOSPC on a private 128-MiB filesystem: shared PostgreSQL stops, committed heaps survive, and freeing space/restarting restores exact mirroring and continued writes (12 checks).
- [x] Qualify bounded full-PGDATA ENOSPC: a failed application table extension aborts without changing committed heaps; freeing space restores exact mirroring and continued writes (12 checks).
- [x] Compare same-instance mirroring against remote logical pull with identical runtime/batching/storage, INSERT and mixed workloads, fresh volumes and reversed-order repetitions (eight fixtures, 56 checks). Total CPU is similar in these bounded runs; remote pull moves most replication CPU/memory/WAL away from the application side. See [comparison](docs/local-pull-comparison.md).
- [x] Profile publication phases and add ordered typed DELETE key filtering. Reversed-order local comparisons show 21% lower median CPU on small mirrors and 35% with 20,000 existing rows/table; 189 packaged qualification checks and `make check` pass. See [optimization report](docs/local-delete-filter.md).
- [x] Enforce opt-in coalescing between live-stream publications, preserve pressure and receiver-exit draining, and expose flush reasons. Counterbalanced mixed workloads use about 31% less CPU and 16% fewer mirror requests, with a higher sampled publication-delay upper bound. See [window report](docs/local-coalescing-windows.md).
- [x] Profile UPDATE costs and add guarded convert-once handling for wide multi-row runs. Counterbalanced 48-column/32-row pipeline trials show about 10% lower median CPU; narrow paths are retained. Exact types, unchanged TOAST, schema evolution, fallback and wide crash recovery pass. See [UPDATE report](docs/local-update-conversion.md).
- [ ] Qualify production-scale sustained contention, general memory admission, deployment-specific disk/power-loss recovery and production sizing before release.

Date: 13 November 2026. Planning baseline: lake_replication commit `86666533fc597b75741f398e17305153eca55714`. Checked items have implementation and retained prototype evidence; unchecked items remain release or scope-expansion gates.

Implementation started in this repository on 13 November 2026, from worktree
HEAD `d5e05d2780796dd9979e99a08edfdf57c2266a3d` with existing local changes.
See [implementation status and compatibility matrix](docs/push-replication.md)
and [the source SQL feasibility probe](tests/push_capture/README.md). Source SQL
crash/read-view checks, the shared refactor regression gates and the synthetic
importer now pass. The native vertical slice, bounded paused/online baselines,
mutations, conservative retention and pre-upload budgets also pass. Production
API/packaging, scoped source roles and bounded physical maintenance now have
prototype evidence. Bounded opt-in ordered DDL now passes; remaining production
qualification gates stay open.

## Objective and compatibility contract

Add a Snowflake-style source extension that decodes committed changes and publishes transactional Iceberg change logs through pg_lake. Import those logs into lake_replication's existing destination journal, then use the existing publisher to materialize the mirror.

This is an **additional mode**. The existing destination-pull pgoutput mode remains the default, supported, and independently installable. Source-side logical decoding remains the change-capture mechanism in both modes; the new mode changes deployment and transport. It does not implement physical-WAL decoding or require a shadow PostgreSQL.

Reference: Snowflake describes separate decode/capture/apply stages, per-table change logs, an ordered metadata log, and source-side schema/snapshot coordination. This checklist adapts that architecture; it does not assume its implementation or all guarantees are available from stock pg_lake. [Engineering article](https://www.snowflake.com/en/blog/engineering/postgres-to-snowflake-replication-mirroring/).

```text
Existing default:
  source pgoutput -> existing receiver -> destination journal -> publisher -> Iceberg mirror

New optional mode:
  source decoding worker -> source capture worker using pg_lake
    -> Iceberg change logs + ordered manifest/control log
    -> new log importer -> existing destination journal -> publisher -> Iceberg mirror
```

## 0. Lock down scope and record the baseline

- [x] Record source/destination PostgreSQL builds, pg_lake revision/runtime patchset, object-store configuration, source commit, and artifact hashes.
- [x] Run the existing logical replication correctness/recovery tests as a before-change baseline.
- [x] Define proposed mode names: `logical_pull` (default) and `iceberg_push` (optional). Keep source mode separate from the existing `lake_target = 'iceberg'` destination option.
- [x] Keep existing subscription syntax, permissions, checkpoints, logical slot behavior, and installation requirements compatible.
- [x] Scope the first prototype to PostgreSQL 18, one source database, one fixed two-table group, supported non-null primary keys, frozen schema, and the qualified pg_lake local PostgreSQL catalog.
- [x] Record that the new mode requires source installation privileges, pg_lake/object-store access on the source, and a source resource budget. Existing mode requires no new source extension.
- [x] Exclude physical decoding, REST-catalog guarantees, live views, automatic table discovery, online mode switching, and automatic failover from the first prototype.

**Gate:** an explicit compatibility matrix and reproducible logical-mode baseline exist before shared code changes.

## 1. Prove source-side transactional capture with SQL

Do this before building a custom decoder.

- [x] Create private source-side Iceberg change tables and a manifest table using the pinned pg_lake build.
- [x] Create ordinary PostgreSQL capture-state tables alongside them.
- [x] Demonstrate one transaction appending changes to both selected-table logs, adding a complete batch manifest, and advancing capture state.
- [x] Verify rollback leaves no committed batch or advanced checkpoint.
- [x] Kill the capture backend before commit, during pg_lake upload/catalog boundaries, and after commit with a lost response.
- [x] Reconnect and resolve commit outcome using the durable batch identity/checkpoint before retrying inserts.
- [x] Define the first read path: importer queries logs through source PostgreSQL/pg_lake in a consistent read transaction. This avoids introducing an unqualified external catalog reader.
- [x] Record that this prototype still needs source-catalog availability to import. Object storage does not by itself provide a standalone discovery/coordination service.

**Gate:** local-catalog log writes and capture progress are atomic and ambiguous commits are recoverable on the source deployment. Existing destination-side evidence is not sufficient proof.

## 2. Define the durable event and manifest protocol

- [x] Define stream identity: immutable stream UUID, source system identifier/database identity, and persisted capture generation.
- [x] Define table registration identity independently of SQL name and reusable PostgreSQL OID.
- [x] Define ordered event coordinates: commit/end LSN plus transaction event ordinal, schema version, and table registration identity. Do not order by timestamp or XID alone.
- [x] Represent INSERT, UPDATE, DELETE, TRUNCATE, snapshot lifecycle, and schema/control events, even if the first implementation rejects some of them.
- [x] Preserve SQL NULL, absent columns, unchanged values, old keys, and full old rows as distinct cases.
- [x] Define manifests with stream sequence, batch ID, source coordinates, transaction boundaries, table/schema references, expected counts, and deterministic payload digests.
- [x] Specify a contiguous manifest sequence: source LSN gaps caused by unrelated WAL are not missing batches.
- [x] Define bounded row/file chunks and oversized-transaction behavior. A chunk is not a committed source transaction.
- [x] Start with small scalar types and prototype payload encodings; avoid designing a universal type system before a working vertical slice.
- [x] Version the protocol; reject unknown versions and same-identity/different-content replay.

**Gate:** synthetic events roundtrip into the existing journal with stable identity despite different transport chunking.

## 3. Extract shared staging interfaces without changing logical mode

- [x] Extract source-neutral row/transaction types from `worker/src/journal.rs`, which currently imports pgoutput types.
- [x] Extract reusable admission/staging helpers from `worker/src/main.rs`; retain the existing pgoutput decoder and receiver as their first caller.
- [x] Reuse existing relation/type registration, large-transaction staging, byte/count admission, managed generation fencing, and journal deduplication.
- [x] Keep the existing publisher and pg_lake apply functions as the only final-mirror write path.
- [x] Ensure new metadata tables and migrations preserve existing dump/restore, upgrade, stop/start, and clone-binding rules.
- [x] Run the logical baseline after this refactor before adding source workers.

**Gate:** the shared refactor produces no logical-mode behavior change or new mandatory source dependency.

## 4. Add an optional source extension and lifecycle

- [x] Add a separately installable companion extension, provisionally `lake_replication_capture`, with its own control schema. Do not require installing it for existing pull subscriptions.
- [x] Provide registration, start, stop, status, and controlled-reset functions for capture streams.
- [x] Integrate source worker registration/reaping with the existing pg_extension_base lifecycle approach, with source-specific role and identity checks.
- [x] Make source registration transactional: a rolled-back registration must not start durable capture work.
- [x] Persist desired state, configuration generation, owner, slot name, object-store destination, and selected-table definitions.
- [x] Fence duplicate workers and stale generations; copied/restored source state starts stopped until explicitly rebound.
- [x] Give capture workers scoped privileges and object-store credentials; keep credentials out of process arguments, payload logs, and exported manifests. Dedicated PostgreSQL operator/read-only consumer grants pass; Deployment-level source/destination prefix isolation passes; untrusted operators sharing one engine remain outside scope.
- [x] Confirm no application DML trigger or commit callback synchronously performs object-store uploads.

**Gate:** start/stop/restart/drop and failed registration leave no duplicate worker, accidental upload, or orphan slot.

## 5. Implement source-side logical decoding

- [x] Use PostgreSQL's logical decoding/output-plugin APIs and historic catalog handling. Do not implement an independent physical tuple decoder.
- [x] Create one owned persistent logical slot per capture stream; require logical WAL and a qualified replica identity.
- [ ] Implement an output plugin/decoder worker that produces versioned row/schema descriptors and provisional bounded file chunks.
- [x] Keep object-store writes and pg_lake SQL out of decoding callbacks; let the capture worker own publication transactions.
- [x] Preserve transaction/subtransaction ordering; discard aborted work and keep in-progress streamed fragments invisible.
- [x] Handle multi-row INSERT, interleaved transactions, and commit order explicitly.
- [x] Separate decoded position from durable capture position. Decoded temporary files do not authorize slot advancement.
- [x] Rebuild lost provisional files from protected WAL after restart; reject missing required history.
- [x] Bound native reorder-buffer memory/spill accounting, encoded transaction size and SQL execution time; fail with a diagnostic rather than exposing a partial transaction. These are high-water/record-boundary limits, not absolute process RSS or filesystem quotas.
- [x] First implement INSERT plus abort handling. Reject UPDATE/DELETE/TRUNCATE on selected tables until their implementation milestone passes.

**Gate:** committed source events match a reference pgoutput run for the supported workload; aborted rows never enter a complete capture batch.

## 6. Implement transactional capture into Iceberg logs

- [x] Let the capture worker finalize batches only on complete source-transaction boundaries.
- [x] Append per-table changes and ordered manifest/control records through pg_lake, and advance source capture state in that same PostgreSQL transaction.
- [x] Reconcile prior batch identity after an ambiguous commit before replaying any append.
- [x] Advance the logical slot only after durable capture or confirmed reconciliation. Retain PostgreSQL's necessary decoding/catalog history.
- [x] Add durable control-only progress for idle/no-selected-change periods where needed; do not treat an observed WAL-end pointer as a decoded safe frontier. The owner can enqueue a bounded transactional progress request after baseline activation; its actual decoded commit becomes a control-only manifest and an ordinary zero-row journal/publication transaction. Requests are explicit rather than automatic idle heartbeats.
- [x] Define private log schemas, ownership, file sizing, compression, and a starting time/byte batch policy. The qualified runtime uses Parquet/Snappy and an approximate 512 MiB file target; complete read/publication payloads are capped at 64 MiB/100,000 events. Optional grouping allows 1–32 complete transactions and a 0–1000 ms coalescing interval; defaults preserve single-transaction capture.
- [x] Prevent capture recursion: logs, manifests, and capture-state writes must not become captured user changes.
- [x] Prove a stopped importer does not stop capture until the configured log-retention/storage limit is reached. Retained payload and generation/attempt budgets now fence source uploads; these do not measure physical object bytes.

**Gate:** two-table batches survive capture restarts and lost acknowledgements without gaps or duplicate live log rows.

## 7. Add the destination log importer

- [x] Add an importer executable as an alternative to the current receiver, not a replacement for it.
- [x] Read manifests in stream sequence and validate generation, source identity, schemas, counts, and digests before accepting a complete batch.
- [x] Pin a consistent log read view. Never combine arbitrary latest per-table Iceberg snapshots and assume group consistency.
- [x] Register normalized relations and feed transactions through the shared journal APIs in source commit order.
- [x] Persist imported progress atomically with complete destination journal admission. For segmented transactions, advance only at durable committed completion.
- [x] Reconcile interrupted imports and identical replay; reject conflicting replay.
- [x] Integrate existing journal admission and generation fencing; an importer must not bypass managed-worker controls.
- [x] Leave publisher batching, target mutation behavior, and publication checkpoint atomicity unchanged.
- [x] Expose separate captured, imported/staged, and published frontiers. A running importer is not evidence of a current mirror.

**Gate:** synthetic and live source logs materialize the same rows through the existing publisher as direct logical capture, including multi-table transactions.

## 8. Add bootstrap and controlled resnapshot

- [x] First prove empty-table activation under an explicit write/DDL pause with a documented baseline boundary.
- [x] Add existing rows through a paused, coordinated snapshot; make baseline activation and subsequent change replay unambiguous.
- [x] Then implement online snapshot/slot alignment using a documented PostgreSQL consistency protocol. Do not pair an unrelated snapshot with a sampled current WAL LSN.
- [x] Persist snapshot generation, chunk inventory, schema versions, completion, and activation events in the ordered control stream.
- [x] Build destination generations privately and publish all selected tables at one coherent transaction boundary.
- [x] On incomplete snapshot restart, either prove resumability or discard the incomplete generation and recopy.
- [x] On slot invalidation/history loss, stop and create a new snapshot generation; never skip ahead silently.
- [x] Leave table addition/removal and mode changes as controlled rebuilds initially. Reject activating a second writer against an existing mirror.

**Gate:** concurrent source writes around snapshot completion create neither overlap duplicates nor missing rows; interrupted activation cannot expose a partial group.

## 9. Complete mutations, TOAST, and ordered schema support

- [x] Add UPDATE/DELETE, key changes, repeated changes to one key, and DELETE/reinsert; preserve existing exactly-one-match/type semantics where applicable.
- [x] Test unchanged TOAST markers, large values, NULL transitions, and inline/external changes. Do not fetch today's source row to reconstruct historical values.
- [x] Add TRUNCATE as an ordered transaction barrier.
- [x] Enforce a selected-table DDL guard before claiming fixed-schema safety; a README restriction alone is insufficient.
- [x] Add source-side DDL capture with normalized before/after definitions and stable table/column identities. Initial definitions are recorded under registration locks; the controlled activation API requires a stopped initialized actor and lease, with explicitly capable consumers.
- [x] Preserve DML–DDL–DML and DDL-only transaction ordering through decode, capture, import, and destination apply. Optional ordered publication commits schema, data, apply markers and checkpoint together; the ordinary publisher fails closed.
- [x] Start with explicitly safe nullable additions/renames. Stop on unsupported defaults, type conversions, rewrites, or table lifecycle changes. Generations are bounded to 32 revisions per table; initial nullability is preserved.
- [x] Test schema changes occurring while decoding is behind. Retained definitions and receipt versions at the confirmed slot boundary handle delayed decoding and real lost-COMMIT/backend crash replay without current-catalog reconstruction.

**Gate:** supported DDL/mutations reproduce exact data and schema; unsupported operations cannot advance capture/application past an unrepresentable change.

## 10. Retention, resource limits, and source impact

- [x] Maintain independent decoded, captured, imported, and published progress; document which frontier protects each artifact.
- [x] Define retention acknowledgements for every registered consumer. Never expire a log needed by a lagging consumer or active snapshot.
- [x] Initially prune conservatively through confirmed destination publication; design any earlier imported-journal retention policy separately.
- [x] Distinguish logical log-row retention from Iceberg snapshot expiry and physical object cleanup.
- [x] Schedule log compaction, snapshot expiry, and delayed orphan cleanup; verify that readers and replay references remain valid.
- [x] Set native spill/log/WAL high-water marks and recovery behavior when budgets are exhausted. Physical filesystem quota/outage tests remain separate.
- [ ] Add source-to-capture lag, capture-to-import lag, journal-to-publish lag, WAL retained bytes, spool bytes, log bytes, upload failures, conflicts, and resource-use metrics. Source status exposes retained WAL, capture lag, connections, attempts and retention acknowledgements, plus best-effort cumulative capture failure/conflict counts and last failure time/SQLSTATE. Destination status separates observed capture/import/publication frontiers, observation age and committed pending journal counts/bytes. Full error/resource telemetry remains open.
- [ ] Benchmark primary CPU/RSS, write latency, WAL volume, object operations, metadata growth, and total end-to-end cost against direct logical pull. Bounded burst and 120-transaction paced comparisons now record source/destination CPU and sampled RSS/PSS, transaction timing, WAL/database growth, observed S3 API/byte counts, frontiers and maintenance. Six fresh counterbalanced fixtures provide three runs per mode order with descriptive medians/ranges. A subsequent 1,000-transaction run per mode at four transactions/second also passes, with push using 8.4 times source/engine CPU and 1.59 times total database CPU in that local run. Long-duration/load, cold-cache and production sizing experiments remain.
- [x] Treat low source overhead as an experiment outcome, not an assumption. Source pg_lake installation and uploads have a real resource cost.

**Gate:** a stalled destination cannot cause unbounded silent source resource consumption, and published latency/resource results include maintenance costs.

## 11. Fault tests and additive release qualification

- [x] Decoder crash with an unfinished transaction and partial spool files.
- [x] Capture crash before upload, after upload/before PostgreSQL commit, and after commit/before slot advancement.
- [x] Lost capture COMMIT response and slot replay after source restart.
- [x] Importer crash before/after journal admission and before imported checkpoint completion.
- [x] Existing publisher crash/ambiguous commit while consuming push-mode transactions.
- [x] Object-store outage, source-catalog outage, destination outage, disk-full/high-water events, and corrupt/missing batch data. Physical ENOSPC is qualified on an isolated native spill filesystem; full PGDATA/WAL disk exhaustion remains a separate deployment gate.
- [x] Snapshot crash before/after activation and unsupported schema change while capture is delayed. Both paused/online upload and lost-COMMIT-response cases pass; unsupported DDL remains frozen rather than captured.
- [ ] Duplicate/stale workers, clone/restore startup, credential rotation, stop/start/drop, and partial installation/upgrade. Lifecycle/clone/owner fencing, credential rotation, ordinary installation failure/termination rollback and explicit SIGKILL publication rollback (including partial repair retry) pass. Verified committed staging cleanup recovery and unpublished planning/preparation discard also pass after SIGKILL, preserving original contents/modes. Interrupted journal deletion, power-loss repair and version upgrade qualification remain open.
- [x] Run pull and push streams simultaneously for disjoint mirrors; verify identity, selection, resource, and lifecycle isolation.
- [ ] Run all existing logical-mode qualification gates after the new mode is installed and after it is removed. Existing archive upgrade/restore, replication, interrupted initial-copy and uninstaller cases now pass on fresh volumes in both states; other logical test targets remain.
- [x] Package source capture as optional; document separate source/destination installation and supported catalog topology. Native ARM64 prototype archives pin the PostgreSQL build and pg_lake runtime; production release qualification remains open.
- [x] Add an explicit optional subscription/API selector only after the harness-driven prototype passes. Existing rows appear as `logical_pull` in the optional mode view; no core-row migration or automatic switch is required. The separate companion creates only explicit new push bindings.
- [ ] Keep automatic promotion/failover behind a later gate: synchronized slot readiness, source lineage, consumer progress, and divergence repair must all be tested.

**Gate:** the new mode is opt-in, existing logical deployments remain operational without source installation, and each published guarantee has retained evidence.

## Suggested implementation layout

The prototype uses these paths; destination metadata is an optional companion:

```text
extensions/lake_replication_capture/   optional source SQL/control, decoding plugin, workers
worker/src/events.rs                  source-neutral event definitions
worker/src/staging.rs                 shared journal admission helpers
worker/src/log_importer.rs            new receiver alternative
extensions/lake_replication_push/      optional destination metadata/API
tests/push_capture/                   source capture/rollback/replay probes
tests/push_replication/               importer, snapshot, lifecycle, end-to-end faults
deploy/push/                          source pg_lake + existing destination + MinIO fixture
docs/push-replication.md              architecture, limits, setup, recovery
```

Extend the current supervisor to select a receiver by configured mode and use mode-specific continuity checks. Source capture slots belong to the capture extension; destination push subscriptions must not create/drop those slots through the existing pull cleanup path.

## Deliver in reviewable slices

1. **PR1 — SQL feasibility probe:** source-side atomic log/checkpoint commit and failure evidence; no production behavior changes.
2. **PR2 — Shared event/staging interfaces:** existing pull mode passes unchanged.
3. **PR3 — Synthetic log importer:** feed fixture manifests into the existing journal/publisher; prove replay and atomic imported progress.
4. **PR4 — Optional source capture extension:** lifecycle plus INSERT/abort decoding and transactional log publication.
5. **PR5 — First vertical slice:** empty-start two-table replication, reconnect/replay, source outage, and publisher recovery.
6. **PR6 — Snapshot and mutable-row support:** paused COPY first, then qualified online handoff, updates/deletes/TOAST/TRUNCATE.
7. **PR7 — DDL, retention, operations:** staged scope expansion with resource and failure evidence.
8. **PR8 — Optional packaging/API and regression qualification:** opt-in mode; default logical mode preserved.

The first demonstrable prototype ends at PR5. It must stream committed changes from a source extension through Iceberg logs into the existing lake_replication publisher, survive an ambiguous capture commit/import restart, and leave the original logical approach working. Later slices are required before claiming general mirroring support.

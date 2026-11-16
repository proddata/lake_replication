# Streamed direct publication and recovery

The experimental direct publisher can now handle a source transaction larger than its ordinary 4,096-change/8-MiB buffer. Set `LAKE_DIRECT_STREAMING=1` to enable protocol 4 streaming and private disposable spill files. The ordinary immediate/coalesced path remains the default; neither experiment is installed or launched by release bundles.

The new path normalizes and writes changes incrementally to disk, verifies them during replay, and applies bounded chunks inside **one PostgreSQL/pg_lake transaction**. Final Iceberg changes and the source commit checkpoint commit together. A 5,000-change fixture carries 11,123,893 serialized input bytes through 43 chunks and produces one publication. This establishes correctness for that fixture, not throughput or a capacity maximum. Streamed mode currently publishes one source transaction at a time and does not coalesce them.

## Replay authority

Temporary files never authorize acknowledgement. Each record has a length, sequence number, top transaction ID and SHA-256 checksum. Subtransaction offsets let rollback truncate the aborted suffix; top-level abort discards the whole file. Files use private owned directories, exclusive namespace ownership and restrictive permissions. Restart deletes stale recognized files and decodes retained WAL again. Unknown artifacts and checksum failures stop publication.

A separate durable `replay_lsn` protects interleaved transactions. Suppose large transaction L is streaming when later transaction S commits. S can publish, advancing `published_lsn`, while `replay_lsn` and source feedback stay before L. A restart reconstructs L from WAL and skips already-published S. Once no transaction remains in spill, the worker persists the new replay floor before acknowledging it. Ordinary mode refuses an unfinished streamed replay boundary. Source identity, timeline, publication, frozen descriptor and slot-history checks still apply.

The protocol follows PostgreSQL's [streaming transaction rules](https://www.postgresql.org/docs/18/logicaldecoding-streaming.html) and [logical message formats](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html). Subtransaction IDs belong to their top-level transaction; they are not required to equal it.

## Bounds

| Setting | Default | Allowed range / meaning |
| --- | --- | --- |
| `LAKE_DIRECT_CHUNK_BYTES` | 1 MiB | 256 bytes–8 MiB, further limited by `LAKE_DIRECT_MAX_BYTES` |
| `LAKE_DIRECT_MAX_ROWS` | 4,096 | 1–4,096 changes per apply chunk |
| `LAKE_DIRECT_SPILL_BYTES` | 256 MiB | 1 byte–1 GiB across active files, including record headers |
| `LAKE_DIRECT_ACTIVE_TRANSACTIONS` | 16 | 1–64 concurrent transactions |
| `LAKE_DIRECT_SPILL_DIRECTORY` | `/tmp/lake-direct-spill` | Private owned directory; its parent must exist |

One change must fit its chunk budget; oversized individual values fail closed. The receiver also limits logical messages to 8 MiB, relations to 64, and tracked subtransactions to 4,096 per active transaction. Apply statements have a 30-second timeout. Spill quota, filesystem errors, descriptor changes and apply failures stop without acknowledging pending changes. These are input and temporary-file limits, **not a measured RSS ceiling**: the wire reader, JSON/SQL encoding, native PostgreSQL transaction and pgduck need additional memory. Large resumable initial copies remain a separate gate.

`LAKE_DIRECT_DEBUG_STREAMING=1` forces immediate PostgreSQL streaming with a 64-KiB decoding work-memory setting for qualification. Leave it unset in ordinary experiments. Empty transactions emitted by this debug mode are discarded without counting a publication or generating redundant control updates.

## Storage outages and WAL pressure

If object storage is unavailable, final publication fails and source feedback remains at the safe replay floor. Application heap commits can continue; retained source WAL grows. After storage returns, a manual worker restart replays the pending changes exactly. Automatic process supervision and connection recovery are not implemented. [Bounded deadlock recovery](wal-direct-deadlock-recovery.md) now retries protocol-confirmed 40P01 aborts by rewinding and revalidating the entire streamed transaction; it does not retry object-storage or transport failures.

PostgreSQL's `max_slot_wal_keep_size` can remove required history at a checkpoint. It is not a hard disk-usage ceiling, and stopping the consumer cannot stop WAL growth from application writes. Both direct modes refuse an invalidated slot without replacing it or advancing their checkpoint. Recovery after history loss requires an explicit fresh snapshot. The qualification preserves the previous complete mirrors, removes invalid experiment control/slot state deliberately, and bootstraps fresh mirrors before resuming. This test procedure is not an automatic production rebuild API.

## Qualification

[Retained evidence](evidence/2026-11-16-wal-direct-streaming.json) records:

- 79 streamed-publication checks: large atomic chunks, nested savepoint rollback, top abort, interleaved commit/restart, stale-file cleanup, spill quota, actual isolated tmpfs ENOSPC, corruption, later-chunk rollback, response loss, ordinary unforced framing and three real shared-server crashes around upload/commit.
- 134 regression checks for the ordinary publisher, coalescing and bounded snapshot bootstrap on the same runtime.
- 54 outage/retention checks across streamed and ordinary/coalesced modes: stopped storage, continued application commits, checkpoint/feedback preservation, exact recovery, actual `wal_removed` invalidation and explicit snapshot rebuild preserving old mirrors.

Three spill unit tests additionally cover quota/subabort replay, corruption and private namespace ownership/cleanup. All-feature Rust tests, `make check` and diff checks pass. Earlier development attempts and failed harness assertions remain in `.qualification`; the retained passing runs identify their exact images, source hashes and successful cleanup.

No sustained-load, large-transaction performance, production memory admission, power-loss, restore/promotion, managed credentials or direct-worker lifecycle claim follows from these bounded tests. Existing CPU comparisons remain tied to their earlier coalescing runtime.

## Run

Build `experiments/wal_direct/Dockerfile` over the qualified runtime as `lake-local-iceberg:wal-direct-v9`, and select pinned storage images as in the [experiment README](../experiments/wal_direct/README.md). Each target provisions and removes private Compose volumes:

```sh
make local-iceberg-wal-direct-streaming-test
WAL_DIRECT_STREAMING_CASE=ordinary make local-iceberg-wal-direct-streaming-test
make local-iceberg-wal-direct-outages-test
WAL_DIRECT_OUTAGE_STREAMING=0 make local-iceberg-wal-direct-outages-test
```

For each of `before_upload_completion`, `after_upload_completion` and `after_commit`, select the matching test-only fault image and set `WAL_DIRECT_STREAMING_CRASH_POINT`. Recreate disposable experiment schemas when changing versions; no in-place private-schema upgrade is provided.

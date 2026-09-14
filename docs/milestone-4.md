# Milestone 4: updates, deletes, and truncate

Status: primary-key UPDATE and DELETE plus TRUNCATE are implemented and tested
through the complete source-to-Iceberg path.

## Protocol and journal changes

The pgoutput decoder now accepts `UPDATE`, `DELETE`, and `TRUNCATE` messages in
addition to the existing insert subset. UPDATE supports an optional key or full
old tuple followed by the required new tuple. DELETE requires a key or full old
tuple. TRUNCATE records every relation in the message.

Journal tuple payloads are now JSON objects keyed by source column name. This
avoids ambiguity when pgoutput sends only replica-identity columns. Migration
`0004_mutations.sql` converts retained positional insert tuples through their
recorded relation schema before enforcing the new shape.

For an UPDATE whose identity columns did not change, PostgreSQL can omit the old
tuple. The receiver derives the identity from key-flagged columns in the new
tuple. If a key changed, the explicit old key is retained. Unchanged-TOAST
markers are omitted from the update object so the publisher leaves those
destination columns untouched.

## Application policy

The batch publisher applies changes in source transaction and change order:

- INSERT populates a complete destination record;
- UPDATE matches the old named identity and sets only fields present in the new
  tuple;
- DELETE matches the old named identity; and
- TRUNCATE truncates the mapped Iceberg table.

Every UPDATE or DELETE must match exactly one destination row. Zero or multiple
matches abort the entire batch without advancing its published checkpoint. This
is deliberately stricter than silently skipping a missing row.

The function name `publish_insert_batch` is retained for migration compatibility
in this prototype, although it now applies every supported operation.

## Evidence

Run `mutations_20260914_063517` passed all 13 assertions. The
[retained results](evidence/2026-09-14-mutation-results.json) cover:

- a two-row initial insert;
- an UPDATE that retained its primary key;
- an UPDATE that changed its primary key;
- a DELETE later in the same source transaction;
- a later INSERT and TRUNCATE published in order in one batch; and
- convergence of received, staged, published, and acknowledged LSNs.

Four source commits containing seven changes produced three Iceberg snapshots.
After the key-changing update and delete, source and destination both contained
only `20=twenty`. After the final truncate, both were empty. The final checkpoint
converged at `0/3A19738`.

The evolved schema also passed all earlier integration suites: 16 journal, nine
transport, and 14 insert-publisher assertions.

## Replica-identity scope

| Source configuration | Current behavior |
|---|---|
| Primary key / default identity | UPDATE and DELETE proven, including key change |
| Explicit identity index | Decoder uses key flags; end-to-end proof pending |
| `REPLICA IDENTITY FULL` | Full-old matching implemented; duplicate/full-row edge cases pending |
| No usable identity | PostgreSQL rejects published UPDATE/DELETE; receiver also rejects an identity-less update |
| INSERT or TRUNCATE | No replica identity required |

The protocol formats and publication requirements follow the
[PostgreSQL 18 logical replication message specification](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html)
and [publication identity rules](https://www.postgresql.org/docs/18/logical-replication-publication.html).

## Remaining boundaries

- True unchanged-TOAST transitions need a large external-value end-to-end test.
- Binary tuple values are retained as tagged hex but are not converted by the
  publisher yet.
- Streamed protocol-v4 transaction segments are not decoded.
- Mutation application currently issues one destination statement per journal
  change. Set-based delete-file planning and compaction remain future work.
- Initial synchronization, schema evolution, TLS/authentication, and HA remain
  pending.

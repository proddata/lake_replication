# Source-push protocol v1

This is the bounded source-push prototype contract; frozen schema is the default. It is separate from the
JSONB encoding in the source SQL feasibility fixture. No production subscription
selector is enabled by defining this protocol.

## Identity and order

A stream has an immutable nonzero 128-bit UUID, PostgreSQL system identifier,
database OID and persisted positive capture generation. Source restore/rebind
creates a new generation; consumers never infer continuity across generations.
Each selected table has an immutable nonzero registration UUID and positive
schema version, independent of its SQL name and source OID. Registration maps
that identity to a source-neutral relation descriptor with stable positive
column IDs, names, PostgreSQL scalar type descriptors and primary-key flags.
The first importer supports bigint/integer/text/boolean, non-null primary keys
and two explicitly registered tables. It rejects unregistered schemas.

One complete source transaction is one manifest in v1. Manifests use contiguous
positive stream sequence numbers. Commit/end LSNs must increase across manifests;
unrelated WAL gaps are valid. Transaction fields preserve XID, begin/commit/end
LSNs and PostgreSQL-epoch commit microseconds. XID/time are diagnostic data, never
ordering keys. A transaction event has a zero-based, contiguous ordinal across
all tables; transport chunk boundaries do not affect that ordinal.

Batch identity is deterministic: the first 16 bytes of SHA-256 over the domain
`lake-push-batch-v1`, stream identity/generation and transaction end LSN. The
manifest digest covers all canonical metadata and event bytes, including the
sequence and transaction coordinates. Persist the complete receipt atomically
with source log appends and capture progress. Same identity/different content is
a hard error; a digest alone never authorizes skipping an unknown batch.

Capture may commit several complete transaction manifests together. Grouping is
not a protocol transaction boundary: every constituent keeps its own sequence,
source coordinates, batch identity and digest. Consumers admit complete
constituents in sequence; regrouping after source recovery changes none of those
identities. Slot advancement requires every selected constituent in the decoded
window to be durably captured or reconciled.
The source writer may pack several constituent transactions into the same
physical log file. File boundaries never define transaction identity or order.

## Row and control values

Rows carry ordered column-ID/value pairs. Values distinguish absent, SQL NULL,
unchanged, and a UTF-8 PostgreSQL text value. Empty text is a value. Primary-key
identity and full old rows have different tags. UPDATE also distinguishes an
omitted old identity from a supplied identity. Consumers must not fetch current
source rows to reconstruct historical values. The importer normalizes INSERT, UPDATE, DELETE and TRUNCATE into the existing
journal operations. New UPDATE values may carry unchanged TOAST markers. An
unchanged primary-key value is resolved only from the supplied historical old
key or full old row; missing historical identity fails closed. Non-key markers
remain unchanged journal values. Identity values must be non-null. The source supports primary-key
replica identity only (default or an explicitly selected primary-key index).
Distinct replica-identity indexes and REPLICA IDENTITY FULL remain unsupported.

The event vocabulary includes INSERT, UPDATE, DELETE and TRUNCATE, plus ordered
snapshot begin/chunk/complete/activate, schema and progress controls. Unsupported control kinds fail closed. A paused snapshot uses the strict
begin/chunk/complete/activate sequence described below. Schema controls require explicit ordered-schema
activation and retained before/after definitions. Frozen bindings reject them.
Unknown tags/versions are always rejected.

## Encoding and bounds

The binary representation is canonical: network byte order, fixed-width unsigned
integers, two's-complement signed commit microseconds, exact length prefixes,
strict UTF-8, sorted column IDs and no trailing bytes. A manifest carries source
identity, sequence, deterministic batch ID, source transaction boundaries,
event count, ordered table/schema/count references and a SHA-256 digest. The
digest covers the versioned manifest header and the ordered length-framed events.
Table references sort by registration UUID and schema version. A reference count
includes every event associated with that table, including future controls.

Prototype limits: 64 MiB per complete encoded batch, 8 MiB per event, 100,000
events, 1,600 fields per row, two distinct table identities and up to 66
table/version references (33 versions per table). Manifest headers are bounded
to 4 KiB. Frozen capture still emits version 1; the additional framing capacity
does not enable Schema-control admission. A batch above a
limit is rejected without advancing capture/slot/import progress. The initial
capture must retain protected WAL and report the required larger budget or
unsupported transaction. It must not expose a partial transaction.

Transport may split these bytes into bounded files or rows. A transport chunk
is provisional storage, never a source commit. Reassemble the complete bounded
batch, validate canonical encoding, expected counts and digest, then admit it.
Changing transport chunk sizes leaves the batch identity and digest unchanged.

## Source read and destination admission

Append both private Iceberg table logs, ordered manifest and PostgreSQL receipt/
capture checkpoint in one source transaction. Capture recursion excludes these
objects. Only that durable source commit or reconciliation of the same identity
permits logical slot advancement; provisional files and observed WAL-end do not.

The importer opens one repeatable-read source PostgreSQL/pg_lake transaction,
reads ordered manifests and their referenced log records under that snapshot,
and validates the complete batch before destination writes. Source catalog
availability remains necessary. Arbitrary latest Iceberg table snapshots cannot
replace this read view.

The destination binds exactly one stream/generation to a subscription and
records imported sequence, batch identity and digest atomically with complete
journal admission. The journal remains the only input to the existing publisher.
Identical retry is reconciled; conflicting retry, sequence gaps, backwards
coordinates and unregistered identities stop import without advancing progress.
The managed worker generation fence and existing byte/count admission apply to
every import transaction. The importer must never create, drop or advance source
capture slots. Captured, imported and published frontiers remain distinct.

## Explicit progress barriers

A Progress control (tag 9) has no table and carries one nonzero 16-byte request
UUID. Request UUIDs must be distinct within a batch. It can appear alone or
between DML events; it cannot form part of a snapshot activation sequence.
The source owner enqueues the request in a protected heap table after baseline
activation. Native decoding emits the control only for that row's committed
INSERT, using the actual source transaction's commit/end LSNs. Its completion
sequence/end LSN is stored atomically with the capture manifest and receipt.
Rolled-back requests produce no control, and capture metadata/completion updates
never enqueue another request.

Normalization preserves the source transaction boundary while omitting the
marker from row mutations. A control-only batch stages an ordinary empty journal
transaction. The unchanged publisher commits its publication checkpoint with
zero affected rows; actual publication acknowledgement can then permit source
log pruning. A progress barrier does not discard WAL for older unfinished
transactions: the slot's restart position continues to protect them until their
later commits can be decoded. Observed WAL-end values never authorize this
progress. Requests and their retained completion rows have a finite per-generation
budget and remain explicit rather than automatic idle heartbeats.

## Bounded paused baseline

The baseline is the first manifest of a new generation. Its synthetic transaction
has XID/time zero and begin=commit=end-1, where end is the newly created slot's
consistent point. The control token is capture generation u64 followed by end
LSN u64 in network byte order. Events are exactly begin(token), INSERT rows,
chunk(token + u32 row count), complete(token), activate(token). All commit in
one bounded source transaction and admit as one destination journal transaction.
No partial control sequence authorizes publication. The source holds SHARE locks
on both tables through slot creation, row reading, and publication commit.

## Registry and retained continuity

Immutable source registration descriptors must match the destination registry
before import. The importer reads stream progress and retained-floor metadata
in the same repeatable-read view as log rows. An expected sequence at or below
the pruning floor is a retention gap, including when no live manifests remain.
It checks the source receipt at the destination's known imported frontier to
reject backwards or divergent source history. Replay receipts survive logical
log pruning for the bounded generation; an observed slot-confirmed position
alone is insufficient evidence for safely discarding them across crash recovery.

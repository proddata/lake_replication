# Milestone 6: unchanged TOAST and large values

Status: unchanged-TOAST preservation and external/inline/NULL value transitions
are proven through the complete source-to-Iceberg path.

## Handling rule

PostgreSQL can replace an unchanged external TOAST column in an UPDATE message
with the `u` tuple marker instead of resending the value. The receiver omits
that field from the named update post-image. The publisher generates its SET
list only from fields present in the post-image, so the existing Iceberg value
is left untouched.

This differs from SQL NULL: a pgoutput null marker becomes an explicit JSON null
and therefore sets the destination column to NULL. A newly changed external or
inline value is carried normally and replaces the prior value.

## Evidence

Run `toast_20260914_102520` passed all 11 assertions. The
[retained results](evidence/2026-09-14-toast-results.json) cover seven committed
source transactions and seven published journal changes.

The source column was configured with `STORAGE EXTERNAL`, filled with 128,000
bytes of low-compressibility data, and verified to have rows in its physical
TOAST table. Source and Iceberg length/MD5 pairs matched at each relevant step:

| Transition | Result |
|---|---|
| Initial external value | `128000:92831171b76416bd603a9d0fe9b9972d` |
| Marker-only update | Same length and checksum preserved |
| External replacement | `128000:d3067ecf438e0c25f94ff02cb9e23840` |
| External to inline | `6:03fdad155b7548884584c7c39b0c5cd2` |
| Inline to NULL | Explicit NULL reached Iceberg |
| NULL to external | `128000:d258e032696398e51c8b0a8f71298871` |
| External to NULL | Final source and Iceberg state `NULL:NULL:v7` |

The journal post-image for the marker-only update contained the changed marker
but no payload key, proving the preservation came from unchanged-TOAST handling
rather than retransmission of the large value. All four checkpoints converged
at `0/5129D00`.

The tuple marker behavior follows PostgreSQL 18's
[logical replication message specification](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-TUPLEDATA).

## Remaining boundaries

- The test covers text-format TOAST values. Binary transfer is retained as
  tagged hex in the journal but not converted by the publisher.
- UPDATE matching still depends on a valid replica identity; unchanged TOAST
  values are not usable as identity fields.
- Very large values remain bounded by the receiver's 64 MiB wire-frame guard
  and destination SQL statement construction. Parameterized/binary journal
  writes are needed before production-scale payload testing.
- Schema evolution, initial synchronization, supervised services,
  TLS/authentication, and HA remain pending.

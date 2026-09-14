# Milestone 8: bounded streaming initial copy

Status: initial snapshot rows now flow incrementally from PostgreSQL `COPY TO
STDOUT` into durable destination staging with row and byte bounds.

## Data path

The wire client recognizes PostgreSQL's `CopyOutResponse`, `CopyData`, and
`CopyDone` messages and invokes a consumer for each data frame. The initial-sync
worker no longer collects the source result set in a `Vec` before staging it.

Each source row is converted to JSON and hex-encoded inside PostgreSQL. Hex keeps
COPY's row delimiter unambiguous even when values contain newlines, backslashes,
quotes, or multibyte UTF-8. The worker carries at most:

- one incomplete COPY row;
- one complete row being decoded; and
- a staging chunk bounded by both `LAKE_INITIAL_COPY_CHUNK_ROWS` (default 1,000)
  and `LAKE_INITIAL_COPY_CHUNK_BYTES` (default 8 MiB).

A single row can exceed the byte threshold and is staged alone. The existing
64 MiB PostgreSQL wire-message guard remains an upper limit for an individual
wire frame.

Every completed chunk is committed to ordinary destination staging immediately,
so `initial_copies.rows_copied` advances while the source COPY is still active.
The Iceberg destination remains unchanged until the final atomic transition
defined in Milestone 7.

## Evidence

Run `initial_20260914_105011` passed all 18 assertions. The retained
[results](evidence/2026-09-14-bounded-copy-results.json) demonstrate:

- durable progress at 25 rows before process completion;
- the previous Iceberg state still visible after that first chunk;
- an injected error after 50 rows with no checkpoint advancement;
- exact transport of newline, backslash, quote, and Unicode content;
- completion of the 100-row snapshot and subsequent three-change WAL catch-up;
- no missing or duplicate identities in the final 100-row destination.

## Remaining boundaries

- Hex transport doubles source-row payload size. PostgreSQL binary COPY or a
  length-delimited JSON framing format would reduce bandwidth and CPU overhead.
- Destination chunks are embedded in SQL literals. Parameterized extended-query
  transport or COPY into the staging heap is needed for larger individual rows.
- The probe establishes bounded code paths and live chunk progress; long-duration
  load tests with measured resident memory and backpressure remain pending.
- Publication discovery, coordinated multi-table synchronization, row filters,
  column lists, and automatic destination creation remain pending.

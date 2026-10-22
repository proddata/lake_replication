# Empty filtered streamed transaction: receiver progress failure

> Historical prototype record. Commands and source paths below refer to the
> [preserved implementation](https://github.com/proddata/lake_replication/tree/7ef18381810075dddd26137ee959770591b95863/experiments/base_workers) before cleanup.
> Current lifecycle tests live in `tests/lifecycle`; retained admission helpers
> are described in [experiments/admission](../experiments/admission/README.md).

A large source transaction outside a subscription's publication can leave the
receiver repeatedly failing with `streamed transaction ... has no changes`.
This reproduced in the qualified base image both with and without admission;
switching lifecycle frameworks or changing publication permits does not fix it.
The receiver retries while the subscription can still report `streaming`, so
that state alone does not establish forward progress.

## Reproducer and evidence

```sh
python3 experiments/base_workers/admission_measure.py --skip-comparison \
  --fairness-variants unbounded --copy-rows 2000000 --seed-after-subscriptions
```

The private fixture creates three streaming subscriptions with separate small
publications, then bulk-inserts two million rows into another source table in a
single transaction. The copied table's publication is separate. Receivers for
the small publications subsequently retry the empty streamed transaction instead
of catching up. The bounded run completed the large initial copy with its two
million rows while a stream remained stuck; pgduck stayed healthy. The baseline
diagnostic detects the same receiver error early and retains it as a finding.

The [retained diagnostic evidence](evidence/2026-10-20-empty-filtered-stream-diagnostic-results.json)
contains both runs, immutable image/source/archive fingerprints, parameters,
source transaction records and PostgreSQL log excerpts/hashes. Fixtures were
removed after diagnosis. This is a correctness diagnostic, not a completed
fairness or performance cohort.

## Original code path and subsequent fix

The receiver's `Message::StreamCommit` path calls `commit_stream` before WAL
feedback. `migrations/0005_streaming.sql` explicitly rejects an empty set of
streamed changes; the ordinary journal staging API also requires a nonempty
array. This matches the observed SQL error. The admission overlay changes the
native publication/initial-copy guard, not either of these receive/staging paths.

The required fix is a durable no-op/filtered-transaction path preserving staging,
publication and acknowledgement ordering. Simply skipping the error or
advancing source feedback risks breaking checkpoint invariants or skipping
unpublished work. Tests must include pending earlier transactions, replay,
restart, later real changes and source-slot/checkpoint consistency. The subsequent
[SQL fix and regression results](empty-filtered-stream-fix-results.md) now pass 75 checks. A separate native 200 ms receive-timeout failure was then
fixed by the [receiver lifetime change](receiver-progress-fix-results.md), whose
96-check run includes the full native two-million-row scenario.

For the separate admission coexistence measurement, dataset preparation now
precedes all replication slots. That isolates copying an existing large table
alongside small source transactions; it does not qualify large unrelated source
transactions while those slots already exist.

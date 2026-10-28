# Pressure visibility before adaptive copy pacing

The shared byte-budget experiment showed that a fixed byte ceiling does not
adapt when processing capacity falls below that ceiling. Before implementing a
feedback controller, this diagnostic checks what its available signals can see.
It changes only test observation, using the unchanged beta.3 archive and fresh
volumes. No production worker, migration or pacing policy changes.

The opt-in `--capture-stream-pressure` flag replaces each ordinary publication
count query with one aggregate that also measures committed unpublished work,
its destination staging age and its source commit age. A separate comparison
uses the fixture's known committed transaction prefix and the destination's
published count to estimate committed work still outstanding. Their difference
exposes work not yet committed into the destination transaction journal. This
fixture oracle is test-only; it is not a proposed production dependency or
control signal. Prefix capture precedes the destination query, so newer
publications can overtake that prefix; counts are clamped and receiver health
remains explicitly unknown. Samples are not an atomic cross-system snapshot.

```sh
export QUALIFICATION_ARCHIVE=/absolute/path/to/candidate.tar.gz
caffeinate -is python3 tests/admission/contention.py --copies 0 \
  --cpuset 0-1 --capture-stream-pressure
caffeinate -is python3 tests/admission/contention.py --copies 4 \
  --cpuset 0-1 --capture-stream-pressure
```

## Local results, 2026-10-28

[Retained evidence](evidence/2026-10-28-pressure-visibility-results.json)
records the sequential S2 runs, exact input identities, result hashes and
per-stream summaries. The first control predates automatic capture of the
helper source; an unchanged helper snapshot is retained separately and marked
as supplemental. The contention run includes automatic helper provenance.

| Workload | Streaming p95 range | Largest sampled destination queue | Largest sampled fixture work absent from queue |
|---|---:|---:|---:|
| Streaming only | 241–254 ms | 1 transaction | 1 transaction |
| Four large copies | 17.96–19.09 s | 1 transaction | 35–38 transactions per stream |

During contention, each stream had 31–36 samples with an empty destination
queue while the fixture had committed unpublished work older than two seconds.
The oldest sampled outstanding work reached 17.96–19.08 seconds. Copy completion
was 67.44 seconds. Exact final data and clock continuity
passed in both runs; no engine OOM or PostgreSQL crash was observed. Source
scheduling passed in the control, but two contention streams exceeded its
one-interval tolerance. Retain that offered-load failure alongside the failed
latency/catch-up limits; the contention run is not a passing offered-load
profile. The instrumented
control passes, but this single comparison is not an overhead-equivalence test.

An empty destination queue cannot safely authorize more bulk staging in this
workload: substantial committed work can be held up before it reaches that
queue. The snapshots do not identify which upstream phase caused that delay.
The fixture oracle avoids assuming that absence of queued work means absence
of demand; it must not be carried into production as a hidden dependency.

## Next implementation boundary

Current `service_status.heartbeat_at` reports supervisor activity. The persistent
supervisor updates it while monitoring its receiver, so freshness does not
prove that receiver ingest is current. The receiver handles keepalives in
`worker/src/main.rs` but currently does not publish their WAL progress metadata
for a controller. Checkpoint timestamps advance with durable staging and
publication; an old checkpoint also cannot distinguish idle input from delayed
ingest by itself.

The next isolated overlay should expose receiver progress/freshness alongside
destination queue age, with a deliberate idle-source and clock contract. Test
quiet sources, filtered WAL, long open transactions, blocked staging, receiver
restart and missing/stale telemetry. Keep resource telemetry distinct from
recovery checkpoints and WAL acknowledgement: observing a server WAL endpoint
must never advance durable feedback. Use the fixture oracle to validate the
new signal, then compare a controller against repeated unchanged controls.
Do not build a queue-only production controller from the current views.


The [receiver progress overlay](receiver-progress-prototype.md) now exposes
these separate observations and passes 19 lifecycle checks. It also reveals
per-transaction receiver generation churn. Its health/idle contract is still
explicitly incomplete; no adaptive production policy has been adopted.

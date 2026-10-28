# Bounded receiver lifetime and cycle attribution

This is an isolated receiver/supervisor overlay. It changes no production worker
source, extension SQL, version or default. The experiment asks whether repeating
a receiver process and source preflight after every committed transaction is
contributing to the previously observed upstream lag.

## Timing method and first finding

The supervisor records monotonic phase times for source preflight, receiver
spawn, receiver-exit detection, publisher invocation and cycle exit. Private
worker JSON logs are retained before volume removal. After measurement, each
stream database exports source commit time, journal insertion time and the
transaction's actual publication commit time. Database OIDs connect the tables
to their corresponding worker log files; XIDs match durable-stage log events.

`staged_at` is an internal journal insertion marker, not the staging commit.
The receiver's staged log follows its original staging and feedback calls.
Publisher invocation includes process startup, connections, SQL and exit; it is
not pure engine execution time. These distinctions are retained in the audit.
Nearest-rank percentiles match the existing fairness observer. Timing capture
fails validation if records, stage events, supervisor phases or clock checks
are missing. Future cycle-enabled runs also gate on that audit automatically.

With the original one-transaction lifetime under S2, source-to-journal p95 was
roughly 14.6–16.3 seconds, versus under 0.9 seconds from journal insertion to
publication commit. Each stream accumulated approximately 12–13 seconds in
between-cycle gaps over 120 source transactions. The cycle gap includes service
reporting and the existing 100 ms outer poll sleep. Preflight also repeated for
each cycle. This supports testing process lifetime; it does not prove a single
cause for every delay.

One control stream also had a receiver `EAGAIN`/"Resource temporarily unavailable"
error followed by a one-second supervisor retry. Its data recovered exactly.
The warning/error counts and original log hash remain retained; the comparison
is not a clean paired estimate of process-lifetime overhead. The other two
control streams also had high lag without that error.

## Bounded lifetime experiment

The temporary supervisor requests up to 64 committed transactions per receiver
process. It retains its existing ability to publish while that receiver is
alive. Before staging each ordinary or streamed committed transaction, the
receiver checks that its subscription has fewer than eight committed
unpublished journal transactions. At eight it waits, keeping its source
feedback at the original durable frontier. Each count query is autocommit;
no journal transaction or engine admission slot is held while sleeping.
A 30-second guard wait fails into the ordinary retry path.

This prototype relies on native source-slot ownership supplying one producer.
It is not an atomic admission API for other/manual writers. The limit bounds
a transaction count, not bytes, individual transaction size, uncommitted stream
segments, bulk-copy staging, WAL retention or total memory. One parsed committed
transaction can also be waiting outside the journal. The normal publisher batch
limits and four engine stream permits are unchanged. This is not a production
resource guarantee.

The lifecycle fixture holds all four engine stream permits, creates more source
transactions than the journal allowance, observes eight unpublished
transactions staying at eight and the receiver waiting, then kills that
receiver. After releasing the permits, the normal supervisor resumes and
publishes exactly the expected 17 rows and sum 153. The slot never acknowledges
the ninth waiting transaction as though it were staged. This extends the
receiver diagnostic's earlier 19 checks to 25 passing lifecycle/backpressure
checks.

## Local results, 2026-10-28

[Retained evidence](evidence/2026-10-28-bounded-receiver-cycle-results.json)
includes every run in this comparison, input/source identities, exact overlaid
Rust variants, log/table/result hashes, operational checks, phase summaries and
independent audits. All three bounded runs have identical overlaid Rust source
hashes. The archive is the retained beta.3 package, SHA-256
`4494a0ce95c45613753abd4529dc00ec04f446516a76a012eb15ce6e98be7164`.

S2 shares CPUs `0-1` across source, destination/workers, engine and storage.
P2 gives destination/workers and engine CPUs `0-1`, source CPU `2`, and storage
CPU `3`. P2 therefore uses more total CPU capacity. It does not establish a
minimum production configuration or count as a passing two-CPU whole-fixture
qualification. Both profiles share the local M5's Docker VM memory and storage.

| Profile and receiver limit | Streaming p95 range | Copy completion | Full gate |
|---|---:|---:|---|
| S2, 1 transaction | 14.98–16.66 s | 65.83 s | Failed latency and source schedule |
| S2, 64 transactions / journal 8 | 1.65–3.05 s | 64.62 s | One latency failure; two source-schedule failures |
| S2, same bounded repeat | 1.34–1.63 s | 64.99 s | Latency passes; two source-schedule failures |
| P2, same bounded build | 526–554 ms | 54.31 s | All original limits pass |

Every run passed exact final data and clock continuity and observed no engine
OOM or PostgreSQL crash. All receiver/cycle audits passed. No warning/error
events were observed in the three bounded workload logs. The first bounded run
passed all 25 live checks before measurement. Its measured journal peaks were
4, 8 and 5 unpublished transactions; the repeat peaked at 4, 3 and 3.

The bounded S2 runs observed only one completed receiver cycle per stream,
followed by a second still-running cycle at capture, instead of roughly 120
completed cycles in the control. The first bounded run's between-cycle gap was
about 0.1 seconds per stream rather than 12–13 accumulated seconds. Publication
still ran repeatedly while the receiver was alive. Source-to-journal p95 fell
to roughly 0.85–1.9 seconds in that run. The journal-to-publication interval
increased, consistent with more work now being staged ahead of publication.
The guard exposes and bounds that queue instead of letting it grow indefinitely.

These are exploratory sequential runs, not a randomized paired cohort or cloud
headroom qualification. The P2 intervention moves source and storage together,
so it does not isolate which service caused the shared-resource pressure.
The source-scheduling failures in S2 remain release findings; a passing latency
number does not override them. No timing limit was relaxed.

## Reproduce and next step

```sh
export QUALIFICATION_ARCHIVE=/absolute/path/to/candidate.tar.gz
# Original lifetime with phase observation.
caffeinate -is python3 tests/admission/contention.py --copies 4 --cpuset 0-1 \
  --capture-stream-pressure --receiver-progress --cycle-timing
# Bounded longer lifetime, including cancellation/recovery and cap checks.
caffeinate -is python3 tests/admission/contention.py --copies 4 --cpuset 0-1 \
  --capture-stream-pressure --receiver-progress --cycle-timing \
  --receiver-batch 64 --receiver-operations
# Explicit topology control, using additional source/storage CPUs.
caffeinate -is python3 tests/admission/contention.py --copies 4 --cpuset 0-1 \
  --source-cpuset 2 --storage-cpuset 3 --capture-stream-pressure \
  --receiver-progress --cycle-timing --receiver-batch 64
```

The isolated builds passed four receiver and ten supervisor Rust tests.
`make check` passed, including 16 admission tests. No remote workflow was run.

Keep the bounded longer-lived receiver as a candidate, not a production default.
The next local intervention should control concurrent initial-copy staging to
protect the shared source/destination resource profile, while preserving this
journal bound and original gates. Test a separate chunk-scoped staging permit;
do not extend the existing final-publication permit across an entire COPY.
Before promotion, also enforce a managed byte/count admission contract and test
slow publication, short source timeouts, concurrent subscriptions, stop/drop,
crash recovery and both native architectures. The earlier interrupted-run OOM
and the isolated control's transient receiver error remain open findings.

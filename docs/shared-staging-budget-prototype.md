# Shared staging budget: local prototype protocol

This is an isolated diagnostic, not a production migration. The production
worker sources, extension SQL, bulk permit and four streaming slots remain
unchanged. No remote workflow is part of this protocol.

## Resource profiles and decision rules

The primary qualification **target for this experiment**, P2, assigns two CPUs
(`0-1`) to destination PostgreSQL, initial-sync/receiver/publisher workers and
pgduck. Source PostgreSQL uses CPU `2`; MinIO uses CPU `3`. They still share the
local Linux Docker VM's reported 7.65 GiB memory and underlying Mac storage.
pgduck remains configured with a 2 GB engine limit and one query thread; this
is not a whole-instance memory bound. The host observer is
outside these affinities. This specifies test topology; it does not declare a
supported minimum hardware configuration or equate M5 cores with cloud vCPUs.

S2 shares CPUs `0-1` across all four services. S1 shares CPU `0` across all four.
These are overload-sensitivity profiles, with failures retained. The existing
workload remains four copies of two million 128-byte-payload rows and three
streams of 120 transactions, 20 rows each, offered every 500 ms.

The latency limit remains 2 seconds p95, final catch-up 5 seconds and copy
completion 180 seconds. A local M5 pass does not establish cloud headroom or
justify a fixed default. Report completion time and sensitivity alongside lag;
never promote a barely passing copy time as sufficient release evidence.

Initial P2 comparisons are: unchanged archive (retained control), accounting
only (zero rate), and shared ceilings of 24 and 16 MiB/s. These rates are
exploratory points, not production defaults or statistical qualification.
The shared burst and maximum chunk size are 256 KiB. Test the stronger
backpressure point under S2, then S1 if its result helps assess the supported
envelope. Do not retune the published timing limits to obtain a pass.

## Mechanism and scope

A temporary Rust build adds a call before staging each chunk. It sends the
serialized JSON chunk's UTF-8 byte length to one diagnostic coordinator database,
then sleeps after the coordinator transaction commits and before the
destination append starts. The source COPY snapshot remains open, so a slower
copy can increase source WAL/snapshot retention. Neither is bounded here.
All subscriptions use the same coordinator, including subscriptions in different
destination databases. The shared reservation allowance does not multiply when another copy starts.
It governs eligibility times: delayed workers can still bunch their subsequent
staging calls. It is not a strict instantaneous write-rate cap. This measures logical serialized bytes, not wire bytes,
WAL bytes, CPU work, a memory ceiling or a full-copy I/O reservation.

The coordinator is a small SQL token/debt bucket in a disposable `analytics`
database. One row serializes reservations; committed debt schedules later
requests. There is one initial burst. Credits are unlogged and reset to one burst
after a database crash; they do not participate in data recovery. The byte debt
refills with elapsed database wall time; backwards time grants no credit and a
forward jump restores at most one burst. Reservations exceeding two seconds of
wait or the chunk/burst size fail closed. This prototype is intended for audited,
stable-clock measurement; a production implementation still needs a clock and
lifecycle contract, as well as a suitable shared-state mechanism.

The worker keeps one bounded-timeout coordinator connection. Its ordinary
supervisor can kill it during sleep. An abandoned reservation temporarily costs
credit and refills with time; it does not leak a lock. A transaction rollback
reverts a reservation. Retries are not exactly-once resource accounting; an
ambiguous committed reservation can be charged twice. Data staging and recovery
continue through the original production functions.

Successful measured runs are audited against the expected 8,000 staging chunks
and 1,267,563,584 serialized bytes across four identical copies. A second audit
checks the aggregate reservation envelope and bounded waits. Failed or duplicate
accounting is not a valid performance result.

Accounting-only mode uses the same coordinator calls and counter updates but
returns no delay. This distinguishes coordinator overhead and incidental pacing
from the effect of the byte ceiling. The fixture adds a diagnostic schema and
one connection per initial-copy process; this database arrangement is not a
proposal for a new production dependency.

## Validation and reproduction

`--budget-operations` checks deterministic refill/debt, the burst bound,
concurrent conservation, cross-database scope, rollback, time reversal/forward
jumps, lock timeout and the bounded queue horizon. It also disables a real native
subscription while its initial-copy process sleeps on reserved credit, verifies
that no rows were staged, then resumes it and checks exact data and slot cleanup.
All operation state is reset before the measured workload.

```sh
export QUALIFICATION_ARCHIVE=/absolute/path/to/candidate.tar.gz
python3 tests/admission/contention.py --copies 4 --budget-mib 0 \
  --budget-operations --source-cpuset 2 --storage-cpuset 3
python3 tests/admission/contention.py --copies 4 --budget-mib 24 \
  --source-cpuset 2 --storage-cpuset 3
python3 tests/admission/contention.py --copies 4 --budget-mib 16 \
  --source-cpuset 2 --storage-cpuset 3
python3 tests/admission/contention.py --copies 4 --budget-mib 16
```

Every run uses fresh volumes and retains exact overlaid Rust sources, installed
SQL, image/archive identities, source hashes, resource samples, private worker
logs and coordinator counters. The binary/SQL overlay is explicit. It cannot be
reported as unmodified package qualification or production adoption.


## Local results and decision

[Retained evidence](evidence/2026-10-27-shared-staging-budget-results.json)
includes raw-result hashes, audits, exact overlaid source variants, operational
checks and every attempted budget run, including invalid attempts. Rates below
are aggregate reservation rates across all four copies.

| Profile | Mode | Streaming p95 range | Copy duration | Result |
|---|---|---:|---:|---|
| P2 | Accounting only | 2.71–2.83 s | 55.70 s | Lag finding |
| P2 | 24 MiB/s | 1.08–1.61 s | 67.40 s | Passed original limits |
| P2 | 16 MiB/s | 756–820 ms | 87.46 s | Passed original limits |
| S2 | 16 MiB/s | 1.66–1.74 s | 90.15 s | Passed, narrow lag margin |
| S2 | Accounting only | 15.08–16.90 s | 69.29 s | Lag finding |
| S1, valid guarded repeat | 16 MiB/s | 52.59–54.86 s | 137.46 s | Lag/catch-up finding |

Every valid row completed exact data checks and reserved precisely 8,000 chunks
and 1,267,563,584 bytes. Positive-rate runs passed the shared reservation-envelope
audit. All rows above passed the wall/monotonic timing audit and observed no
engine OOM or PostgreSQL crash. Each accounting-only control also passed all 26
live budget-operation checks, including concurrent requests, rollback,
cross-database scope, bounded waits, native disable during budget sleep, exact
resume and source-slot cleanup.

The S2 accounting-only control also exceeded the source-scheduling tolerance
for two streams; retain that overload finding rather than treating it as a
passing offered-load profile. The valid S1 repeat met the source schedule and
still failed lag and catch-up.

The S2 accounting-only control was added after the S2 limited run to separate
RPC overhead from rate effects under the shared profile. This exploratory order
is retained; it is not an alternating paired cohort or an overhead-equivalence
claim. A first accounting attempt failed before staging any rows because of
prototype connection initialization; the corrected named-connection API was
used for the results above. Early driver descriptions still referred to the
pause-only overlay. For budget runs that text is obsolete: they rebuild the
initial-sync binary, as their overlaid sources and image metadata show.

Two attempted S1 runs crossed large VM wall-clock jumps and are invalid timing
samples. They remain retained without using their durations as performance
results. One of those interrupted runs also hit a real internal DuckDB OOM
(`76.5 MiB` allocation failure at `1.7 GiB/1.8 GiB` used). That safety observation
is not dismissed because the clock audit failed. The valid S1 repeat did not
reproduce it; suspend/resume and engine-memory behavior remain open questions.
The driver now aborts promptly on a detected clock discontinuity, and the audit
rejects it. A temporary macOS `caffeinate -is` guard was used for the final repeat;
it changes no permanent power setting or production behavior.

**Do not migrate this static rate as a production default.** The shared budget
works mechanically and improves the two-CPU profiles, but a fixed absolute byte
ceiling is not capacity aware. In the valid S1 run it imposed only 17.68 aggregate
seconds of requested sleep across 8,000 calls while lag exceeded 52 seconds:
processing throughput itself fell below the configured ceiling, so it provided
little backpressure. Lowering a static ceiling can also consume most of the copy
deadline. The M5 results do not establish cloud headroom.

The next local policy comparison needs either pressure-aware aggregate control
or an explicit, measured capacity/configuration contract, plus the independent
resume/OOM investigation. No production worker/migration changed, no default
rate was selected, and no new remote workflow was dispatched.


Follow-up [streaming-only controls](streaming-only-capacity-baseline.md) pass
on S2; S1 has one p95 finding and one passing repeat. This supports a further
isolated pressure-aware experiment while keeping baseline variability explicit.

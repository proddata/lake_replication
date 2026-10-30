# Post-commit bulk pacing diagnostic

The [native chunk-store trial](staging-chunk-prototype.md#prospective-native-result)
copies in 87.09 seconds but misses all three streaming latency limits, while
both per-row controls pass. Source CPU usage increases and the engine receives
less CPU under the same shared two-CPU budget. Slow streaming transactions
precede observed bulk publication. This motivates testing backpressure on the
whole bulk pipeline rather than just changing engine publication admission.

## Prespecified treatment

Use the unchanged archived worker's existing
`LAKE_INITIAL_COPY_CHUNK_PAUSE_MS=40`. After the managed staging transaction returns
from COMMIT, the worker clears its pending rows, logs progress and then sleeps.
It holds neither the SQL staging permit nor its transaction generation fence
while sleeping. The synchronous source COPY callback also waits, allowing
socket backpressure to constrain source work. No extra source session, CPU,
engine slot, observer query or memory budget is introduced.

The disposable executable wrapper records its configuration, original image ID,
wrapper checksum and archived executable SHA-256 before/after installation.
Controls explicitly set the pause to zero; the candidate sets 40 ms. Both keep
1,000 rows and 8 MiB of row JSON text per transaction, retaining the existing
single-oversized-row exception. The canonical worker command line is preserved
for native stop/crash tests. Production defaults and schema remain unchanged.

The fixed comparison is unpaced/paced/unpaced **chunk storage** on one host with
fresh volumes. All three use split checkpoints, identical source schedules and
query-return clock sampling. Runtime/input identity checks reject mismatched
pause declarations, changed byte targets, worker/image changes or an undeclared
SQL storage overlay. This isolates pacing; it is separate from the earlier
per-row/chunk/per-row comparison.

```sh
QUALIFICATION_ARCHIVE=/path/to/verified/arm64/archive.tar.gz \
  python3 tests/admission/checkpoint_split_probe.py --cases managed \
  --chunk-store --chunk-pause-ms 40

QUALIFICATION_ARCHIVE=/path/to/verified/arm64/archive.tar.gz \
  python3 tests/admission/checkpoint_compare.py --chunk-store \
  --chunk-pause-ms 40 --cpuset 0-1
```

Require existing cancellation, fencing, exact-data, byte/row flushing and
oversized-row checks, then the complete local comparison and one-CPU candidate
sensitivity before a prospective native trial. Keep all observations, failed
gates and existing thresholds. There are no automatic retries or best-result
selection. At 2,000 transactions per copy, the setting adds 80 seconds of explicit
sleep per worker; actual admission/source waits can overlap it. Copy duration
must still pass 180 seconds with headroom; a static pause is not a guarantee
under arbitrary data, worker counts or host capacity.

The native workflow requires `native_probe=true`, `probe_arch=arm64`,
`staging_chunk_comparison=true` and `staging_chunk_pause_ms=40`. It remains a
bounded three-workload diagnostic. A failed control retains its finding even
when the candidate passes; the workflow does not turn such observations green.
No passing diagnostic qualifies an unmodified production archive or supplies the
hardened checkpoint/storage upgrade, direct-write API or restore contract.

## Local result

The paced configuration passes all 131 archive, integrity, native lifecycle and
chunk-boundary assertions. `make check`, 40 admission unit tests and workflow
lint pass. Frozen input and raw-result hashes match the final harness sources.

| Cohort | Unpaced before | 40 ms candidate | Unpaced after | Comparison validity |
|---|---:|---:|---:|---|
| Initial | 44.58 s | 104.50 s | 44.04 s | Invalid: both controls observe only three publications |
| Complete confirmation | 44.52 s | 103.65 s | 45.93 s | Invalid: first control observes only three publications |

Both 40 ms observations independently pass every workload gate: streaming p95
is 361–372 ms and 317–367 ms, with four-way publication overlap, valid source
schedules and clock audits, exact data, and no engine OOM or PostgreSQL crash.
The final confirmation control also passes; no passing controls are combined
across cohorts. A complete confirmation was declared before the initial final
control returned. All six observations and failed gates are retained. There
are no further unchanged local retries and no local causal performance claim.

The separate paced one-CPU sensitivity passes at 124.59-second copy duration
and 1.173–1.264-second streaming p95. Source lateness stays around 10 ms. Clock,
overlap, exact-data and safety checks pass, with no engine OOM or PostgreSQL
crash. This is not a matched one-CPU speed comparison.

[Local evidence](evidence/2026-10-30-staging-pacing-local.json) records all checks,
both complete cohorts, the prespecified confirmation policy, coverage misses,
one-CPU sensitivity, archive/input/image/worker/configuration hashes and the
basis for the native trial.

The two complete local comparisons remain inconclusive. Independently valid
paced observations, one-CPU sensitivity and safety checks justify one prospective
native comparison to establish full coverage on the slower runner, where the
previous unpaced chunk cohort did observe all four publications. The native
trial keeps the same fixed unpaced/40 ms/unpaced sequence and all gates. Invalid
controls would still invalidate its comparison; failed latency controls remain
findings. No local finding is reclassified or waived to trigger that trial.
Production storage, defaults and migration status remain unchanged.

# Convert wide UPDATE rows once

The shared publisher now converts a new tuple once through a lateral PostgreSQL table-function scan for wide, multi-row UPDATE runs. Previously, each non-JSONB SET projection invoked `jsonb_populate_record` on the same tuple again. The change is implemented by migration `0034_update_row_conversion.sql`.

Eligibility is deliberately limited to at least 32 assigned columns and at least 16 rows, with supported built-in scalar types throughout the target. Smaller runs, narrower assignment sets, domains and custom input types retain the previous plan. This measured gate avoids adding a lateral scan where the diagnostic trials showed inconsistent benefits. It does not assert a general optimal threshold.

JSONB columns retain their explicit wire-text cast; they do not use the composite function's JSON-string interpretation. Old-key predicates and typed membership filtering remain in place, with destination-qualified identifiers to avoid ambiguity with the new row alias. The same UPDATE statements and per-change match-count validation remain; no source events are reordered, combined or skipped by this change.

## Diagnosis

The [current-worker phase profile](evidence/2026-11-15-local-update-phase-profile.json), after the coalescing correction, records approximately 4.58 seconds in UPDATE calls, 2.57 seconds in DELETE calls, 2.21 seconds in INSERT calls and 1.14 seconds in publication COMMIT. UPDATE averages 19.26 ms over 238 calls. These are instrumented elapsed wall times, not CPU, and nested totals must not be summed with enclosing publication statement timings.

The [conversion plan matrix](evidence/2026-11-15-local-update-conversion-plans.json) tests 2/16/32/48-column targets and 1/16/32-row updates, with four trials per variant in alternating order. Every EXPLAIN ANALYZE trial executes inside a transaction and rolls back after checking all assigned values. Raw plans remain in the referenced private qualification directory. The retained evidence stores timings and input provenance without duplicating large JSON constants from every plan.

The matrix shows no consistent advantage for single-row or narrower updates. At 48 columns/32 rows, median isolated execution falls from 36.34 to 29.13 ms; at 32 columns/16 rows it falls from 26.88 to 25.27 ms. Candidate plans include a lateral function scan executing once per source row. These destination-only diagnostics exclude COMMIT and capture, so their percentages are not pipeline speedups.

## Full-pipeline comparison

The [uninstrumented comparison](evidence/2026-11-15-local-update-conversion-performance.json) uses identical cached v7 worker/native binaries and fresh private volumes for each variant. Candidate fixtures install only migration 0034 before initial copy; control uses the installed 0033 function. Both use corrected one-second coalescing, journal capacity 32 transactions/64 MiB, identical storage and the same two-second observers. No diagnostic timing wrappers run in these comparisons.

Each source table starts with 1,000 rows. The application commits 120 transactions at four offered transactions/second, updating a rolling 32-row range in each of two tables per transaction. All non-key fields are changed. The narrow case has two columns; the wide case has 48. Every row and every column matches its source after publication, with equal final digests across variants/repetitions. Initial copy and final value verification are outside the resource interval. Two repetitions reverse mode order in the second pair.

| Columns | Control CPU median (range) | Candidate CPU median (range) | Interpretation |
| ---: | ---: | ---: | --- |
| 2 | 10.59 s (10.47–10.71) | 10.45 s (10.26–10.64) | Existing small-run plan retained; no established speedup |
| 48 | 17.89 s (16.90–18.87) | 16.08 s (15.63–16.52) | About 10% lower median pipeline CPU in this workload |

Wide-case PostgreSQL CPU falls from 10.45 to 8.95 seconds; engine CPU is 7.43 versus 7.13 seconds. That is consistent with reduced PostgreSQL conversion work, rather than a new Iceberg write algorithm. Source COMMIT p99, sampled publication delay and object-request figures are retained in full; these short trials do not establish latency improvements. Mirror requests are similar (992 versus 1,001 median), as is WAL (36.71 versus 36.78 MiB).

CPU includes PostgreSQL, its workers, the engine and container observers, excluding host/controller/MinIO. All containers share a local host and caches. Two counterbalanced pairs provide descriptive evidence, not statistical confidence intervals, production sizing or capacity limits. The larger isolated SQL gain is diluted by the rest of the pipeline. This does not accelerate the common narrow, single-row UPDATE case.

## Correctness and recovery

Direct comparisons with the original function cover numeric/float/temporal/binary/JSONB values, arbitrary identifiers, omitted large fields, bulk key changes, threshold boundaries, row/statement trigger counts, missing/duplicate-match rollback, and custom-domain conversion behavior. Migration rollback preserves function body/privileges, installation preserves privileges, and the function remains an extension member.

The packaged streaming fixture verifies actual 48-column source tables with external TOAST, 32-row update runs, omitted unchanged payload values, exact JSONB numbers beyond a float mantissa, nullable-column evolution and key changes. Journal inspection confirms that streamed tuples meet the wide-run gate while omitting the large payload.

Wide crash tests use 48 columns and 32 rows per target. Both post-upload and post-COMMIT failures operate on qualifying wide UPDATEs; staged tuple checks prove eligibility. Committed source heaps and durable admission survive, publication checkpoints match COMMIT outcome, and retries restore exact mirrors.

## Reproduce and install

Select cached fixture images, then use:

```sh
make local-iceberg-update-conversion-test
make local-iceberg-update-stream-test
make local-iceberg-update-conversion-compare
LOCAL_ICEBERG_WIDE_UPDATES=1 make local-iceberg-fault-test
```

The comparison uses a control image without migration 0034 and applies candidate SQL explicitly; the correctness/stream targets can use the packaged v8 image. The separate `update_conversion_probe.py` script runs the diagnostic plan matrix.

Migration 0034 is appended to fresh current beta5 installs and beta4-to-beta5 upgrades. Existing databases already on this unreleased beta5 need an explicit migration refresh through the installation workflow; replacing installed SQL files alone does not update existing function definitions. Frozen beta4 SQL remains unchanged. No new GUC or journal schema is introduced.

The [packaged qualification](evidence/2026-11-15-local-update-conversion-runtime.json) passes 185 checks across seven fixtures: wide API/migration/fallback checks (31), streamed wide updates (14), qualifying wide publication crashes (29), logical compatibility (54), scalar and full-row identity (18 each), and functional lifecycle/schema/restart checks (21). Every fixture cleans up successfully. `make check`, all-feature Rust tests and the 12 dashboard tests also pass.

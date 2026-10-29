# Native beta.4 probe findings and heartbeat fence correction

[Build CI 40839966901](https://github.com/proddata/lake_replication/actions/runs/40839966901)
passes at `a2282cd`. The subsequent
[native probe 40840260661](https://github.com/proddata/lake_replication/actions/runs/40840260661)
fails on both architectures for different reasons.

| Architecture | Result |
|---|---|
| ARM64 | All 65 managed checks and exact-data checks pass. Four copies finish in 157.78 seconds. Streaming p95 is 3.013/4.378/4.383 seconds against a two-second limit, so fairness fails. Source scheduling, publication gaps, final catch-up, overlap and clock checks pass. No observed engine OOM or PostgreSQL crash. |
| AMD64 | The upstream image builds, but Docker runs out of disk while staging cache export. Replication tests do not start. Only 13 GiB remained after the storage-cache cleanup, versus 34 GiB on ARM64. |

## Heartbeat conflict

ARM64 logs contain 21 `service_status` row-lock failures from `assert_worker`:
17 during acknowledgement and four during transaction staging. The worker
fence uses `FOR SHARE NOWAIT` on a row that routine heartbeat updates modify.
Those lock modes conflict, causing a normal heartbeat to abort receiver work.
This is a reproduced defect and a contributor to retries; it is not proof that
all native latency misses have the same cause.

The correction uses `FOR KEY SHARE NOWAIT` on the service row. Native stop
continues taking explicit `FOR UPDATE`, and configuration fencing retains
`FOR SHARE` on the launch configuration. Thus ordinary heartbeat updates can
proceed while stop and configuration replacement still exclude new admissions.
See PostgreSQL's [row-lock compatibility rules](https://www.postgresql.org/docs/18/explicit-locking.html#LOCKING-ROWS).

A deterministic regression holds a heartbeat update open in one transaction.
The previous clean CI archive fails worker admission; the corrected archive
admits it. Additional cases hold the stop control lock and an actual
uncommitted generation update, both of which must reject worker admission.
The full corrected managed suite passes 68 checks, including native stop,
restart, obsolete-generation fencing and abrupt staging disconnect recovery.

## Local contention evidence

Both corrected runs use the unchanged archive workload, with all services on
CPUs 0–1 and no SQL/binary overlay. Timing targets are unchanged.

| Attempt | Copy duration | Stream p95 | Result |
|---|---:|---:|---|
| First | 75.21 s | 0.484–0.545 s | Timing/data checks pass; only three overlapping copy publications observed, so the run remains inconclusive for the required four-copy exposure. |
| Repeat | 75.44 s | 0.620–0.663 s | All gates pass, including four overlapping publications. |

Both runs have zero `service_status` lock errors. These M5-host measurements
support the correction but do not replace native validation.

## Cache export correction

Streaming Docker output into gzip avoids an extra client-side uncompressed
archive. Docker can still stage image blobs internally; the AMD64 failure is
in `/var/lib/docker/tmp/docker-export-...`, before gzip can consume them.

The workflow now frees unused Android/.NET SDKs on the ephemeral AMD64 probe
runner and prunes prerequisite build cache before export, retaining disk
snapshots. Cache export/upload are optional optimizations: failed export skips
cache upload, cleans the local archive, and allows replication tests to use
the already-built image. Runtime validation remains required. Cache load
integrity checks remain mandatory. GitHub exposes the pre-override result as
[`steps.<id>.outcome`](https://docs.github.com/en/actions/reference/workflows-and-actions/contexts#steps-context),
which controls whether upload is attempted.

The already-saved ARM64 cache remains reusable: upstream build/loader inputs
are unchanged. The AMD64 disk preparation and corrected replication behavior
still require native verification. No further remote run was started while
investigating and testing these changes locally.

[Machine-readable evidence](evidence/2026-10-29-native-beta4-probe-findings.json)
retains the native findings, archive/input hashes, baseline regression failure,
both local measurements and successful managed regressions. The candidate is
still not release-qualified.

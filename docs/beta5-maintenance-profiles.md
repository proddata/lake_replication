# Bounded maintenance investigation

Both local ARM64 ten-minute maintenance profiles pass against the unchanged
`b0a703a` packaged candidate and supported pg_lake runtime. Snapshot expiry
bounds the active metadata document, while physical file retention determines
when retired objects can be reclaimed. Neither run satisfies the 24-hour gate.

| Measurement | Snapshot-only control | Disposable bounded-test |
|---|---:|---:|
| Snapshot history | 60 seconds | 60 seconds |
| Orphaned-file retention | Ten days | 120 seconds |
| Source transactions | 1,151 | 1,154 |
| Assertions / maintenance passes | 118 / 17 | 118 / 17 |
| Final allocated object storage | 335.09 MiB | 99.95 MiB |
| Peak allocated object storage | 335.09 MiB | 123.27 MiB |
| Storage range during final three minutes | 234.20–335.09 MiB | 92.12–118.12 MiB |
| Final post-maintenance deletion-queue entries | 12,958 | 3,241 |
| Final post-maintenance oldest queued object | 587.06 seconds | 122.52 seconds |
| Peak destination / engine cgroup memory | 259.5 / 999.7 MiB | 388.8 / 735.8 MiB |

The runs use identical recorded harness sources, archive checksum, prerequisite
and runtime image identities, storage fixtures, CPU placement and resource
limits. The selected maintenance profile is the only command-line difference.
Each pass targets the fixture tables, limits compaction/removal work, and ends
with an exact comparison of all replicated rows and values. Current metadata
documents after the last pass total approximately 136 KiB in the control and
134 KiB in the bounded test. The control's protected retired object remains;
the bounded test observes its physical removal after eligibility. Failed-removal
counts remain zero.

Both recover exactly after supervisor SIGKILL, a ten-second storage outage,
destination restart and a disabled-subscription backlog. There are no observed
OOM events or unexpected container deaths. Subscription removal and fixture
teardown complete without errors. A supplemental observer checks all 1,929
references from the two current Iceberg metadata states directly in MinIO
during the bounded run, after destination restart. This verifies physical
objects independently of query caches at that observation point.

## Retention and coordination findings

The pinned source defaults are a 1,800-second snapshot history, ten-day
orphaned-file retention and 600-second autovacuum interval. Snapshot history
also controls the metadata-log length. Superseded metadata JSON enters the
deletion queue, so expiring snapshots alone cannot immediately remove its
stored predecessors while ten-day retention still applies.

The experiment disables automatic maintenance on its fixture tables and runs
targeted `VACUUM (VERBOSE) table` about every 30 seconds. It temporarily holds
the private subscription's publication-checkpoint row lock: publication waits,
while ingress and source writes continue within their existing staging bounds.
This uses internal tables and is not a supported public maintenance interface.
The production worker, libraries and global defaults are unchanged.

Preflight verifies an open repeatable-read result and a protected physical
metadata object across maintenance. That reader is younger than the retention
window. This does not prove that arbitrarily old readers or restored backups
remain safe after a shortened deletion interval. The deletion-queue source
filters by retention age; this investigation establishes no additional reader
age protection. The 120-second setting is suitable only for this disposable
experiment, with a deliberately reduced physical recovery horizon. The
60-second snapshot setting also reduces available time travel.

## Limits and next decision

The bounded profile demonstrates repeated reclamation over ten minutes. One
run per profile cannot establish long-term stability, a memory leak diagnosis
or statistical performance differences. Cgroup memory includes filesystem
cache. Visibility observations include polling, maintenance and faults and
are not the native qualification's streaming latency benchmark.

Storage now excludes MinIO's ephemeral staging directory, with bounded retries
for files removed during observation. Disk-free checks still cover staging.
The earlier unmaintained 912.8 MiB result is useful context but uses a different
sampler scope and harness; it is not the paired control in this report.

Before a full-day run, define the required snapshot/time-travel, longest-reader
and backup/restore windows, then select physical retention and storage capacity
to cover them. Keep the production ten-day default pending that decision.
Validate the chosen policy with readers and recovery across its boundaries,
and provide an operator maintenance path that avoids conflict with publication.
The experimental profiles always report `qualifies_24_hour_soak: false`, even
when asked to run longer. A reviewed endurance profile and resource budget
remain necessary.

## Retained evidence

[Structured evidence](evidence/2026-11-06-beta5-maintenance-profiles.json)
contains both run identities, checkpoints, resource summaries, raw-artifact
checksums, measured input hashes and the independent reference check. Raw
sources/logs remain under the recorded local qualification directories. The
supplemental observer's measured source is retained separately; later changes
only make its output filenames unique and retain error details.

Five excluded attempts remain recorded: unsupported whole-catalog VACUUM syntax
with a relation list; an uncoordinated vacuum warning; a disappearing MinIO
temporary file during sampling; an attempted all-permit barrier that could
wait on its own permit; and a clock-discontinuity rejection. The final pair
uses targeted maintenance and a publication-row barrier, the same bounded
sampler, and the unchanged clock guard. No failed attempt counts as evidence
for the accepted profiles. `make check` passes, including 20 soak unit tests.

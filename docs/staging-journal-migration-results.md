# Staging and journal admission production migration

The beta.4 candidate integrates the selected staging-gate design into the shipped
extension and native workers. This is production-path implementation under local
qualification, not a released or fully qualified beta. The historical
[prototype](staging-gate-prototype.md) remains evidence for the policy choice;
its worker injection helpers target the earlier source at `b224180`.

## Implementation

Initial-copy workers acquire a distinct instance-wide transaction permit before
PostgreSQL analyzes/converts the append payload to JSONB. The permit is held on
the same destination backend through commit. The existing one bulk permit and
four streaming slots remain independent. Managed destination sessions set
`client_connection_check_interval=100ms` to allow PostgreSQL to notice socket
closure while a worker is queued or executing.

The receiver remains connected for up to 64 transactions. SQL admission enforces
per-subscription limits of eight committed unpublished transactions and 64 MiB of
normalized JSONB payload. Checkpoint locking serializes concurrent producers.
A full queue returns a retry result without advancing the checkpoint; streamed
commit retries retain their durable segments. The receiver sends its previous
durable feedback LSN while waiting, with no destination transaction open. A
single oversized transaction fails explicitly instead of retrying forever.
These are logical journal limits, excluding physical duplication, decode buffers
and incomplete streamed transactions; they do not bound total instance memory.

Receiver, publisher and initial-sync destination operations hold
service/configuration generation fences through commit. The supervisor retains
its separate control/maintenance connection and the existing launcher ownership
protocol. Stop cancels that subscription's named destination queries before
trying to take the control row, with a five-second drain deadline. Re-enable
advances the generation. Failed wire synchronization closes and poisons the
connection; synchronized server errors roll back before reuse. Live cycle
counters now update before a long-lived receiver exits.

Migration 0025 is shipped as beta.3 → beta.4 and as part of a fresh beta.4
installation. All eight previous SQL artifacts remain frozen byte for byte.
The journal policy is included in extension configuration dumps. Upgrade requires
subscriptions stopped and new binaries installed using the documented server
restart procedure; there is no online handoff or SQL downgrade.

## Evidence scope

Local candidates were built as arm64 Linux archives from a dirty working tree
based on `b224180`; they are development artifacts, not final release artifacts.
The checks use exported archives without SQL or binary overlays unless explicitly
noted. Local Docker CPU affinity on the M5 host is a contention sensitivity test,
not evidence for a minimum cloud instance size.

Candidate 3 archive SHA-256:
`1730826923f02578f8c2b4725f9c05f2b76604cc538d93789aedabc03c1ca64a`.
The archive includes the final worker behavior, including live counters. Later
operator documentation and observer changes do not change those binaries.

- `.qualification/qualification_20261029_065824_production_admission`: 70 checks
  pass across managed admission and the unchanged beta.3 archive upgrade. This
  includes concurrent producer reservations, count/byte limits, streamed retry,
  native eight-transaction saturation, source feedback, disable/resume, obsolete
  generation rejection, native staging waiter/holder cleanup and exact data.
- `.qualification/qualification_20261029_065056_production_admission`: managed,
  locks and pool checks pass. Four-copy data/latency gates pass, but the observer
  matches only unwrapped SQL and reports zero active copy publications. Retained
  as inconclusive overlap evidence. The observer now also recognizes named
  managed queries wrapped in `BEGIN … COMMIT`; thresholds are unchanged.
- The first draft fresh lane passed 57 checks at
  `.qualification/qualification_20261029_063126`. It predates final fencing/wire
  cleanup/live-counter changes and does not qualify the final candidate.
- `make check` passes, including Rust wire-recovery tests. Loopback TCP tests
  require permission to bind local sockets in the agent sandbox.

The first two-CPU measurement at
`.qualification/qualification_20261029_065951_production_admission` completes
copies in 74.78 seconds with stream p95 533–587 ms and exact data. It fails two
source scheduling checks (maximum lateness 0.593/0.598 seconds against 0.5), and
uses the obsolete observer. The observer correction attempt at
`.qualification/qualification_20261029_070221_production_admission` fails on an
extra SQL parenthesis; it is retained as a fixture error.

The corrected-observer run at
`.qualification/qualification_20261029_070325_production_admission` passes all
unchanged gates: 74.40-second copies, p95 448/898/579 ms, four simultaneously
active or queued copy publications, and source maximum lateness
0.010/0.187/0.205 seconds. All services share CPUs 0–1 and the measured archive
has no SQL/binary overlay. This passing run does not erase the preceding source
scheduling miss. Native repeatability remains unqualified.

Candidate 4 updates bundled operator documentation; all 16 production payload
files (workers, library, control and SQL) are byte-identical to candidate 3. Its
archive SHA-256 is
`f1fc4c365209e72f89d250a0a97a027b1b9f8e3249822f3bac9d3681f61f8db4`.
The first final fresh attempt at
`.qualification/qualification_20261029_070613_base_lifecycle` fails on the old
credential-rotation generation expectation (2 rather than 3): connection update
and re-enable now each advance the generation. Expectations for the two rotations
are corrected to 3 and 5; production code is unchanged.

The final successful archive checks are:

| Lane | Result | Evidence directory under `.qualification/` |
|---|---|---|
| Fresh lifecycle, restore and uninstall | 119 checks pass | `qualification_20261029_070818_base_lifecycle` |
| Forced supervisor/receiver crashes | 17 checks pass | `qualification_20261029_070958_base_faults` |
| Empty/filtered and persistent receiver recovery, archive only | 96 checks pass | `qualification_20261029_071020_empty_filtered` |
| Managed admission, abrupt staging disconnect, active-copy cancellation, sixteen-stream resource regression, beta.3 upgrade | 169 checks pass | `qualification_20261029_071431_production_admission` |
| beta.2 stopped-server upgrade and SQL rollback | 24 checks pass | `qualification_20261029_071825_production_admission` |
| beta.1 stopped-server handoff, adoption, restart and catch-up | 47 checks pass | `qualification_20261029_071851_base_upgrade` |

Schema and rebuild cases pass in
`qualification_20261029_071117_production_admission`; its following cancellation
case fails because its observer also expects unwrapped SQL. That observer is
corrected using the same managed-query match, and cancellation subsequently
passes while observing a real logged engine write. Lock and pool cases pass in
the earlier candidate-3 run; the production payload is identical. The native
staging crash tests pause supervision and kill the initial-sync child without
DISABLE or backend cancellation, verify backend/permit cleanup and rollback,
then resume supervision and verify exact data.

The [machine-readable evidence](evidence/2026-10-29-staging-journal-migration-results.json)
retains successful, failed and superseded attempts, input/result hashes, payload
identity, source scheduling and measured latency. Raw logs and source snapshots
remain in the referenced local qualification directories.
Do not transfer previous beta.3 gate PASS status to this candidate.

## Remaining release work

Local integration checks are complete. Run short native probes
on both architectures before repeating full remote qualification. The historical
interrupted-run engine OOM and AMD64 Docker/receiver hang remain open. Native
final-commit qualification, compatibility, security/supply-chain review,
coordinated PostgreSQL/object-store recovery and a 24-hour packaged soak remain
required by [the release gates](../BETA-RELEASE.md).

# Supported-runtime native qualification

[Full run 42182134146](https://github.com/proddata/lake_replication/actions/runs/42182134146)
uses the CI archives at `6d1ef13` with the supported pg_lake prerequisite
patchset selected by default. No receiver SQL or worker binary overlay is used.

AMD64 passes all 661 checks: fresh lifecycle, verified TLS/SCRAM, forced child
crashes, receiver regressions, coordinated physical recovery, production
admission and both historical package handoffs. The valid four-copy workload
finishes in 162.50 seconds; streaming p95 is 1.606 / 1.707 / 1.563 seconds.
There is no engine OOM or unexpected PostgreSQL crash.

ARM64 passes 119 lifecycle, 19 TLS and 17 child-crash checks. The receiver lane
passes its first 63 assertions, then `docker compose exec ... pkill -KILL ...`
times out after 15 seconds. The cleanup attempt to issue the same kill also
times out; later log capture and fixture teardown succeed. Host evidence shows
ample available memory and disk. Later ARM64 recovery, admission/fairness and
package-handoff steps are skipped. This architecture has not passed full
runtime qualification.

## Receiver fixture correction

The test starts an asynchronous receiver with stdout and stderr pipes, but does
not drain them until after inspecting database state and issuing SIGKILL. A
verbose streamed transaction can fill those pipes and block Docker output
handling. The fixture now redirects asynchronous receiver/source output to
retained files. The kill command still has its 15-second limit, and waiting for
the receiver still has its 10-second limit. A new assertion requires the Docker
exec receiver to exit with SIGKILL status 137. No production worker code changes.

A focused control demonstrates that a child writing 1 MiB to each unread pipe
cannot finish until its output is drained. The file-backed helper completes
before any output is read and preserves both complete streams. Existing bounded
cleanup and primary-failure preservation checks also pass.

The original unmodified probe passes all 96 checks locally against the exact
failing ARM64 archive, so the native timeout is not consistently reproduced.
The corrected local probe passes all 97 checks against that same archive.
SIGKILL completes in 0.058 seconds, and the interrupted receiver log contains
4,432,709 bytes. This establishes a real backpressure hazard and removes it;
it does not conclusively prove that this caused the CI timeout. Native ARM64
requalification remains necessary, with the failure retained.

[Structured evidence](evidence/2026-11-06-beta5-full-runtime-qualification.json)
records both native architectures, exact archive and prerequisite identities,
raw-result and artifact checksums, the local original/fixed probes and retained
log hashes. All general checks pass. Full qualification on the corrected
fixture, the release-candidate soak and final publication gates remain open.

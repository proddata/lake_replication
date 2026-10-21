# Receiver connection lifetime and supervision: fix results

The receiver now retains its logical replication connection through quiet
source decoding periods. The native two-million-row excluded-transaction case
passes, with prompt disable/drop and recovery after source interruption. All
96 integration checks and `make check` pass on PostgreSQL 18/arm64 with pg_lake.

## Changes

The supervisor starts its receiver with `LAKE_RECEIVER_PERSISTENT=1`. For that
child, the existing 200 ms socket timeout becomes a feedback tick: it sends only
the last durably staged LSN and continues reading. Standalone receivers retain
their existing idle-exit behavior unless explicitly run in persistent mode.
There is no larger fixed decoding deadline or feedback beyond durable staging.

The wire client retains header/body offsets across timeouts. Its framing reader
uses counted reads rather than `read_exact`, which cannot report partial bytes
consumed before an error. This prevents a resumed read from interpreting the
middle of a message as a new header. Repeated timeout tests cover every header
and body boundary, consecutive messages, zero-length bodies, EOF and invalid
lengths. This reader operates above both plain TCP and rustls transports; this
integration fixture uses plain local connections, not a fresh TLS qualification.

The supervisor monitors its receiver while it runs, checks stop/pause state on
its configured poll interval, drains already-staged work without waiting for a
new source commit, and updates idle heartbeats. Healthy monitoring resets retry
backoff. A child guard kills/reaps the receiver on return and error paths.
Receiver logs flow directly to the supervisor log rather than accumulating an
unbounded streamed-transaction output buffer. Receiver failures retain their
structured child log; the supervisor records the exit status and log reference.

A `FOR SHARE ... NOWAIT` probe of the service row also handles DROP's uncommitted
stop request. A normal MVCC read sees the old running state while DROP holds
that row locked and waits for source-slot cleanup. The nonblocking probe makes
the supervisor stop its receiver on that control-row conflict, releasing the
source slot before DROP commits. Other concurrent writes to that control row
can likewise cancel the receive cycle and cause a safe retry. Source cleanup
and authorization logic are unchanged. The C changes are explanatory comments;
the tested native extension binary is the previously qualified archive's binary.

## Validation and retained evidence

```sh
make receiver-progress-test
make check
```

The [retained evidence](evidence/2026-10-21-receiver-progress-fix-results.json)
records all 96 checks, source fingerprints, unchanged archive checksum, generated
SQL fingerprints, test image ID and worker-log hash. Raw data and complete native
worker logs remain in
`.qualification/qualification_20261021_064606_empty_filtered/`.

| Check | Result |
| --- | --- |
| Fresh/upgrade, empty commits, replay, rollback and restart | Pass |
| Idle receiver retains its source connection beyond 200 ms | Pass |
| Real change after excluded two-million-row transaction | Pass |
| Native filtered commit is durably published | Pass |
| Disable while idle | 0.769 s, source slot released |
| Publish real work staged by a standalone receiver before enable | Pass, no new source commit needed |
| Source shutdown/restart and resumed replication | Pass |
| Disable during an uncommitted streamed transaction | 0.157 s, source slot released |
| Enable after that source commit | Exact replay, no duplicate rows or leftover fragments |
| Drop while receiving | 0.434 s, source slot removed |
| Destination crash/reinitialization signatures | None |

`make check` passes 32 Rust tests, including the new framing tests, plus the
existing Python checks, generated-SQL checks, formatting and compose validation.
The baseline install SQL still matches the previously qualified archive.
All private containers and volumes were removed. The validation run itself did not commit or push.

The initial implementation passed bulk progress, disable and outage recovery
but exposed DROP waiting about six seconds and leaving its source slot. The
NOWAIT probe fixed that; the final run above includes actual source-slot removal.
The earlier [native timeout diagnostic](evidence/2026-10-21-native-bulk-timeout-diagnostic-results.json)
remains historical evidence, not a current failure of these tested sources.

## Scope and next work

This is source-build integration evidence: current SQL and rebuilt Rust workers
were installed over the prior qualified native archive in a disposable image.
A rebuilt release archive, package upgrades, amd64, a TLS integration rerun,
network blackholes and long-running performance/soak remain unqualified. Source
shutdown/restart recovery was tested; there is no new hard elapsed-time deadline
for a live connection with no inbound data.

Publisher execution remains synchronous within the supervisor. These stop
measurements cover idle/streamed reception, not arbitrary long-running publisher
SQL. Existing launcher/descendant hardening and physical-takeover requirements
remain separate release work; this change does not close those release gates.

Return next to admission specific to engine work: retain all ownership checks,
keep PostgreSQL-only staging outside the engine permit, and test bounded capacity
that reserves streaming progress while a bulk copy publishes atomically. Existing
admission performance results describe their original images and are unchanged.

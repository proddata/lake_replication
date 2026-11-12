# Source-slot cleanup retry

The update-filter investigation found that dropping a running subscription could
leave its source replication slot behind. Replication had applied the updates
correctly; the failure occurred during teardown in the historical SQL control.

Commit `30866e27f7df5c93d7a36d7c7be59771eed5949c` marks the C cleanup retry's
success flag `volatile`. PostgreSQL's `PG_TRY`/`PG_CATCH` uses setjmp/longjmp;
the flag must retain its value across a failed dblink operation. The change
preserves the existing bounded retry and source TLS policy.

A frozen native ARM64 archive includes that commit, the transactional TLS
refresh packaging and the uncommitted update-key filter. Concurrent demo edits
are excluded. Archive identity, input hashes, controls and raw result locations
are retained in [structured evidence](evidence/2026-11-12-source-slot-cleanup.json).

## Regression coverage

`tests/qualification/slot_cleanup.py` installs the archive into private Docker
fixtures and checks three DROP operations while native receivers hold their
source slots. Each subscription first completes initial copy and applies a new
source update. DROP must remove the source slot and subscription control row,
without restarting the destination postmaster.

A separate case disables a subscription, then holds its slot with an independent
`pg_recvlogical` client. The test starts DROP and waits for the source to report
an actual busy-slot error. DROP must remain pending while the slot is held.
After terminating the independent receiver, DROP must retry successfully and
remove the slot. This forces the error/retry path instead of depending on the
native receiver's shutdown timing.

The fixed archive passes all 30 checks and private fixture cleanup. The probe
is included in full packaged lifecycle CI. The same archive also passes the
57-check native packaged integration suite and all 94 update-filter type,
rollback and upgrade checks. Repository `make check` passes; no private
containers, networks or volumes from the packaged integration suite remain.
These are local regression results;
final-commit native amd64/arm64 qualification and the deferred 24-hour soak remain
open. The earlier failed archive and its evidence remain preserved.

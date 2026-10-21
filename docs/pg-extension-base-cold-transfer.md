# Controlled cold writer transfer

Status: the isolated rehearsal passes **40/40 assertions**. It demonstrates a
manual cold transfer with a complete private Docker writer inventory. It is not
production fencing, automatic failover, a lease or a generally supported restore.

## Tested transition

The [retained run](evidence/2026-10-19-base-cold-transfer-results.json) reuses the
exact publication-qualified image on PostgreSQL 18.6/Linux arm64. It performs a
real physical backup into a separate volume and uses a separate pgduck engine
and socket for the candidate. Production runtime sources remain unchanged.

| Phase | Verified behavior |
| --- | --- |
| Freeze original | Replication wrapper drains, source slot becomes inactive, Iceberg autovacuum is disabled, and checkpoint/catalog/object manifest is recorded. |
| Quarantine candidate | Background-worker launcher is disabled; deployment identity differs; direct publication is rejected. Stored origin remains unchanged. |
| Fence old writers | Transfer refuses while old PostgreSQL runs, then refuses while old pgduck still runs. Both must be stopped, with the same reviewed container IDs, zero container PID and no automatic restart policy. |
| Validate candidate | Physical cluster/database identities, full launch configuration, source identity, staging/publication checkpoints, Iceberg location and metadata content match the frozen manifest. Wrong checkpoint or slot configuration is refused before authorization. |
| Grant and enable | Only after the previous gates pass, the candidate starts under the transferred external profile. It remains durably stopped until explicit ENABLE. |
| Resume | Candidate catches up through the original retained source slot, applies updates/deletes/inserts to the same Iceberg location, and retains one initial copy. |
| Restart | The transferred profile survives an ordinary restart and replication continues. Old writer containers remain stopped. |
| Review failback | Once source acknowledgements exceed the old backup's staged checkpoint, returning to the stale original data directory is refused without starting it. |

The source continues accepting writes during the quarantined interval. Its slot
must remain inactive, durable, valid and no further acknowledged than the
candidate's staged checkpoint before transfer. The tested source identity and
publication remain unchanged; source failover/timeline changes are outside this
cohort and remain subject to the supervisor's continuity checks.

## Procedure represented by the rehearsal

1. Inventory every writer and the resources it can modify. Disable independent
   Iceberg maintenance, drain replication and other SQL writes, and record a
   reviewed manifest of configuration, identities, checkpoints, object locations
   and the current Iceberg metadata content.
2. Take a consistent physical PostgreSQL backup while that Iceberg snapshot stays
   frozen. Start its candidate with worker dispatch disabled and a quarantine
   identity. A backup alone does not grant permission to write shared objects.
3. Fence the original PostgreSQL and all descendants, pgduck, maintenance and any
   other storage clients. In this fixture, stopping both tracked containers ends
   their process namespaces, and automatic restart is disabled. Production needs
   an authoritative fencing mechanism, not an observation that a process appears
   stopped or a network request timed out.
4. Use a separate candidate engine/socket and validate the full reviewed launch
   config, source identity/slot, both checkpoints and the exact Iceberg catalog,
   object location and snapshot content. Fail closed on missing, stale or
   conflicting state. The script's manifest hash is a comparison aid, not a
   signed authorization or an independently trusted inventory.
5. Recheck the writer inventory and source slot, then transfer the external
   startup profile and enable the background-worker launcher. Do not rewrite
   stored origin identifiers or checkpoint LSNs to make a failed check pass.
   Explicitly enable the subscription only after validation and verify data,
   source progress, metadata progression and restart recovery.
6. Treat failback as a new reviewed transition. The old data directory becomes
   stale after the transferred owner acknowledges newer source WAL. It cannot
   safely consume that slot again merely by restoring the old startup profile.
   A new consistent backup of the current owner or the independently tested
   fresh-rebuild route is required when the old checkpoints no longer qualify.

The executable [transfer rehearsal](../experiments/base_workers/transfer.py)
contains deliberate refusal tests for live writers, incomplete fencing, wrong
checkpoint/configuration manifests and stale failback. It is hard-coded to
private fixtures and is not a production transfer command. Consumer routing is
not changed or qualified.

## Boundaries

This procedure uses a frozen PostgreSQL-catalog Iceberg snapshot. It does not
recreate a writable table with CREATE at a non-empty location, so it does not
resolve the earlier full logical dump/restore limitation. It is not an arbitrary
old-backup recovery or an in-place SQL rebind API. External REST-catalog takeover,
long rollback retention, source WAL loss and unknown writers remain unqualified.

Docker state inspection is only a test-controller gate. Another administrator
could restart the old container, or an untracked host could reuse its credentials
and profile. The fixture has one controller and a complete inventory; production
requires durable fencing of those possibilities. Local row locks and deployment
labels are not storage-level fencing tokens. A distributed lease must prevent an
expired owner from committing, rather than merely expire in a control database.

The copied and original engines are distinct. Iceberg autovacuum remains disabled
through the test; resuming maintenance is a separate operational step after
exclusive ownership is established. Direct privileged SQL and other storage
clients are outside the worker SQL guards and must be included in fencing.

`make check` passed. Harness/runtime hashes were verified, all private containers
and volumes were removed, and production worker/migration/extension files still
match the archived source revision. Exact harness and logs remain under
`.qualification/qualification_20261019_210537_transfer/`; retained JSON includes
their hashes. The initial fixture attempt reached no authorization grant because
its engine was not ready; the completed run waits for engine health before
snapshot validation. No new runtime implementation was needed for this rehearsal.
New work remains local, uncommitted and unpushed.

Next requalify the complete prototype's multi-database behavior, diagnostics,
performance and supported package transition. Architecture coverage, worker-slot
limits and the long soak remain open before adoption.

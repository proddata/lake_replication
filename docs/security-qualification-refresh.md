# Local security qualification refresh — 2026-11-12

Clean source commit `30866e27f7df5c93d7a36d7c7be59771eed5949c` passes
the requested local archive qualification on native ARM64. Both supported
architecture archives were rebuilt; AMD64 runtime qualification was not run.
The [evidence manifest](evidence/2026-11-12-security-qualification-refresh.json)
records exact archive hashes, source revisions, prerequisite/runtime identities,
check counts, raw-evidence hashes, and superseded attempts.

| Check | Result |
|---|---|
| Clean-checkout `make check` | Passed |
| ARM64 and AMD64 archives | Built; all 135 inner checksums per archive and worker ELF architectures verified |
| ARM64 install, upgrade, backup/restore, replication, credentials and recovery suite | 57/57 checks passed |
| ARM64 archive TLS/SCRAM suite | 21/21 checks passed |
| Actual pre-policy beta.5 archive to new beta.5 refresh | 26/26 checks passed |
| Qualification containers and volumes | Removed; absence verified for all three final projects |

The three archive suites contain 104 recorded checks, including repeated archive
identity checks. They are not the full multi-architecture release workflow.

## Existing beta.5 database refresh

Qualification exposed a packaging gap: an earlier beta.5 database needs the new
TLS helper, but same-version `ALTER EXTENSION UPDATE` cannot install it. Commit
`ce7f5cda79790bb1a1879ce485a3af7d3178f369` packages
`DEVELOPMENT_UPDATES/0029_source_tls_policy.sql` with the library path expanded.
It refuses enabled subscriptions and registers the newly created function as an
extension member when applied outside a normal extension upgrade.

The handoff test starts the actual archived pre-policy beta.5 package, completes
a snapshot, drains the subscription, replaces the installed package with
PostgreSQL stopped, and applies only the refresh extracted from the new archive.
It verifies refusal while enabled, transactional rollback, restricted helper
execution, fixed definer context, extension membership, unchanged checkpoints and
initial-copy state, backlog replay, streaming, restart recovery, source slot
cleanup, and removal of the refreshed helper with the extension.

## Cleanup retry failure and fix

The first full replication run on `ce7f5cd` passed 44 checks and failed the 45th:
`DROP SUBSCRIPTION` returned while its active source slot remained. That failed
run is retained in the evidence manifest, alongside the earlier passing refresh
probe; neither is substituted for the final candidate results.

Commit `30866e2` makes the cleanup success flag `volatile`, preserving it across
the `PG_TRY`/`longjmp` error path. The final archive suite observes an actual
"slot is active" source error and then successful cleanup, exercising the retry
path that failed previously. TLS cleanup and the package handoff also pass on
this exact rebuilt candidate.

## Scope and remaining gates

Archives are retained under `dist/release/security-30866e2/`. Local raw logs and
result files are retained under `.qualification/security-20261112-30866e2/`;
the committed manifest identifies their hashes. The build used a clean detached
checkout and excludes the unrelated demo and update-performance working-tree
changes. Those changes are not qualified by these results.

Native AMD64 runtime qualification, remote CI and the full release workflow on
this revision, a fresh archive vulnerability audit, the deferred 24-hour soak,
and signed publication/download verification remain outstanding. The existing
[open security TODOs](security-todos.md) remain open. This local refresh does not
close the release gates or supersede the earlier full native evidence for a
different candidate.

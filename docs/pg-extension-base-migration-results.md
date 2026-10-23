# Production pg_extension_base migration — beta.2 candidate

The main extension now uses pg_extension_base for launcher lifecycle management.
This is production source, not an adapter applied under `experiments/`. The
candidate passes 279 integration checks on Linux arm64/PostgreSQL 18.6 with the
pinned pg_lake 3.6 prerequisite. `make check` passes, including 35 Rust tests.
The migration is preserved as a local checkpoint; it has not been pushed or published.

## Runtime and upgrade boundary

The extension directly requires `pg_extension_base` and carries its preload
annotation. Its SQL registers one `internal -> internal` launcher per installed
database. The callback uses the connection supplied by the framework; static
`RegisterBackgroundWorker` registration is removed. Fresh configurations need
only the base preload, without a configured launcher database or session preload.
Legacy explicit preload/database settings are accepted for package handoff.

The launcher retains dynamic worker handles outside SPI transaction memory,
waits for retirement before replacing them and frees retired handles. Wrappers
own a private Rust process group, use interruptible SIGCHLD/latch waits, stop
and reap descendants on exit, and use Linux subreaping to avoid passing orphaned
children to a PostgreSQL postmaster running as PID 1. Logs include the database
OID so subscriptions with equal IDs in different databases have separate files.

Transient source-slot release races retry for at most five seconds after source
identity/WAL validation; persistent contention remains fatal. An active slot is
never consumed. The receiver fixes from `0401314` remain in the candidate.

New subscriptions record destination system identifier/database OID. Dispatch
and source-slot cleanup reject copied bindings, even when the stored name matches
the new database. Names must also match, so database renames remain conservatively
fenced. Origin helpers use fixed search paths, narrow grants and security-definer
reads instead of granting administrators broad control-system access. A
`pg_read_all_data` role cannot invoke the management helper.

Version boundaries are explicit:

- Frozen `0.1.0` and beta.1 install/upgrade SQL match the prior qualified archive.
- Beta.2 fresh install includes migrations 0001–0023.
- Beta.1-to-beta.2 runs 0022 (empty commits) and 0023 (base lifecycle).
- The direct development-baseline-to-beta.2 path is also shipped.

The handoff requires draining subscriptions, stopping PostgreSQL, replacing the
package, restarting, upgrading SQL, then explicitly enabling subscriptions. The
new binary never starts the old static owner before registration. Enabled legacy
rows are refused. Existing rows require explicit verified original-destination
adoption with `SET LOCAL lake_replication.allow_legacy_binding_upgrade=on` in the
upgrade transaction. The upgrade cannot infer missing legacy provenance; copies
require a rebuild. See [the operator procedure](upgrade-and-rollback.md).

## Accepted qualification

[Retained evidence](evidence/2026-10-21-base-lifecycle-migration-results.json)
includes source hashes, both archive identities, all checks, raw-run references
and log hashes. All accepted runs use the same executable/SQL payload.

| Lane | Passing checks | Main coverage |
| --- | ---: | --- |
| Fresh packaged lifecycle | 119 | Baseline upgrade and backup/restore, role boundaries, two live databases, extension CREATE/DROP rollback, restart, launcher replacement, template/failed database drop, clone fences, paused-supervisor cleanup, uninstall |
| Packaged beta.1 handoff | 47 | Real old static owner, full restart, no duplicate owner, enabled-row refusal, rollback, explicit adoption, retained checkpoint/copy/slot, catch-up, restart and native drop |
| Packaged receiver regression | 96 | Empty/mixed streams, two-million-row excluded commit, staging/feedback/publication order, source outage, disable/drop and interrupted-stream replay |
| Forced Rust child crashes | 17 | Supervisor/receiver SIGKILL, descendant reaping, one replacement wrapper, resumed exact data and unchanged postmaster |

Ten enable/disable cycles return worker-handle memory to its original usage.
Forced-child tests target the Rust processes, not a PostgreSQL wrapper or
postmaster SIGKILL. They establish the stated cleanup/recovery boundary, not
recovery from every possible OS/process-tree failure.

Run the lanes against an exported archive using:

```sh
QUALIFICATION_ARCHIVE=/path/to/beta.2-linux-arm64.tar.gz make lifecycle-test
QUALIFICATION_ARCHIVE=/path/to/beta.2-linux-arm64.tar.gz make lifecycle-upgrade-test
QUALIFICATION_ARCHIVE=/path/to/beta.2-linux-arm64.tar.gz make lifecycle-fault-test
QUALIFICATION_ARCHIVE=/path/to/beta.2-linux-arm64.tar.gz \
  python3 tests/streaming/empty_filtered.py --native-bulk
```

The upgrade lane defaults to the retained local beta.1 arm64 archive; use its
`--old-archive` argument to select a different qualification platform/archive.
All private containers and volumes were removed. Raw evidence remains under
`.qualification/qualification_20261021_171331_base_lifecycle/`,
`qualification_20261021_171332_base_upgrade/`,
`qualification_20261021_171013_empty_filtered/` and
`qualification_20261021_171013_base_faults/`.

## Artifact identities

The runtime-qualified archive SHA256 is
`d31a0d11e4609841f93fc118172dd19abbd0865007e2fcf35e9f9ed8618c10f6`.
After validation, compatibility/removal documentation was corrected to distinguish
historical beta.1 claims from this candidate. The local candidate is
`dist/release/lake_replication-0.1.0-beta.2-linux-arm64.tar.gz`, SHA256
`963e6dd28b4b20fa443e9f43a72f510bb7d7a73c1297ef66d510b610bf69d17e`.

A byte-for-byte comparison verifies that every executable, library, SQL/control
file, installer, uninstaller and all other files are identical. Only
`COMPATIBILITY.md`, `README.md` and the corresponding `SHA256SUMS` differ; both
manifests verify. The evidence retains this documentation-only refresh explicitly.
These archives were built from the dirty tree before the migration checkpoint
commit. Full source fingerprints identify the tested code; the archives are not
signed release artifacts.

## Remaining work

Admission scheduling is deliberately separate: no blanket engine permit or
wrapper pool has been adopted. The [current-runtime admission recheck](engine-admission-results.md)
passes the original restart and coexistence workloads without admission. The
historical OOM did not reproduce. A follow-up four-copy test exposes streaming
latency pressure; bulk-only admission passes its diagnostic and is the candidate
to qualify before production adoption.
Local origin checks are not a distributed lease, storage fencing, support for
physical takeover or authorization to share Iceberg objects between writers.

AMD64 and additional pg_lake revisions need candidate-specific qualification;
remote CI, a packaged soak, security/supply-chain review and a coordinated
backup/object-store recovery drill remain release work. TLS integration and
network-blackhole behavior were not requalified here. No online handoff or SQL
downgrade is provided. The beta release gates remain open.

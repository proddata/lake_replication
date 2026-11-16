# Native publication lock experiment

This child image exposes a private, superuser-only SQL bridge to the pinned
native `LockTableForUpdate` function. It uses the existing native lock identity
and transaction lifetime. It does not reproduce the internal advisory key in
SQL or change native compaction/INSERT behavior globally.

The optional SQL wrapper acquires locks for the complete frozen target mapping
in OID order before applying any changes. This includes every mapped target
before the first streamed chunk, even if that chunk only mentions one table.
The existing target eligibility/DDL guard runs before these locks; normal
schema checks and complete change ordering still run in the original apply
function. Locks persist until publication COMMIT or rollback. Deadlock retry
remains enabled for other conflicts.

Build the isolated runtime:

```sh
docker build -f experiments/wal_direct/prelock/Dockerfile \
  -t lake-local-iceberg:wal-direct-prelock-v14 .
```

Select this image and the pinned storage images from the direct recovery report.
Set `WAL_DIRECT_PRELOCK=1` to install the wrapper in a private fixture, then run:

```sh
make local-iceberg-wal-direct-prelock-test
python3 tests/local_iceberg/wal_direct_maintenance.py --transactions 1200 --interval .125
```

The environment flag belongs to the test harness. It does not enable a
production subscription mode. Installing the wrapper against a runtime without
the native bridge fails; no fallback silently runs without coordination. The
runtime marker records source/library hashes and each fixture retains it.
The script refuses an unrecognized native source hash. The vendor checkout,
release prerequisite patchset and normal direct SQL install remain unchanged.

This adds serialization against native compaction before publication writes.
It can move or lengthen lock waits and does not guarantee lower CPU/latency.
A fixed mapping does not establish lock-order compatibility with arbitrary
external writers. Native rebuild/upgrade compatibility, managed ownership,
production privileges and general DDL remain separate work.

# Admission experiment helpers

Admission policy is still experimental. The production extension now uses
pg_extension_base, while the measured one-permit policy remains unadopted.
See the [findings and next policy experiment](../../docs/pg-extension-base-admission-performance-results.md).

This directory retains reusable measurement code:

- `compare.py` and `sample.py`: workload execution, visibility bounds and resource sampling.
- `scale.py`: multi-database restart and worker-budget cases.
- `admission_measure.py`: timing validation, bulk-copy coexistence and paired analysis.
- `admission_probe.py`: transaction admission, cancellation and ownership checks.
- `analyze.py`: analysis of recorded comparison cohorts.
- `test_*.py`: visibility, statistical-decision and clock-audit regressions, run by `make check`.
- `admission.c.inc` and `admission.sql`: reference implementation of the measured
  one-permit policy, retained for the scheduling redesign. The SQL depends on
  historical prototype ownership guards; it is not an installable production migration.

The workload and case modules are libraries for the next policy driver. Their
old command-line entry points, which rebuilt obsolete launcher variants or
required historical cached image IDs, have been removed. They do not silently
substitute current binaries for the previously measured ones. The admission
transaction checks require an explicitly supplied experimental image exposing
the prototype admission functions and views; they are not production smoke tests.

Run the retained regressions with:

```sh
python3 -m unittest discover -s experiments/admission -p 'test_*.py'
```

Historical commands, exact source fingerprints and Dockerfiles are available
in [the pre-cleanup snapshot](https://github.com/proddata/lake_replication/tree/7ef18381810075dddd26137ee959770591b95863/experiments/base_workers).
Recorded evidence under `docs/evidence` is unchanged. Replaying those results
requires that historical source and its original package/image prerequisites.
Use the current packaged lifecycle lanes for release qualification.

## Current runtime experiment

`reserved_probe.py`, `Dockerfile.reserved`, `reserved.c.inc` and `reserved.sql`
test engine-only admission on the migrated runtime. They compare the unmodified
archive, one global engine permit, and separate bulk/streaming permits. They do
not patch production source. See the [protocol and decision limits](../../docs/engine-admission-experiment.md).
The [current results](../../docs/engine-admission-results.md) identify four-copy
latency pressure and support qualifying a bulk-only permit before production adoption.

```sh
python3 experiments/admission/reserved_probe.py --variants reserved bounded --cases locks
python3 experiments/admission/reserved_probe.py --variants unbounded reserved --cases scale fairness
```

For current bulk pressure, compare four simultaneous copies and the bulk-only
candidate, which leaves streaming concurrency unchanged:

```sh
python3 experiments/admission/reserved_probe.py --variants unbounded --cases fairness --bulk-copies 4
python3 experiments/admission/reserved_probe.py --variants bulk_only --cases locks scale fairness --bulk-copies 4
```

`operational_probe.py` qualifies the bulk-only overlay against schema admission
and DDL timeouts, lock-order deadlock abort/retry, concurrent independent schema
and rebuild operations, promotion rollback, and cancellation during a real
native engine copy request. It uses disposable fixtures and saves exact input
snapshots. Engine debug logs stay private because they may contain fixture
connection information. See the [accepted results and exclusions](../../docs/bulk-admission-operations-results.md).

```sh
python3 experiments/admission/operational_probe.py --cases schema rebuild cancel
```

`performance_probe.py` runs five alternating pairs of the current archive and
bulk-only overlay for steady streaming, catch-up and single-copy coexistence.
It preserves the 120-transaction, 100-row, 128-byte journal-observer protocol
(500 ms steady / 100 ms catch-up), with ten warm-up transactions and five idle
seconds. Single-copy controls retain two million copied rows and three live
streams; they do not require a bulk waiter when only one bulk job is offered.
The p95 ratio limit is 1.10, catch-up throughput floor is 0.95, and single-copy
duration ratio limit is 1.10. Exact whole-pair bootstrap intervals are descriptive
for these local runs. An inconclusive result does not pass the gate.

```sh
python3 experiments/admission/performance_probe.py
```

The [completed paired results](../../docs/bulk-admission-performance-results.md)
pass the local performance margins. Production migration and packaged release
qualification remain separate work.

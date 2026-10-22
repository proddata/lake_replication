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

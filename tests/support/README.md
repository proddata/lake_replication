# Shared qualification helpers

These libraries support the packaged admission and soak runners. They are test
infrastructure and are not installed in release archives.

| Module | Used for |
|---|---|
| `compare.py`, `sample.py` | Persistent SQL sessions, visibility measurement and Linux resource sampling. Both admission and soak runners use these. |
| `scale.py` | Multi-database lifecycle, worker-budget checks and resource inventory. |
| `admission_measure.py`, `source_workload.py` | Concurrent copy/streaming measurements, source workload generation, publication phases and clock validation. |
| `admission_cases.py` | Production schema contention, rebuild rollback and initial-copy cancellation/recovery cases. |
| `analyze.py`, `performance_analysis.py`, `resampling.py` | Paired-cohort validation, statistical decisions and exact resampling. Retained raw variant names permit analysis of historical measurements. |

The `test_*.py` files retain visibility-bound, sampling, clock-audit, incomplete
cohort and statistical-decision regressions. Run them with:

```sh
python3 -m unittest discover -s tests/support -p 'test_*.py'
```

`make check` includes this suite. Packaged integration coverage remains under
`tests/admission` and `tests/soak`; their evidence captures the shared helper
sources actually used. This directory contains no historical overlay builders
or alternative extension implementations. Old replay commands require the
[archived experiment source](https://github.com/proddata/lake_replication/tree/7602e0402c5e83f4a3aadaf3f8029844e90d4d5a/experiments/admission)
and its original package/image prerequisites. Historical reports and evidence
under `docs/` retain their original paths and hashes.

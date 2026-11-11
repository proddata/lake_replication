# Historical experiments

Shared measurement and operational helpers now live in
[`tests/support`](../tests/support/README.md). Production package qualification
lives under `tests/admission`, `tests/lifecycle` and `tests/soak`.

Historical admission overlays, alternative extension implementations and their
build/replay drivers have been removed from the active tree. They are not part
of the installed extension or release package. Reports and evidence remain
under `docs/`, with their recorded paths and source hashes unchanged.

To reproduce an old result, use the exact source snapshot, archive and runtime
identities in that result. The
[last source before this cleanup](https://github.com/proddata/lake_replication/tree/7602e0402c5e83f4a3aadaf3f8029844e90d4d5a/experiments/admission)
contains the admission experiment code and its historical command reference.
Older launcher experiments remain in the
[earlier archived snapshot](https://github.com/proddata/lake_replication/tree/7ef18381810075dddd26137ee959770591b95863/experiments/base_workers).

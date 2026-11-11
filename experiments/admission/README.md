# Archived admission experiments

The reusable measurement, sampling, timing, analysis and operational case
libraries have moved to [`tests/support`](../../tests/support/README.md).
Current admission qualification uses `tests/admission/probe.py`; soak validation
uses `tests/soak/probe.py`. Neither runner imports historical experiment code.

The old single-permit, reserved-capacity and streaming-pool C/SQL overlays,
Dockerfiles and build/replay drivers are preserved in
[the pre-cleanup source snapshot](https://github.com/proddata/lake_replication/tree/7602e0402c5e83f4a3aadaf3f8029844e90d4d5a/experiments/admission).
That snapshot also retains the original CLI instructions. Use the source and
package/image identities recorded with a historical result when replaying it.
Reports and evidence under `docs/` are unchanged by this cleanup.

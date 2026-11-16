# Performance and sizing

Reports describe the exact workload/runtime tested. Passing short tests do not establish production capacity; failed configurations and maintenance costs are part of the result. For the latest direct experiment, start with native lock coordination, then follow the baseline and profiling reports.

- [Acquiring native table locks before direct publication](wal-direct-prelock.md).
- [Sustained direct publication with native maintenance](wal-direct-sustained-maintenance.md).
- [Direct publication slowdown: file metadata and maintenance](wal-direct-publication-profile.md).
- [Direct publication load and memory sizing](wal-direct-sizing.md).
- [Same-instance mirroring versus remote logical pull](local-pull-comparison.md).
- [Coalescing windows between publications](local-coalescing-windows.md).
- [Ordered DELETE key-filter optimization](local-delete-filter.md).
- [Convert wide UPDATE rows once](local-update-conversion.md).
- [Direct publication with bounded coalescing](wal-direct-coalescing.md).
- [WAL-backed direct publication: first performance comparison](wal-direct-performance.md).

[Documentation home](../README.md).

# Same-instance mirroring versus remote logical pull

These measurements use the v5 runtime before the subsequent [DELETE key-filter optimization](local-delete-filter.md) and [coalescing-window fix](local-coalescing-windows.md). Both changes are shared by the two topologies; their local-only before/after results cannot update this topology comparison.

Both paths use the existing pgoutput receiver, journal and Iceberg publisher. This experiment changes placement: the journal/publisher/engine run beside the application database in the same-instance mode, or beside a separate destination PostgreSQL in remote-pull mode. The remote source is plain PostgreSQL from the identical runtime image; it loads no replication or Iceberg extension. Both topologies use one engine.

Total measured CPU is similar, with no consistent winner across these two workloads. Remote pull substantially reduces application-side CPU, memory and WAL by moving the journal and publisher to the destination. Same-instance mirroring reduces deployment count and shares the application instance’s resource and failure boundaries.

The [retained evidence](evidence/2026-11-15-local-versus-remote-pull.json) contains all eight fresh fixtures, 56 passing checks, runtime identities, input hashes, per-run samples and observed object operations. Final source/mirror rows match in every fixture, final row digests match across topology/repetition, and every fixture cleans up successfully.

## Workloads and method

- INSERTs: 1,000 transactions at an offered 100 transactions/second, four rows per table, two tables, 256-byte text cells; 4,000 final rows per table.
- Mixed: 120 transactions at an offered four transactions/second; each inserts four rows per table and, after the first, updates one and deletes one row from the preceding transaction in each table; 361 final rows per table, including 119 updated rows. Mixed operations retain global source order.
- Both: one-second publication coalescing, independent INSERT grouping enabled, journal limit 32 transactions/64 MiB, identical runtime/storage images and destination settings. The same runtime image also supplies the plain remote source, ensuring identical PostgreSQL binaries.
- Two repetitions per workload/topology; order reverses between repetitions. Fixtures run sequentially on fresh private volumes, with no competing qualification jobs. The remote destination retains the fixture’s unused empty application heaps/publication; these are never selected by its receiver.
- All containers run on the same local host. This tests topology placement and local container networking, with no WAN or host-failure isolation. No cold-cache clearing, statistical significance or production capacity maximum is established.

Values below are medians of the two per-run measurements; p99/p95 values are medians of per-run quantiles. CPU includes container resource observers and excludes host/controller/MinIO CPU. Application-side CPU includes the local engine in same-instance mode; remote application-side CPU is its source PostgreSQL only. The extra remote container requires one additional resource-sampling call. PSS is sampled process memory, excluding unmapped filesystem cache; it is not a hard memory requirement or reservation. Publication delay is an observed upper bound including two-second sampling/query delay.

## CPU and latency

| Workload | Topology | Application-side CPU (s) | Total PG + engine CPU (s) | Application COMMIT p99 (ms) | Publication p95 upper bound (s) |
| --- | --- | ---: | ---: | ---: | ---: |
| inserts | same instance | 7.08 | 7.08 | 4.65 | 2.51 |
| inserts | remote pull | 1.23 | 7.60 | 4.76 | 2.50 |
| mixed | same instance | 28.36 | 28.36 | 7.61 | 4.12 |
| mixed | remote pull | 0.97 | 27.39 | 7.23 | 3.34 |

For INSERTs, total CPU ranges are 6.94–7.23 seconds locally and 7.32–7.87 seconds remotely. For mixed mutations, ranges are 26.47–30.26 locally and 27.28–27.49 remotely. These short repeated runs do not support a general throughput or efficiency ranking. COMMIT p99 is similar under these offered loads; this does not establish behavior under application-server saturation.

Application PostgreSQL alone, excluding the local engine, uses 4.75 versus 1.23 CPU seconds for INSERTs, and 11.40 versus 0.97 for mixed mutations. Thus the application-side difference is not solely the engine’s placement.

## Memory, WAL and object operations

| Workload | Topology | Application-side peak PSS (MiB) | Total peak PSS (MiB) | Application WAL (MiB) | Total WAL (MiB) | Observed mirror S3 requests |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| inserts | same instance | 210.0 | 210.0 | 10.09 | 10.09 | 384 |
| inserts | remote pull | 36.9 | 233.9 | 3.41 | 10.43 | 384 |
| mixed | same instance | 252.9 | 252.9 | 12.22 | 12.22 | 2381 |
| mixed | remote pull | 34.8 | 273.0 | 0.89 | 12.78 | 2451 |

Total WAL counts the shared database once in local mode, and both databases in remote mode. Local application WAL includes journal/publication writes; remote application WAL excludes those destination writes. PSS peaks use simultaneous samples within each run rather than summing unrelated service peaks. Object counts cover the final mirror prefix during measurement; full per-API/byte categories and separate bucket requests remain in the evidence.

INSERT drain times are 1.64 seconds locally versus 1.62 remotely; mixed drain times are 0.55 versus 0.39 seconds. Maximum sampled backlogs range from 41–42 versus 42–45 INSERT transactions, and 10–11 versus 7–8 mixed transactions. Backlog includes committed transactions still in WAL, not just admitted journal transactions.

## Run it again

Select cached `PUSH_PG_LAKE_IMAGE`, `PUSH_STORAGE_IMAGE` and `PUSH_STORAGE_INIT_IMAGE`, then run `make local-iceberg-pull-compare`. The script supports `--smoke`, `--workload inserts|mixed` and `--repetitions 1..4`. All credentials and volumes are private disposable test fixtures; no application deployment is changed.

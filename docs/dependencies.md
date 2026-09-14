# Initial dependency inventory

This is an initial inventory, not a completed transitive license audit. No
third-party source is vendored into this repository. Source downloads and
patching occur inside the development image.

| Component | Selection | Upstream license/source |
|---|---|---|
| New project code | This repository | PostgreSQL License, see LICENSE |
| Rust worker core | Rust standard library only in the current slice | PostgreSQL License, see LICENSE |
| PostgreSQL | 18 Bookworm image pinned by digest | [PostgreSQL License](https://www.postgresql.org/about/licence/) |
| pg_lake | `8180647d4535fdb1ed223f92f301ba6094a8ec3e` | [Apache-2.0](https://github.com/Snowflake-Labs/pg_lake/blob/8180647d4535fdb1ed223f92f301ba6094a8ec3e/LICENSE) |
| DuckDB and extensions | Resolved by pinned pg_lake build | Each upstream license must be inventoried; patches retain upstream terms |
| Apache Avro | pg_lake submodule plus upstream patch | [Apache-2.0](https://github.com/apache/avro/blob/main/LICENSE.txt) |
| vcpkg | `2025.10.17` | [MIT](https://github.com/microsoft/vcpkg/blob/2025.10.17/LICENSE.txt) |
| Azure SDK for C++ | vcpkg dependency resolution | [MIT](https://github.com/Azure/azure-sdk-for-cpp/blob/main/LICENSE.txt) |
| MinIO server | `RELEASE.2025-04-22T22-12-26Z` | [AGPL-3.0](https://github.com/minio/minio/blob/RELEASE.2025-04-22T22-12-26Z/LICENSE) |
| MinIO client | `RELEASE.2025-04-16T18-13-26Z` | [AGPL-3.0](https://github.com/minio/mc/blob/RELEASE.2025-04-16T18-13-26Z/LICENSE) |
| Debian toolchain and runtime packages | Bookworm repositories | Mixed; inspect `/usr/share/doc/*/copyright` in built image |

MinIO is a separate local S3 test service; no MinIO code is copied into this
project. It is not a permissively licensed bundled dependency. Production
storage selection and distribution obligations need explicit review before
packaging. The replication code is independently developed and does not use
proprietary Crunchy Data implementation code or WalShadow code.

Remaining work: lock all runtime image digests and fetched dependencies, export
an SBOM and copyright files, review patched DuckDB extension dependencies, and
add automated inventory/license checks to CI.

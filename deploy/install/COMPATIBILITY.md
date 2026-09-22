# pg_lake_replication beta compatibility

This beta is a separate artifact installed next to an existing PostgreSQL and
pg_lake installation. It does not bundle either dependency.

| Component | Supported beta target | Validated configuration |
|---|---|---|
| PostgreSQL | 18.x | 18.6 |
| pg_lake | Local PostgreSQL catalog; extension version 3.6 | commit `8180647d4535fdb1ed223f92f301ba6094a8ec3e` |
| Operating system | Linux with Debian 12 (Bookworm)-compatible glibc | Debian 12 container images pinned in the build |
| CPU architecture | amd64, arm64 | amd64 and arm64 release artifacts |
| Object storage | S3-compatible storage supported by pg_lake | MinIO `RELEASE.2025-04-22T22-12-26Z` in the lab |

The exact pg_lake commit is a validated point, not an installer requirement or
a claim that adjacent revisions are compatible. Broader pg_lake revision
coverage has not yet been established. Test the full replication probe against
the intended pg_lake build before production use.

REST-catalog-backed destination tables are unsupported in this beta. The
supported destination is pg_lake's local PostgreSQL catalog. PostgreSQL 16, 17,
and 19 are also unsupported until their own compatibility lanes pass.

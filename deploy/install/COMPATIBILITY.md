# pg_lake_replication beta compatibility

This beta is a separate artifact installed next to an existing PostgreSQL and
pg_lake installation. It does not bundle either dependency.

| Component | Supported beta target | Validated configuration |
|---|---|---|
| PostgreSQL | 18.x | 18.6 |
| pg_lake | Local PostgreSQL catalog; extension version 3.6 | mainline commits `8180647d4535fdb1ed223f92f301ba6094a8ec3e` through `f609ecb17891383f021aebb0e249fe2994ed57bb` |
| Operating system | Linux with Debian 12 (Bookworm)-compatible glibc | Debian 12 container images pinned in the build |
| CPU architecture | amd64, arm64 | amd64 and arm64 release artifacts |
| Object storage | S3-compatible storage supported by pg_lake | MinIO `RELEASE.2025-04-22T22-12-26Z` in the lab |

The validated interval follows one pg_lake ancestry path from 2026-09-10
through 2026-09-22. The same beta archive passed the full artifact-only suite
at both endpoints; intermediate commits were not each tested independently.
This is a compatibility claim, not an installer-enforced commit requirement.
Test the full replication probe against builds outside this interval before
production use.

REST-catalog-backed destination tables are unsupported in this beta. The
supported destination is pg_lake's local PostgreSQL catalog. PostgreSQL 16, 17,
and 19 are also unsupported until their own compatibility lanes pass.

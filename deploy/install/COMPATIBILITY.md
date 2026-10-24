# lake_replication beta compatibility

This candidate is installed next to PostgreSQL and pg_lake; neither dependency
is bundled. Qualification is specific to the candidate archive and platform.

| Component | Candidate target | Current qualification scope |
|---|---|---|
| PostgreSQL | 18.x | PostgreSQL 18.6 |
| pg_lake | Local PostgreSQL catalog; extension version 3.6 | Pinned commit `8180647d4535fdb1ed223f92f301ba6094a8ec3e` |
| pg_extension_base | Supplied by the supported pg_lake installation | 3.6, preloaded at server startup |
| Operating system | Linux with Debian 12-compatible glibc | Pinned Debian 12 container images |
| CPU architecture | amd64 and arm64 build targets | beta.3 candidate qualification runs locally on arm64; native amd64 qualification pending |
| Object storage | S3-compatible storage supported by pg_lake | MinIO `RELEASE.2025-04-22T22-12-26Z` |

The earlier beta.1 archive passed artifact-only tests at pg_lake commits
`8180647d4535fdb1ed223f92f301ba6094a8ec3e` and
`f609ecb17891383f021aebb0e249fe2994ed57bb` on one ancestry path, twelve days
apart. That historical interval and earlier amd64 builds do not qualify the
new beta.3 bulk-admission implementation. Re-run the compatibility lane before
extending this candidate's validated range.

REST-catalog destinations, PostgreSQL 16/17/19, online static-to-base handoff,
automatic physical takeover, distributed fencing and a SQL downgrade remain
outside this candidate's qualified scope. Follow UPGRADE.md for the stopped-
server handoff to beta.3. Consult the repository's release gates before
publishing or treating a candidate as a supported release.

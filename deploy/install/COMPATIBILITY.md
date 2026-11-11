# lake_replication beta compatibility

This candidate is installed next to PostgreSQL and pg_lake; neither dependency
is bundled. Qualification is specific to the candidate archive and platform.

| Component | Candidate target | Current qualification scope |
|---|---|---|
| PostgreSQL | 18.x | PostgreSQL 18.6 |
| pg_lake | Local PostgreSQL catalog; extension version 3.6 with `lake-replication-manifest-cleanup-v1` for concurrent-copy latency | Full supported-runtime native qualification passes at `8180647d4535fdb1ed223f92f301ba6094a8ec3e` for candidate `b0a703a` |
| pg_extension_base | Supplied by the supported pg_lake installation | 3.6, preloaded at server startup |
| Operating system | Linux with Debian 12-compatible glibc | Pinned Debian 12 container images |
| CPU architecture | amd64 and arm64 build targets | beta.5 full native qualification passes on arm64 and amd64 at `b0a703a`, 662 checks each |
| Object storage | S3-compatible storage supported by pg_lake | MinIO `RELEASE.2025-04-22T22-12-26Z` |

Candidate `b0a703a`, including the rustls security update, passes full native
qualification on ARM64 and AMD64 at the pinned revision using the supported
runtime patchset. The retained evidence is in
[`docs/evidence/2026-11-06-beta5-full-native-runtime-pass.json`](https://github.com/proddata/lake_replication/blob/main/docs/evidence/2026-11-06-beta5-full-native-runtime-pass.json).
The same accepted CI-built ARM64 archive now passes 57 artifact-only checks
with the supported runtime at each exact pg_lake endpoint:
`8180647d4535fdb1ed223f92f301ba6094a8ec3e` and
`f609ecb17891383f021aebb0e249fe2994ed57bb`. They are twelve days apart on one
ancestry path. These are two tested points, not a guarantee for every intervening
commit. These endpoint probes run on native ARM64; the full native AMD64 suite
separately qualifies the pinned endpoint. The frozen committed harness excludes
the uncommitted Dockerfile optimization. See the
[compatibility refresh evidence](https://github.com/proddata/lake_replication/blob/main/docs/evidence/2026-11-11-beta5-compatibility-refresh.json)
for exact archive hashes, prerequisite/library identities and scope. Changed
release archives require fresh evidence.

The current runtime prerequisite applies the repository-maintained
`lake-replication-manifest-cleanup-v1` patchset to the pinned upstream source.
Its exact unguarded patch passes native pipeline, managed-worker, independent
Iceberg snapshot and four-copy latency diagnostics on ARM64 and AMD64 at
`e0d34f6`; the subsequent full supported-runtime suite passes at `b0a703a`.
The accepted archive's compatibility refresh passes both endpoints. Final
release-commit qualification and remaining release gates are still required
before publication. The marker at
`$(pg_config --sharedir)/pg_lake/lake-replication-patchset.json` records the exact
upstream revision, patchset and both library checksums. The worker installer
does not apply this prerequisite or enforce its revision; deploy it separately.

REST-catalog destinations, PostgreSQL 16/17/19, online static-to-base handoff,
automatic physical takeover, distributed fencing and a SQL downgrade remain
outside this candidate's qualified scope. Follow UPGRADE.md for the stopped-
server handoff to beta.5. Consult the repository's release gates before
publishing or treating a candidate as a supported release.

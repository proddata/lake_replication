# Merged update-filter candidate: native qualification

Merged candidate `d5e05d2780796dd9979e99a08edfdf57c2266a3d` passes full
[native lifecycle qualification](https://github.com/proddata/lake_replication/actions/runs/43177860675)
on AMD64 and ARM64. Both use the exact archives produced by the successful
[default-branch push CI run](https://github.com/proddata/lake_replication/actions/runs/43177452431).
The archive security audit also passes in that CI run.

Each architecture passes 790 recorded checks: 788 runtime/upgrade checks and
two storage-preflight checks. Downloaded artifact provenance identifies the
same source commit and CI run; the reports, their hashes, artifact identities
and archive checksums are retained in
[structured evidence](evidence/2026-11-12-beta5-d5e05d2-native-qualification.json).
Raw downloaded artifacts remain in the local `.qualification/worker-identity/`
evidence directory and in GitHub Actions until artifact expiry.

| Suite | Checks per architecture |
|---|---:|
| Source-pinned storage preflight | 2 |
| Fresh packaged lifecycle | 119 |
| Archived worker TLS/SCRAM | 21 |
| Forced worker crashes | 17 |
| Receiver regressions and native bulk | 97 |
| Packaged update types, rollback and upgrade | 94 |
| Active DROP and forced busy-slot cleanup | 30 |
| Coordinated physical recovery/rollback | 30 |
| Production admission and beta.2 upgrade | 333 |
| Beta.1 package handoff | 47 |

The metadata reports these archive SHA-256 checksums:

- AMD64: `5f997236ce541cda8871138564931791b4ccf7b3dbdc8b85a1dbfda9698298fa`
- ARM64: `f844f0c6ffc26d963e63734f4bfdf235cda7aa07506e47a007a957aff36cd601`

This closes native qualification for this exact candidate, including the
update-key filter, source TLS policy, and cleanup retry fix. It does not
qualify subsequent explicit-worker-login changes or other modified archives.
The compatibility-range refresh, deferred 24-hour soak, worker privilege and
subscription-isolation review, retention/resource review, signed publication
and independent downloaded-release verification remain open.

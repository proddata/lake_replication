# Exact candidate native qualification: 8154ccf

Merged candidate `8154ccfface14a1b5b4926f7531bac3e7fad89aa` passes [default-branch CI 43822064964](https://github.com/proddata/lake_replication/actions/runs/43822064964) and [full packaged lifecycle qualification 43822411522](https://github.com/proddata/lake_replication/actions/runs/43822411522) on native ARM64 and AMD64.

The lifecycle workflow verifies exact source/artifact provenance and runs fresh installation/lifecycle, TLS/SCRAM, crash/replay, logical regressions, update types and refresh/upgrade, slot cleanup, explicit worker identity, runtime grants/denials, coordinated physical rollback, admission and historical package-upgrade paths. Both architecture jobs complete successfully. [Retained run metadata](../evidence/2026-11-16-beta5-8154ccf-lifecycle.json) records the exact commit and job/step conclusions.

The earlier `c6815eb` run failed on both architectures because its development refresh contained only migration 0030, while a fresh install included 0031–0034. The cumulative beta.5 refresh fix is included in this candidate and that qualification check now passes on both architectures.

This qualifies the default journal pipeline and packaged artifacts at this commit. The local experimental WAL-backed direct publisher and snapshot initializer are not included in these archives. Compatibility-range refresh, the 24-hour soak, signed publication and independent download verification remain open; this is not a release approval.

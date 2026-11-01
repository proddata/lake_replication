# Beta.5 dependency security review

The 2026-11-01 review found eight open Dependabot alerts representing four
unique rustls advisories, each reported against both Cargo.toml and Cargo.lock.
The qualified `14dc705` archive pins rustls 0.23.45. The next candidate pins
**0.23.46**, changing only that crate's version and checksum in Cargo.lock.
The fix follows the upstream [0.23.46 release](https://github.com/rustls/rustls/releases/tag/v/0.23.46).

| Advisory | Severity | Area | Patched version |
|---|---|---|---|
| [GHSA-c8m5-c952-6cm4](https://github.com/rustls/rustls/security/advisories/GHSA-c8m5-c952-6cm4) | Moderate | Debug formatting of session secrets | 0.23.46 |
| [GHSA-h2v8-394m-p7vv](https://github.com/rustls/rustls/security/advisories/GHSA-h2v8-394m-p7vv) | Low | Brotli certificate decompression allocation | 0.23.46 |
| [GHSA-w44r-93j7-qv28](https://github.com/rustls/rustls/security/advisories/GHSA-w44r-93j7-qv28) | Low | Per-name key-exchange hints | 0.23.46 |
| [GHSA-vp88-798v-h67f](https://github.com/rustls/rustls/security/advisories/GHSA-vp88-798v-h67f) | Low | Buffered application data after ECH rejection | 0.23.46 |

The configured features are `ring`, `std` and `tls12`; default features are off.
The worker uses the ring crypto provider, ordinary certificate verification and
no client authentication. It does not configure ECH, Brotli decompression or a
custom session store. Those observations narrow exposure but do not replace
applying the patch. Both root-certificate and server-name verification remain
required by the existing verify-full contract.

`make check` passes with the update. Archive-level TLS/SCRAM validation passes
19 checks: plaintext refusal, untrusted CA and wrong-name rejection in the actual
initial-sync child log, unchanged checkpoints on refusal, corrected-trust copy,
live replication, encrypted source sessions, exact values and native slot cleanup.
The new full native workflow repeats these checks on both architectures.

The package now ships Cargo.lock and verified notice files for all 42 native
registry dependencies (including build dependencies conservatively). Each crate
archive must match its Cargo.lock checksum. License identifiers are restricted
to MIT, Apache-2.0, ISC, BSD-3-Clause, Zlib and Unlicense; all declared notice files
are retained, including conjunctions such as ring's Apache-2.0 AND ISC. The
project's PostgreSQL license and the installed Rust compiler/runtime copyright
and license notices are also included. PostgreSQL, pg_lake and system libraries
are separately installed host dependencies; MinIO is a test fixture, not shipped
in the release archive.

The local audit uses checksum-verified Syft 1.55.0 and Grype 0.121.0. The final
archive's SBOM contains 55 packages and explicitly covers every one of the 42
native registry dependencies. The validated 2026-10-31 vulnerability database
reports **zero matches**. A separate pre-patch Cargo.lock control reports all four
known rustls advisories using that database, confirming actual scanner coverage.
No vulnerability suppressions or exclusions were added. [Local evidence](evidence/2026-11-01-beta5-local-release-gates.json)
records the archive/notice/SBOM hashes, database identity, controls and tests.

CI now audits both exact exported archives: it checks provenance and checksums,
verifies notice/lock consistency and policy, generates Syft/SPDX SBOMs, requires
coverage of every native dependency, and fails on high/critical vulnerabilities.
Both audit tools have pinned versions and binary SHA256s. Evidence is uploaded
independently from the replication fixtures.

Fresh CI archives and full native requalification of the patched runtime remain
pending. The successful full qualification of `14dc705` is the pre-patch baseline;
it must not be treated as qualification of the new worker bytes. The security
gate remains partial until both clean CI archive audits pass and Dependabot has
confirmed the pinned update. The 24-hour frozen-candidate soak remains separate.

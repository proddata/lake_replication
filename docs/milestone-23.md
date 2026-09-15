# Milestone 23: certificate-verified PostgreSQL TLS

Status: every worker connection can now use PostgreSQL's SSLRequest negotiation
followed by certificate-verified TLS and SCRAM-SHA-256. Initial copy, ordinary
metadata queries, source preflight, logical CopyBoth traffic, and destination
publication all share the same transport implementation.

## Strict transport policy

Only two modes are accepted:

- `verify-full` encrypts the session, validates the certificate chain against an
  explicit CA file, and verifies the configured server DNS name or IP address;
- `disable` uses plaintext and exists for loopback laboratories and explicit
  operator exceptions.

Insecure opportunistic modes such as `prefer`, encryption without certificate
verification, and CA-only verification are intentionally unsupported. If no mode
is configured, loopback addresses default to `disable` and every non-loopback
source or destination defaults to `verify-full`. Consequently, a remote endpoint
without a configured root certificate fails during configuration instead of
silently using plaintext.

PostgreSQL recommends `verify-full` for security-sensitive connections because it
protects against eavesdropping and server impersonation. Its behavior is defined
in the
[PostgreSQL TLS documentation](https://www.postgresql.org/docs/18/libpq-ssl.html).

## Configuration

Source settings are:

```text
LAKE_SOURCE_TLS_MODE=verify-full
LAKE_SOURCE_TLS_ROOT_CERT=/run/secrets/source-ca.crt
LAKE_SOURCE_TLS_SERVER_NAME=database.example.com  # optional override
```

The destination uses the corresponding `LAKE_DESTINATION_TLS_*` settings. When
the server-name override is absent, the host portion of the endpoint address is
verified. CA files may contain multiple PEM certificates, but malformed or empty
files are rejected.

The implementation pins `rustls` 0.23.45 with its ring provider and
`rustls-pemfile` 2.2.0. Rustls does not expose a certificate-verification bypass
through the client path used here.

## Evidence

The disposable lab generates a short-lived CA and server certificate at runtime;
no private key or certificate is committed. It configures PostgreSQL to reject
non-TLS TCP connections and requires SCRAM authentication inside TLS.

Run `tls_20260915_102010` passed all eight assertions. The retained
[results](evidence/2026-09-15-tls-results.json) demonstrate:

- server-side rejection of plaintext TCP;
- an encrypted administrative control session;
- worker rejection of an untrusted CA;
- worker rejection of a mismatched server name;
- successful certificate-verified initial copy;
- observed TLS on live worker connections;
- exact logical-stream publication over TLS; and
- clean supervisor shutdown.

Run the isolated probe with `make tls-test`. It removes its PostgreSQL data and
certificate volumes in a `finally` block and uses the main laboratory only as the
pg_lake destination.

## Remaining boundaries

- The source path is exercised end to end; destination TLS uses the identical
  transport but is not yet run against a second TLS-enabled pg_lake cluster.
- Client-certificate authentication and certificate revocation lists are not
  implemented.
- SCRAM channel binding (`SCRAM-SHA-256-PLUS`) is not yet implemented. Strict
  certificate and hostname verification still authenticates the server before
  credentials or replication data are exchanged.
- Root-certificate rotation requires a worker restart because configuration is
  read at process startup.

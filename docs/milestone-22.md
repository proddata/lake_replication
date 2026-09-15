# Milestone 22: SCRAM authentication and source least privilege

Status: every worker connection now supports PostgreSQL SCRAM-SHA-256 password
authentication. Source and destination usernames and passwords are configured
independently, while the existing `LAKE_USER` setting remains a backward-compatible
username fallback for trust-authenticated laboratories.

## Authentication protocol

The wire client handles PostgreSQL's `AuthenticationSASL`,
`AuthenticationSASLContinue`, `AuthenticationSASLFinal`, and subsequent
`AuthenticationOk` sequence. It selects only `SCRAM-SHA-256`; legacy cleartext,
MD5, GSS, SSPI, and unsupported SASL mechanisms remain explicit protocol errors.

SCRAM proof generation, SASLprep compatibility, random nonces, bounded iteration
counts, and server-signature verification use the maintained
[`postgres-protocol` 0.6.12](https://github.com/sfackler/rust-postgres)
implementation under its MIT OR Apache-2.0 license. PostgreSQL defines the
exchange in its
[SASL authentication protocol](https://www.postgresql.org/docs/18/sasl-authentication.html).

## Configuration

The worker accepts:

| Setting | Purpose |
|---|---|
| `LAKE_SOURCE_USER` | Source query and replication role |
| `LAKE_SOURCE_PASSWORD` | Source SCRAM password |
| `LAKE_DESTINATION_USER` | Destination control and publication role |
| `LAKE_DESTINATION_PASSWORD` | Destination SCRAM password |
| `LAKE_USER` | Backward-compatible fallback for either username |

Passwords are read from the process environment, are never interpolated into SQL
or child-process arguments, and are not included in protocol or worker errors.
The supervisor removes the source password from the destination-only publisher
child's environment.
Production secret-file or workload-identity injection remains deployment work.

## Least-privilege source role

The disposable source uses a role with `LOGIN` and `REPLICATION`, database
`CONNECT`, schema `USAGE`, and `SELECT` on the published table. The role is not a
superuser and has neither database nor role creation privileges. The integration
probe verifies that it can perform initial copy and logical streaming but cannot
insert into the source table.

## Evidence

Run `scram_20260915_101007` passed all ten assertions. The retained
[results](evidence/2026-09-15-scram-results.json) cover:

- non-superuser replication-role attributes and read-only table access;
- source-write denial;
- explicit rejection of an incorrect password;
- absence of attempted and configured passwords from errors, normal worker
  output, and PostgreSQL logs;
- authenticated initial synchronization;
- exact authenticated logical-stream publication; and
- clean supervisor shutdown.

Run the isolated probe with `make scram-test`. Credentials are supplied as
test-only process environment values, and the probe removes its dedicated
PostgreSQL container and volume in a `finally` block.

## Security boundary

SCRAM without TLS does not authenticate the server or encrypt replicated data.
PostgreSQL explicitly recommends certificate-verified TLS to prevent server
spoofing on network connections. This milestone therefore enables password
authentication but does not declare remote connections production-safe. TLS
with hostname and CA verification is the next security gate.

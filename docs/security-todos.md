# Open security TODOs

These items remain open after the SQL literal, log-file, and source TLS
hardening work. They document outstanding work; they do not indicate that
worker privilege isolation has been implemented or qualified.

- [ ] **Require an explicit destination worker login.** Remove the implicit
  `postgres` fallback for launcher-managed subscriptions and reject missing
  worker identity configuration before starting workers. Document how existing
  installations configure a dedicated login. Workers currently default to
  `postgres`, so their effective database privileges depend on that account and
  the deployment's authentication configuration.

- [ ] **Define and qualify minimum worker grants.** Provide a documented grant
  recipe covering the control schema, journal, destination schema, and Iceberg
  tables without requiring a superuser account. Exercise initial copy,
  publication, recovery, schema changes, credential rotation, and cleanup using
  that login. Add negative checks proving it cannot access unrelated tables or
  perform unrelated administrative operations.

- [ ] **Scope credential access to assigned subscriptions.** Associate worker
  logins with the subscriptions they may operate and enforce that binding in
  both `source_credentials(bigint)` and `source_tls_settings(bigint)`. Today,
  membership in `lake_replication_worker` permits these getters across
  subscriptions. Test cross-subscription denial, login reassignment, and
  revocation, including attempts to impersonate another login through session
  settings. Until this is implemented, treat that role as trusted across all
  subscriptions in the database.

Review the resulting threat model and release implications before closing these
items; existing dependency scans and replication qualification do not establish
that these privilege boundaries are enforced.

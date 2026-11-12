# Open security TODOs

These items remain open after the SQL literal, log-file, and source TLS
hardening work. They document outstanding work; they do not indicate that
worker privilege isolation or the additional data-protection boundaries below
have been implemented or qualified.

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

- [ ] **Qualify access to replicated data in journal and staging tables.**
  Include `transactions`, `changes`, streamed transaction storage, and both
  initial-copy stores in the minimum-grants review. These hold source row data
  independently of the final Iceberg tables. Verify access for ordinary users,
  worker logins, administrators, and `pg_read_all_data`, including access through
  views and helper functions. The RLS policy protecting `source_connections`
  does not itself protect these separate data stores. Define the intended
  cross-subscription access model before deciding whether additional isolation
  is required; do not describe this review item as an established ACL bypass.

- [ ] **Document and qualify retention across all data copies.** Published
  journal transactions already have checkpoint-aware maintenance and a default
  one-day retention policy. Document what remains in unpublished or incomplete
  transactions, interrupted initial copies, PostgreSQL WAL, logical/physical
  backups, and worker error logs. Verify cleanup after successful publication,
  failed copies, subscription removal, and restore, without deleting data needed
  for replay. Explain that deleting live rows does not remove copies retained in
  backups or WAL, and define the operator's backup and log retention obligations.

- [ ] **Qualify resource limits beyond committed journal payload.** Current
  managed journal admission defaults to eight committed unpublished transactions
  and 64 MiB of normalized payload per subscription. The
  [operations guide](operations.md#staging-and-journal-admission-beta4) explicitly
  excludes decode buffers, incomplete streamed transactions, and physical
  storage overhead from that budget. Initial-copy admission limits concurrent
  sessions rather than total memory or disk use. Exercise large/open source
  transactions, interrupted copies, publication outages, and multiple
  subscriptions; measure process memory, total staging/journal storage, and
  retained source WAL. Define aggregate limits or deployment controls, alerts,
  and a recovery procedure that preserves acknowledged data. Keep source WAL
  exhaustion and any retention-limit-induced slot invalidation in that procedure.

Review the resulting threat model and release implications before closing these
items; existing dependency scans and replication qualification do not establish
that these privilege, retention, and resource boundaries are enforced.

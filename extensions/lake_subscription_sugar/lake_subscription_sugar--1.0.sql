\echo Use "CREATE EXTENSION lake_subscription_sugar" to load this file. \quit

-- Force the ProcessUtility_hook active in the session that just ran
-- CREATE EXTENSION, without waiting for a fresh connection to pick up
-- session_preload_libraries (see the operator note in
-- docs/native-subscription-spike.md for making that automatic for every
-- future session too).
LOAD 'lake_subscription_sugar';

-- Membership (or superuser) required to run CREATE/ALTER/DROP SUBSCRIPTION
-- ... WITH (lake_target = 'iceberg'); enforced by RequireAuthorized() in
-- lake_subscription_sugar.c. NOLOGIN: this role exists purely to be
-- GRANTed to real login roles, e.g. GRANT lake_replication_admin TO app;
-- Cluster-wide like every PostgreSQL role, so DROP EXTENSION does not
-- remove it -- an operator who wants it gone drops it explicitly.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lake_replication_admin') THEN
        CREATE ROLE lake_replication_admin NOLOGIN;
    END IF;
END
$$;

COMMENT ON ROLE lake_replication_admin IS
    'Membership (or superuser) required to run CREATE/ALTER/DROP '
    'SUBSCRIPTION ... WITH (lake_target = ''iceberg''). '
    'GRANT lake_replication_admin TO <role> to authorize it.';

-- Owned by this extension; DROP EXTENSION lake_subscription_sugar drops
-- this table too. Deliberately separate from lake_replication's own
-- shipped schema, since this state exists only to support this
-- prototype's DROP SUBSCRIPTION handling (dropping the source-side
-- replication slot for real), not the replication path itself.
CREATE TABLE pending_cleanup (
    subscription_id bigint PRIMARY KEY,
    source_conninfo text NOT NULL,
    source_slot_name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE pending_cleanup IS
    'Source conninfo/slot name recorded at CREATE SUBSCRIPTION time so '
    'DROP SUBSCRIPTION can drop the source-side replication slot for '
    'real; DropSubscriptionStmt carries no conninfo of its own. May '
    'contain a cleartext password if one was embedded in CONNECTION -- '
    'see the credential-handling note in lake_subscription_sugar.c. No '
    'PUBLIC privileges are granted on this table (the default for a new '
    'table already; this REVOKE is defensive, matching how PostgreSQL '
    'itself protects pg_subscription.subconninfo).';

REVOKE ALL ON pending_cleanup FROM PUBLIC;

-- Non-secret launch configuration for the launcher background worker
-- (lake_subscription_sugar_launcher_main) and the per-subscription
-- supervisor workers it starts (lake_subscription_sugar_worker_main), both
-- in lake_subscription_sugar.c. Deliberately separate from pending_cleanup
-- (which holds the CONNECTION string and may hold a password): everything
-- here is address/name metadata a background worker with no other context
-- needs to rebuild the same argv/env CREATE SUBSCRIPTION itself would have
-- passed, on every (re)start -- including an automatic one, possibly long
-- after the original session that ran CREATE SUBSCRIPTION is gone.
--
-- desired_state is the launcher's own source of truth for whether a
-- subscription's supervisor process should exist right now: CREATE
-- SUBSCRIPTION inserts 'running', ALTER ... DISABLE updates it to
-- 'stopped' (ALTER ... ENABLE back to 'running'), and DROP SUBSCRIPTION
-- deletes the row outright, which the launcher treats the same as
-- 'stopped' for a subscription it is tracking. This is intentionally not
-- the same thing lake_replication.service_status.desired_state drives:
-- that one is polled by the supervisor process itself, from the inside,
-- to decide whether to pause its receiver/publisher cycles; this one is
-- polled by the launcher, from the outside, to decide whether the OS
-- process running that supervisor should exist at all.
CREATE TABLE worker_launch_config (
    subscription_id bigint PRIMARY KEY,
    slot_name text NOT NULL,
    publication_name text NOT NULL,
    source_database text NOT NULL,
    destination_database text NOT NULL,
    source_address text NOT NULL,
    destination_address text NOT NULL,
    destination_schema text NOT NULL,
    desired_state text NOT NULL DEFAULT 'running'
        CHECK (desired_state IN ('running', 'stopped')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE worker_launch_config IS
    'Non-secret per-subscription launch configuration the launcher '
    'background worker and the supervisor workers it starts read on '
    'every (re)start; see lake_subscription_sugar_launcher_main and '
    'lake_subscription_sugar_worker_main in lake_subscription_sugar.c.';

REVOKE ALL ON worker_launch_config FROM PUBLIC;

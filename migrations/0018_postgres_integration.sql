BEGIN;

-- The PostgreSQL hook, launcher, and their durable configuration are part of
-- the lake_replication product. Earlier development builds carried these
-- objects in a separate lake_subscription_sugar prototype extension.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'lake_replication_admin'
    ) THEN
        CREATE ROLE lake_replication_admin NOLOGIN;
    END IF;
END
$$;

COMMENT ON ROLE lake_replication_admin IS
    'Membership (or superuser) required to manage lake_replication Iceberg subscriptions.';

CREATE TABLE lake_replication.pending_cleanup (
    subscription_id bigint PRIMARY KEY,
    source_conninfo text NOT NULL,
    source_slot_name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE lake_replication.pending_cleanup IS
    'Protected source connection metadata retained so DROP SUBSCRIPTION can remove the source slot.';

REVOKE ALL ON lake_replication.pending_cleanup FROM PUBLIC;

CREATE TABLE lake_replication.worker_launch_config (
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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    credential_file text
        CHECK (credential_file IS NULL OR credential_file LIKE '/%'),
    config_generation bigint NOT NULL DEFAULT 1
        CHECK (config_generation > 0)
);

COMMENT ON TABLE lake_replication.worker_launch_config IS
    'Non-secret durable launch configuration read by the PostgreSQL launcher and per-subscription supervisors.';

REVOKE ALL ON lake_replication.worker_launch_config FROM PUBLIC;

-- Activate the ProcessUtility hook in the session installing or upgrading the
-- extension. The static launcher itself is registered only when the library is
-- present in shared_preload_libraries at postmaster startup.
LOAD 'lake_replication';

COMMIT;

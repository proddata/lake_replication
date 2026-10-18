BEGIN;

-- Keep slot cleanup metadata separate from the reusable source connection.
CREATE TABLE lake_replication.source_connections (
    subscription_id bigint PRIMARY KEY,
    conninfo text NOT NULL,
    source_user text,
    source_password text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE lake_replication.source_connections IS
    'Restricted source connection data used by replication workers and source slot cleanup.';

REVOKE ALL ON lake_replication.source_connections FROM PUBLIC;
GRANT SELECT, INSERT, DELETE ON lake_replication.pending_cleanup
    TO lake_replication_admin;

-- pg_read_all_data overrides ordinary table ACLs but does not bypass RLS.
ALTER TABLE lake_replication.source_connections ENABLE ROW LEVEL SECURITY;

CREATE POLICY source_connections_admin ON lake_replication.source_connections
    TO lake_replication_admin
    USING (true)
    WITH CHECK (true);

GRANT SELECT, INSERT, UPDATE, DELETE ON lake_replication.source_connections
    TO lake_replication_admin;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'lake_replication_worker'
    ) THEN
        CREATE ROLE lake_replication_worker NOLOGIN;
    END IF;
END
$$;

COMMENT ON ROLE lake_replication_worker IS
    'May retrieve source credentials for lake_replication worker processes; grant only to the worker login.';

-- The function owner is the extension installer. Runtime workers have no
-- direct table access, and callers with pg_read_all_data cannot execute it.
CREATE FUNCTION lake_replication.source_credentials(p_subscription_id bigint)
RETURNS TABLE(source_user text, source_password text)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, lake_replication
AS $$
    SELECT p.source_user, p.source_password
    FROM lake_replication.source_connections AS p
    WHERE p.subscription_id = p_subscription_id
$$;

REVOKE ALL ON FUNCTION lake_replication.source_credentials(bigint) FROM PUBLIC;
GRANT USAGE ON SCHEMA lake_replication TO lake_replication_worker;
GRANT EXECUTE ON FUNCTION lake_replication.source_credentials(bigint)
    TO lake_replication_worker;

COMMIT;

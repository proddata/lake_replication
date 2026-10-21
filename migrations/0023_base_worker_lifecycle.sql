BEGIN;

-- Package replacement requires a stopped server. The new binary never registers
-- the old static launcher, even if legacy preload/database settings are retained.
DO $$
BEGIN
    IF EXISTS (SELECT FROM pg_stat_activity
               WHERE datname=current_database()
                 AND backend_type IN ('lake_replication launcher', 'lake_replication supervisor'))
       OR EXISTS (SELECT FROM lake_replication.worker_launch_config
                   WHERE desired_state <> 'stopped') THEN
        RAISE EXCEPTION 'stop subscriptions and restart PostgreSQL with the new binaries before the lifecycle upgrade'
            USING ERRCODE='55000';
    END IF;
    IF EXISTS (SELECT FROM lake_replication.worker_launch_config)
       AND current_setting('lake_replication.allow_legacy_binding_upgrade', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'legacy destination bindings require explicit verified adoption'
            USING ERRCODE='55000',
            HINT='Verify this is the original destination, drain subscriptions, then SET LOCAL lake_replication.allow_legacy_binding_upgrade=on in the upgrade transaction. Copies require a rebuild.';
    END IF;
END;
$$;

ALTER TABLE lake_replication.worker_launch_config
    ADD COLUMN origin_system_identifier numeric(20,0),
    ADD COLUMN origin_database_oid oid;

-- Expose only the identity primitive needed by authorized native management,
-- without granting the admin role broader pg_control_system access.
CREATE FUNCTION lake_replication.destination_system_identifier()
RETURNS numeric LANGUAGE sql STABLE SECURITY DEFINER
SET search_path=pg_catalog,lake_replication
AS $$ SELECT system_identifier::numeric FROM pg_catalog.pg_control_system() $$;
GRANT USAGE ON SCHEMA lake_replication TO lake_replication_admin;
REVOKE ALL ON FUNCTION lake_replication.destination_system_identifier() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.destination_system_identifier() TO lake_replication_admin;

-- This applies only to explicitly adopted, stopped legacy rows. New rows record
-- their origin in the native CREATE path. Copied/restored rows retain that origin.
UPDATE lake_replication.worker_launch_config
   SET origin_system_identifier=(SELECT system_identifier FROM pg_control_system()),
       origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())
 WHERE destination_database=current_database();

CREATE FUNCTION lake_replication.worker_binding_matches(bigint)
RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER
SET search_path=pg_catalog,lake_replication
AS $$
    SELECT EXISTS (
        SELECT FROM lake_replication.worker_launch_config c
         WHERE c.subscription_id=$1
           AND c.destination_database=current_database()
           AND c.origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
           AND c.origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())
    )
$$;
REVOKE ALL ON FUNCTION lake_replication.worker_binding_matches(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.worker_binding_matches(bigint) TO lake_replication_admin;

CREATE FUNCTION lake_replication.base_launcher(internal)
RETURNS internal LANGUAGE C
AS 'MODULE_PATHNAME', 'lake_replication_base_launcher';
REVOKE ALL ON FUNCTION lake_replication.base_launcher(internal) FROM PUBLIC;
SELECT extension_base.register_worker('lake_replication launcher', 'lake_replication.base_launcher');

COMMIT;

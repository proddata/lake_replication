-- Experimental origin fields; NULL legacy/copied rows are never auto-blessed.
ALTER TABLE lake_replication.worker_launch_config
  ADD COLUMN origin_system_identifier numeric(20,0),
  ADD COLUMN origin_database_oid oid;

CREATE FUNCTION lake_replication.prototype_binding_matches(bigint)
RETURNS boolean LANGUAGE sql STABLE
SET search_path = pg_catalog, lake_replication
AS $$
  SELECT EXISTS (
    SELECT 1 FROM lake_replication.worker_launch_config c
    WHERE c.subscription_id=$1
      AND c.destination_database=current_database()
      AND c.origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
      AND c.origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())
  )
$$;
REVOKE ALL ON FUNCTION lake_replication.prototype_binding_matches(bigint) FROM PUBLIC;

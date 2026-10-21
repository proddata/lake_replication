-- Runtime identity is supplied at postmaster startup, outside copied PGDATA.
-- No backfill: existing rows require a separately qualified transition.
ALTER TABLE lake_replication.worker_launch_config ADD COLUMN origin_instance_id text;

CREATE OR REPLACE FUNCTION lake_replication.prototype_binding_matches(bigint)
RETURNS boolean LANGUAGE sql STABLE
SET search_path = pg_catalog, lake_replication
AS $$
 SELECT EXISTS (
  SELECT 1 FROM lake_replication.worker_launch_config c WHERE c.subscription_id=$1
   AND c.destination_database=current_database()
   AND c.origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
   AND c.origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())
   AND c.origin_instance_id <> ''
   AND c.origin_instance_id=current_setting('lake_replication.prototype_instance_id')
 )
$$;

CREATE FUNCTION lake_replication.prototype_assert_writer(p_subscription_id bigint)
RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE c worker_launch_config%ROWTYPE;
BEGIN
 -- Lock the row through the whole publication transaction. Compare the row
 -- returned by this lock, rather than re-reading an older statement snapshot.
 SELECT * INTO c FROM worker_launch_config WHERE subscription_id=p_subscription_id FOR SHARE;
 IF NOT FOUND OR c.destination_database IS DISTINCT FROM current_database()
    OR c.origin_system_identifier IS DISTINCT FROM (SELECT system_identifier FROM pg_control_system())
    OR c.origin_database_oid IS DISTINCT FROM (SELECT oid FROM pg_database WHERE datname=current_database())
    OR coalesce(c.origin_instance_id,'')=''
    OR c.origin_instance_id IS DISTINCT FROM current_setting('lake_replication.prototype_instance_id')
 THEN
   RAISE EXCEPTION 'prototype: publication ownership rejected for subscription %',p_subscription_id
     USING ERRCODE='55000';
 END IF;
END
$$;
REVOKE ALL ON FUNCTION lake_replication.prototype_assert_writer(bigint) FROM PUBLIC;

-- Guard native worker write entry points before validation/DDL/data effects.
-- Internal helpers without a subscription parameter remain within these
-- transactions; this is not a sandbox against arbitrary superuser SQL/DML.
DO $$
DECLARE f record; definition text; n integer := 0;
BEGIN
 FOR f IN SELECT p.oid,p.proname FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace
   WHERE ns.nspname='lake_replication' AND p.proname IN
   ('publish_insert_batch','ensure_iceberg_destination','reconcile_relation_schema',
    'begin_initial_copy','append_initial_copy_rows','finish_initial_copy',
    'finish_initial_sync_group','reset_initial_copy','reset_incomplete_initial_sync','assign_destination')
 LOOP
   IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE oid=f.oid AND 'p_subscription_id'=ANY(proargnames))
     THEN RAISE EXCEPTION 'unexpected prototype function signature: %',f.proname; END IF;
   definition := pg_get_functiondef(f.oid);
   IF definition !~ '\mBEGIN\M' THEN RAISE EXCEPTION 'unexpected function body: %',f.proname; END IF;
   EXECUTE regexp_replace(definition,'\mBEGIN\M','BEGIN'||chr(10)||' PERFORM lake_replication.prototype_assert_writer(p_subscription_id);');
   n := n+1;
 END LOOP;
 IF n <> 10 THEN RAISE EXCEPTION 'expected 10 guarded functions, got %',n; END IF;
END $$;

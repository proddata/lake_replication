BEGIN;

CREATE FUNCTION lake_replication.admit_initial_copy() RETURNS void
LANGUAGE C VOLATILE PARALLEL UNSAFE AS 'MODULE_PATHNAME','lake_replication_admit_initial_copy';
CREATE FUNCTION lake_replication.assert_initial_copy_lease() RETURNS void
LANGUAGE C VOLATILE PARALLEL UNSAFE AS 'MODULE_PATHNAME','lake_replication_assert_initial_copy_lease';
REVOKE ALL ON FUNCTION lake_replication.admit_initial_copy() FROM PUBLIC;
REVOKE ALL ON FUNCTION lake_replication.assert_initial_copy_lease() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.admit_initial_copy() TO lake_replication_admin;
GRANT EXECUTE ON FUNCTION lake_replication.assert_initial_copy_lease() TO lake_replication_admin;
CREATE VIEW lake_replication.initial_copy_admission AS
 SELECT l.pid,a.datname AS database_name,CASE WHEN l.objid<84 THEN (l.objid::bigint-79)::integer END AS slot,
 CASE WHEN l.objid<84 AND l.granted THEN 'admitted' ELSE 'waiting_for_initial_copy' END AS admission_state,
 l.waitstart,a.wait_event_type,a.wait_event
 FROM pg_catalog.pg_locks l LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='advisory' AND l.database=0 AND l.classid=1280460112
 AND l.objid BETWEEN 80 AND 84 AND l.objsubid=1 AND l.mode='ExclusiveLock';
REVOKE ALL ON lake_replication.initial_copy_admission FROM PUBLIC;
GRANT SELECT ON lake_replication.initial_copy_admission TO lake_replication_admin;
COMMENT ON VIEW lake_replication.initial_copy_admission IS
 'Four instance-wide session leases for native initial-copy pipelines; acquired before source snapshots and held through publication. Not a hard CPU or snapshot-retention bound.';

CREATE OR REPLACE FUNCTION lake_replication.append_initial_copy_rows(
 p_subscription_id bigint,p_remote_relation_id oid,p_schema_version bigint,p_start_ordinal bigint,p_rows jsonb
) RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE v_state text; v_count bigint;
BEGIN
 IF coalesce(nullif(current_setting('lake_replication.worker_generation',true),''),'0')::bigint>0 THEN
  PERFORM lake_replication.assert_initial_copy_lease();
 END IF;
 IF p_start_ordinal<0 OR jsonb_typeof(p_rows) IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'invalid initial-copy row chunk' USING ERRCODE='22023';
 END IF;
 IF EXISTS(SELECT FROM jsonb_array_elements(p_rows) r WHERE jsonb_typeof(r)<>'object') THEN
  RAISE EXCEPTION 'initial-copy rows must be JSON objects' USING ERRCODE='22023';
 END IF;
 SELECT state INTO STRICT v_state FROM initial_copies WHERE subscription_id=p_subscription_id
  AND remote_relation_id=p_remote_relation_id AND relation_schema_version=p_schema_version FOR UPDATE;
 IF v_state<>'copying' THEN
  RAISE EXCEPTION 'initial copy is not accepting rows (state %)',v_state USING ERRCODE='55000';
 END IF;
 v_count:=jsonb_array_length(p_rows);
 IF v_count>0 THEN
  INSERT INTO initial_copy_chunks(subscription_id,remote_relation_id,relation_schema_version,row_ordinal,rows)
   VALUES(p_subscription_id,p_remote_relation_id,p_schema_version,p_start_ordinal,p_rows);
 END IF;
 UPDATE initial_copies SET rows_copied=rows_copied+v_count WHERE subscription_id=p_subscription_id
  AND remote_relation_id=p_remote_relation_id AND relation_schema_version=p_schema_version;
 RETURN v_count;
END $$;
CREATE OR REPLACE FUNCTION lake_replication.assert_worker(p_subscription_id bigint,p_generation bigint)
RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE desired text; generation bigint; launch_desired text;
BEGIN
 IF p_generation<=0 THEN RAISE EXCEPTION 'managed worker generation is required' USING ERRCODE='55000'; END IF;
 -- Heartbeats take NO KEY UPDATE on this row. Allow them while retaining
 -- conflict with stop's explicit FOR UPDATE and with deletion of the service.
 SELECT desired_state INTO desired FROM service_status WHERE subscription_id=p_subscription_id FOR KEY SHARE NOWAIT;
 IF NOT FOUND OR desired<>'running' THEN
  RAISE EXCEPTION 'managed worker is stopped or obsolete' USING ERRCODE='55000';
 END IF;
 SELECT config_generation,desired_state INTO generation,launch_desired FROM worker_launch_config
 WHERE subscription_id=p_subscription_id FOR SHARE NOWAIT;
 IF NOT FOUND OR generation<>p_generation OR launch_desired<>'running' THEN
  RAISE EXCEPTION 'managed worker is stopped or obsolete' USING ERRCODE='55000';
 END IF;
 PERFORM 1 FROM checkpoint_ingress i JOIN checkpoint_publication p USING(subscription_id)
 WHERE i.subscription_id=p_subscription_id AND i.staged_lsn>=p.published_lsn;
 IF NOT FOUND THEN
  RAISE EXCEPTION 'checkpoint state is incomplete or invalid' USING ERRCODE='55000';
 END IF;
 -- Lazy creation avoids generating duplicate policy rows during pg_dump restore.
 INSERT INTO journal_limits(subscription_id) VALUES(p_subscription_id) ON CONFLICT DO NOTHING;
END $$;

CREATE FUNCTION lake_replication.validate_runtime_state() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_replication,pg_temp AS $$
BEGIN
 IF EXISTS(SELECT FROM checkpoint_ingress i FULL JOIN checkpoint_publication p USING(subscription_id)
  WHERE i.subscription_id IS NULL OR p.subscription_id IS NULL OR p.published_lsn>i.staged_lsn) THEN
  RAISE EXCEPTION 'checkpoint state is incomplete or invalid' USING ERRCODE='23514';
 END IF;
 IF EXISTS(SELECT FROM initial_copy_chunks c LEFT JOIN initial_copies p
  USING(subscription_id,remote_relation_id,relation_schema_version) WHERE p.subscription_id IS NULL)
 OR EXISTS(SELECT FROM initial_copy_legacy_rows c LEFT JOIN initial_copies p
  USING(subscription_id,remote_relation_id,relation_schema_version) WHERE p.subscription_id IS NULL)
 OR EXISTS(SELECT FROM initial_copy_legacy_rows r JOIN initial_copy_chunks c
  USING(subscription_id,remote_relation_id,relation_schema_version) WHERE c.row_span @> r.row_ordinal) THEN
  RAISE EXCEPTION 'initial-copy storage is orphaned or overlapping' USING ERRCODE='23514';
 END IF;
END $$;
REVOKE ALL ON FUNCTION lake_replication.validate_runtime_state() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.validate_runtime_state() TO lake_replication_admin;
COMMENT ON FUNCTION lake_replication.validate_runtime_state() IS
 'Validate split checkpoint and cross-store staging integrity after a stopped logical restore before resuming subscriptions.';
-- pg_temp is implicitly searched first for relations unless explicitly placed
-- last. Harden all extension-owned SQL helpers, including earlier definer APIs.
DO $$ DECLARE f record; BEGIN
 FOR f IN SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
  JOIN pg_depend d ON d.classid='pg_proc'::regclass AND d.objid=p.oid
   AND d.refclassid='pg_extension'::regclass AND d.deptype='e'
  WHERE n.nspname='lake_replication' AND d.refobjid=(SELECT oid FROM pg_extension WHERE extname='lake_replication')
   AND (p.prosecdef OR EXISTS(SELECT FROM unnest(p.proconfig) c WHERE c LIKE 'search_path=%')) LOOP
  EXECUTE format('ALTER FUNCTION %s SET search_path=pg_catalog,lake_replication,pg_temp',f.oid::regprocedure);
 END LOOP;
END $$;
COMMIT;

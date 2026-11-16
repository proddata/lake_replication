-- Diagnostic-only wrappers in a disposable fixture, never release installation.
BEGIN;
CREATE UNLOGGED TABLE lake_direct.profile_calls (
 boundary pg_lsn NOT NULL, operation text NOT NULL, change_rows bigint NOT NULL,
 elapsed_ms double precision NOT NULL
);
ALTER FUNCTION lake_direct.publish(pg_lsn,pg_lsn,pg_lsn,jsonb,bigint) RENAME TO profile_original_publish;
CREATE FUNCTION lake_direct.publish(p_previous pg_lsn,p_commit pg_lsn,p_end pg_lsn,p_changes jsonb,p_transactions bigint DEFAULT 1)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,lake_direct AS $$
BEGIN
 PERFORM set_config('lake_direct.profile_boundary',p_end::text,true);
 RETURN lake_direct.profile_original_publish(p_previous,p_commit,p_end,p_changes,p_transactions);
END $$;
ALTER FUNCTION lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[]) RENAME TO profile_original_apply_change_run;
CREATE FUNCTION lake_replication.apply_change_run(p_operation text,p_destination regclass,p_old jsonb[],p_new jsonb[])
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
DECLARE started timestamptz:=clock_timestamp(); affected bigint; elapsed double precision;
BEGIN
 affected:=lake_replication.profile_original_apply_change_run(p_operation,p_destination,p_old,p_new);
 elapsed:=extract(epoch FROM clock_timestamp()-started)*1000;
 INSERT INTO lake_direct.profile_calls VALUES(current_setting('lake_direct.profile_boundary')::pg_lsn,
  p_operation,CASE WHEN p_operation='delete' THEN cardinality(p_old) ELSE cardinality(p_new) END,elapsed);
 RETURN affected;
END $$;
REVOKE ALL ON FUNCTION lake_direct.publish(pg_lsn,pg_lsn,pg_lsn,jsonb,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION lake_replication.apply_change_run(text,regclass,jsonb[],jsonb[]) FROM PUBLIC;
COMMIT;

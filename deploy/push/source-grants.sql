\set ON_ERROR_STOP on
-- Administrator-applied, one trusted capture operator per source database.
-- This role can manage this database's capture control plane and has PostgreSQL
-- REPLICATION capabilities. It is not an isolation boundary between streams.
\if :{?capture_login}
\else
  DO $$ BEGIN RAISE EXCEPTION 'Set -v capture_login=existing_login'; END $$;
\endif
BEGIN;
SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:'capture_login' AND rolcanlogin
    AND rolreplication AND rolinherit AND NOT (rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls)
    AND NOT EXISTS(SELECT FROM pg_roles elevated WHERE
        (elevated.rolsuper OR elevated.rolcreaterole OR elevated.rolcreatedb OR elevated.rolbypassrls
         OR elevated.rolname IN ('pg_read_server_files','pg_write_server_files','pg_execute_server_program','pg_signal_backend'))
        AND pg_has_role(pg_roles.oid,elevated.oid,'MEMBER'))) AS valid \gset
\if :valid
\else
  DO $$ BEGIN RAISE EXCEPTION 'capture_login must be a dedicated LOGIN REPLICATION INHERIT role without elevated administrative attributes'; END $$;
\endif
GRANT CONNECT,TEMPORARY ON DATABASE :"DBNAME" TO :"capture_login";
GRANT lake_read_write TO :"capture_login";
GRANT USAGE ON SCHEMA lake_capture,lake_capture_log TO :"capture_login";
GRANT SELECT ON lake_capture.streams TO :"capture_login";
REVOKE INSERT ON lake_capture.streams FROM :"capture_login";
GRANT INSERT(stream_id,owner,origin_system_identifier,origin_database_oid,table_a,table_b,
    table_a_id,table_b_id,slot_name,credential_file,object_prefix,log_locations,bootstrap_mode)
    ON lake_capture.streams TO :"capture_login";
GRANT UPDATE(desired_state,config_generation,bound_postmaster,last_error,credential_file,bootstrap_mode,
    capture_failure_count,capture_conflict_count,last_failure_at,last_failure_sqlstate,
    progress_request_count,max_progress_requests,
    baseline_lsn,snapshot_state,captured_sequence,captured_commit_lsn,captured_end_lsn,
    retained_log_bytes,generation_log_bytes,pruned_through_sequence,reserved_upload_bytes,
    upload_attempt_count,maintenance_attempt_count,max_wal_bytes,max_decode_spill_bytes,decode_work_mem_kb,transactions_per_read,publication_interval_ms,
    max_retained_log_bytes,max_generation_log_bytes,max_generation_batches,max_reserved_upload_bytes,
    max_upload_attempts,max_maintenance_attempts) ON lake_capture.streams TO :"capture_login";
GRANT SELECT,INSERT,UPDATE ON lake_capture.receipts,lake_capture.upload_attempts,
    lake_capture.consumers TO :"capture_login";
GRANT SELECT,INSERT,UPDATE(captured_sequence,captured_end_lsn) ON lake_capture.progress_requests TO :"capture_login";
GRANT SELECT,INSERT ON lake_capture.schema_definitions TO :"capture_login";
GRANT SELECT,INSERT ON lake_capture_log.registrations TO :"capture_login";
GRANT SELECT,INSERT,DELETE,MAINTAIN ON lake_capture_log.events_a,lake_capture_log.events_b,
    lake_capture_log.controls,lake_capture_log.manifests TO :"capture_login";
GRANT SELECT ON lake_capture.status,lake_capture_log.progress,lake_capture_log.receipt_state TO :"capture_login";
GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO :"capture_login";
GRANT EXECUTE ON FUNCTION lake_capture.lock_private_logs(boolean) TO :"capture_login";
GRANT EXECUTE ON FUNCTION lake_capture.describe_relation(regclass) TO :"capture_login";
GRANT EXECUTE ON FUNCTION lake_capture.describe_relation_json(regclass) TO :"capture_login";
SELECT format('GRANT EXECUTE ON FUNCTION %s TO %I',p.oid::regprocedure,:'capture_login')
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
    WHERE n.nspname='lake_capture' AND l.lanname IN ('plpgsql','sql') AND p.proname NOT LIKE 'guard_%' \gexec
-- Separately grant SELECT, MAINTAIN on only the two explicitly selected application tables.
GRANT EXECUTE ON FUNCTION lake_capture.validate_relation(regclass) TO :"capture_login";
COMMIT;

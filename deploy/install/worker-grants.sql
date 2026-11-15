\set ON_ERROR_STOP on
\if :{?worker_login}
\else
  DO $$ BEGIN RAISE EXCEPTION 'Set -v worker_login=existing_login' USING ERRCODE='22023'; END $$;
\endif
\if :{?destination_schema}
\else
  DO $$ BEGIN RAISE EXCEPTION 'Set -v destination_schema=existing_dedicated_schema' USING ERRCODE='22023'; END $$;
\endif

BEGIN;
-- Run as the extension installer. Use a fresh, unprivileged LOGIN and a
-- dedicated destination schema. This recipe is database-wide for the
-- replication control plane; it does not isolate subscriptions or storage.
SELECT EXISTS (
    SELECT FROM pg_catalog.pg_roles
    WHERE rolname=:'worker_login' AND rolcanlogin AND rolinherit
      AND NOT (rolsuper OR rolcreaterole OR rolcreatedb OR rolreplication OR rolbypassrls)
      AND NOT pg_catalog.pg_has_role(oid, 'lake_replication_admin', 'MEMBER')
      AND NOT EXISTS (
          SELECT FROM pg_catalog.pg_roles AS elevated
          WHERE (elevated.rolsuper OR elevated.rolcreaterole OR elevated.rolcreatedb
                 OR elevated.rolreplication OR elevated.rolbypassrls
                 OR elevated.rolname IN ('pg_read_all_data','pg_write_all_data',
                     'pg_read_server_files','pg_write_server_files',
                     'pg_execute_server_program','pg_signal_backend'))
            AND pg_catalog.pg_has_role(pg_roles.oid,elevated.oid,'MEMBER')
      )
) AS worker_login_valid \gset
\if :worker_login_valid
\else
  DO $$ BEGIN RAISE EXCEPTION 'worker_login must be an unprivileged INHERIT login without administrator membership' USING ERRCODE='22023'; END $$;
\endif
SELECT EXISTS (
    SELECT FROM pg_catalog.pg_namespace
    WHERE nspname=:'destination_schema'
      AND nspname NOT IN ('public','lake_replication','information_schema')
      AND nspname NOT LIKE 'pg_%'
) AS destination_schema_valid \gset
\if :destination_schema_valid
\else
  DO $$ BEGIN RAISE EXCEPTION 'destination_schema must be an existing dedicated application schema' USING ERRCODE='22023'; END $$;
\endif

-- Iceberg UPDATE tracking creates PostgreSQL temporary tables.
GRANT CONNECT, TEMPORARY ON DATABASE :"DBNAME" TO :"worker_login";
GRANT lake_replication_worker, lake_read_write TO :"worker_login";
GRANT USAGE, CREATE ON SCHEMA :"destination_schema" TO :"worker_login";

GRANT SELECT ON lake_replication.sources, lake_replication.subscriptions,
    lake_replication.worker_launch_config, lake_replication.type_mappings,
    lake_replication.journal_maintenance_policies, lake_replication.journal_limits
    TO :"worker_login";
GRANT UPDATE (state,last_error) ON lake_replication.subscriptions TO :"worker_login";
GRANT UPDATE (timeline) ON lake_replication.sources TO :"worker_login";
-- FOR UPDATE of the maintenance policy requires UPDATE on one column.
GRANT UPDATE (enabled) ON lake_replication.journal_maintenance_policies TO :"worker_login";
GRANT SELECT, INSERT, UPDATE, DELETE ON
    lake_replication.relations,
    lake_replication.transactions, lake_replication.batches,
    lake_replication.streamed_transactions, lake_replication.streamed_changes,
    lake_replication.initial_copies, lake_replication.initial_copy_rows,
    lake_replication.initial_copy_chunks
    TO :"worker_login";
GRANT SELECT, UPDATE ON lake_replication.checkpoints,
    lake_replication.checkpoint_ingress, lake_replication.checkpoint_publication
    TO :"worker_login";
GRANT SELECT, INSERT, DELETE ON lake_replication.changes TO :"worker_login";
GRANT SELECT, INSERT ON lake_replication.relation_type_contracts TO :"worker_login";
GRANT SELECT, INSERT, UPDATE ON
    lake_replication.service_status, lake_replication.journal_maintenance_status
    TO :"worker_login";
GRANT USAGE ON SEQUENCE lake_replication.transactions_transaction_id_seq,
    lake_replication.batches_batch_id_seq TO :"worker_login";
GRANT EXECUTE ON FUNCTION
    lake_replication.assert_worker(bigint,bigint),
    lake_replication.admit_bulk(), lake_replication.admit_stream(),
    lake_replication.admit_staging(), lake_replication.admit_initial_copy(),
    lake_replication.assert_initial_copy_lease(),
    lake_replication.try_stage_transaction(bigint,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb),
    lake_replication.try_commit_stream(bigint,bigint,pg_lsn,pg_lsn,timestamptz)
    TO :"worker_login";
COMMIT;

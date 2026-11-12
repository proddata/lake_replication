BEGIN;

DO $$ BEGIN
    IF EXISTS (SELECT FROM lake_replication.worker_launch_config WHERE desired_state <> 'stopped')
       OR EXISTS (SELECT FROM pg_stat_activity WHERE datname=current_database()
                  AND backend_type='lake_replication supervisor') THEN
        RAISE EXCEPTION 'stop subscriptions before applying the source TLS policy refresh'
            USING ERRCODE='55000';
    END IF;
END $$;

-- Shares the protected CONNECTION policy with managed workers, without exposing
-- conninfo or passwords. Keep the same trust boundary as source_credentials.
CREATE FUNCTION lake_replication.source_tls_settings(bigint)
RETURNS text[] LANGUAGE C STABLE STRICT SECURITY DEFINER
SET search_path=pg_catalog,lake_replication,pg_temp
AS 'MODULE_PATHNAME', 'lake_replication_source_tls_settings';
REVOKE ALL ON FUNCTION lake_replication.source_tls_settings(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.source_tls_settings(bigint)
    TO lake_replication_worker;

-- CREATE/ALTER EXTENSION records membership automatically. A same-version
-- development refresh must register its newly created function explicitly.
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid
        WHERE d.classid='pg_proc'::regclass
          AND d.objid='lake_replication.source_tls_settings(bigint)'::regprocedure
          AND d.refclassid='pg_extension'::regclass
          AND e.extname='lake_replication' AND d.deptype='e'
    ) THEN
        ALTER EXTENSION lake_replication ADD FUNCTION lake_replication.source_tls_settings(bigint);
    END IF;
END $$;

COMMIT;

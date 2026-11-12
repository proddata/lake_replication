BEGIN;

-- Shares the protected CONNECTION policy with managed workers, without exposing
-- conninfo or passwords. Keep the same trust boundary as source_credentials.
CREATE FUNCTION lake_replication.source_tls_settings(bigint)
RETURNS text[] LANGUAGE C STABLE STRICT SECURITY DEFINER
SET search_path=pg_catalog,lake_replication,pg_temp
AS 'MODULE_PATHNAME', 'lake_replication_source_tls_settings';
REVOKE ALL ON FUNCTION lake_replication.source_tls_settings(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.source_tls_settings(bigint)
    TO lake_replication_worker;

COMMIT;

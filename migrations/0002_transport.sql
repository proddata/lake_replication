BEGIN;

CREATE OR REPLACE FUNCTION lake_replication.register_relation(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_source_namespace name,
    p_source_relation name,
    p_replica_identity "char",
    p_columns jsonb
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_relation relations%ROWTYPE;
    v_schema_version bigint;
BEGIN
    IF jsonb_typeof(p_columns) IS DISTINCT FROM 'array'
       OR jsonb_array_length(p_columns) = 0 THEN
        RAISE EXCEPTION 'relation columns must be a non-empty JSON array'
            USING ERRCODE = '22023';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_subscription_id::text || ':' || p_remote_relation_id::text, 0)
    );
    SELECT * INTO v_relation
      FROM relations
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
     ORDER BY schema_version DESC
     LIMIT 1;

    IF FOUND
       AND v_relation.source_namespace = p_source_namespace
       AND v_relation.source_relation = p_source_relation
       AND v_relation.replica_identity = p_replica_identity
       AND v_relation.columns = p_columns THEN
        RETURN v_relation.schema_version;
    END IF;

    v_schema_version := COALESCE(v_relation.schema_version, 0) + 1;
    INSERT INTO relations (
        subscription_id, remote_relation_id, schema_version,
        source_namespace, source_relation, replica_identity, columns
    ) VALUES (
        p_subscription_id, p_remote_relation_id, v_schema_version,
        p_source_namespace, p_source_relation, p_replica_identity, p_columns
    );
    RETURN v_schema_version;
END;
$$;

COMMIT;

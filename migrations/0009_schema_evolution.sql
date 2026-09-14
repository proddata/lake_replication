BEGIN;

CREATE OR REPLACE FUNCTION lake_replication.record_schema_error(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_error text
) RETURNS void
LANGUAGE sql
SET search_path = pg_catalog, lake_replication
AS $$
    UPDATE relations SET state='paused'
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND schema_version=p_schema_version;
    UPDATE subscriptions SET state='paused', last_error=p_error
     WHERE subscription_id=p_subscription_id;
$$;

CREATE OR REPLACE FUNCTION lake_replication.reconcile_relation_schema(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint
) RETURNS text
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_new relations%ROWTYPE;
    v_old relations%ROWTYPE;
    v_new_contract relation_type_contracts%ROWTYPE;
    v_old_contract relation_type_contracts%ROWTYPE;
    v_old_count integer;
    v_new_count integer;
    v_error text;
    v_column record;
    v_destination_names text[];
    v_destination_types text[];
    v_destination_not_null boolean[];
    v_expected_old_types text[];
BEGIN
    SELECT * INTO STRICT v_new FROM relations
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND schema_version=p_schema_version
     FOR UPDATE;
    SELECT * INTO STRICT v_new_contract FROM relation_type_contracts
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND relation_schema_version=p_schema_version;
    IF v_new.destination_relation IS NOT NULL THEN
        UPDATE relations SET state='ready'
         WHERE subscription_id=p_subscription_id
           AND remote_relation_id=p_remote_relation_id
           AND schema_version=p_schema_version;
        RETURN 'ready';
    END IF;

    SELECT * INTO v_old FROM relations
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND schema_version < p_schema_version
       AND destination_relation IS NOT NULL
     ORDER BY schema_version DESC LIMIT 1;
    IF NOT FOUND THEN
        v_error := format('relation %s version %s has no assigned predecessor',
                          p_remote_relation_id, p_schema_version);
        PERFORM record_schema_error(p_subscription_id, p_remote_relation_id,
                                    p_schema_version, v_error);
        RETURN 'paused:' || v_error;
    END IF;
    SELECT * INTO STRICT v_old_contract FROM relation_type_contracts
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND relation_schema_version=v_old.schema_version;

    v_old_count := jsonb_array_length(v_old_contract.columns);
    v_new_count := jsonb_array_length(v_new_contract.columns);
    IF v_new_contract.mapping_version <> v_old_contract.mapping_version THEN
        v_error := 'type mapping version changed during active replication';
    ELSIF v_new_count < v_old_count THEN
        v_error := 'published columns were removed or reordered';
    ELSIF EXISTS (
        SELECT 1
          FROM jsonb_array_elements(v_old_contract.columns) WITH ORDINALITY old_col(value, n)
          JOIN jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY new_col(value, n)
            USING (n)
         WHERE old_col.value->>'name' IS DISTINCT FROM new_col.value->>'name'
            OR old_col.value->>'source_type' IS DISTINCT FROM new_col.value->>'source_type'
    ) THEN
        v_error := 'an existing published column changed name, order, or type';
    ELSIF EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY c(value, n)
         WHERE n > v_old_count
           AND (c.value->>'not_null')::boolean
    ) THEN
        v_error := 'new published columns must be nullable for online addition';
    END IF;
    IF v_error IS NOT NULL THEN
        PERFORM record_schema_error(p_subscription_id, p_remote_relation_id,
                                    p_schema_version, v_error);
        RETURN 'paused:' || v_error;
    END IF;

    SELECT array_agg(attname::text ORDER BY attnum),
           array_agg(format_type(atttypid,atttypmod) ORDER BY attnum),
           array_agg(attnotnull ORDER BY attnum)
      INTO v_destination_names, v_destination_types, v_destination_not_null
      FROM pg_attribute
     WHERE attrelid=v_old.destination_relation AND attnum > 0 AND NOT attisdropped;
    SELECT array_agg(m.destination_type ORDER BY c.n)
      INTO v_expected_old_types
      FROM jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY c(value, n)
      JOIN type_mappings m
        ON m.mapping_version=v_new_contract.mapping_version
       AND m.source_type=c.value->>'source_type'
     WHERE c.n <= v_old_count;
    IF cardinality(v_destination_names) <> v_old_count
       OR EXISTS (
           SELECT 1 FROM generate_series(1, v_old_count) n
            WHERE v_destination_names[n] IS DISTINCT FROM
                      v_new_contract.columns->(n-1)->>'name'
               OR v_destination_types[n] IS DISTINCT FROM v_expected_old_types[n]
               OR (v_destination_not_null[n]
                   AND NOT (v_new_contract.columns->(n-1)->>'not_null')::boolean)
       ) THEN
        v_error := 'existing destination columns are incompatible with the new contract';
        PERFORM record_schema_error(p_subscription_id, p_remote_relation_id,
                                    p_schema_version, v_error);
        RETURN 'paused:' || v_error;
    END IF;

    BEGIN
        FOR v_column IN
            SELECT c.value->>'name' AS column_name, m.destination_type
              FROM jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY c(value, n)
              JOIN type_mappings m
                ON m.mapping_version=v_new_contract.mapping_version
               AND m.source_type=c.value->>'source_type'
             WHERE c.n > v_old_count ORDER BY c.n
        LOOP
            EXECUTE format('ALTER TABLE %s ADD COLUMN %I %s',
                           v_old.destination_relation,
                           v_column.column_name, v_column.destination_type);
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        v_error := format('destination ALTER failed: %s', SQLERRM);
        PERFORM record_schema_error(p_subscription_id, p_remote_relation_id,
                                    p_schema_version, v_error);
        RETURN 'paused:' || v_error;
    END;

    UPDATE relations SET destination_relation=v_old.destination_relation, state='ready'
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND schema_version=p_schema_version;
    UPDATE subscriptions SET state='streaming', last_error=NULL
     WHERE subscription_id=p_subscription_id;
    RETURN 'ready';
END;
$$;

COMMIT;

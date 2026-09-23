BEGIN;

-- Iceberg decimal values require an explicit precision and scale. PostgreSQL
-- permits wider and more unusual numeric typmods than Iceberg, so mapping
-- version 1 accepts only the lossless Iceberg range. Character typmods are
-- preserved in destination DDL; unbounded varchar is also safe.
CREATE OR REPLACE FUNCTION lake_replication.resolve_destination_type(
    p_mapping_version integer,
    p_source_type text
) RETURNS text
LANGUAGE plpgsql
STABLE
STRICT
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_destination text;
    v_parts text[];
    v_precision integer;
    v_scale integer;
    v_length integer;
BEGIN
    SELECT destination_type INTO v_destination
      FROM type_mappings
     WHERE mapping_version = p_mapping_version
       AND source_type = p_source_type;
    IF FOUND THEN
        RETURN v_destination;
    END IF;
    IF p_mapping_version <> 1 THEN
        RETURN NULL;
    END IF;

    v_parts := regexp_match(p_source_type, '^numeric\(([0-9]+),([0-9]+)\)$');
    IF v_parts IS NOT NULL THEN
        v_precision := v_parts[1]::integer;
        v_scale := v_parts[2]::integer;
        IF v_precision BETWEEN 1 AND 38
           AND v_scale BETWEEN 0 AND v_precision THEN
            RETURN format('numeric(%s,%s)', v_precision, v_scale);
        END IF;
        RETURN NULL;
    END IF;

    IF p_source_type = 'character varying' THEN
        RETURN p_source_type;
    END IF;
    v_parts := regexp_match(
        p_source_type, '^character varying\(([1-9][0-9]*)\)$');
    IF v_parts IS NOT NULL THEN
        v_length := v_parts[1]::integer;
        IF v_length <= 10485760 THEN
            RETURN format('character varying(%s)', v_length);
        END IF;
        RETURN NULL;
    END IF;

    v_parts := regexp_match(p_source_type, '^character\(([1-9][0-9]*)\)$');
    IF v_parts IS NOT NULL THEN
        v_length := v_parts[1]::integer;
        IF v_length <= 10485760 THEN
            RETURN format('character(%s)', v_length);
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

-- Replace the callers as part of the upgrade. A 0.1.0 installation created by
-- an older bundle joined type_mappings directly and would otherwise bypass the
-- resolver added above.
CREATE OR REPLACE FUNCTION lake_replication.register_type_contract(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_mapping_version integer,
    p_columns jsonb
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_relation_columns jsonb;
    v_existing relation_type_contracts%ROWTYPE;
BEGIN
    IF jsonb_typeof(p_columns) IS DISTINCT FROM 'array'
       OR jsonb_array_length(p_columns) = 0 THEN
        RAISE EXCEPTION 'type contract must be a non-empty JSON array'
            USING ERRCODE = '22023';
    END IF;
    SELECT columns INTO STRICT v_relation_columns FROM relations
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND schema_version = p_schema_version;
    IF jsonb_array_length(v_relation_columns) <> jsonb_array_length(p_columns)
       OR EXISTS (
           SELECT 1
             FROM jsonb_array_elements(v_relation_columns) WITH ORDINALITY
                  AS relation_column(value, ordinality)
             JOIN jsonb_array_elements(p_columns) WITH ORDINALITY
                  AS contract_column(value, ordinality) USING (ordinality)
            WHERE relation_column.value->>'name'
                  IS DISTINCT FROM contract_column.value->>'name'
       ) THEN
        RAISE EXCEPTION 'type contract columns do not match relation descriptor'
            USING ERRCODE = '42804';
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_columns) AS source_column(value)
         WHERE jsonb_typeof(source_column.value->'not_null') IS DISTINCT FROM 'boolean'
            OR jsonb_typeof(source_column.value->'generated') IS DISTINCT FROM 'boolean'
            OR source_column.value->>'source_type' IS NULL
    ) THEN
        RAISE EXCEPTION 'invalid type contract column' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_columns) AS source_column(value)
         WHERE (source_column.value->>'generated')::boolean
    ) THEN
        RAISE EXCEPTION 'generated source columns are not supported by mapping version %',
            p_mapping_version USING ERRCODE = '0A000';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM jsonb_array_elements(p_columns) AS source_column(value)
         WHERE resolve_destination_type(
                   p_mapping_version,
                   source_column.value->>'source_type') IS NULL
    ) THEN
        RAISE EXCEPTION 'type contract contains a type unsupported by mapping version %',
            p_mapping_version USING ERRCODE = '0A000';
    END IF;

    SELECT * INTO v_existing FROM relation_type_contracts
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version;
    IF FOUND THEN
        IF v_existing.mapping_version <> p_mapping_version
           OR v_existing.columns <> p_columns THEN
            RAISE EXCEPTION 'conflicting type contract for relation % version %',
                p_remote_relation_id, p_schema_version USING ERRCODE = '23505';
        END IF;
        RETURN;
    END IF;
    INSERT INTO relation_type_contracts VALUES (
        p_subscription_id, p_remote_relation_id, p_schema_version,
        p_mapping_version, p_columns, clock_timestamp()
    );
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.ensure_iceberg_destination(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_destination_schema name,
    p_destination_relation name,
    p_create_missing boolean
) RETURNS regclass
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_contract relation_type_contracts%ROWTYPE;
    v_destination regclass;
    v_definitions text;
    v_expected_names text[];
    v_actual_names text[];
    v_expected_types text[];
    v_actual_types text[];
    v_expected_not_null boolean[];
    v_actual_not_null boolean[];
    v_relkind "char";
BEGIN
    SELECT * INTO STRICT v_contract FROM relation_type_contracts
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version;

    SELECT string_agg(format('%I %s%s', source_column.value->>'name',
                             resolve_destination_type(
                                 v_contract.mapping_version,
                                 source_column.value->>'source_type'),
                             CASE WHEN (source_column.value->>'not_null')::boolean
                                  THEN ' NOT NULL' ELSE '' END),
                      ', ' ORDER BY source_column.ordinality),
           array_agg(source_column.value->>'name' ORDER BY source_column.ordinality),
           array_agg(resolve_destination_type(
                         v_contract.mapping_version,
                         source_column.value->>'source_type')
                     ORDER BY source_column.ordinality),
           array_agg((source_column.value->>'not_null')::boolean
                     ORDER BY source_column.ordinality)
      INTO v_definitions, v_expected_names, v_expected_types, v_expected_not_null
      FROM jsonb_array_elements(v_contract.columns) WITH ORDINALITY
           AS source_column(value, ordinality);

    v_destination := to_regclass(format('%I.%I', p_destination_schema, p_destination_relation));
    IF v_destination IS NULL THEN
        IF NOT p_create_missing THEN
            RAISE EXCEPTION 'destination %.% does not exist',
                p_destination_schema, p_destination_relation USING ERRCODE = '42P01';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=p_destination_schema) THEN
            RAISE EXCEPTION 'destination schema % does not exist', p_destination_schema
                USING ERRCODE = '3F000';
        END IF;
        EXECUTE format('CREATE TABLE %I.%I (%s) USING iceberg',
                       p_destination_schema, p_destination_relation, v_definitions);
        v_destination := to_regclass(format('%I.%I', p_destination_schema, p_destination_relation));
    END IF;

    SELECT relkind INTO STRICT v_relkind FROM pg_class WHERE oid=v_destination;
    IF v_relkind <> 'f' THEN
        RAISE EXCEPTION 'destination % must be a writable Iceberg foreign table', v_destination
            USING ERRCODE = '42809';
    END IF;
    SELECT array_agg(attname::text ORDER BY attnum),
           array_agg(format_type(atttypid, atttypmod) ORDER BY attnum),
           array_agg(attnotnull ORDER BY attnum)
      INTO v_actual_names, v_actual_types, v_actual_not_null
      FROM pg_attribute
     WHERE attrelid=v_destination AND attnum > 0 AND NOT attisdropped;
    IF v_actual_names IS DISTINCT FROM v_expected_names
       OR v_actual_types IS DISTINCT FROM v_expected_types
       OR EXISTS (
           SELECT 1 FROM generate_subscripts(v_actual_not_null, 1) AS subscript
            WHERE v_actual_not_null[subscript] AND NOT v_expected_not_null[subscript]
       ) THEN
        RAISE EXCEPTION 'destination schema mismatch for %: expected names/types/nullability %/%/%, got %/%/%',
            v_destination, v_expected_names, v_expected_types, v_expected_not_null,
            v_actual_names, v_actual_types, v_actual_not_null USING ERRCODE = '42804';
    END IF;

    PERFORM assign_destination(
        p_subscription_id, p_remote_relation_id, p_schema_version, v_destination
    );
    RETURN v_destination;
END;
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
    SELECT array_agg(resolve_destination_type(
                         v_new_contract.mapping_version,
                         c.value->>'source_type') ORDER BY c.n)
      INTO v_expected_old_types
      FROM jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY c(value, n)
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
            SELECT c.value->>'name' AS column_name,
                   resolve_destination_type(
                       v_new_contract.mapping_version,
                       c.value->>'source_type') AS destination_type
              FROM jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY c(value, n)
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

BEGIN;

CREATE TABLE lake_replication.type_mappings (
    mapping_version integer NOT NULL CHECK (mapping_version > 0),
    source_type text NOT NULL,
    destination_type text NOT NULL,
    binary_compatible boolean NOT NULL DEFAULT false,
    PRIMARY KEY (mapping_version, source_type)
);

INSERT INTO lake_replication.type_mappings (
    mapping_version, source_type, destination_type, binary_compatible
) VALUES
    (1, 'smallint', 'smallint', true),
    (1, 'integer', 'integer', true),
    (1, 'bigint', 'bigint', true),
    (1, 'boolean', 'boolean', true),
    (1, 'text', 'text', true),
    (1, 'real', 'real', true),
    (1, 'double precision', 'double precision', true),
    (1, 'date', 'date', true),
    (1, 'timestamp without time zone', 'timestamp without time zone', true),
    (1, 'timestamp with time zone', 'timestamp with time zone', true),
    (1, 'uuid', 'uuid', true),
    (1, 'jsonb', 'jsonb', false),
    (1, 'bytea', 'bytea', true);

CREATE TABLE lake_replication.relation_type_contracts (
    subscription_id bigint NOT NULL,
    remote_relation_id oid NOT NULL,
    relation_schema_version bigint NOT NULL,
    mapping_version integer NOT NULL,
    columns jsonb NOT NULL CHECK (jsonb_typeof(columns) = 'array'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (subscription_id, remote_relation_id, relation_schema_version),
    FOREIGN KEY (subscription_id, remote_relation_id, relation_schema_version)
        REFERENCES lake_replication.relations
            (subscription_id, remote_relation_id, schema_version)
        ON DELETE CASCADE
);

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
          LEFT JOIN type_mappings m
            ON m.mapping_version = p_mapping_version
           AND m.source_type = source_column.value->>'source_type'
         WHERE m.source_type IS NULL
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
                             m.destination_type,
                             CASE WHEN (source_column.value->>'not_null')::boolean
                                  THEN ' NOT NULL' ELSE '' END),
                      ', ' ORDER BY source_column.ordinality),
           array_agg(source_column.value->>'name' ORDER BY source_column.ordinality),
           array_agg(m.destination_type ORDER BY source_column.ordinality),
           array_agg((source_column.value->>'not_null')::boolean
                     ORDER BY source_column.ordinality)
      INTO v_definitions, v_expected_names, v_expected_types, v_expected_not_null
      FROM jsonb_array_elements(v_contract.columns) WITH ORDINALITY
           AS source_column(value, ordinality)
      JOIN type_mappings m
        ON m.mapping_version = v_contract.mapping_version
       AND m.source_type = source_column.value->>'source_type';

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

COMMIT;

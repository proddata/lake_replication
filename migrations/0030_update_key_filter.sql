BEGIN;

-- Push supported single-column identity membership into the Iceberg scan.
-- The source-order grouping, 500-row/4-MiB bounds and row-count checks remain.

CREATE OR REPLACE FUNCTION lake_replication.apply_change_run(
    p_operation text,
    p_destination regclass,
    p_old jsonb[],
    p_new jsonb[]
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_count bigint;
    v_distinct bigint;
    v_expected bigint;
    v_set_clause text;
    v_where_clause text;
    v_old_item jsonb;
    v_rows bigint;
    v_identity_key text;
    v_identity_columns bigint;
    v_key_type oid;
    v_key_not_null boolean;
    v_key_query text;
    v_update_query text;
    v_bool_keys boolean[];
    v_int2_keys smallint[];
    v_int4_keys integer[];
    v_int8_keys bigint[];
    v_text_keys text[];
    v_varchar_keys varchar[];
    v_uuid_keys uuid[];
BEGIN
    PERFORM lake_replication.admit_stream();
    v_expected := CASE WHEN p_operation = 'delete'
                       THEN cardinality(p_old) ELSE cardinality(p_new) END;
    IF v_expected IS NULL OR v_expected = 0 THEN
        RAISE EXCEPTION 'empty change run' USING ERRCODE = '22023';
    END IF;

    IF p_operation = 'insert' THEN
        EXECUTE format(
            'INSERT INTO %s
             SELECT (jsonb_populate_record(NULL::%s, source.new_tuple)).*
               FROM unnest($1::jsonb[]) AS source(new_tuple)',
            p_destination, p_destination
        ) USING p_new;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        RETURN v_count;
    END IF;

    -- Equality lets PostgreSQL use a merge/hash join for ordinary non-null
    -- replica identities. Preserve null-safe matching for nullable keys.
    SELECT string_agg(format(
               '%1$I %3$s
                (jsonb_populate_record(NULL::%2$s, source.old_tuple)).%1$I',
               key, p_destination,
               CASE WHEN p_operation = 'update' AND NOT EXISTS (
                    SELECT 1 FROM unnest(p_old) AS old(value)
                     WHERE NOT (value ? key) OR value->key = 'null'::jsonb
               ) THEN '=' ELSE 'IS NOT DISTINCT FROM' END),
               ' AND ' ORDER BY key)
      INTO v_where_clause
      FROM jsonb_object_keys(p_old[1]) AS item(key);

    IF p_operation = 'update' THEN
        -- Keep key arrays typed. Textual round trips can lose float precision
        -- or character padding; record fields can retain a cached array type
        -- when the same backend later applies a different identity type.
        SELECT count(*), min(key) INTO v_identity_columns, v_identity_key
          FROM jsonb_object_keys(p_old[1]) AS item(key);
        IF v_identity_columns = 1 AND NOT EXISTS (
            SELECT 1 FROM unnest(p_old) AS old(value)
             WHERE NOT (value ? v_identity_key)
                OR value->v_identity_key = 'null'::jsonb
        ) THEN
            SELECT atttypid, attnotnull INTO v_key_type, v_key_not_null
              FROM pg_attribute
             WHERE attrelid = p_destination AND attname = v_identity_key
               AND attnum > 0 AND NOT attisdropped;
            -- Restrict membership pushdown to common key types qualified on
            -- the pinned FDW. Formatting-sensitive and other scalar types,
            -- composite and nullable identities retain the original predicate
            -- and exactly-one-match checks below.
            IF v_key_not_null AND v_key_type = ANY (ARRAY[
                'boolean'::regtype::oid, 'smallint'::regtype::oid,
                'integer'::regtype::oid, 'bigint'::regtype::oid,
                'text'::regtype::oid, 'character varying'::regtype::oid,
                'uuid'::regtype::oid
            ]) THEN
                v_key_query := format(
                    'SELECT array_agg((jsonb_populate_record(NULL::%s, source.old_tuple)).%I) AS keys
                       FROM unnest($1::jsonb[]) AS source(old_tuple)',
                    p_destination, v_identity_key
                );
                -- This is redundant with the identity join, but is separately
                -- shippable to pg_lake. Filter before transferring/sorting rows
                -- in PostgreSQL rather than scanning the full target per run.
                v_where_clause := v_where_clause || format(
                    ' AND destination.%I = ANY ($3)', v_identity_key);
            END IF;
        END IF;

        SELECT string_agg(format(
                   '%1$I = (jsonb_populate_record(NULL::%2$s, source.new_tuple)).%1$I',
                   key, p_destination), ', ' ORDER BY key)
          INTO v_set_clause
          FROM jsonb_object_keys(p_new[1]) AS item(key);
        v_update_query := format(
            'WITH source AS (
                 SELECT old_tuple, new_tuple, ordinality
                   FROM unnest($1::jsonb[], $2::jsonb[])
                        WITH ORDINALITY AS input(old_tuple, new_tuple, ordinality)
             ), applied AS (
                 UPDATE %s AS destination SET %s
                   FROM source
                  WHERE %s
                  RETURNING source.ordinality
             )
             SELECT count(*), count(DISTINCT ordinality) FROM applied',
            p_destination, v_set_clause, v_where_clause
        );
        IF v_key_query IS NULL THEN
            EXECUTE v_update_query INTO v_count, v_distinct USING p_old, p_new;
        ELSE
            CASE v_key_type
                WHEN 'boolean'::regtype::oid THEN
                    EXECUTE v_key_query INTO v_bool_keys USING p_old;
                    EXECUTE v_update_query INTO v_count, v_distinct USING p_old, p_new, v_bool_keys;
                WHEN 'smallint'::regtype::oid THEN
                    EXECUTE v_key_query INTO v_int2_keys USING p_old;
                    EXECUTE v_update_query INTO v_count, v_distinct USING p_old, p_new, v_int2_keys;
                WHEN 'integer'::regtype::oid THEN
                    EXECUTE v_key_query INTO v_int4_keys USING p_old;
                    EXECUTE v_update_query INTO v_count, v_distinct USING p_old, p_new, v_int4_keys;
                WHEN 'bigint'::regtype::oid THEN
                    EXECUTE v_key_query INTO v_int8_keys USING p_old;
                    EXECUTE v_update_query INTO v_count, v_distinct USING p_old, p_new, v_int8_keys;
                WHEN 'text'::regtype::oid THEN
                    EXECUTE v_key_query INTO v_text_keys USING p_old;
                    EXECUTE v_update_query INTO v_count, v_distinct USING p_old, p_new, v_text_keys;
                WHEN 'character varying'::regtype::oid THEN
                    EXECUTE v_key_query INTO v_varchar_keys USING p_old;
                    EXECUTE v_update_query INTO v_count, v_distinct USING p_old, p_new, v_varchar_keys;
                WHEN 'uuid'::regtype::oid THEN
                    EXECUTE v_key_query INTO v_uuid_keys USING p_old;
                    EXECUTE v_update_query INTO v_count, v_distinct USING p_old, p_new, v_uuid_keys;
                ELSE RAISE EXCEPTION 'unhandled update key type %', v_key_type;
            END CASE;
        END IF;
    ELSIF p_operation = 'delete' THEN
        SELECT string_agg(format(
                   '%1$I IS NOT DISTINCT FROM
                    (jsonb_populate_record(NULL::%2$s, $1)).%1$I',
                   key, p_destination), ' AND ' ORDER BY key)
          INTO v_where_clause
          FROM jsonb_object_keys(p_old[1]) AS item(key);
        v_count := 0;
        FOREACH v_old_item IN ARRAY p_old LOOP
            EXECUTE format('DELETE FROM %s AS destination WHERE %s',
                           p_destination, v_where_clause)
                USING v_old_item;
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            IF v_rows <> 1 THEN
                RAISE EXCEPTION 'delete for relation % matched % rows instead of exactly one',
                    p_destination, v_rows USING ERRCODE = 'P0002';
            END IF;
            v_count := v_count + v_rows;
        END LOOP;
        RETURN v_count;
    ELSE
        RAISE EXCEPTION 'unsupported change run operation %', p_operation
            USING ERRCODE = '22023';
    END IF;

    IF v_count <> v_expected OR v_distinct <> v_expected THEN
        RAISE EXCEPTION '% run for relation % matched % rows across % identities instead of exactly one per change',
            p_operation, p_destination, v_count, v_distinct USING ERRCODE = 'P0002';
    END IF;
    RETURN v_count;
END;
$$;

COMMIT;

BEGIN;

-- Apply consecutive independent changes in bounded SQL runs. UPDATE uses one
-- destination statement per run and verifies one affected row per identity.
-- DELETE remains row-wise because the pinned pg_lake revision crashes on a
-- DELETE USING a CTE source of unnested JSON values.

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
BEGIN
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
        SELECT string_agg(format(
                   '%1$I = (jsonb_populate_record(NULL::%2$s, source.new_tuple)).%1$I',
                   key, p_destination), ', ' ORDER BY key)
          INTO v_set_clause
          FROM jsonb_object_keys(p_new[1]) AS item(key);
        EXECUTE format(
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
        ) INTO v_count, v_distinct USING p_old, p_new;
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

CREATE OR REPLACE FUNCTION lake_replication.publish_insert_batch(
    p_subscription_id bigint,
    p_batch_key text,
    p_first_end_lsn pg_lsn,
    p_last_end_lsn pg_lsn
) RETURNS TABLE (batch_id bigint, published_lsn pg_lsn, inserted_rows bigint)
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_batch_id bigint;
    v_batch_state text;
    v_change record;
    v_run_operation text;
    v_run_destination regclass;
    v_run_old jsonb[] := ARRAY[]::jsonb[];
    v_run_new jsonb[] := ARRAY[]::jsonb[];
    v_run_old_keys text[];
    v_run_new_keys text[];
    v_old_keys text[];
    v_new_keys text[];
    v_run_groupable boolean := false;
    v_change_groupable boolean;
    v_can_append boolean;
    v_run_bytes bigint := 0;
    v_change_bytes bigint;
    v_affected_rows bigint := 0;
    v_rows bigint;
BEGIN
    v_batch_id := prepare_batch(
        p_subscription_id, p_batch_key, p_first_end_lsn, p_last_end_lsn
    );
    SELECT state INTO STRICT v_batch_state FROM batches WHERE batches.batch_id = v_batch_id;
    IF v_batch_state = 'published' THEN
        RETURN QUERY SELECT v_batch_id, p_last_end_lsn, 0::bigint;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM transactions t
        JOIN changes c USING (transaction_id)
        JOIN relations r ON r.subscription_id=t.subscription_id
          AND r.remote_relation_id=c.remote_relation_id
          AND r.schema_version=c.relation_schema_version
        WHERE t.batch_id=v_batch_id AND r.destination_relation IS NULL
    ) THEN
        RAISE EXCEPTION 'batch % contains a relation without a destination', v_batch_id
            USING ERRCODE = '55000';
    END IF;

    FOR v_change IN
        SELECT c.*, t.end_lsn, r.destination_relation
          FROM transactions t JOIN changes c USING (transaction_id)
          JOIN relations r ON r.subscription_id=t.subscription_id
            AND r.remote_relation_id=c.remote_relation_id
            AND r.schema_version=c.relation_schema_version
         WHERE t.batch_id=v_batch_id ORDER BY t.end_lsn, c.change_index
    LOOP
        IF v_change.operation = 'truncate' THEN
            IF v_run_operation IS NOT NULL THEN
                v_affected_rows := v_affected_rows + apply_change_run(
                    v_run_operation, v_run_destination, v_run_old, v_run_new);
                v_run_operation := NULL;
                v_run_old := ARRAY[]::jsonb[];
                v_run_new := ARRAY[]::jsonb[];
                v_run_bytes := 0;
            END IF;
            EXECUTE format('TRUNCATE %s', v_change.destination_relation);
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            v_affected_rows := v_affected_rows + v_rows;
            CONTINUE;
        END IF;

        IF v_change.operation = 'insert' THEN
            v_old_keys := NULL;
            v_new_keys := NULL;
        ELSE
            SELECT array_agg(key ORDER BY key) INTO v_old_keys
              FROM jsonb_object_keys(v_change.old_tuple) AS item(key);
            IF v_change.operation = 'update' THEN
                SELECT array_agg(key ORDER BY key) INTO v_new_keys
                  FROM jsonb_object_keys(v_change.new_tuple) AS item(key);
            ELSE
                v_new_keys := NULL;
            END IF;
        END IF;
        v_change_groupable := v_change.operation <> 'update'
            OR v_change.new_tuple @> v_change.old_tuple;
        v_change_bytes := coalesce(octet_length(v_change.old_tuple::text), 0)
                        + coalesce(octet_length(v_change.new_tuple::text), 0);
        v_can_append := v_run_operation = v_change.operation
            AND v_run_destination = v_change.destination_relation
            AND v_run_groupable AND v_change_groupable
            AND v_run_old_keys IS NOT DISTINCT FROM v_old_keys
            AND v_run_new_keys IS NOT DISTINCT FROM v_new_keys
            AND greatest(cardinality(v_run_old), cardinality(v_run_new)) < 500
            AND v_run_bytes + v_change_bytes <= 4194304
            AND (v_change.operation = 'insert'
                 OR NOT (v_change.old_tuple = ANY(v_run_old)));

        IF v_run_operation IS NOT NULL AND NOT v_can_append THEN
            v_affected_rows := v_affected_rows + apply_change_run(
                v_run_operation, v_run_destination, v_run_old, v_run_new);
            v_run_operation := NULL;
            v_run_old := ARRAY[]::jsonb[];
            v_run_new := ARRAY[]::jsonb[];
            v_run_bytes := 0;
        END IF;
        IF v_run_operation IS NULL THEN
            v_run_operation := v_change.operation;
            v_run_destination := v_change.destination_relation;
            v_run_old_keys := v_old_keys;
            v_run_new_keys := v_new_keys;
            v_run_groupable := v_change_groupable;
        END IF;
        IF v_change.operation <> 'insert' THEN
            v_run_old := array_append(v_run_old, v_change.old_tuple);
        END IF;
        IF v_change.operation <> 'delete' THEN
            v_run_new := array_append(v_run_new, v_change.new_tuple);
        END IF;
        v_run_bytes := v_run_bytes + v_change_bytes;
    END LOOP;

    IF v_run_operation IS NOT NULL THEN
        v_affected_rows := v_affected_rows + apply_change_run(
            v_run_operation, v_run_destination, v_run_old, v_run_new);
    END IF;
    PERFORM finish_batch(v_batch_id);
    RETURN QUERY SELECT v_batch_id, p_last_end_lsn, v_affected_rows;
END;
$$;

COMMIT;

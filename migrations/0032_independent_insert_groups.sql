BEGIN;

CREATE FUNCTION lake_replication.lock_insert_group_targets(regclass[]) RETURNS boolean
AS 'MODULE_PATHNAME','lake_replication_lock_insert_group_targets'
LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION lake_replication.lock_insert_group_targets(regclass[]) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.lock_insert_group_targets(regclass[])
    TO lake_replication_admin,lake_replication_worker;

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
    v_group_inserts boolean := false;
    v_targets regclass[];
BEGIN
    PERFORM lake_replication.admit_stream();
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

    -- Regroup only a wholly INSERT-only batch. Mixed operations retain the
    -- original global source order. Pin mappings before checking target locks.
    IF coalesce(current_setting('lake_replication.group_independent_inserts',true),'off')::boolean
       AND EXISTS(SELECT FROM transactions t JOIN changes c USING(transaction_id) WHERE t.batch_id=v_batch_id)
       AND NOT EXISTS(SELECT FROM transactions t JOIN changes c USING(transaction_id)
                      WHERE t.batch_id=v_batch_id AND c.operation<>'insert') THEN
        PERFORM 1 FROM relations r WHERE EXISTS(
            SELECT FROM transactions t JOIN changes c USING(transaction_id)
             WHERE t.batch_id=v_batch_id AND t.subscription_id=r.subscription_id
               AND c.remote_relation_id=r.remote_relation_id AND c.relation_schema_version=r.schema_version)
            ORDER BY r.remote_relation_id,r.schema_version FOR SHARE OF r;
        SELECT array_agg(DISTINCT r.destination_relation) INTO v_targets
          FROM transactions t JOIN changes c USING(transaction_id) JOIN relations r
            ON r.subscription_id=t.subscription_id AND r.remote_relation_id=c.remote_relation_id
           AND r.schema_version=c.relation_schema_version WHERE t.batch_id=v_batch_id;
        v_group_inserts := lock_insert_group_targets(v_targets);
    END IF;

    FOR v_change IN
        SELECT c.*, t.end_lsn, r.destination_relation
          FROM transactions t JOIN changes c USING (transaction_id)
          JOIN relations r ON r.subscription_id=t.subscription_id
            AND r.remote_relation_id=c.remote_relation_id
            AND r.schema_version=c.relation_schema_version
         WHERE t.batch_id=v_batch_id ORDER BY
            CASE WHEN v_group_inserts THEN r.destination_relation::oid ELSE 0::oid END,
            t.end_lsn, c.change_index
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

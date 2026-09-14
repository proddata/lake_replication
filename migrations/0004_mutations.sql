BEGIN;

ALTER TABLE lake_replication.changes
    ADD COLUMN old_tuple jsonb,
    ALTER COLUMN new_tuple DROP NOT NULL,
    DROP CONSTRAINT changes_operation_check,
    DROP CONSTRAINT changes_new_tuple_check;

UPDATE lake_replication.changes c
   SET new_tuple = (
       SELECT jsonb_object_agg(
                  column_value->>'name', c.new_tuple -> ((ordinality - 1)::integer))
         FROM lake_replication.transactions t
         JOIN lake_replication.relations r
           ON r.subscription_id = t.subscription_id
         CROSS JOIN LATERAL jsonb_array_elements(r.columns) WITH ORDINALITY
              AS source_column(column_value, ordinality)
        WHERE t.transaction_id = c.transaction_id
          AND r.remote_relation_id = c.remote_relation_id
          AND r.schema_version = c.relation_schema_version
   )
 WHERE jsonb_typeof(c.new_tuple) = 'array';

ALTER TABLE lake_replication.changes
    ADD CONSTRAINT changes_operation_check
        CHECK (operation IN ('insert', 'update', 'delete', 'truncate')),
    ADD CONSTRAINT changes_tuple_shape_check CHECK (
        (operation = 'insert' AND old_tuple IS NULL
                              AND jsonb_typeof(new_tuple) = 'object')
        OR (operation = 'update' AND jsonb_typeof(old_tuple) = 'object'
                                  AND jsonb_typeof(new_tuple) = 'object')
        OR (operation = 'delete' AND jsonb_typeof(old_tuple) = 'object'
                                  AND new_tuple IS NULL)
        OR (operation = 'truncate' AND old_tuple IS NULL AND new_tuple IS NULL)
    );

CREATE OR REPLACE FUNCTION lake_replication.stage_transaction(
    p_subscription_id bigint,
    p_xid bigint,
    p_begin_lsn pg_lsn,
    p_commit_lsn pg_lsn,
    p_end_lsn pg_lsn,
    p_commit_time timestamptz,
    p_changes jsonb
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_transaction_id bigint;
    v_existing transactions%ROWTYPE;
    v_staged_lsn pg_lsn;
BEGIN
    IF jsonb_typeof(p_changes) IS DISTINCT FROM 'array' OR jsonb_array_length(p_changes) = 0 THEN
        RAISE EXCEPTION 'changes must be a non-empty JSON array' USING ERRCODE = '22023';
    END IF;
    IF p_begin_lsn > p_end_lsn OR p_commit_lsn > p_end_lsn THEN
        RAISE EXCEPTION 'invalid transaction LSN interval' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_changes) AS change
         WHERE change->>'operation' NOT IN ('insert', 'update', 'delete', 'truncate')
            OR jsonb_typeof(change->'remote_relation_id') IS DISTINCT FROM 'number'
            OR jsonb_typeof(change->'relation_schema_version') IS DISTINCT FROM 'number'
            OR CASE change->>'operation'
                 WHEN 'insert' THEN change ? 'old_tuple'
                     OR jsonb_typeof(change->'new_tuple') IS DISTINCT FROM 'object'
                 WHEN 'update' THEN jsonb_typeof(change->'old_tuple') IS DISTINCT FROM 'object'
                     OR jsonb_typeof(change->'new_tuple') IS DISTINCT FROM 'object'
                     OR change->'old_tuple' = '{}'::jsonb OR change->'new_tuple' = '{}'::jsonb
                 WHEN 'delete' THEN jsonb_typeof(change->'old_tuple') IS DISTINCT FROM 'object'
                     OR change ? 'new_tuple' OR change->'old_tuple' = '{}'::jsonb
                 WHEN 'truncate' THEN change ? 'old_tuple' OR change ? 'new_tuple'
                 ELSE true
               END
    ) THEN
        RAISE EXCEPTION 'invalid change envelope' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM jsonb_array_elements(p_changes) AS change
          LEFT JOIN relations r
            ON r.subscription_id = p_subscription_id
           AND r.remote_relation_id = (change->>'remote_relation_id')::oid
           AND r.schema_version = (change->>'relation_schema_version')::bigint
         WHERE r.subscription_id IS NULL
            OR EXISTS (
                SELECT 1 FROM jsonb_object_keys(
                    coalesce(change->'old_tuple', '{}'::jsonb) ||
                    coalesce(change->'new_tuple', '{}'::jsonb)
                ) AS supplied(column_name)
                WHERE NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(r.columns) AS expected(column_value)
                     WHERE expected.column_value->>'name' = supplied.column_name
                )
            )
    ) THEN
        RAISE EXCEPTION 'change references an unknown relation version or column'
            USING ERRCODE = '22023';
    END IF;

    SELECT staged_lsn INTO STRICT v_staged_lsn
      FROM checkpoints WHERE subscription_id = p_subscription_id FOR UPDATE;
    SELECT * INTO v_existing FROM transactions
     WHERE subscription_id = p_subscription_id AND commit_lsn = p_commit_lsn;
    IF FOUND THEN
        IF v_existing.xid <> p_xid OR v_existing.begin_lsn <> p_begin_lsn
           OR v_existing.end_lsn <> p_end_lsn OR v_existing.commit_time <> p_commit_time
           OR v_existing.payload <> p_changes THEN
            RAISE EXCEPTION 'conflicting replay at commit LSN %', p_commit_lsn
                USING ERRCODE = '23505';
        END IF;
        RETURN false;
    END IF;
    IF p_end_lsn <= v_staged_lsn THEN
        RAISE EXCEPTION 'new transaction end LSN % does not advance staged LSN %',
            p_end_lsn, v_staged_lsn USING ERRCODE = '22023';
    END IF;

    INSERT INTO transactions (
        subscription_id, xid, begin_lsn, commit_lsn, end_lsn, commit_time, payload
    ) VALUES (
        p_subscription_id, p_xid, p_begin_lsn, p_commit_lsn, p_end_lsn,
        p_commit_time, p_changes
    ) RETURNING transaction_id INTO v_transaction_id;
    INSERT INTO changes (
        transaction_id, change_index, remote_relation_id,
        relation_schema_version, operation, old_tuple, new_tuple
    )
    SELECT v_transaction_id, ordinality::integer - 1,
           (change->>'remote_relation_id')::oid,
           (change->>'relation_schema_version')::bigint,
           change->>'operation', change->'old_tuple', change->'new_tuple'
      FROM jsonb_array_elements(p_changes) WITH ORDINALITY AS item(change, ordinality);
    UPDATE checkpoints
       SET received_lsn = p_end_lsn, staged_lsn = p_end_lsn,
           updated_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id;
    RETURN true;
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
    v_set_clause text;
    v_where_clause text;
    v_rows bigint;
    v_affected_rows bigint := 0;
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
        CASE v_change.operation
        WHEN 'insert' THEN
            EXECUTE format(
                'INSERT INTO %s SELECT (jsonb_populate_record(NULL::%s, $1)).*',
                v_change.destination_relation, v_change.destination_relation
            ) USING v_change.new_tuple;
        WHEN 'update' THEN
            SELECT string_agg(format(
                    '%1$I = (jsonb_populate_record(NULL::%2$s, $1)).%1$I', key,
                    v_change.destination_relation), ', ' ORDER BY key)
              INTO v_set_clause FROM jsonb_object_keys(v_change.new_tuple) AS item(key);
            SELECT string_agg(format(
                    '%1$I IS NOT DISTINCT FROM (jsonb_populate_record(NULL::%2$s, $2)).%1$I',
                    key, v_change.destination_relation), ' AND ' ORDER BY key)
              INTO v_where_clause FROM jsonb_object_keys(v_change.old_tuple) AS item(key);
            EXECUTE format('UPDATE %s SET %s WHERE %s',
                v_change.destination_relation, v_set_clause, v_where_clause)
                USING v_change.new_tuple, v_change.old_tuple;
        WHEN 'delete' THEN
            SELECT string_agg(format(
                    '%1$I IS NOT DISTINCT FROM (jsonb_populate_record(NULL::%2$s, $1)).%1$I',
                    key, v_change.destination_relation), ' AND ' ORDER BY key)
              INTO v_where_clause FROM jsonb_object_keys(v_change.old_tuple) AS item(key);
            EXECUTE format('DELETE FROM %s WHERE %s',
                v_change.destination_relation, v_where_clause) USING v_change.old_tuple;
        WHEN 'truncate' THEN
            EXECUTE format('TRUNCATE %s', v_change.destination_relation);
        END CASE;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_change.operation IN ('update', 'delete') AND v_rows <> 1 THEN
            RAISE EXCEPTION '% for relation % matched % rows instead of exactly one',
                v_change.operation, v_change.remote_relation_id, v_rows
                USING ERRCODE = 'P0002';
        END IF;
        v_affected_rows := v_affected_rows + v_rows;
    END LOOP;
    PERFORM finish_batch(v_batch_id);
    RETURN QUERY SELECT v_batch_id, p_last_end_lsn, v_affected_rows;
END;
$$;

COMMIT;

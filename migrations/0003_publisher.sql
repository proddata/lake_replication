BEGIN;

CREATE OR REPLACE FUNCTION lake_replication.assign_destination(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_destination_relation regclass
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_columns jsonb;
    v_relkind "char";
    v_source_names text[];
    v_destination_names text[];
BEGIN
    SELECT columns INTO STRICT v_columns
      FROM relations
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND schema_version = p_schema_version
     FOR UPDATE;

    SELECT relkind INTO STRICT v_relkind FROM pg_class WHERE oid = p_destination_relation;
    IF v_relkind <> 'f' THEN
        RAISE EXCEPTION 'destination % must be a foreign table', p_destination_relation
            USING ERRCODE = '42809';
    END IF;

    SELECT array_agg(column_value->>'name' ORDER BY ordinality)
      INTO v_source_names
      FROM jsonb_array_elements(v_columns) WITH ORDINALITY
           AS source_column(column_value, ordinality);
    SELECT array_agg(attname::text ORDER BY attnum)
      INTO v_destination_names
      FROM pg_attribute
     WHERE attrelid = p_destination_relation AND attnum > 0 AND NOT attisdropped;
    IF v_source_names IS DISTINCT FROM v_destination_names THEN
        RAISE EXCEPTION 'source columns % do not match destination columns %',
            v_source_names, v_destination_names USING ERRCODE = '42804';
    END IF;

    UPDATE relations SET destination_relation = p_destination_relation
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND schema_version = p_schema_version;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.next_batch(
    p_subscription_id bigint,
    p_max_transactions bigint,
    p_max_rows bigint,
    p_max_bytes bigint
) RETURNS TABLE (
    first_end_lsn pg_lsn,
    last_end_lsn pg_lsn,
    transaction_count bigint,
    row_count numeric,
    byte_count numeric
)
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    IF p_max_transactions <= 0 OR p_max_rows <= 0 OR p_max_bytes <= 0 THEN
        RAISE EXCEPTION 'batch thresholds must be positive' USING ERRCODE = '22023';
    END IF;
    RETURN QUERY
    WITH per_transaction AS (
        SELECT t.transaction_id, t.end_lsn,
               count(c.*)::bigint AS change_count,
               octet_length(t.payload::text)::bigint AS payload_bytes
          FROM transactions t
          JOIN changes c USING (transaction_id)
         WHERE t.subscription_id = p_subscription_id AND t.state = 'staged'
         GROUP BY t.transaction_id, t.end_lsn
    ), running AS (
        SELECT *, row_number() OVER (ORDER BY end_lsn) AS transaction_number,
               sum(change_count) OVER (ORDER BY end_lsn) AS running_rows,
               sum(payload_bytes) OVER (ORDER BY end_lsn) AS running_bytes
          FROM per_transaction
    ), selected AS (
        SELECT * FROM running
         WHERE transaction_number = 1
            OR (transaction_number <= p_max_transactions
                AND running_rows <= p_max_rows
                AND running_bytes <= p_max_bytes)
    )
    SELECT min(end_lsn), max(end_lsn), count(*),
           sum(change_count), sum(payload_bytes)
      FROM selected
    HAVING count(*) > 0;
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
    v_destination regclass;
    v_rows bigint;
    v_inserted_rows bigint := 0;
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
        SELECT 1
          FROM transactions t
          JOIN changes c USING (transaction_id)
          JOIN relations r
            ON r.subscription_id = t.subscription_id
           AND r.remote_relation_id = c.remote_relation_id
           AND r.schema_version = c.relation_schema_version
         WHERE t.batch_id = v_batch_id AND r.destination_relation IS NULL
    ) THEN
        RAISE EXCEPTION 'batch % contains a relation without a destination', v_batch_id
            USING ERRCODE = '55000';
    END IF;

    FOR v_destination IN
        SELECT DISTINCT r.destination_relation
          FROM transactions t
          JOIN changes c USING (transaction_id)
          JOIN relations r
            ON r.subscription_id = t.subscription_id
           AND r.remote_relation_id = c.remote_relation_id
           AND r.schema_version = c.relation_schema_version
         WHERE t.batch_id = v_batch_id
         ORDER BY r.destination_relation
    LOOP
        EXECUTE format(
            'INSERT INTO %s
             SELECT (jsonb_populate_record(NULL::%s, source_rows.row_data)).*
               FROM (
                 SELECT t.end_lsn, c.change_index,
                        jsonb_object_agg(
                            source_column.column_value->>''name'',
                            c.new_tuple -> ((source_column.ordinality - 1)::integer)
                            ORDER BY source_column.ordinality
                        ) AS row_data
                   FROM lake_replication.transactions t
                   JOIN lake_replication.changes c USING (transaction_id)
                   JOIN lake_replication.relations r
                     ON r.subscription_id = t.subscription_id
                    AND r.remote_relation_id = c.remote_relation_id
                    AND r.schema_version = c.relation_schema_version
                  CROSS JOIN LATERAL jsonb_array_elements(r.columns) WITH ORDINALITY
                       AS source_column(column_value, ordinality)
                  WHERE t.batch_id = $1 AND r.destination_relation = $2
                  GROUP BY t.end_lsn, c.change_index
                  ORDER BY t.end_lsn, c.change_index
               ) AS source_rows',
            v_destination, v_destination
        ) USING v_batch_id, v_destination;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_inserted_rows := v_inserted_rows + v_rows;
    END LOOP;

    PERFORM finish_batch(v_batch_id);
    RETURN QUERY SELECT v_batch_id, p_last_end_lsn, v_inserted_rows;
END;
$$;

COMMIT;

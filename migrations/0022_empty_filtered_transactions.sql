BEGIN;

-- A source commit with no published changes must still enter the ordered
-- journal before WAL feedback. The publisher consumes it without target writes.
-- Keep replay checks and checkpoint locking identical to data transactions.

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
    IF jsonb_typeof(p_changes) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'changes must be a JSON array' USING ERRCODE = '22023';
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

CREATE OR REPLACE FUNCTION lake_replication.commit_stream(
    p_subscription_id bigint,
    p_xid bigint,
    p_commit_lsn pg_lsn,
    p_end_lsn pg_lsn,
    p_commit_time timestamptz
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_stream streamed_transactions%ROWTYPE;
    v_changes jsonb;
BEGIN
    SELECT * INTO STRICT v_stream FROM streamed_transactions
     WHERE subscription_id=p_subscription_id AND xid=p_xid FOR UPDATE;
    SELECT jsonb_agg(change ORDER BY change_index) INTO v_changes
      FROM streamed_changes
     WHERE subscription_id=p_subscription_id AND xid=p_xid;
    -- Filtered streamed transactions still carry a durable, ordered commit.
    v_changes := coalesce(v_changes, '[]'::jsonb);
    PERFORM stage_transaction(
        p_subscription_id, p_xid, v_stream.begin_lsn, p_commit_lsn,
        p_end_lsn, p_commit_time, v_changes
    );
    DELETE FROM streamed_transactions
     WHERE subscription_id=p_subscription_id AND xid=p_xid;
    RETURN jsonb_array_length(v_changes);
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
               count(c.transaction_id)::bigint AS change_count,
               octet_length(t.payload::text)::bigint AS payload_bytes
          FROM transactions t
          -- Keep filtered commits in the same ordered batch as data commits.
          LEFT JOIN changes c USING (transaction_id)
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

COMMIT;

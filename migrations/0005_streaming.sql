BEGIN;

CREATE TABLE lake_replication.streamed_transactions (
    subscription_id bigint NOT NULL REFERENCES lake_replication.subscriptions ON DELETE CASCADE,
    xid bigint NOT NULL CHECK (xid BETWEEN 0 AND 4294967295),
    begin_lsn pg_lsn NOT NULL,
    next_change_index bigint NOT NULL DEFAULT 0 CHECK (next_change_index >= 0),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (subscription_id, xid)
);

CREATE TABLE lake_replication.streamed_changes (
    subscription_id bigint NOT NULL,
    xid bigint NOT NULL,
    change_index bigint NOT NULL CHECK (change_index >= 0),
    change jsonb NOT NULL CHECK (jsonb_typeof(change) = 'object'),
    PRIMARY KEY (subscription_id, xid, change_index),
    FOREIGN KEY (subscription_id, xid)
        REFERENCES lake_replication.streamed_transactions ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION lake_replication.begin_stream(
    p_subscription_id bigint, p_xid bigint, p_begin_lsn pg_lsn
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    DELETE FROM streamed_transactions
     WHERE subscription_id=p_subscription_id AND xid=p_xid;
    INSERT INTO streamed_transactions(subscription_id, xid, begin_lsn)
    VALUES (p_subscription_id, p_xid, p_begin_lsn);
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.append_stream_segment(
    p_subscription_id bigint, p_xid bigint, p_changes jsonb
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_next bigint;
    v_count bigint;
BEGIN
    IF jsonb_typeof(p_changes) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'stream segment must be a JSON array' USING ERRCODE='22023';
    END IF;
    v_count := jsonb_array_length(p_changes);
    SELECT next_change_index INTO STRICT v_next
      FROM streamed_transactions
     WHERE subscription_id=p_subscription_id AND xid=p_xid FOR UPDATE;
    INSERT INTO streamed_changes(subscription_id, xid, change_index, change)
    SELECT p_subscription_id, p_xid, v_next + ordinality - 1, change
      FROM jsonb_array_elements(p_changes) WITH ORDINALITY AS item(change, ordinality);
    UPDATE streamed_transactions SET next_change_index = v_next + v_count
     WHERE subscription_id=p_subscription_id AND xid=p_xid;
    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.abort_stream(
    p_subscription_id bigint, p_xid bigint
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_count bigint;
BEGIN
    SELECT next_change_index INTO v_count FROM streamed_transactions
     WHERE subscription_id=p_subscription_id AND xid=p_xid;
    DELETE FROM streamed_transactions
     WHERE subscription_id=p_subscription_id AND xid=p_xid;
    RETURN coalesce(v_count, 0);
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
    IF v_changes IS NULL OR jsonb_array_length(v_changes) = 0 THEN
        RAISE EXCEPTION 'streamed transaction % has no changes', p_xid
            USING ERRCODE='22023';
    END IF;
    PERFORM stage_transaction(
        p_subscription_id, p_xid, v_stream.begin_lsn, p_commit_lsn,
        p_end_lsn, p_commit_time, v_changes
    );
    DELETE FROM streamed_transactions
     WHERE subscription_id=p_subscription_id AND xid=p_xid;
    RETURN jsonb_array_length(v_changes);
END;
$$;

COMMIT;

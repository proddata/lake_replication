BEGIN;

-- The supervisor needs to know whether the persistent source slot was
-- supplied by the caller when it restarts an interrupted initial copy.
ALTER TABLE lake_replication.worker_launch_config
    ADD COLUMN create_slot boolean NOT NULL DEFAULT true;

-- An exported snapshot cannot survive a worker or server restart. Discard
-- incomplete staging before acquiring a fresh snapshot. The checkpoint and
-- ready-copy updates in finish_initial_sync_group are atomic, so a nonzero
-- checkpoint means this subscription must never run its initial copy again.
CREATE FUNCTION lake_replication.reset_incomplete_initial_sync(
    p_subscription_id bigint
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_checkpoint checkpoints%ROWTYPE;
    v_removed bigint;
BEGIN
    SELECT * INTO STRICT v_checkpoint FROM checkpoints
     WHERE subscription_id = p_subscription_id FOR UPDATE;
    IF v_checkpoint.received_lsn <> '0/0'
       OR v_checkpoint.staged_lsn <> '0/0'
       OR v_checkpoint.published_lsn <> '0/0'
       OR v_checkpoint.acknowledged_lsn <> '0/0' THEN
        RAISE EXCEPTION 'initial copy cannot restart after the checkpoint advanced'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (
        SELECT 1 FROM subscriptions
         WHERE subscription_id = p_subscription_id AND state = 'streaming'
    ) THEN
        RAISE EXCEPTION 'streaming subscription cannot restart initial copy'
            USING ERRCODE = '55000';
    END IF;

    UPDATE relations SET state = 'ready'
     WHERE subscription_id = p_subscription_id AND state IN ('copying', 'error');
    DELETE FROM initial_copies WHERE subscription_id = p_subscription_id;
    GET DIAGNOSTICS v_removed = ROW_COUNT;
    UPDATE subscriptions SET state = 'disabled', last_error = NULL
     WHERE subscription_id = p_subscription_id;
    RETURN v_removed;
END;
$$;

COMMIT;

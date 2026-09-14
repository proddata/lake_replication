BEGIN;

ALTER TABLE lake_replication.service_status
    ADD COLUMN reconnections bigint NOT NULL DEFAULT 0 CHECK (reconnections >= 0),
    ADD COLUMN last_reconnected_at timestamptz,
    ADD COLUMN last_connection_error text;

CREATE OR REPLACE FUNCTION lake_replication.report_service_reconnected(
    p_subscription_id bigint,
    p_error text
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    IF p_error = '' THEN
        RAISE EXCEPTION 'connection error must not be empty' USING ERRCODE='22023';
    END IF;
    UPDATE service_status
       SET reconnections=reconnections+1,
           last_reconnected_at=clock_timestamp(),
           last_connection_error=p_error,
           heartbeat_at=clock_timestamp()
     WHERE subscription_id=p_subscription_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'service status for subscription % does not exist', p_subscription_id
            USING ERRCODE='P0002';
    END IF;
END;
$$;

CREATE OR REPLACE VIEW lake_replication.service_health AS
SELECT s.subscription_id, s.subscription_name, s.state AS subscription_state,
       ss.desired_state, ss.state AS service_state, ss.supervisor_pid,
       ss.started_at, ss.heartbeat_at, ss.consecutive_failures, ss.total_failures,
       ss.receiver_cycles, ss.publisher_cycles, ss.last_error, ss.next_retry_at,
       ss.stopped_at, c.received_lsn, c.staged_lsn, c.published_lsn,
       c.acknowledged_lsn,
       (SELECT count(*) FROM lake_replication.transactions t
         WHERE t.subscription_id=s.subscription_id AND t.state='staged') AS staged_transactions,
       (SELECT count(*) FROM lake_replication.changes ch
          JOIN lake_replication.transactions t USING (transaction_id)
         WHERE t.subscription_id=s.subscription_id AND t.state='staged') AS staged_changes,
       (SELECT count(*) FROM lake_replication.streamed_transactions st
         WHERE st.subscription_id=s.subscription_id) AS streamed_transactions,
       ss.reconnections, ss.last_reconnected_at, ss.last_connection_error
  FROM lake_replication.subscriptions s
  JOIN lake_replication.service_status ss USING (subscription_id)
  JOIN lake_replication.checkpoints c USING (subscription_id);

COMMIT;

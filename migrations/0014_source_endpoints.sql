BEGIN;

ALTER TABLE lake_replication.service_status
    ADD COLUMN active_source_address text,
    ADD COLUMN source_switches bigint NOT NULL DEFAULT 0 CHECK (source_switches >= 0),
    ADD COLUMN last_source_switch_at timestamptz,
    ADD COLUMN last_source_candidate_errors text;

DROP FUNCTION lake_replication.report_source_healthy(
    bigint, numeric, oid, bigint, pg_lsn
);

CREATE FUNCTION lake_replication.report_source_healthy(
    p_subscription_id bigint,
    p_system_identifier numeric,
    p_database_oid oid,
    p_timeline bigint,
    p_confirmed_lsn pg_lsn,
    p_source_address text,
    p_candidate_errors text
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_previous_address text;
BEGIN
    IF p_source_address = '' THEN
        RAISE EXCEPTION 'source address must not be empty' USING ERRCODE='22023';
    END IF;
    UPDATE sources src
       SET timeline=GREATEST(src.timeline, p_timeline)
      FROM subscriptions sub
     WHERE sub.subscription_id=p_subscription_id
       AND sub.source_id=src.source_id
       AND src.system_identifier=p_system_identifier
       AND src.database_oid=p_database_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'observed source identity does not match subscription %',
            p_subscription_id USING ERRCODE='55000';
    END IF;
    SELECT active_source_address INTO STRICT v_previous_address
      FROM service_status WHERE subscription_id=p_subscription_id FOR UPDATE;
    UPDATE service_status
       SET source_checked_at=clock_timestamp(),
           observed_system_identifier=p_system_identifier,
           observed_database_oid=p_database_oid,
           observed_timeline=p_timeline,
           observed_confirmed_lsn=p_confirmed_lsn,
           last_source_error=NULL,
           active_source_address=p_source_address,
           source_switches=source_switches +
               CASE WHEN v_previous_address IS NOT NULL
                          AND v_previous_address IS DISTINCT FROM p_source_address
                    THEN 1 ELSE 0 END,
           last_source_switch_at=
               CASE WHEN v_previous_address IS NOT NULL
                          AND v_previous_address IS DISTINCT FROM p_source_address
                    THEN clock_timestamp() ELSE last_source_switch_at END,
           last_source_candidate_errors=NULLIF(p_candidate_errors, ''),
           heartbeat_at=clock_timestamp()
     WHERE subscription_id=p_subscription_id;
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
       ss.reconnections, ss.last_reconnected_at, ss.last_connection_error,
       ss.source_checked_at, ss.observed_system_identifier,
       ss.observed_database_oid, ss.observed_timeline,
       ss.observed_confirmed_lsn, ss.source_check_failures, ss.last_source_error,
       ss.active_source_address, ss.source_switches, ss.last_source_switch_at,
       ss.last_source_candidate_errors
  FROM lake_replication.subscriptions s
  JOIN lake_replication.service_status ss USING (subscription_id)
  JOIN lake_replication.checkpoints c USING (subscription_id);

COMMIT;

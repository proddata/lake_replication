BEGIN;

ALTER TABLE lake_replication.service_status
    ADD COLUMN source_checked_at timestamptz,
    ADD COLUMN observed_system_identifier numeric(20, 0),
    ADD COLUMN observed_database_oid oid,
    ADD COLUMN observed_timeline bigint,
    ADD COLUMN observed_confirmed_lsn pg_lsn,
    ADD COLUMN source_check_failures bigint NOT NULL DEFAULT 0
        CHECK (source_check_failures >= 0),
    ADD COLUMN last_source_error text;

CREATE OR REPLACE FUNCTION lake_replication.report_source_healthy(
    p_subscription_id bigint,
    p_system_identifier numeric,
    p_database_oid oid,
    p_timeline bigint,
    p_confirmed_lsn pg_lsn
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
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
    UPDATE service_status
       SET source_checked_at=clock_timestamp(),
           observed_system_identifier=p_system_identifier,
           observed_database_oid=p_database_oid,
           observed_timeline=p_timeline,
           observed_confirmed_lsn=p_confirmed_lsn,
           last_source_error=NULL,
           heartbeat_at=clock_timestamp()
     WHERE subscription_id=p_subscription_id;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.report_source_error(
    p_subscription_id bigint,
    p_error text
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    IF p_error = '' THEN
        RAISE EXCEPTION 'source error must not be empty' USING ERRCODE='22023';
    END IF;
    UPDATE subscriptions
       SET state='error', last_error=p_error
     WHERE subscription_id=p_subscription_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'subscription % does not exist', p_subscription_id
            USING ERRCODE='P0002';
    END IF;
    UPDATE service_status
       SET state='error', heartbeat_at=clock_timestamp(),
           source_check_failures=source_check_failures+1,
           last_source_error=p_error, next_retry_at=NULL
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
       ss.observed_confirmed_lsn, ss.source_check_failures, ss.last_source_error
  FROM lake_replication.subscriptions s
  JOIN lake_replication.service_status ss USING (subscription_id)
  JOIN lake_replication.checkpoints c USING (subscription_id);

COMMIT;

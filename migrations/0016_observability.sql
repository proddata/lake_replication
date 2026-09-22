BEGIN;

ALTER TABLE lake_replication.service_status
    ADD COLUMN observed_current_lsn pg_lsn,
    ADD COLUMN last_failure_at timestamptz;

CREATE OR REPLACE FUNCTION lake_replication.report_service_failure(
    p_subscription_id bigint,
    p_error text,
    p_retry_milliseconds bigint
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    IF p_error = '' OR p_retry_milliseconds < 0 THEN
        RAISE EXCEPTION 'invalid service failure report' USING ERRCODE='22023';
    END IF;
    UPDATE service_status
       SET state='backoff', heartbeat_at=clock_timestamp(),
           consecutive_failures=consecutive_failures+1,
           total_failures=total_failures+1, last_error=p_error,
           last_failure_at=clock_timestamp(),
           next_retry_at=clock_timestamp() + p_retry_milliseconds * INTERVAL '1 millisecond'
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
           last_source_error=p_error, last_failure_at=clock_timestamp(),
           next_retry_at=NULL
     WHERE subscription_id=p_subscription_id;
END;
$$;

DROP FUNCTION lake_replication.report_source_healthy(
    bigint, numeric, oid, bigint, pg_lsn, text, text
);

CREATE FUNCTION lake_replication.report_source_healthy(
    p_subscription_id bigint,
    p_system_identifier numeric,
    p_database_oid oid,
    p_timeline bigint,
    p_confirmed_lsn pg_lsn,
    p_current_lsn pg_lsn,
    p_source_address text,
    p_candidate_errors text
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_previous_address text;
BEGIN
    IF p_source_address = '' OR p_current_lsn < p_confirmed_lsn THEN
        RAISE EXCEPTION 'invalid source health report' USING ERRCODE='22023';
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
           observed_current_lsn=p_current_lsn,
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
       ss.last_source_candidate_errors, ss.observed_current_lsn,
       ss.last_failure_at
  FROM lake_replication.subscriptions s
  JOIN lake_replication.service_status ss USING (subscription_id)
  JOIN lake_replication.checkpoints c USING (subscription_id);

CREATE VIEW lake_replication.subscription_status AS
SELECT h.*,
       extract(epoch FROM clock_timestamp() - h.heartbeat_at)::double precision
           AS heartbeat_age_seconds,
       GREATEST(pg_wal_lsn_diff(h.received_lsn, h.published_lsn), 0)::bigint
           AS journal_lag_bytes,
       CASE WHEN h.observed_current_lsn IS NULL THEN NULL
            ELSE GREATEST(
                pg_wal_lsn_diff(h.observed_current_lsn, h.published_lsn), 0
            )::bigint
       END AS source_lag_bytes,
       CASE
           WHEN h.desired_state = 'stopped' THEN 'stopped'
           WHEN h.subscription_state = 'error' OR h.service_state = 'error'
               THEN 'critical'
           WHEN h.heartbeat_at < clock_timestamp() - INTERVAL '30 seconds'
               THEN 'critical'
           WHEN h.subscription_state <> 'streaming'
                OR h.service_state IN ('starting', 'paused', 'backoff')
                OR h.consecutive_failures > 0
               THEN 'degraded'
           ELSE 'healthy'
       END AS health_state,
       CASE
           WHEN h.desired_state = 'stopped' THEN 'operator requested stop'
           WHEN h.subscription_state = 'error' THEN
               coalesce(h.last_source_error, h.last_error, 'subscription error')
           WHEN h.service_state = 'error' THEN
               coalesce(h.last_source_error, h.last_error, 'service error')
           WHEN h.heartbeat_at < clock_timestamp() - INTERVAL '30 seconds'
               THEN 'heartbeat is older than 30 seconds'
           WHEN h.service_state = 'backoff' THEN
               coalesce(h.last_error, 'service is retrying')
           WHEN h.subscription_state = 'paused' OR h.service_state = 'paused'
               THEN 'replication is paused'
           WHEN h.subscription_state <> 'streaming' OR h.service_state = 'starting'
               THEN 'replication is starting'
           WHEN h.consecutive_failures > 0 THEN 'service has consecutive failures'
           ELSE NULL
       END AS health_reason
  FROM lake_replication.service_health h;

COMMENT ON VIEW lake_replication.subscription_status IS
    'Operator status contract: service health, heartbeat age, queue/source lag, '
    'retry state, cycle counts, and the most recent errors per subscription.';

CREATE VIEW lake_replication.metrics AS
SELECT s.subscription_id,
       s.subscription_name,
       metric.name AS metric,
       metric.value::double precision AS value
  FROM lake_replication.subscription_status s
 CROSS JOIN LATERAL (
    VALUES
      ('lake_replication_subscription_healthy',
       CASE WHEN s.health_state = 'healthy' THEN 1::numeric ELSE 0::numeric END),
      ('lake_replication_heartbeat_age_seconds', s.heartbeat_age_seconds::numeric),
      ('lake_replication_source_lag_bytes', s.source_lag_bytes::numeric),
      ('lake_replication_journal_lag_bytes', s.journal_lag_bytes::numeric),
      ('lake_replication_consecutive_failures', s.consecutive_failures::numeric),
      ('lake_replication_failures_total', s.total_failures::numeric),
      ('lake_replication_receiver_cycles_total', s.receiver_cycles::numeric),
      ('lake_replication_publisher_cycles_total', s.publisher_cycles::numeric),
      ('lake_replication_staged_transactions', s.staged_transactions::numeric),
      ('lake_replication_staged_changes', s.staged_changes::numeric),
      ('lake_replication_reconnections_total', s.reconnections::numeric),
      ('lake_replication_source_switches_total', s.source_switches::numeric)
 ) AS metric(name, value)
 WHERE metric.value IS NOT NULL;

COMMENT ON VIEW lake_replication.metrics IS
    'Long-form numeric metrics for SQL monitoring and PostgreSQL exporters; '
    'subscription_id and subscription_name are metric labels.';

COMMIT;

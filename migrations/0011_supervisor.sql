BEGIN;

CREATE TABLE lake_replication.service_status (
    subscription_id bigint PRIMARY KEY
        REFERENCES lake_replication.subscriptions ON DELETE CASCADE,
    desired_state text NOT NULL DEFAULT 'running'
        CHECK (desired_state IN ('running', 'stopped')),
    state text NOT NULL DEFAULT 'starting'
        CHECK (state IN ('starting', 'running', 'backoff', 'paused', 'stopped', 'error')),
    supervisor_pid bigint,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    consecutive_failures bigint NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    total_failures bigint NOT NULL DEFAULT 0 CHECK (total_failures >= 0),
    receiver_cycles bigint NOT NULL DEFAULT 0 CHECK (receiver_cycles >= 0),
    publisher_cycles bigint NOT NULL DEFAULT 0 CHECK (publisher_cycles >= 0),
    last_error text,
    next_retry_at timestamptz,
    stopped_at timestamptz
);

CREATE OR REPLACE FUNCTION lake_replication.start_service(
    p_subscription_id bigint,
    p_supervisor_pid bigint
) RETURNS void
LANGUAGE sql
SET search_path = pg_catalog, lake_replication
AS $$
    INSERT INTO service_status (subscription_id, desired_state, state, supervisor_pid)
    VALUES (p_subscription_id, 'running', 'starting', p_supervisor_pid)
    ON CONFLICT (subscription_id) DO UPDATE
       SET desired_state='running', state='starting', supervisor_pid=EXCLUDED.supervisor_pid,
           started_at=clock_timestamp(), heartbeat_at=clock_timestamp(),
           consecutive_failures=0, next_retry_at=NULL, stopped_at=NULL
$$;

CREATE OR REPLACE FUNCTION lake_replication.request_service_stop(
    p_subscription_id bigint
) RETURNS void
LANGUAGE sql
SET search_path = pg_catalog, lake_replication
AS $$
    UPDATE service_status
       SET desired_state='stopped', heartbeat_at=clock_timestamp()
     WHERE subscription_id=p_subscription_id
$$;

CREATE OR REPLACE FUNCTION lake_replication.report_service_cycle(
    p_subscription_id bigint,
    p_state text,
    p_receiver_increment bigint,
    p_publisher_increment bigint
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    IF p_state NOT IN ('running', 'paused')
       OR p_receiver_increment < 0 OR p_publisher_increment < 0 THEN
        RAISE EXCEPTION 'invalid service cycle report' USING ERRCODE='22023';
    END IF;
    UPDATE service_status
       SET state=p_state, heartbeat_at=clock_timestamp(), consecutive_failures=0,
           receiver_cycles=receiver_cycles+p_receiver_increment,
           publisher_cycles=publisher_cycles+p_publisher_increment,
           next_retry_at=NULL
     WHERE subscription_id=p_subscription_id;
END;
$$;

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
           next_retry_at=clock_timestamp() + p_retry_milliseconds * INTERVAL '1 millisecond'
     WHERE subscription_id=p_subscription_id;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.report_service_stopped(
    p_subscription_id bigint
) RETURNS void
LANGUAGE sql
SET search_path = pg_catalog, lake_replication
AS $$
    UPDATE service_status
       SET desired_state='stopped', state='stopped', supervisor_pid=NULL,
           heartbeat_at=clock_timestamp(), next_retry_at=NULL,
           stopped_at=clock_timestamp()
     WHERE subscription_id=p_subscription_id
$$;

CREATE VIEW lake_replication.service_health AS
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
         WHERE st.subscription_id=s.subscription_id) AS streamed_transactions
  FROM lake_replication.subscriptions s
  JOIN lake_replication.service_status ss USING (subscription_id)
  JOIN lake_replication.checkpoints c USING (subscription_id);

COMMIT;

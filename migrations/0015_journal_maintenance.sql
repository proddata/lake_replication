BEGIN;

CREATE TABLE IF NOT EXISTS lake_replication.journal_maintenance_policies (
    subscription_id bigint PRIMARY KEY
        REFERENCES lake_replication.subscriptions ON DELETE CASCADE,
    enabled boolean NOT NULL DEFAULT true,
    retention_interval interval NOT NULL DEFAULT INTERVAL '1 day'
        CHECK (retention_interval >= INTERVAL '0'),
    cleanup_interval interval NOT NULL DEFAULT INTERVAL '5 minutes'
        CHECK (cleanup_interval >= INTERVAL '1 second'),
    max_transactions_per_run integer NOT NULL DEFAULT 1000
        CHECK (max_transactions_per_run BETWEEN 1 AND 1000000),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS lake_replication.journal_maintenance_status (
    subscription_id bigint PRIMARY KEY
        REFERENCES lake_replication.subscriptions ON DELETE CASCADE,
    runs bigint NOT NULL DEFAULT 0 CHECK (runs >= 0),
    last_started_at timestamptz,
    last_completed_at timestamptz,
    last_safe_cleanup_lsn pg_lsn,
    last_transactions_deleted bigint NOT NULL DEFAULT 0
        CHECK (last_transactions_deleted >= 0),
    last_changes_deleted bigint NOT NULL DEFAULT 0
        CHECK (last_changes_deleted >= 0),
    last_batches_deleted bigint NOT NULL DEFAULT 0
        CHECK (last_batches_deleted >= 0),
    last_bytes_deleted bigint NOT NULL DEFAULT 0
        CHECK (last_bytes_deleted >= 0),
    total_transactions_deleted bigint NOT NULL DEFAULT 0
        CHECK (total_transactions_deleted >= 0),
    total_changes_deleted bigint NOT NULL DEFAULT 0
        CHECK (total_changes_deleted >= 0),
    total_batches_deleted bigint NOT NULL DEFAULT 0
        CHECK (total_batches_deleted >= 0),
    total_bytes_deleted bigint NOT NULL DEFAULT 0
        CHECK (total_bytes_deleted >= 0)
);

ALTER TABLE lake_replication.journal_maintenance_status
    ADD COLUMN IF NOT EXISTS failures bigint NOT NULL DEFAULT 0
        CHECK (failures >= 0),
    ADD COLUMN IF NOT EXISTS last_error text;

CREATE OR REPLACE FUNCTION lake_replication.initialize_journal_maintenance()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    INSERT INTO journal_maintenance_policies (subscription_id)
    VALUES (NEW.subscription_id);
    INSERT INTO journal_maintenance_status (subscription_id)
    VALUES (NEW.subscription_id);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS subscriptions_initialize_journal_maintenance
    ON lake_replication.subscriptions;
CREATE TRIGGER subscriptions_initialize_journal_maintenance
AFTER INSERT ON lake_replication.subscriptions
FOR EACH ROW EXECUTE FUNCTION lake_replication.initialize_journal_maintenance();

INSERT INTO lake_replication.journal_maintenance_policies (subscription_id)
SELECT subscription_id FROM lake_replication.subscriptions
ON CONFLICT (subscription_id) DO NOTHING;

INSERT INTO lake_replication.journal_maintenance_status (subscription_id)
SELECT subscription_id FROM lake_replication.subscriptions
ON CONFLICT (subscription_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS transactions_published_cleanup_idx
    ON lake_replication.transactions (subscription_id, published_at, end_lsn)
    WHERE state = 'published';

CREATE INDEX IF NOT EXISTS batches_published_cleanup_idx
    ON lake_replication.batches (subscription_id, published_at, last_end_lsn)
    WHERE state = 'published';

CREATE OR REPLACE FUNCTION lake_replication.configure_journal_maintenance(
    p_subscription_id bigint,
    p_enabled boolean,
    p_retention_interval interval,
    p_cleanup_interval interval,
    p_max_transactions_per_run integer
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    IF p_enabled IS NULL
       OR p_retention_interval IS NULL
       OR p_retention_interval < INTERVAL '0'
       OR p_cleanup_interval IS NULL
       OR p_cleanup_interval < INTERVAL '1 second'
       OR p_max_transactions_per_run IS NULL
       OR p_max_transactions_per_run NOT BETWEEN 1 AND 1000000 THEN
        RAISE EXCEPTION 'invalid journal maintenance policy'
            USING ERRCODE = '22023';
    END IF;

    INSERT INTO journal_maintenance_policies (
        subscription_id, enabled, retention_interval, cleanup_interval,
        max_transactions_per_run
    ) VALUES (
        p_subscription_id, p_enabled, p_retention_interval, p_cleanup_interval,
        p_max_transactions_per_run
    )
    ON CONFLICT (subscription_id) DO UPDATE
       SET enabled = EXCLUDED.enabled,
           retention_interval = EXCLUDED.retention_interval,
           cleanup_interval = EXCLUDED.cleanup_interval,
           max_transactions_per_run = EXCLUDED.max_transactions_per_run,
           updated_at = clock_timestamp();

    INSERT INTO journal_maintenance_status (subscription_id)
    VALUES (p_subscription_id)
    ON CONFLICT (subscription_id) DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.run_journal_maintenance(
    p_subscription_id bigint,
    p_force boolean DEFAULT false
) RETURNS TABLE (
    did_run boolean,
    safe_cleanup_lsn pg_lsn,
    transactions_deleted bigint,
    changes_deleted bigint,
    batches_deleted bigint,
    bytes_deleted bigint
)
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_policy journal_maintenance_policies%ROWTYPE;
    v_status journal_maintenance_status%ROWTYPE;
    v_checkpoint checkpoints%ROWTYPE;
    v_transaction_ids bigint[];
    v_transaction_bytes bigint := 0;
    v_change_bytes bigint := 0;
    v_cutoff timestamptz;
BEGIN
    IF p_force IS NULL THEN
        RAISE EXCEPTION 'force flag must not be null' USING ERRCODE = '22023';
    END IF;

    SELECT * INTO STRICT v_policy
      FROM journal_maintenance_policies
     WHERE subscription_id = p_subscription_id
     FOR UPDATE;
    SELECT * INTO STRICT v_status
      FROM journal_maintenance_status
     WHERE subscription_id = p_subscription_id
     FOR UPDATE;
    SELECT * INTO STRICT v_checkpoint
      FROM checkpoints
     WHERE subscription_id = p_subscription_id
     FOR UPDATE;

    safe_cleanup_lsn := LEAST(
        v_checkpoint.published_lsn, v_checkpoint.acknowledged_lsn
    );
    did_run := false;
    transactions_deleted := 0;
    changes_deleted := 0;
    batches_deleted := 0;
    bytes_deleted := 0;

    IF NOT v_policy.enabled
       OR (NOT p_force
           AND v_status.last_completed_at IS NOT NULL
           AND clock_timestamp() <
               v_status.last_completed_at + v_policy.cleanup_interval) THEN
        RETURN NEXT;
        RETURN;
    END IF;

    did_run := true;
    v_cutoff := clock_timestamp() - v_policy.retention_interval;
    UPDATE journal_maintenance_status
       SET last_started_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id;

    SELECT array_agg(eligible.transaction_id ORDER BY eligible.end_lsn),
           count(*)::bigint,
           coalesce(sum(pg_column_size(eligible.payload)), 0)::bigint
      INTO v_transaction_ids, transactions_deleted, v_transaction_bytes
      FROM (
          SELECT t.transaction_id, t.end_lsn, t.payload
            FROM transactions t
           WHERE t.subscription_id = p_subscription_id
             AND t.state = 'published'
             AND t.end_lsn <= safe_cleanup_lsn
             AND t.published_at <= v_cutoff
           ORDER BY t.end_lsn, t.transaction_id
           LIMIT v_policy.max_transactions_per_run
      ) AS eligible;

    IF v_transaction_ids IS NOT NULL THEN
        SELECT count(*)::bigint,
               coalesce(sum(
                   coalesce(pg_column_size(c.old_tuple), 0) +
                   coalesce(pg_column_size(c.new_tuple), 0)
               ), 0)::bigint
          INTO changes_deleted, v_change_bytes
          FROM changes c
         WHERE c.transaction_id = ANY (v_transaction_ids);

        DELETE FROM transactions t
         WHERE t.transaction_id = ANY (v_transaction_ids);
    END IF;

    WITH eligible AS (
        SELECT b.batch_id
          FROM batches b
         WHERE b.subscription_id = p_subscription_id
           AND b.state = 'published'
           AND b.last_end_lsn <= safe_cleanup_lsn
           AND b.published_at <= v_cutoff
           AND NOT EXISTS (
               SELECT 1 FROM transactions t WHERE t.batch_id = b.batch_id
           )
         ORDER BY b.last_end_lsn, b.batch_id
         LIMIT v_policy.max_transactions_per_run
    ), removed AS (
        DELETE FROM batches b
         USING eligible e
         WHERE b.batch_id = e.batch_id
         RETURNING b.batch_id
    )
    SELECT count(*)::bigint INTO batches_deleted FROM removed;

    bytes_deleted := v_transaction_bytes + v_change_bytes;
    UPDATE journal_maintenance_status
       SET runs = runs + 1,
           last_completed_at = clock_timestamp(),
           last_safe_cleanup_lsn = safe_cleanup_lsn,
           last_transactions_deleted = transactions_deleted,
           last_changes_deleted = changes_deleted,
           last_batches_deleted = batches_deleted,
           last_bytes_deleted = bytes_deleted,
           total_transactions_deleted =
               total_transactions_deleted + transactions_deleted,
           total_changes_deleted = total_changes_deleted + changes_deleted,
           total_batches_deleted = total_batches_deleted + batches_deleted,
           total_bytes_deleted = total_bytes_deleted + bytes_deleted,
           last_error = NULL
     WHERE subscription_id = p_subscription_id;

    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.report_journal_maintenance_failure(
    p_subscription_id bigint,
    p_error text
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    IF p_error IS NULL OR p_error = '' THEN
        RAISE EXCEPTION 'maintenance error must not be empty'
            USING ERRCODE = '22023';
    END IF;
    UPDATE journal_maintenance_status
       SET failures = failures + 1,
           last_error = p_error
     WHERE subscription_id = p_subscription_id;
END;
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

    BEGIN
        PERFORM * FROM run_journal_maintenance(p_subscription_id, false);
    EXCEPTION WHEN OTHERS THEN
        PERFORM report_journal_maintenance_failure(
            p_subscription_id, SQLSTATE || ': ' || SQLERRM
        );
    END;
END;
$$;

CREATE OR REPLACE VIEW lake_replication.journal_maintenance_health AS
SELECT s.subscription_id, s.subscription_name,
       p.enabled, p.retention_interval, p.cleanup_interval,
       p.max_transactions_per_run,
       LEAST(c.published_lsn, c.acknowledged_lsn) AS safe_cleanup_lsn,
       counts.staged_transactions, counts.publishing_transactions,
       counts.published_transactions, counts.retained_changes,
       counts.retained_bytes, counts.oldest_published_at,
       st.runs, st.last_started_at, st.last_completed_at,
       st.last_safe_cleanup_lsn, st.last_transactions_deleted,
       st.last_changes_deleted, st.last_batches_deleted,
       st.last_bytes_deleted, st.total_transactions_deleted,
       st.total_changes_deleted, st.total_batches_deleted,
       st.total_bytes_deleted, st.failures, st.last_error
  FROM lake_replication.subscriptions s
  JOIN lake_replication.checkpoints c USING (subscription_id)
  JOIN lake_replication.journal_maintenance_policies p USING (subscription_id)
  JOIN lake_replication.journal_maintenance_status st USING (subscription_id)
  CROSS JOIN LATERAL (
      SELECT count(*) FILTER (WHERE t.state = 'staged') AS staged_transactions,
             count(*) FILTER (WHERE t.state = 'publishing') AS publishing_transactions,
             count(*) FILTER (WHERE t.state = 'published') AS published_transactions,
             (SELECT count(*)
                FROM lake_replication.changes ch
                JOIN lake_replication.transactions tx USING (transaction_id)
               WHERE tx.subscription_id = s.subscription_id) AS retained_changes,
             coalesce(sum(pg_column_size(t.payload)), 0)::bigint +
             coalesce((SELECT sum(
                            coalesce(pg_column_size(ch.old_tuple), 0) +
                            coalesce(pg_column_size(ch.new_tuple), 0)
                       )
                       FROM lake_replication.changes ch
                       JOIN lake_replication.transactions tx USING (transaction_id)
                      WHERE tx.subscription_id = s.subscription_id), 0)::bigint
                 AS retained_bytes,
             min(t.published_at) FILTER (WHERE t.state = 'published')
                 AS oldest_published_at
        FROM lake_replication.transactions t
       WHERE t.subscription_id = s.subscription_id
  ) AS counts;

COMMIT;

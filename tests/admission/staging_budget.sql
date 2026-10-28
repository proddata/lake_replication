-- Disposable diagnostic coordinator, not an extension migration.
CREATE SCHEMA staging_budget;
REVOKE ALL ON SCHEMA staging_budget FROM PUBLIC;
CREATE TABLE staging_budget.settings (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    rate_bytes_per_second bigint NOT NULL CHECK (rate_bytes_per_second >= 0),
    burst_bytes bigint NOT NULL CHECK (burst_bytes > 0),
    max_wait_seconds double precision NOT NULL CHECK (max_wait_seconds > 0)
);
INSERT INTO staging_budget.settings VALUES (true, __RATE_BYTES__, __BURST_BYTES__, 2.0);
-- Credit is ephemeral resource state. A database crash resets it to one burst;
-- source rows/checkpoints remain governed by the unchanged production SQL.
CREATE UNLOGGED TABLE staging_budget.credit (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    available_bytes double precision NOT NULL,
    refilled_at timestamptz NOT NULL,
    first_reserved_at timestamptz NOT NULL,
    reserved_bytes bigint NOT NULL DEFAULT 0,
    requests bigint NOT NULL DEFAULT 0,
    sum_wait_seconds double precision NOT NULL DEFAULT 0,
    max_wait_seconds double precision NOT NULL DEFAULT 0
);
CREATE FUNCTION staging_budget.reserve_at(p_bytes bigint, p_now timestamptz)
RETURNS double precision LANGUAGE plpgsql
SET search_path=pg_catalog,staging_budget AS $$
DECLARE
    config settings%ROWTYPE;
    bucket credit%ROWTYPE;
    tokens double precision;
    delay double precision;
BEGIN
    SELECT * INTO STRICT config FROM settings;
    IF p_bytes <= 0 OR p_bytes > config.burst_bytes OR p_now IS NULL THEN
        RAISE EXCEPTION 'request must fit the positive diagnostic burst size'
            USING ERRCODE='22023';
    END IF;
    INSERT INTO credit(singleton,available_bytes,refilled_at,first_reserved_at)
        VALUES(true,config.burst_bytes,p_now,p_now) ON CONFLICT DO NOTHING;
    SELECT * INTO STRICT bucket FROM credit WHERE singleton FOR UPDATE;
    IF config.rate_bytes_per_second=0 THEN
        tokens:=config.burst_bytes;
        delay:=0;
    ELSE
        tokens:=least(config.burst_bytes, bucket.available_bytes +
            greatest(0,extract(epoch FROM p_now-bucket.refilled_at)) * config.rate_bytes_per_second);
        tokens:=tokens-p_bytes;
        delay:=greatest(0,-tokens/config.rate_bytes_per_second);
        IF delay > config.max_wait_seconds THEN
            RAISE EXCEPTION 'diagnostic budget reservation exceeds bounded wait horizon'
                USING ERRCODE='55000';
        END IF;
    END IF;
    UPDATE credit SET available_bytes=tokens,
        refilled_at=greatest(p_now,bucket.refilled_at),
        reserved_bytes=reserved_bytes+p_bytes, requests=requests+1,
        sum_wait_seconds=sum_wait_seconds+delay,
        max_wait_seconds=greatest(max_wait_seconds,delay)
    WHERE singleton;
    RETURN delay;
END;
$$;
-- Refresh the clock after obtaining the row lock. Queued requests must not
-- reserve using a time sampled before another transaction's reservation.
CREATE FUNCTION staging_budget.reserve(p_bytes bigint)
RETURNS double precision LANGUAGE plpgsql
SET search_path=pg_catalog,staging_budget AS $$
DECLARE
    config settings%ROWTYPE;
BEGIN
    SELECT * INTO STRICT config FROM settings;
    INSERT INTO credit(singleton,available_bytes,refilled_at,first_reserved_at)
        VALUES(true,config.burst_bytes,clock_timestamp(),clock_timestamp()) ON CONFLICT DO NOTHING;
    PERFORM 1 FROM credit WHERE singleton FOR UPDATE;
    RETURN reserve_at(p_bytes,clock_timestamp());
END;
$$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA staging_budget FROM PUBLIC;

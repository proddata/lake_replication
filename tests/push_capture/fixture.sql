-- Disposable SQL feasibility fixture, not an installed extension or wire protocol.
CREATE SCHEMA capture_probe;
REVOKE ALL ON SCHEMA capture_probe FROM PUBLIC;

CREATE TABLE capture_probe.progress (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    sequence bigint NOT NULL,
    end_lsn pg_lsn NOT NULL
);
INSERT INTO capture_probe.progress VALUES (true, 0, '0/0');

-- Ordinary PostgreSQL uniqueness arbitrates retries; Iceberg has no unique key.
CREATE TABLE capture_probe.receipts (
    batch_id uuid PRIMARY KEY,
    sequence bigint UNIQUE NOT NULL,
    end_lsn pg_lsn NOT NULL,
    payload jsonb NOT NULL,
    digest text NOT NULL
);
CREATE TABLE capture_probe.log_a (
    batch_id text, ordinal bigint, payload text
) USING iceberg;
CREATE TABLE capture_probe.log_b (
    batch_id text, ordinal bigint, payload text
) USING iceberg;
CREATE TABLE capture_probe.manifests (
    sequence bigint, batch_id text, end_lsn text,
    count_a bigint, count_b bigint, digest text
) USING iceberg;

CREATE FUNCTION capture_probe.append_batch(
    batch uuid, frontier pg_lsn, rows_a jsonb, rows_b jsonb
) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    previous capture_probe.receipts%ROWTYPE;
    current_progress capture_probe.progress%ROWTYPE;
    body jsonb := jsonb_build_array(rows_a, rows_b);
    body_digest text := encode(sha256(convert_to(body::text, 'UTF8')), 'hex');
    next_sequence bigint;
BEGIN
    IF jsonb_typeof(rows_a) IS DISTINCT FROM 'array'
       OR jsonb_typeof(rows_b) IS DISTINCT FROM 'array'
       OR batch IS NULL OR frontier IS NULL THEN
        RAISE EXCEPTION 'batch identity, frontier and row arrays required';
    END IF;
    SELECT * INTO STRICT current_progress FROM capture_probe.progress
        WHERE singleton FOR UPDATE;
    SELECT * INTO previous FROM capture_probe.receipts WHERE batch_id = batch;
    IF FOUND THEN
        IF previous.end_lsn <> frontier OR previous.payload <> body
           OR previous.digest <> body_digest THEN
            RAISE EXCEPTION 'conflicting batch replay' USING ERRCODE = '23505';
        END IF;
        RETURN false;
    END IF;
    IF frontier <= current_progress.end_lsn THEN
        RAISE EXCEPTION 'capture frontier must increase';
    END IF;
    next_sequence := current_progress.sequence + 1;
    INSERT INTO capture_probe.log_a
        SELECT batch::text, ordinality, value::text
        FROM jsonb_array_elements(rows_a) WITH ORDINALITY;
    INSERT INTO capture_probe.log_b
        SELECT batch::text, ordinality, value::text
        FROM jsonb_array_elements(rows_b) WITH ORDINALITY;
    INSERT INTO capture_probe.manifests VALUES (
        next_sequence, batch::text, frontier::text,
        jsonb_array_length(rows_a), jsonb_array_length(rows_b), body_digest);
    INSERT INTO capture_probe.receipts VALUES (
        batch, next_sequence, frontier, body, body_digest);
    UPDATE capture_probe.progress SET sequence = next_sequence, end_lsn = frontier
        WHERE singleton;
    RETURN true;
END;
$$;
REVOKE ALL ON ALL TABLES IN SCHEMA capture_probe FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA capture_probe FROM PUBLIC;

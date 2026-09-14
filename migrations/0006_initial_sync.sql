BEGIN;

CREATE TABLE lake_replication.initial_copies (
    subscription_id bigint NOT NULL
        REFERENCES lake_replication.subscriptions ON DELETE CASCADE,
    remote_relation_id oid NOT NULL,
    relation_schema_version bigint NOT NULL,
    destination_relation regclass NOT NULL,
    consistent_lsn pg_lsn NOT NULL,
    snapshot_name text NOT NULL,
    state text NOT NULL CHECK (state IN ('copying', 'ready', 'error')),
    overwrite_destination boolean NOT NULL,
    rows_copied bigint NOT NULL DEFAULT 0 CHECK (rows_copied >= 0),
    last_error text,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (subscription_id, remote_relation_id, relation_schema_version),
    FOREIGN KEY (subscription_id, remote_relation_id, relation_schema_version)
        REFERENCES lake_replication.relations
            (subscription_id, remote_relation_id, schema_version)
        ON DELETE CASCADE
);

CREATE TABLE lake_replication.initial_copy_rows (
    subscription_id bigint NOT NULL,
    remote_relation_id oid NOT NULL,
    relation_schema_version bigint NOT NULL,
    row_ordinal bigint NOT NULL CHECK (row_ordinal >= 0),
    row_data jsonb NOT NULL CHECK (jsonb_typeof(row_data) = 'object'),
    PRIMARY KEY (
        subscription_id, remote_relation_id,
        relation_schema_version, row_ordinal
    ),
    FOREIGN KEY (subscription_id, remote_relation_id, relation_schema_version)
        REFERENCES lake_replication.initial_copies
            (subscription_id, remote_relation_id, relation_schema_version)
        ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION lake_replication.begin_initial_copy(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_destination_relation regclass,
    p_consistent_lsn pg_lsn,
    p_snapshot_name text,
    p_overwrite_destination boolean
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_checkpoint checkpoints%ROWTYPE;
BEGIN
    IF p_consistent_lsn = '0/0' OR p_snapshot_name = '' THEN
        RAISE EXCEPTION 'initial copy requires a consistent LSN and exported snapshot'
            USING ERRCODE = '22023';
    END IF;

    SELECT * INTO STRICT v_checkpoint FROM checkpoints
     WHERE subscription_id = p_subscription_id FOR UPDATE;
    IF v_checkpoint.received_lsn <> '0/0'
       OR v_checkpoint.staged_lsn <> '0/0'
       OR v_checkpoint.published_lsn <> '0/0'
       OR v_checkpoint.acknowledged_lsn <> '0/0' THEN
        RAISE EXCEPTION 'initial copy cannot start after replication checkpoints advance'
            USING ERRCODE = '55000';
    END IF;

    PERFORM assign_destination(
        p_subscription_id, p_remote_relation_id,
        p_schema_version, p_destination_relation
    );

    INSERT INTO initial_copies (
        subscription_id, remote_relation_id, relation_schema_version,
        destination_relation, consistent_lsn, snapshot_name, state,
        overwrite_destination
    ) VALUES (
        p_subscription_id, p_remote_relation_id, p_schema_version,
        p_destination_relation, p_consistent_lsn, p_snapshot_name, 'copying',
        p_overwrite_destination
    );

    UPDATE relations SET state = 'copying'
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND schema_version = p_schema_version;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.append_initial_copy_rows(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_start_ordinal bigint,
    p_rows jsonb
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_count bigint;
    v_state text;
BEGIN
    IF p_start_ordinal < 0 OR jsonb_typeof(p_rows) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'invalid initial-copy row chunk' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_rows) AS source_row(row_data)
         WHERE jsonb_typeof(source_row.row_data) IS DISTINCT FROM 'object'
    ) THEN
        RAISE EXCEPTION 'initial-copy rows must be JSON objects' USING ERRCODE = '22023';
    END IF;

    SELECT state INTO STRICT v_state FROM initial_copies
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version
     FOR UPDATE;
    IF v_state <> 'copying' THEN
        RAISE EXCEPTION 'initial copy is not accepting rows (state %)', v_state
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO initial_copy_rows (
        subscription_id, remote_relation_id, relation_schema_version,
        row_ordinal, row_data
    )
    SELECT p_subscription_id, p_remote_relation_id, p_schema_version,
           p_start_ordinal + ordinality - 1, source_row.row_data
      FROM jsonb_array_elements(p_rows) WITH ORDINALITY
           AS source_row(row_data, ordinality);
    GET DIAGNOSTICS v_count = ROW_COUNT;

    UPDATE initial_copies SET rows_copied = rows_copied + v_count
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version;
    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.fail_initial_copy(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_error text
) RETURNS void
LANGUAGE sql
SET search_path = pg_catalog, lake_replication
AS $$
    UPDATE initial_copies SET state = 'error', last_error = p_error
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version
       AND state = 'copying';
    UPDATE relations SET state = 'error'
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND schema_version = p_schema_version
       AND state = 'copying';
$$;

CREATE OR REPLACE FUNCTION lake_replication.reset_initial_copy(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM initial_copies
         WHERE subscription_id = p_subscription_id
           AND remote_relation_id = p_remote_relation_id
           AND relation_schema_version = p_schema_version
           AND state = 'ready'
    ) THEN
        RAISE EXCEPTION 'a completed initial copy cannot be reset'
            USING ERRCODE = '55000';
    END IF;
    DELETE FROM initial_copies
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version;
    UPDATE relations SET state = 'ready'
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND schema_version = p_schema_version;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.finish_initial_copy(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_expected_rows bigint
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_copy initial_copies%ROWTYPE;
    v_actual_rows bigint;
    v_destination_rows bigint;
BEGIN
    SELECT * INTO STRICT v_copy FROM initial_copies
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version
     FOR UPDATE;
    IF v_copy.state <> 'copying' THEN
        RAISE EXCEPTION 'initial copy is not finishable (state %)', v_copy.state
            USING ERRCODE = '55000';
    END IF;

    SELECT count(*) INTO v_actual_rows FROM initial_copy_rows
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version;
    IF v_actual_rows <> p_expected_rows
       OR v_actual_rows <> v_copy.rows_copied
       OR (v_actual_rows > 0 AND EXISTS (
           SELECT 1 FROM initial_copy_rows
            WHERE subscription_id = p_subscription_id
              AND remote_relation_id = p_remote_relation_id
              AND relation_schema_version = p_schema_version
            GROUP BY subscription_id, remote_relation_id, relation_schema_version
           HAVING min(row_ordinal) <> 0 OR max(row_ordinal) <> v_actual_rows - 1
       )) THEN
        RAISE EXCEPTION 'initial copy row set is incomplete: expected %, staged %',
            p_expected_rows, v_actual_rows USING ERRCODE = '22023';
    END IF;

    EXECUTE format('SELECT count(*) FROM %s', v_copy.destination_relation)
       INTO v_destination_rows;
    IF v_destination_rows > 0 AND NOT v_copy.overwrite_destination THEN
        RAISE EXCEPTION 'destination % is not empty and overwrite is disabled',
            v_copy.destination_relation USING ERRCODE = '55000';
    END IF;
    IF v_copy.overwrite_destination THEN
        EXECUTE format('TRUNCATE TABLE %s', v_copy.destination_relation);
    END IF;

    EXECUTE format(
        'INSERT INTO %s
         SELECT (jsonb_populate_record(NULL::%s, row_data)).*
           FROM lake_replication.initial_copy_rows
          WHERE subscription_id = $1 AND remote_relation_id = $2
            AND relation_schema_version = $3
          ORDER BY row_ordinal',
        v_copy.destination_relation, v_copy.destination_relation
    ) USING p_subscription_id, p_remote_relation_id, p_schema_version;

    UPDATE checkpoints SET
           received_lsn = v_copy.consistent_lsn,
           staged_lsn = v_copy.consistent_lsn,
           published_lsn = v_copy.consistent_lsn,
           acknowledged_lsn = v_copy.consistent_lsn,
           updated_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id;
    UPDATE relations SET state = 'ready'
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND schema_version = p_schema_version;
    UPDATE initial_copies SET state = 'ready', completed_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version;
    UPDATE subscriptions SET state = 'streaming', last_error = NULL
     WHERE subscription_id = p_subscription_id;
    RETURN v_actual_rows;
END;
$$;

COMMIT;

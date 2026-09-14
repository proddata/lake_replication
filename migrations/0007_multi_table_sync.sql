BEGIN;

CREATE OR REPLACE FUNCTION lake_replication.finish_initial_sync_group(
    p_subscription_id bigint,
    p_expected_tables bigint
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_copy initial_copies%ROWTYPE;
    v_consistent_lsn pg_lsn;
    v_table_count bigint;
    v_total_rows bigint := 0;
    v_actual_rows bigint;
    v_destination_rows bigint;
BEGIN
    IF p_expected_tables <= 0 THEN
        RAISE EXCEPTION 'initial sync must contain at least one table'
            USING ERRCODE = '22023';
    END IF;

    PERFORM 1 FROM checkpoints
     WHERE subscription_id = p_subscription_id FOR UPDATE;

    SELECT count(*), min(consistent_lsn)
      INTO v_table_count, v_consistent_lsn
      FROM initial_copies
     WHERE subscription_id = p_subscription_id AND state = 'copying';
    IF v_table_count <> p_expected_tables THEN
        RAISE EXCEPTION 'expected % copying tables, found %',
            p_expected_tables, v_table_count USING ERRCODE = '22023';
    END IF;
    IF (SELECT count(DISTINCT consistent_lsn) FROM initial_copies
         WHERE subscription_id = p_subscription_id AND state = 'copying') <> 1 THEN
        RAISE EXCEPTION 'initial-copy tables do not share one consistent LSN'
            USING ERRCODE = '22023';
    END IF;
    IF (SELECT count(DISTINCT destination_relation) FROM initial_copies
         WHERE subscription_id = p_subscription_id AND state = 'copying') <> v_table_count THEN
        RAISE EXCEPTION 'initial-copy tables must have distinct destinations'
            USING ERRCODE = '22023';
    END IF;

    FOR v_copy IN
        SELECT * FROM initial_copies
         WHERE subscription_id = p_subscription_id AND state = 'copying'
         ORDER BY remote_relation_id
         FOR UPDATE
    LOOP
        SELECT count(*) INTO v_actual_rows FROM initial_copy_rows
         WHERE subscription_id = v_copy.subscription_id
           AND remote_relation_id = v_copy.remote_relation_id
           AND relation_schema_version = v_copy.relation_schema_version;
        IF v_actual_rows <> v_copy.rows_copied
           OR (v_actual_rows > 0 AND EXISTS (
               SELECT 1 FROM initial_copy_rows
                WHERE subscription_id = v_copy.subscription_id
                  AND remote_relation_id = v_copy.remote_relation_id
                  AND relation_schema_version = v_copy.relation_schema_version
                GROUP BY subscription_id, remote_relation_id, relation_schema_version
               HAVING min(row_ordinal) <> 0 OR max(row_ordinal) <> v_actual_rows - 1
           )) THEN
            RAISE EXCEPTION 'initial copy for relation % has incomplete row ordinals',
                v_copy.remote_relation_id USING ERRCODE = '22023';
        END IF;

        EXECUTE format('SELECT count(*) FROM %s', v_copy.destination_relation)
           INTO v_destination_rows;
        IF v_destination_rows > 0 AND NOT v_copy.overwrite_destination THEN
            RAISE EXCEPTION 'destination % is not empty and overwrite is disabled',
                v_copy.destination_relation USING ERRCODE = '55000';
        END IF;
        v_total_rows := v_total_rows + v_actual_rows;
    END LOOP;

    FOR v_copy IN
        SELECT * FROM initial_copies
         WHERE subscription_id = p_subscription_id AND state = 'copying'
         ORDER BY remote_relation_id
    LOOP
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
        ) USING p_subscription_id, v_copy.remote_relation_id,
                v_copy.relation_schema_version;
    END LOOP;

    UPDATE checkpoints SET
           received_lsn = v_consistent_lsn,
           staged_lsn = v_consistent_lsn,
           published_lsn = v_consistent_lsn,
           acknowledged_lsn = v_consistent_lsn,
           updated_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id;
    UPDATE relations r SET state = 'ready'
      FROM initial_copies i
     WHERE i.subscription_id = p_subscription_id
       AND i.state = 'copying'
       AND r.subscription_id = i.subscription_id
       AND r.remote_relation_id = i.remote_relation_id
       AND r.schema_version = i.relation_schema_version;
    UPDATE initial_copies SET state = 'ready', completed_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id AND state = 'copying';
    UPDATE subscriptions SET state = 'streaming', last_error = NULL
     WHERE subscription_id = p_subscription_id;
    RETURN v_total_rows;
END;
$$;

COMMIT;

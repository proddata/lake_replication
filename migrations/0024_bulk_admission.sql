BEGIN;

-- Keep every installed database drained until its SQL upgrade is complete.
-- Old function bodies must not run alongside the new instance-wide policy.
DO $$
BEGIN
    IF EXISTS (SELECT FROM lake_replication.worker_launch_config WHERE desired_state <> 'stopped')
       OR EXISTS (SELECT FROM pg_stat_activity WHERE datname=current_database()
                   AND backend_type='lake_replication supervisor') THEN
        RAISE EXCEPTION 'stop subscriptions before the engine admission upgrade'
            USING ERRCODE='55000';
    END IF;
END;
$$;

-- Bulk work shares a transaction-scoped lock across databases. Streaming is
-- deliberately unqueued. The lock is held through engine commit; native lock
-- handling supplies cancellation, savepoint rollback and process-death cleanup.
CREATE FUNCTION lake_replication.admit_bulk()
RETURNS void LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', 'lake_replication_admit_bulk';
REVOKE ALL ON FUNCTION lake_replication.admit_bulk() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.admit_bulk() TO lake_replication_admin;

CREATE VIEW lake_replication.bulk_admission AS
 SELECT l.pid, a.datname AS database_name,
        CASE WHEN l.granted THEN 'admitted' ELSE 'waiting_for_writer' END AS admission_state,
        l.waitstart, a.wait_event_type, a.wait_event
 FROM pg_catalog.pg_locks l LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='advisory' AND l.database=0 AND l.classid=1280460112
   AND l.objid=2 AND l.objsubid=1 AND l.mode='ExclusiveLock';
REVOKE ALL ON lake_replication.bulk_admission FROM PUBLIC;
GRANT SELECT ON lake_replication.bulk_admission TO lake_replication_admin;
COMMENT ON VIEW lake_replication.bulk_admission IS
 'Instance-wide managed bulk transaction admission, including waiters; not a memory bound';


CREATE OR REPLACE FUNCTION lake_replication.ensure_iceberg_destination(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint,
    p_destination_schema name,
    p_destination_relation name,
    p_create_missing boolean
) RETURNS regclass
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_contract relation_type_contracts%ROWTYPE;
    v_destination regclass;
    v_definitions text;
    v_expected_names text[];
    v_actual_names text[];
    v_expected_types text[];
    v_actual_types text[];
    v_expected_not_null boolean[];
    v_actual_not_null boolean[];
    v_relkind "char";
BEGIN
    SELECT * INTO STRICT v_contract FROM relation_type_contracts
     WHERE subscription_id = p_subscription_id
       AND remote_relation_id = p_remote_relation_id
       AND relation_schema_version = p_schema_version;

    SELECT string_agg(format('%I %s%s', source_column.value->>'name',
                             resolve_destination_type(
                                 v_contract.mapping_version,
                                 source_column.value->>'source_type'),
                             CASE WHEN (source_column.value->>'not_null')::boolean
                                  THEN ' NOT NULL' ELSE '' END),
                      ', ' ORDER BY source_column.ordinality),
           array_agg(source_column.value->>'name' ORDER BY source_column.ordinality),
           array_agg(resolve_destination_type(
                         v_contract.mapping_version,
                         source_column.value->>'source_type')
                     ORDER BY source_column.ordinality),
           array_agg((source_column.value->>'not_null')::boolean
                     ORDER BY source_column.ordinality)
      INTO v_definitions, v_expected_names, v_expected_types, v_expected_not_null
      FROM jsonb_array_elements(v_contract.columns) WITH ORDINALITY
           AS source_column(value, ordinality);

    v_destination := to_regclass(format('%I.%I', p_destination_schema, p_destination_relation));
    IF v_destination IS NULL THEN
        IF NOT p_create_missing THEN
            RAISE EXCEPTION 'destination %.% does not exist',
                p_destination_schema, p_destination_relation USING ERRCODE = '42P01';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=p_destination_schema) THEN
            RAISE EXCEPTION 'destination schema % does not exist', p_destination_schema
                USING ERRCODE = '3F000';
        END IF;
        PERFORM lake_replication.admit_bulk();
        EXECUTE format('CREATE TABLE %I.%I (%s) USING iceberg',
                       p_destination_schema, p_destination_relation, v_definitions);
        v_destination := to_regclass(format('%I.%I', p_destination_schema, p_destination_relation));
    END IF;

    SELECT relkind INTO STRICT v_relkind FROM pg_class WHERE oid=v_destination;
    IF v_relkind <> 'f' THEN
        RAISE EXCEPTION 'destination % must be a writable Iceberg foreign table', v_destination
            USING ERRCODE = '42809';
    END IF;
    SELECT array_agg(attname::text ORDER BY attnum),
           array_agg(format_type(atttypid, atttypmod) ORDER BY attnum),
           array_agg(attnotnull ORDER BY attnum)
      INTO v_actual_names, v_actual_types, v_actual_not_null
      FROM pg_attribute
     WHERE attrelid=v_destination AND attnum > 0 AND NOT attisdropped;
    IF v_actual_names IS DISTINCT FROM v_expected_names
       OR v_actual_types IS DISTINCT FROM v_expected_types
       OR EXISTS (
           SELECT 1 FROM generate_subscripts(v_actual_not_null, 1) AS subscript
            WHERE v_actual_not_null[subscript] AND NOT v_expected_not_null[subscript]
       ) THEN
        RAISE EXCEPTION 'destination schema mismatch for %: expected names/types/nullability %/%/%, got %/%/%',
            v_destination, v_expected_names, v_expected_types, v_expected_not_null,
            v_actual_names, v_actual_types, v_actual_not_null USING ERRCODE = '42804';
    END IF;

    PERFORM assign_destination(
        p_subscription_id, p_remote_relation_id, p_schema_version, v_destination
    );
    RETURN v_destination;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.reconcile_relation_schema(
    p_subscription_id bigint,
    p_remote_relation_id oid,
    p_schema_version bigint
) RETURNS text
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_new relations%ROWTYPE;
    v_old relations%ROWTYPE;
    v_new_contract relation_type_contracts%ROWTYPE;
    v_old_contract relation_type_contracts%ROWTYPE;
    v_old_count integer;
    v_new_count integer;
    v_error text;
    v_column record;
    v_destination_names text[];
    v_destination_types text[];
    v_destination_not_null boolean[];
    v_expected_old_types text[];
BEGIN
    SELECT * INTO STRICT v_new FROM relations
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND schema_version=p_schema_version
     FOR UPDATE;
    SELECT * INTO STRICT v_new_contract FROM relation_type_contracts
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND relation_schema_version=p_schema_version;
    IF v_new.destination_relation IS NOT NULL THEN
        UPDATE relations SET state='ready'
         WHERE subscription_id=p_subscription_id
           AND remote_relation_id=p_remote_relation_id
           AND schema_version=p_schema_version;
        RETURN 'ready';
    END IF;

    SELECT * INTO v_old FROM relations
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND schema_version < p_schema_version
       AND destination_relation IS NOT NULL
     ORDER BY schema_version DESC LIMIT 1;
    IF NOT FOUND THEN
        v_error := format('relation %s version %s has no assigned predecessor',
                          p_remote_relation_id, p_schema_version);
        PERFORM record_schema_error(p_subscription_id, p_remote_relation_id,
                                    p_schema_version, v_error);
        RETURN 'paused:' || v_error;
    END IF;
    SELECT * INTO STRICT v_old_contract FROM relation_type_contracts
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND relation_schema_version=v_old.schema_version;

    v_old_count := jsonb_array_length(v_old_contract.columns);
    v_new_count := jsonb_array_length(v_new_contract.columns);
    IF v_new_contract.mapping_version <> v_old_contract.mapping_version THEN
        v_error := 'type mapping version changed during active replication';
    ELSIF v_new_count < v_old_count THEN
        v_error := 'published columns were removed or reordered';
    ELSIF EXISTS (
        SELECT 1
          FROM jsonb_array_elements(v_old_contract.columns) WITH ORDINALITY old_col(value, n)
          JOIN jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY new_col(value, n)
            USING (n)
         WHERE old_col.value->>'name' IS DISTINCT FROM new_col.value->>'name'
            OR old_col.value->>'source_type' IS DISTINCT FROM new_col.value->>'source_type'
    ) THEN
        v_error := 'an existing published column changed name, order, or type';
    ELSIF EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY c(value, n)
         WHERE n > v_old_count
           AND (c.value->>'not_null')::boolean
    ) THEN
        v_error := 'new published columns must be nullable for online addition';
    END IF;
    IF v_error IS NOT NULL THEN
        PERFORM record_schema_error(p_subscription_id, p_remote_relation_id,
                                    p_schema_version, v_error);
        RETURN 'paused:' || v_error;
    END IF;

    SELECT array_agg(attname::text ORDER BY attnum),
           array_agg(format_type(atttypid,atttypmod) ORDER BY attnum),
           array_agg(attnotnull ORDER BY attnum)
      INTO v_destination_names, v_destination_types, v_destination_not_null
      FROM pg_attribute
     WHERE attrelid=v_old.destination_relation AND attnum > 0 AND NOT attisdropped;
    SELECT array_agg(resolve_destination_type(
                         v_new_contract.mapping_version,
                         c.value->>'source_type') ORDER BY c.n)
      INTO v_expected_old_types
      FROM jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY c(value, n)
     WHERE c.n <= v_old_count;
    IF cardinality(v_destination_names) <> v_old_count
       OR EXISTS (
           SELECT 1 FROM generate_series(1, v_old_count) n
            WHERE v_destination_names[n] IS DISTINCT FROM
                      v_new_contract.columns->(n-1)->>'name'
               OR v_destination_types[n] IS DISTINCT FROM v_expected_old_types[n]
               OR (v_destination_not_null[n]
                   AND NOT (v_new_contract.columns->(n-1)->>'not_null')::boolean)
       ) THEN
        v_error := 'existing destination columns are incompatible with the new contract';
        PERFORM record_schema_error(p_subscription_id, p_remote_relation_id,
                                    p_schema_version, v_error);
        RETURN 'paused:' || v_error;
    END IF;

    -- Ready-schema checks stay unqueued. Acquire before the DDL handler so
    -- temporary resource contention cannot become a durable schema pause.
    IF v_new_count > v_old_count THEN
        PERFORM lake_replication.admit_bulk();
    END IF;
    BEGIN
        FOR v_column IN
            SELECT c.value->>'name' AS column_name,
                   resolve_destination_type(
                       v_new_contract.mapping_version,
                       c.value->>'source_type') AS destination_type
              FROM jsonb_array_elements(v_new_contract.columns) WITH ORDINALITY c(value, n)
             WHERE c.n > v_old_count ORDER BY c.n
        LOOP
            EXECUTE format('ALTER TABLE %s ADD COLUMN %I %s',
                           v_old.destination_relation,
                           v_column.column_name, v_column.destination_type);
        END LOOP;
    EXCEPTION
    WHEN SQLSTATE '40P01' OR SQLSTATE '55P03' OR SQLSTATE '40001' THEN
        RAISE;
    WHEN OTHERS THEN
        v_error := format('destination ALTER failed: %s', SQLERRM);
        PERFORM record_schema_error(p_subscription_id, p_remote_relation_id,
                                    p_schema_version, v_error);
        RETURN 'paused:' || v_error;
    END;

    UPDATE relations SET destination_relation=v_old.destination_relation, state='ready'
     WHERE subscription_id=p_subscription_id
       AND remote_relation_id=p_remote_relation_id
       AND schema_version=p_schema_version;
    UPDATE subscriptions SET state='streaming', last_error=NULL
     WHERE subscription_id=p_subscription_id;
    RETURN 'ready';
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
    PERFORM lake_replication.admit_bulk();
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
    PERFORM lake_replication.admit_bulk();
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

CREATE OR REPLACE FUNCTION lake_replication.promote_subscription_rebuild(
    p_old_subscription_id bigint,
    p_replacement_subscription_id bigint,
    p_target_schema name,
    p_shadow_suffix text,
    p_backup_suffix text
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_old_subscription subscriptions%ROWTYPE;
    v_new_subscription subscriptions%ROWTYPE;
    v_new_checkpoint checkpoints%ROWTYPE;
    v_relation record;
    v_target regclass;
    v_shadow regclass;
    v_target_name text;
    v_shadow_name text;
    v_backup_name text;
    v_count bigint := 0;
BEGIN
    PERFORM lake_replication.admit_bulk();
    IF p_old_subscription_id = p_replacement_subscription_id
       OR p_shadow_suffix = '' OR p_backup_suffix = ''
       OR p_shadow_suffix = p_backup_suffix THEN
        RAISE EXCEPTION 'invalid rebuild promotion identifiers or suffixes'
            USING ERRCODE='22023';
    END IF;
    SELECT * INTO STRICT v_old_subscription FROM subscriptions
     WHERE subscription_id=p_old_subscription_id FOR UPDATE;
    SELECT * INTO STRICT v_new_subscription FROM subscriptions
     WHERE subscription_id=p_replacement_subscription_id FOR UPDATE;
    IF v_old_subscription.state <> 'paused' THEN
        RAISE EXCEPTION 'old subscription must be paused, found %', v_old_subscription.state
            USING ERRCODE='55000';
    END IF;
    IF v_new_subscription.state <> 'streaming'
       OR v_old_subscription.source_id <> v_new_subscription.source_id
       OR v_old_subscription.publication_names <> v_new_subscription.publication_names THEN
        RAISE EXCEPTION 'replacement subscription is not a ready equivalent source/publication'
            USING ERRCODE='55000';
    END IF;
    SELECT * INTO STRICT v_new_checkpoint FROM checkpoints
     WHERE subscription_id=p_replacement_subscription_id FOR UPDATE;
    IF v_new_checkpoint.published_lsn = '0/0'
       OR v_new_checkpoint.staged_lsn <> v_new_checkpoint.published_lsn THEN
        RAISE EXCEPTION 'replacement subscription has unpublished WAL at %/%',
            v_new_checkpoint.staged_lsn, v_new_checkpoint.published_lsn
            USING ERRCODE='55000';
    END IF;
    IF EXISTS (
        SELECT 1 FROM transactions
         WHERE subscription_id=p_replacement_subscription_id AND state <> 'published'
    ) THEN
        RAISE EXCEPTION 'replacement subscription still has unpublished transactions'
            USING ERRCODE='55000';
    END IF;

    FOR v_relation IN
        SELECT DISTINCT ON (remote_relation_id)
               remote_relation_id, source_relation, destination_relation
          FROM relations
         WHERE subscription_id=p_replacement_subscription_id
           AND state='ready' AND destination_relation IS NOT NULL
         ORDER BY remote_relation_id, schema_version DESC
    LOOP
        v_target_name := v_relation.source_relation::text;
        v_shadow_name := v_target_name || p_shadow_suffix;
        v_backup_name := v_target_name || p_backup_suffix;
        IF length(v_shadow_name) > 63 OR length(v_backup_name) > 63 THEN
            RAISE EXCEPTION 'rebuild relation name exceeds PostgreSQL identifier limit'
                USING ERRCODE='42622';
        END IF;
        v_target := to_regclass(format('%I.%I', p_target_schema, v_target_name));
        v_shadow := to_regclass(format('%I.%I', p_target_schema, v_shadow_name));
        IF v_target IS NULL OR v_shadow IS NULL
           OR v_shadow <> v_relation.destination_relation
           OR NOT EXISTS (
               SELECT 1 FROM relations old_relation
                WHERE old_relation.subscription_id=p_old_subscription_id
                  AND old_relation.remote_relation_id=v_relation.remote_relation_id
                  AND old_relation.destination_relation=v_target
           ) THEN
            RAISE EXCEPTION 'target/shadow pair is missing or not assigned for relation %',
                v_target_name USING ERRCODE='55000';
        END IF;
        IF to_regclass(format('%I.%I', p_target_schema, v_backup_name)) IS NOT NULL THEN
            RAISE EXCEPTION 'backup relation %.% already exists',
                p_target_schema, v_backup_name USING ERRCODE='42P07';
        END IF;
        EXECUTE format('ALTER TABLE %s RENAME TO %I', v_target, v_backup_name);
        EXECUTE format('ALTER TABLE %s RENAME TO %I', v_shadow, v_target_name);
        v_count := v_count + 1;
    END LOOP;
    IF v_count = 0 THEN
        RAISE EXCEPTION 'replacement subscription has no promotable relations'
            USING ERRCODE='55000';
    END IF;

    UPDATE subscriptions SET state='disabled'
     WHERE subscription_id=p_old_subscription_id;
    INSERT INTO rebuild_promotions (
        old_subscription_id, replacement_subscription_id, target_schema,
        shadow_suffix, backup_suffix, old_slot_name, replacement_slot_name
    ) VALUES (
        p_old_subscription_id, p_replacement_subscription_id, p_target_schema,
        p_shadow_suffix, p_backup_suffix,
        v_old_subscription.slot_name, v_new_subscription.slot_name
    );
    RETURN v_count;
END;
$$;

-- Streaming has an independent four-slot transaction pool. There is no
-- single global FIFO and no per-query memory reservation.
CREATE FUNCTION lake_replication.admit_stream()
RETURNS void LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', 'lake_replication_admit_stream';
REVOKE ALL ON FUNCTION lake_replication.admit_stream() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.admit_stream() TO lake_replication_admin;

CREATE VIEW lake_replication.stream_admission AS
 SELECT l.pid, a.datname AS database_name, l.objid::bigint-2 AS slot,
        CASE WHEN l.granted THEN 'admitted' ELSE 'waiting_for_stream' END AS admission_state,
        l.waitstart, a.wait_event_type, a.wait_event
 FROM pg_catalog.pg_locks l LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='advisory' AND l.database=0 AND l.classid=1280460112
   AND l.objid::bigint BETWEEN 3 AND 6 AND l.objsubid=1 AND l.mode='ExclusiveLock';
REVOKE ALL ON lake_replication.stream_admission FROM PUBLIC;
GRANT SELECT ON lake_replication.stream_admission TO lake_replication_admin;
COMMENT ON VIEW lake_replication.stream_admission IS
 'Four instance-wide managed streaming transaction slots, including waiters; not a memory bound';


CREATE OR REPLACE FUNCTION lake_replication.apply_change_run(
    p_operation text,
    p_destination regclass,
    p_old jsonb[],
    p_new jsonb[]
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_count bigint;
    v_distinct bigint;
    v_expected bigint;
    v_set_clause text;
    v_where_clause text;
    v_old_item jsonb;
    v_rows bigint;
BEGIN
    PERFORM lake_replication.admit_stream();
    v_expected := CASE WHEN p_operation = 'delete'
                       THEN cardinality(p_old) ELSE cardinality(p_new) END;
    IF v_expected IS NULL OR v_expected = 0 THEN
        RAISE EXCEPTION 'empty change run' USING ERRCODE = '22023';
    END IF;

    IF p_operation = 'insert' THEN
        EXECUTE format(
            'INSERT INTO %s
             SELECT (jsonb_populate_record(NULL::%s, source.new_tuple)).*
               FROM unnest($1::jsonb[]) AS source(new_tuple)',
            p_destination, p_destination
        ) USING p_new;
        GET DIAGNOSTICS v_count = ROW_COUNT;
        RETURN v_count;
    END IF;

    -- Equality lets PostgreSQL use a merge/hash join for ordinary non-null
    -- replica identities. Preserve null-safe matching for nullable keys.
    SELECT string_agg(format(
               '%1$I %3$s
                (jsonb_populate_record(NULL::%2$s, source.old_tuple)).%1$I',
               key, p_destination,
               CASE WHEN p_operation = 'update' AND NOT EXISTS (
                    SELECT 1 FROM unnest(p_old) AS old(value)
                     WHERE NOT (value ? key) OR value->key = 'null'::jsonb
               ) THEN '=' ELSE 'IS NOT DISTINCT FROM' END),
               ' AND ' ORDER BY key)
      INTO v_where_clause
      FROM jsonb_object_keys(p_old[1]) AS item(key);

    IF p_operation = 'update' THEN
        SELECT string_agg(format(
                   '%1$I = (jsonb_populate_record(NULL::%2$s, source.new_tuple)).%1$I',
                   key, p_destination), ', ' ORDER BY key)
          INTO v_set_clause
          FROM jsonb_object_keys(p_new[1]) AS item(key);
        EXECUTE format(
            'WITH source AS (
                 SELECT old_tuple, new_tuple, ordinality
                   FROM unnest($1::jsonb[], $2::jsonb[])
                        WITH ORDINALITY AS input(old_tuple, new_tuple, ordinality)
             ), applied AS (
                 UPDATE %s AS destination SET %s
                   FROM source
                  WHERE %s
                  RETURNING source.ordinality
             )
             SELECT count(*), count(DISTINCT ordinality) FROM applied',
            p_destination, v_set_clause, v_where_clause
        ) INTO v_count, v_distinct USING p_old, p_new;
    ELSIF p_operation = 'delete' THEN
        SELECT string_agg(format(
                   '%1$I IS NOT DISTINCT FROM
                    (jsonb_populate_record(NULL::%2$s, $1)).%1$I',
                   key, p_destination), ' AND ' ORDER BY key)
          INTO v_where_clause
          FROM jsonb_object_keys(p_old[1]) AS item(key);
        v_count := 0;
        FOREACH v_old_item IN ARRAY p_old LOOP
            EXECUTE format('DELETE FROM %s AS destination WHERE %s',
                           p_destination, v_where_clause)
                USING v_old_item;
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            IF v_rows <> 1 THEN
                RAISE EXCEPTION 'delete for relation % matched % rows instead of exactly one',
                    p_destination, v_rows USING ERRCODE = 'P0002';
            END IF;
            v_count := v_count + v_rows;
        END LOOP;
        RETURN v_count;
    ELSE
        RAISE EXCEPTION 'unsupported change run operation %', p_operation
            USING ERRCODE = '22023';
    END IF;

    IF v_count <> v_expected OR v_distinct <> v_expected THEN
        RAISE EXCEPTION '% run for relation % matched % rows across % identities instead of exactly one per change',
            p_operation, p_destination, v_count, v_distinct USING ERRCODE = 'P0002';
    END IF;
    RETURN v_count;
END;
$$;


CREATE OR REPLACE FUNCTION lake_replication.publish_insert_batch(
    p_subscription_id bigint,
    p_batch_key text,
    p_first_end_lsn pg_lsn,
    p_last_end_lsn pg_lsn
) RETURNS TABLE (batch_id bigint, published_lsn pg_lsn, inserted_rows bigint)
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_batch_id bigint;
    v_batch_state text;
    v_change record;
    v_run_operation text;
    v_run_destination regclass;
    v_run_old jsonb[] := ARRAY[]::jsonb[];
    v_run_new jsonb[] := ARRAY[]::jsonb[];
    v_run_old_keys text[];
    v_run_new_keys text[];
    v_old_keys text[];
    v_new_keys text[];
    v_run_groupable boolean := false;
    v_change_groupable boolean;
    v_can_append boolean;
    v_run_bytes bigint := 0;
    v_change_bytes bigint;
    v_affected_rows bigint := 0;
    v_rows bigint;
BEGIN
    PERFORM lake_replication.admit_stream();
    v_batch_id := prepare_batch(
        p_subscription_id, p_batch_key, p_first_end_lsn, p_last_end_lsn
    );
    SELECT state INTO STRICT v_batch_state FROM batches WHERE batches.batch_id = v_batch_id;
    IF v_batch_state = 'published' THEN
        RETURN QUERY SELECT v_batch_id, p_last_end_lsn, 0::bigint;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM transactions t
        JOIN changes c USING (transaction_id)
        JOIN relations r ON r.subscription_id=t.subscription_id
          AND r.remote_relation_id=c.remote_relation_id
          AND r.schema_version=c.relation_schema_version
        WHERE t.batch_id=v_batch_id AND r.destination_relation IS NULL
    ) THEN
        RAISE EXCEPTION 'batch % contains a relation without a destination', v_batch_id
            USING ERRCODE = '55000';
    END IF;

    FOR v_change IN
        SELECT c.*, t.end_lsn, r.destination_relation
          FROM transactions t JOIN changes c USING (transaction_id)
          JOIN relations r ON r.subscription_id=t.subscription_id
            AND r.remote_relation_id=c.remote_relation_id
            AND r.schema_version=c.relation_schema_version
         WHERE t.batch_id=v_batch_id ORDER BY t.end_lsn, c.change_index
    LOOP
        IF v_change.operation = 'truncate' THEN
            IF v_run_operation IS NOT NULL THEN
                v_affected_rows := v_affected_rows + apply_change_run(
                    v_run_operation, v_run_destination, v_run_old, v_run_new);
                v_run_operation := NULL;
                v_run_old := ARRAY[]::jsonb[];
                v_run_new := ARRAY[]::jsonb[];
                v_run_bytes := 0;
            END IF;
            EXECUTE format('TRUNCATE %s', v_change.destination_relation);
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            v_affected_rows := v_affected_rows + v_rows;
            CONTINUE;
        END IF;

        IF v_change.operation = 'insert' THEN
            v_old_keys := NULL;
            v_new_keys := NULL;
        ELSE
            SELECT array_agg(key ORDER BY key) INTO v_old_keys
              FROM jsonb_object_keys(v_change.old_tuple) AS item(key);
            IF v_change.operation = 'update' THEN
                SELECT array_agg(key ORDER BY key) INTO v_new_keys
                  FROM jsonb_object_keys(v_change.new_tuple) AS item(key);
            ELSE
                v_new_keys := NULL;
            END IF;
        END IF;
        v_change_groupable := v_change.operation <> 'update'
            OR v_change.new_tuple @> v_change.old_tuple;
        v_change_bytes := coalesce(octet_length(v_change.old_tuple::text), 0)
                        + coalesce(octet_length(v_change.new_tuple::text), 0);
        v_can_append := v_run_operation = v_change.operation
            AND v_run_destination = v_change.destination_relation
            AND v_run_groupable AND v_change_groupable
            AND v_run_old_keys IS NOT DISTINCT FROM v_old_keys
            AND v_run_new_keys IS NOT DISTINCT FROM v_new_keys
            AND greatest(cardinality(v_run_old), cardinality(v_run_new)) < 500
            AND v_run_bytes + v_change_bytes <= 4194304
            AND (v_change.operation = 'insert'
                 OR NOT (v_change.old_tuple = ANY(v_run_old)));

        IF v_run_operation IS NOT NULL AND NOT v_can_append THEN
            v_affected_rows := v_affected_rows + apply_change_run(
                v_run_operation, v_run_destination, v_run_old, v_run_new);
            v_run_operation := NULL;
            v_run_old := ARRAY[]::jsonb[];
            v_run_new := ARRAY[]::jsonb[];
            v_run_bytes := 0;
        END IF;
        IF v_run_operation IS NULL THEN
            v_run_operation := v_change.operation;
            v_run_destination := v_change.destination_relation;
            v_run_old_keys := v_old_keys;
            v_run_new_keys := v_new_keys;
            v_run_groupable := v_change_groupable;
        END IF;
        IF v_change.operation <> 'insert' THEN
            v_run_old := array_append(v_run_old, v_change.old_tuple);
        END IF;
        IF v_change.operation <> 'delete' THEN
            v_run_new := array_append(v_run_new, v_change.new_tuple);
        END IF;
        v_run_bytes := v_run_bytes + v_change_bytes;
    END LOOP;

    IF v_run_operation IS NOT NULL THEN
        v_affected_rows := v_affected_rows + apply_change_run(
            v_run_operation, v_run_destination, v_run_old, v_run_new);
    END IF;
    PERFORM finish_batch(v_batch_id);
    RETURN QUERY SELECT v_batch_id, p_last_end_lsn, v_affected_rows;
END;
$$;

COMMIT;

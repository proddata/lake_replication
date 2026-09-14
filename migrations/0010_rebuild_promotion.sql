BEGIN;

CREATE TABLE lake_replication.rebuild_promotions (
    old_subscription_id bigint PRIMARY KEY
        REFERENCES lake_replication.subscriptions,
    replacement_subscription_id bigint NOT NULL UNIQUE
        REFERENCES lake_replication.subscriptions,
    target_schema name NOT NULL,
    shadow_suffix text NOT NULL,
    backup_suffix text NOT NULL,
    old_slot_name name NOT NULL,
    replacement_slot_name name NOT NULL,
    promoted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

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

COMMIT;

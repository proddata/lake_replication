BEGIN;

DO $$ BEGIN
 IF EXISTS(SELECT FROM lake_replication.worker_launch_config WHERE desired_state<>'stopped')
 OR EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor') THEN
  RAISE EXCEPTION 'stop subscriptions and restart with the new binaries before the checkpoint upgrade' USING ERRCODE='55000';
 END IF;
END $$;

-- Refuse custom row security, trigger semantics, or stored user dependencies
-- rather than silently redirecting them to a renamed physical side table.
DO $$ DECLARE relation regclass; BEGIN
 FOREACH relation IN ARRAY ARRAY['lake_replication.checkpoints'::regclass,'lake_replication.initial_copy_rows'::regclass] LOOP
  IF EXISTS(SELECT FROM pg_class WHERE oid=relation AND relowner<>(SELECT extowner FROM pg_extension WHERE extname='lake_replication'))
   OR EXISTS(SELECT FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a WHERE c.oid=relation AND a.grantor<>c.relowner)
   OR EXISTS(SELECT FROM pg_attribute t JOIN pg_class c ON c.oid=t.attrelid CROSS JOIN LATERAL aclexplode(t.attacl) a WHERE t.attrelid=relation AND a.grantor<>c.relowner)
   OR EXISTS(SELECT FROM pg_class WHERE oid=relation AND (relrowsecurity OR relforcerowsecurity))
   OR EXISTS(SELECT FROM pg_trigger WHERE tgrelid=relation AND NOT tgisinternal AND tgname<>'managed_worker_fence')
   OR EXISTS(SELECT FROM pg_depend d JOIN pg_rewrite r ON d.classid='pg_rewrite'::regclass AND d.objid=r.oid
      WHERE d.refclassid='pg_class'::regclass AND d.refobjid=relation AND r.ev_class<>relation
      AND r.ev_class NOT IN ('lake_replication.service_health'::regclass,'lake_replication.journal_maintenance_health'::regclass))
   OR EXISTS(SELECT FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND
      ((d.refclassid='pg_class'::regclass AND d.refobjid=relation) OR
       (d.refclassid='pg_type'::regclass AND d.refobjid=(SELECT reltype FROM pg_class WHERE oid=relation)))) THEN
   RAISE EXCEPTION 'custom dependencies on % require a rehearsed migration',relation USING ERRCODE='55000';
  END IF;
 END LOOP;
 IF EXISTS(SELECT FROM pg_class WHERE oid='lake_replication.initial_copies'::regclass AND (relrowsecurity OR relforcerowsecurity))
 OR EXISTS(SELECT FROM pg_trigger WHERE tgrelid='lake_replication.initial_copies'::regclass AND NOT tgisinternal AND tgname<>'managed_worker_fence') THEN
  RAISE EXCEPTION 'custom initial-copy parent policies require a rehearsed migration' USING ERRCODE='55000';
 END IF;
 IF EXISTS(SELECT FROM pg_constraint WHERE confrelid='lake_replication.initial_copy_rows'::regclass) THEN
  RAISE EXCEPTION 'foreign keys referencing initial_copy_rows require a rehearsed migration' USING ERRCODE='55000';
 END IF;
END $$;

CREATE FUNCTION lake_replication.migration_grant(text) RETURNS void
LANGUAGE C VOLATILE STRICT PARALLEL UNSAFE AS 'MODULE_PATHNAME','lake_replication_migration_grant';
REVOKE ALL ON FUNCTION lake_replication.migration_grant(text) FROM PUBLIC;

ALTER TABLE lake_replication.checkpoints RENAME TO checkpoint_ingress;
CREATE TABLE lake_replication.checkpoint_publication (
 subscription_id bigint PRIMARY KEY REFERENCES lake_replication.checkpoint_ingress ON DELETE CASCADE,
 published_lsn pg_lsn NOT NULL DEFAULT '0/0',
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO lake_replication.checkpoint_publication
 SELECT subscription_id,published_lsn,updated_at FROM lake_replication.checkpoint_ingress;
-- Neither half may disappear independently. Deferral permits atomic paired INSERT
-- and cascade DELETE. Logical data restore uses the documented disabled-trigger pass.
ALTER TABLE lake_replication.checkpoint_ingress ADD CONSTRAINT checkpoint_publication_pair
 FOREIGN KEY(subscription_id) REFERENCES lake_replication.checkpoint_publication
 DEFERRABLE INITIALLY DEFERRED;
CREATE VIEW lake_replication.checkpoints AS
 SELECT i.subscription_id,i.received_lsn,i.staged_lsn,p.published_lsn,i.acknowledged_lsn,
 greatest(i.updated_at,p.updated_at) AS updated_at
 FROM lake_replication.checkpoint_ingress i
 JOIN lake_replication.checkpoint_publication p USING(subscription_id);
ALTER VIEW lake_replication.checkpoints ALTER received_lsn SET DEFAULT '0/0';
ALTER VIEW lake_replication.checkpoints ALTER staged_lsn SET DEFAULT '0/0';
ALTER VIEW lake_replication.checkpoints ALTER published_lsn SET DEFAULT '0/0';
ALTER VIEW lake_replication.checkpoints ALTER acknowledged_lsn SET DEFAULT '0/0';
ALTER VIEW lake_replication.checkpoints ALTER updated_at SET DEFAULT clock_timestamp();

CREATE FUNCTION lake_replication.enforce_checkpoint_frontier() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE staged pg_lsn; published pg_lsn;
BEGIN
 IF TG_OP='UPDATE' AND NEW.subscription_id IS DISTINCT FROM OLD.subscription_id THEN
  RAISE EXCEPTION 'checkpoint identity cannot change' USING ERRCODE='22023';
 END IF;
 IF TG_TABLE_NAME='checkpoint_ingress' THEN
  IF TG_OP='UPDATE' THEN
   IF NEW.received_lsn<OLD.received_lsn OR NEW.staged_lsn<OLD.staged_lsn
      OR NEW.acknowledged_lsn<OLD.acknowledged_lsn THEN
    RAISE EXCEPTION 'checkpoint ingress cannot regress' USING ERRCODE='23514';
   END IF;
   SELECT published_lsn INTO STRICT published FROM checkpoint_publication WHERE subscription_id=NEW.subscription_id;
   IF NEW.staged_lsn<published THEN
    RAISE EXCEPTION 'staged frontier is behind publication' USING ERRCODE='23514';
   END IF;
  END IF;
 ELSE
  IF TG_OP='UPDATE' AND NEW.published_lsn<OLD.published_lsn THEN
   RAISE EXCEPTION 'publication frontier cannot regress' USING ERRCODE='23514';
  END IF;
  -- No ingress row lock: monotonic staging makes a committed upper-bound read safe.
  SELECT staged_lsn INTO STRICT staged FROM checkpoint_ingress WHERE subscription_id=NEW.subscription_id;
  IF NEW.published_lsn>staged THEN
   RAISE EXCEPTION 'publication frontier exceeds durable staging' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION lake_replication.enforce_checkpoint_frontier() FROM PUBLIC;
CREATE TRIGGER checkpoint_frontier BEFORE INSERT OR UPDATE ON lake_replication.checkpoint_ingress
 FOR EACH ROW EXECUTE FUNCTION lake_replication.enforce_checkpoint_frontier();
CREATE TRIGGER checkpoint_frontier BEFORE INSERT OR UPDATE ON lake_replication.checkpoint_publication
 FOR EACH ROW EXECUTE FUNCTION lake_replication.enforce_checkpoint_frontier();
CREATE TRIGGER managed_worker_fence BEFORE INSERT OR UPDATE OR DELETE ON lake_replication.checkpoint_publication
 FOR EACH STATEMENT EXECUTE FUNCTION lake_replication.fence_worker_statement();

CREATE FUNCTION lake_replication.route_checkpoint() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE i checkpoint_ingress%ROWTYPE;
BEGIN
 IF TG_OP='DELETE' THEN
  DELETE FROM checkpoint_ingress WHERE subscription_id=OLD.subscription_id;
  RETURN OLD;
 ELSIF TG_OP='INSERT' THEN
  INSERT INTO checkpoint_ingress(subscription_id,received_lsn,staged_lsn,acknowledged_lsn,updated_at)
   VALUES(NEW.subscription_id,NEW.received_lsn,NEW.staged_lsn,NEW.acknowledged_lsn,NEW.updated_at);
  INSERT INTO checkpoint_publication VALUES(NEW.subscription_id,NEW.published_lsn,NEW.updated_at);
 ELSE
  IF NEW.subscription_id IS DISTINCT FROM OLD.subscription_id THEN
   RAISE EXCEPTION 'checkpoint identity cannot change' USING ERRCODE='22023';
  END IF;
  SELECT * INTO STRICT i FROM checkpoint_ingress WHERE subscription_id=OLD.subscription_id FOR UPDATE;
  IF ROW(NEW.received_lsn,NEW.staged_lsn,NEW.acknowledged_lsn,NEW.updated_at)
     IS DISTINCT FROM ROW(OLD.received_lsn,OLD.staged_lsn,OLD.acknowledged_lsn,OLD.updated_at) THEN
   UPDATE checkpoint_ingress SET
    received_lsn=CASE WHEN NEW.received_lsn IS DISTINCT FROM OLD.received_lsn THEN NEW.received_lsn ELSE i.received_lsn END,
    staged_lsn=CASE WHEN NEW.staged_lsn IS DISTINCT FROM OLD.staged_lsn THEN NEW.staged_lsn ELSE i.staged_lsn END,
    acknowledged_lsn=CASE WHEN NEW.acknowledged_lsn IS DISTINCT FROM OLD.acknowledged_lsn THEN NEW.acknowledged_lsn ELSE i.acknowledged_lsn END,
    updated_at=CASE WHEN NEW.updated_at IS DISTINCT FROM OLD.updated_at THEN NEW.updated_at ELSE i.updated_at END
   WHERE subscription_id=OLD.subscription_id;
  END IF;
  IF NEW.published_lsn IS DISTINCT FROM OLD.published_lsn THEN
   UPDATE checkpoint_publication SET published_lsn=NEW.published_lsn,updated_at=NEW.updated_at
    WHERE subscription_id=OLD.subscription_id;
   IF NOT FOUND THEN RAISE EXCEPTION 'publication checkpoint is missing' USING ERRCODE='55000'; END IF;
  END IF;
 END IF;
 SELECT * INTO STRICT NEW FROM checkpoints WHERE subscription_id=NEW.subscription_id;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION lake_replication.route_checkpoint() FROM PUBLIC;
CREATE TRIGGER route_checkpoint INSTEAD OF INSERT OR UPDATE OR DELETE ON lake_replication.checkpoints
 FOR EACH ROW EXECUTE FUNCTION lake_replication.route_checkpoint();
CREATE TRIGGER managed_worker_fence BEFORE INSERT OR UPDATE OR DELETE ON lake_replication.checkpoints
 FOR EACH STATEMENT EXECUTE FUNCTION lake_replication.fence_worker_statement();

-- Preserve existing effective relation/column grants, including grant options.
-- Do not implicitly grant mutation rights to the management role.
DO $$ DECLARE a record; target text; who text; BEGIN
 FOR a IN SELECT x.* FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) x
  WHERE c.oid='lake_replication.checkpoint_ingress'::regclass LOOP
  who:=CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END;
  FOREACH target IN ARRAY ARRAY['checkpoint_publication','checkpoints'] LOOP
   IF a.privilege_type<>'TRUNCATE' OR target<>'checkpoints' THEN
    PERFORM lake_replication.migration_grant(format('GRANT %s ON lake_replication.%I TO %s%s',a.privilege_type,target,who,
     CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END));
   END IF;
  END LOOP;
 END LOOP;
 FOR a IN SELECT attname,x.* FROM pg_attribute CROSS JOIN LATERAL aclexplode(attacl) x
  WHERE attrelid='lake_replication.checkpoint_ingress'::regclass AND attnum>0 AND NOT attisdropped LOOP
  who:=CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END;
  PERFORM lake_replication.migration_grant(format('GRANT %s (%I) ON lake_replication.checkpoints TO %s%s',a.privilege_type,a.attname,who,
   CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END));
  IF a.attname IN ('subscription_id','published_lsn','updated_at') THEN
   PERFORM lake_replication.migration_grant(format('GRANT %s (%I) ON lake_replication.checkpoint_publication TO %s%s',a.privilege_type,a.attname,who,
    CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END));
  END IF;
 END LOOP;
END $$;

CREATE OR REPLACE FUNCTION lake_replication.stage_transaction(
    p_subscription_id bigint,
    p_xid bigint,
    p_begin_lsn pg_lsn,
    p_commit_lsn pg_lsn,
    p_end_lsn pg_lsn,
    p_commit_time timestamptz,
    p_changes jsonb
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_transaction_id bigint;
    v_existing transactions%ROWTYPE;
    v_staged_lsn pg_lsn;
BEGIN
    IF jsonb_typeof(p_changes) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'changes must be a JSON array' USING ERRCODE = '22023';
    END IF;
    IF p_begin_lsn > p_end_lsn OR p_commit_lsn > p_end_lsn THEN
        RAISE EXCEPTION 'invalid transaction LSN interval' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_changes) AS change
         WHERE change->>'operation' NOT IN ('insert', 'update', 'delete', 'truncate')
            OR jsonb_typeof(change->'remote_relation_id') IS DISTINCT FROM 'number'
            OR jsonb_typeof(change->'relation_schema_version') IS DISTINCT FROM 'number'
            OR CASE change->>'operation'
                 WHEN 'insert' THEN change ? 'old_tuple'
                     OR jsonb_typeof(change->'new_tuple') IS DISTINCT FROM 'object'
                 WHEN 'update' THEN jsonb_typeof(change->'old_tuple') IS DISTINCT FROM 'object'
                     OR jsonb_typeof(change->'new_tuple') IS DISTINCT FROM 'object'
                     OR change->'old_tuple' = '{}'::jsonb OR change->'new_tuple' = '{}'::jsonb
                 WHEN 'delete' THEN jsonb_typeof(change->'old_tuple') IS DISTINCT FROM 'object'
                     OR change ? 'new_tuple' OR change->'old_tuple' = '{}'::jsonb
                 WHEN 'truncate' THEN change ? 'old_tuple' OR change ? 'new_tuple'
                 ELSE true
               END
    ) THEN
        RAISE EXCEPTION 'invalid change envelope' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM jsonb_array_elements(p_changes) AS change
          LEFT JOIN relations r
            ON r.subscription_id = p_subscription_id
           AND r.remote_relation_id = (change->>'remote_relation_id')::oid
           AND r.schema_version = (change->>'relation_schema_version')::bigint
         WHERE r.subscription_id IS NULL
            OR EXISTS (
                SELECT 1 FROM jsonb_object_keys(
                    coalesce(change->'old_tuple', '{}'::jsonb) ||
                    coalesce(change->'new_tuple', '{}'::jsonb)
                ) AS supplied(column_name)
                WHERE NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(r.columns) AS expected(column_value)
                     WHERE expected.column_value->>'name' = supplied.column_name
                )
            )
    ) THEN
        RAISE EXCEPTION 'change references an unknown relation version or column'
            USING ERRCODE = '22023';
    END IF;

    SELECT staged_lsn INTO STRICT v_staged_lsn
      FROM checkpoint_ingress WHERE subscription_id = p_subscription_id FOR UPDATE;
    SELECT * INTO v_existing FROM transactions
     WHERE subscription_id = p_subscription_id AND commit_lsn = p_commit_lsn;
    IF FOUND THEN
        IF v_existing.xid <> p_xid OR v_existing.begin_lsn <> p_begin_lsn
           OR v_existing.end_lsn <> p_end_lsn OR v_existing.commit_time <> p_commit_time
           OR v_existing.payload <> p_changes THEN
            RAISE EXCEPTION 'conflicting replay at commit LSN %', p_commit_lsn
                USING ERRCODE = '23505';
        END IF;
        RETURN false;
    END IF;
    IF p_end_lsn <= v_staged_lsn THEN
        RAISE EXCEPTION 'new transaction end LSN % does not advance staged LSN %',
            p_end_lsn, v_staged_lsn USING ERRCODE = '22023';
    END IF;

    INSERT INTO transactions (
        subscription_id, xid, begin_lsn, commit_lsn, end_lsn, commit_time, payload
    ) VALUES (
        p_subscription_id, p_xid, p_begin_lsn, p_commit_lsn, p_end_lsn,
        p_commit_time, p_changes
    ) RETURNING transaction_id INTO v_transaction_id;
    INSERT INTO changes (
        transaction_id, change_index, remote_relation_id,
        relation_schema_version, operation, old_tuple, new_tuple
    )
    SELECT v_transaction_id, ordinality::integer - 1,
           (change->>'remote_relation_id')::oid,
           (change->>'relation_schema_version')::bigint,
           change->>'operation', change->'old_tuple', change->'new_tuple'
      FROM jsonb_array_elements(p_changes) WITH ORDINALITY AS item(change, ordinality);
    UPDATE checkpoint_ingress
       SET received_lsn = p_end_lsn, staged_lsn = p_end_lsn,
           updated_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.mark_acknowledged(
    p_subscription_id bigint,
    p_acknowledged_lsn pg_lsn
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_staged_lsn pg_lsn;
BEGIN
    SELECT staged_lsn INTO STRICT v_staged_lsn
      FROM checkpoint_ingress WHERE subscription_id = p_subscription_id FOR UPDATE;
    IF p_acknowledged_lsn > v_staged_lsn THEN
        RAISE EXCEPTION 'acknowledged LSN % exceeds staged LSN %',
            p_acknowledged_lsn, v_staged_lsn USING ERRCODE = '22023';
    END IF;
    UPDATE checkpoint_ingress
       SET acknowledged_lsn = GREATEST(acknowledged_lsn, p_acknowledged_lsn),
           updated_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.enforce_journal_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE limits journal_limits%ROWTYPE; pending bigint; used_bytes numeric;
 generation bigint:=coalesce(nullif(current_setting('lake_replication.worker_generation',true),''),'0')::bigint;
BEGIN
 NEW.journal_payload_bytes:=octet_length(NEW.payload::text);
 IF generation>0 THEN PERFORM assert_worker(NEW.subscription_id,generation); END IF;
 SELECT * INTO limits FROM journal_limits WHERE subscription_id=NEW.subscription_id;
 IF NOT FOUND OR NEW.state='published' THEN RETURN NEW; END IF;
 -- Serialize every producer with checkpoint/publication changes, not a racy worker-side count.
 PERFORM 1 FROM checkpoint_ingress WHERE subscription_id=NEW.subscription_id FOR UPDATE;
 IF NEW.journal_payload_bytes>limits.max_payload_bytes THEN
  RAISE EXCEPTION 'transaction payload exceeds journal byte limit' USING ERRCODE='22001';
 END IF;
 SELECT count(*),coalesce(sum(journal_payload_bytes),0) INTO pending,used_bytes FROM transactions
 WHERE subscription_id=NEW.subscription_id AND state<>'published' AND transaction_id<>NEW.transaction_id;
 IF pending>=limits.max_transactions OR used_bytes+NEW.journal_payload_bytes>limits.max_payload_bytes THEN
  RAISE EXCEPTION 'journal admission full' USING ERRCODE='LR001';
 END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION lake_replication.prepare_batch(
    p_subscription_id bigint,
    p_batch_key text,
    p_first_end_lsn pg_lsn,
    p_last_end_lsn pg_lsn
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_batch batches%ROWTYPE;
    v_staged_lsn pg_lsn;
BEGIN
    PERFORM 1 FROM checkpoint_publication WHERE subscription_id=p_subscription_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'publication checkpoint is missing' USING ERRCODE='55000'; END IF;
    SELECT staged_lsn INTO STRICT v_staged_lsn FROM checkpoint_ingress WHERE subscription_id=p_subscription_id;
    IF p_last_end_lsn > v_staged_lsn THEN
        RAISE EXCEPTION 'batch end LSN % exceeds staged LSN %', p_last_end_lsn, v_staged_lsn
            USING ERRCODE = '22023';
    END IF;

    SELECT * INTO v_batch FROM batches
     WHERE subscription_id = p_subscription_id AND batch_key = p_batch_key;
    IF FOUND THEN
        IF v_batch.first_end_lsn <> p_first_end_lsn
           OR v_batch.last_end_lsn <> p_last_end_lsn THEN
            RAISE EXCEPTION 'batch key % was reused with different bounds', p_batch_key
                USING ERRCODE = '23505';
        END IF;
        RETURN v_batch.batch_id;
    END IF;

    INSERT INTO batches (
        subscription_id, batch_key, first_end_lsn, last_end_lsn
    ) VALUES (
        p_subscription_id, p_batch_key, p_first_end_lsn, p_last_end_lsn
    ) RETURNING * INTO v_batch;

    UPDATE transactions
       SET state = 'publishing', batch_id = v_batch.batch_id
     WHERE subscription_id = p_subscription_id
       AND state = 'staged'
       AND end_lsn BETWEEN p_first_end_lsn AND p_last_end_lsn;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'batch % selected no staged transactions', p_batch_key
            USING ERRCODE = '22023';
    END IF;
    RETURN v_batch.batch_id;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.finish_batch(p_batch_id bigint)
RETURNS pg_lsn
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_batch batches%ROWTYPE;
BEGIN
    SELECT * INTO STRICT v_batch FROM batches WHERE batch_id = p_batch_id FOR UPDATE;
    IF v_batch.state = 'published' THEN
        RETURN v_batch.last_end_lsn;
    END IF;

    UPDATE transactions
       SET state = 'published', published_at = clock_timestamp()
     WHERE batch_id = p_batch_id AND state = 'publishing';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'batch % has no publishing transactions', p_batch_id
            USING ERRCODE = '22023';
    END IF;

    UPDATE batches SET state = 'published', published_at = clock_timestamp()
     WHERE batch_id = p_batch_id;
    UPDATE checkpoint_publication
       SET published_lsn = GREATEST(published_lsn, v_batch.last_end_lsn),
           updated_at = clock_timestamp()
     WHERE subscription_id = v_batch.subscription_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'publication checkpoint is missing' USING ERRCODE='55000'; END IF;
    RETURN v_batch.last_end_lsn;
END;
$$;

-- Monotonic frontiers permit conservative snapshot cleanup without locking ingress.
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
     WHERE subscription_id = p_subscription_id;

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

ALTER TABLE lake_replication.checkpoint_ingress DROP COLUMN published_lsn;
SELECT pg_catalog.pg_extension_config_dump('lake_replication.checkpoint_ingress','');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.checkpoint_publication','');
COMMIT;

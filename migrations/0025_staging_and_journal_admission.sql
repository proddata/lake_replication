BEGIN;

DO $$ BEGIN
 IF EXISTS(SELECT FROM lake_replication.worker_launch_config WHERE desired_state<>'stopped')
 OR EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() AND backend_type='lake_replication supervisor') THEN
  RAISE EXCEPTION 'stop subscriptions and restart with the new binaries before the staging admission upgrade' USING ERRCODE='55000';
 END IF;
END $$;

CREATE FUNCTION lake_replication.admit_staging() RETURNS void
LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME','lake_replication_admit_staging';
REVOKE ALL ON FUNCTION lake_replication.admit_staging() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.admit_staging() TO lake_replication_admin;
CREATE VIEW lake_replication.staging_admission AS
 SELECT l.pid,a.datname AS database_name,
 CASE WHEN l.granted THEN 'admitted' ELSE 'waiting_for_staging' END AS admission_state,
 l.waitstart,a.wait_event_type,a.wait_event
 FROM pg_catalog.pg_locks l LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='advisory' AND l.database=0 AND l.classid=1280460112
 AND l.objid=71 AND l.objsubid=1 AND l.mode='ExclusiveLock';
REVOKE ALL ON lake_replication.staging_admission FROM PUBLIC;
GRANT SELECT ON lake_replication.staging_admission TO lake_replication_admin;

CREATE TABLE lake_replication.journal_limits (
 subscription_id bigint PRIMARY KEY REFERENCES lake_replication.subscriptions ON DELETE CASCADE,
 max_transactions bigint NOT NULL DEFAULT 8 CHECK(max_transactions>0),
 max_payload_bytes bigint NOT NULL DEFAULT 67108864 CHECK(max_payload_bytes>0)
);
INSERT INTO lake_replication.journal_limits(subscription_id)
 SELECT subscription_id FROM lake_replication.worker_launch_config;
REVOKE ALL ON lake_replication.journal_limits FROM PUBLIC;
GRANT SELECT,INSERT,UPDATE,DELETE ON lake_replication.journal_limits TO lake_replication_admin;
SELECT pg_catalog.pg_extension_config_dump('lake_replication.journal_limits','');
ALTER TABLE lake_replication.transactions ADD COLUMN journal_payload_bytes bigint NOT NULL DEFAULT 0;
UPDATE lake_replication.transactions SET journal_payload_bytes=octet_length(payload::text);
CREATE INDEX journal_pending_admission_idx ON lake_replication.transactions(subscription_id)
 INCLUDE(journal_payload_bytes) WHERE state<>'published';

-- Lock order matches native stop: service row, then launch configuration.
-- NOWAIT refuses new work while a control transaction is stopping/replacing it.
CREATE FUNCTION lake_replication.assert_worker(p_subscription_id bigint,p_generation bigint)
RETURNS void LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,lake_replication AS $$
DECLARE desired text; generation bigint; launch_desired text;
BEGIN
 IF p_generation<=0 THEN RAISE EXCEPTION 'managed worker generation is required' USING ERRCODE='55000'; END IF;
 SELECT desired_state INTO desired FROM service_status WHERE subscription_id=p_subscription_id FOR SHARE NOWAIT;
 IF NOT FOUND OR desired<>'running' THEN
  RAISE EXCEPTION 'managed worker is stopped or obsolete' USING ERRCODE='55000';
 END IF;
 SELECT config_generation,desired_state INTO generation,launch_desired FROM worker_launch_config
 WHERE subscription_id=p_subscription_id FOR SHARE NOWAIT;
 IF NOT FOUND OR generation<>p_generation OR launch_desired<>'running' THEN
  RAISE EXCEPTION 'managed worker is stopped or obsolete' USING ERRCODE='55000';
 END IF;
 -- Lazy creation avoids generating duplicate policy rows during pg_dump restore.
 INSERT INTO journal_limits(subscription_id) VALUES(p_subscription_id) ON CONFLICT DO NOTHING;
END $$;
REVOKE ALL ON FUNCTION lake_replication.assert_worker(bigint,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.assert_worker(bigint,bigint) TO lake_replication_admin;

-- Cancel only this database/subscription's named destination worker queries.
-- Take the control row without waiting for an in-flight worker fence first.
CREATE OR REPLACE FUNCTION lake_replication.request_service_stop(p_subscription_id bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_replication AS $$
DECLARE backend record; deadline timestamptz:=clock_timestamp()+interval '5 seconds';
BEGIN
 LOOP
  FOR backend IN SELECT pid FROM pg_stat_activity WHERE datname=current_database()
   AND pid<>pg_backend_pid() AND application_name LIKE 'lake_replication_worker/'||p_subscription_id||'/%'
  LOOP PERFORM pg_cancel_backend(backend.pid); END LOOP;
  BEGIN
   PERFORM 1 FROM service_status WHERE subscription_id=p_subscription_id FOR UPDATE NOWAIT;
   EXIT;
  EXCEPTION WHEN lock_not_available THEN
   IF clock_timestamp()>=deadline THEN
    RAISE EXCEPTION 'managed queries did not drain for stop' USING ERRCODE='55000';
   END IF;
   PERFORM pg_sleep(0.05);
  END;
 END LOOP;
 UPDATE service_status SET desired_state='stopped',heartbeat_at=clock_timestamp()
 WHERE subscription_id=p_subscription_id;
END $$;
REVOKE ALL ON FUNCTION lake_replication.request_service_stop(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.request_service_stop(bigint) TO lake_replication_admin;

CREATE FUNCTION lake_replication.fence_worker_statement() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
DECLARE generation bigint:=coalesce(nullif(current_setting('lake_replication.worker_generation',true),''),'0')::bigint;
BEGIN
 IF generation>0 THEN
  PERFORM assert_worker(current_setting('lake_replication.worker_subscription')::bigint,generation);
 END IF;
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION lake_replication.fence_worker_statement() FROM PUBLIC;
DO $$ DECLARE relation text; BEGIN
 FOREACH relation IN ARRAY ARRAY['initial_copies','initial_copy_rows','relations','relation_type_contracts',
  'checkpoints','batches','streamed_transactions','streamed_changes'] LOOP
  EXECUTE format('CREATE TRIGGER managed_worker_fence BEFORE INSERT OR UPDATE OR DELETE ON lake_replication.%I FOR EACH STATEMENT EXECUTE FUNCTION lake_replication.fence_worker_statement()',relation);
 END LOOP;
END $$;

CREATE FUNCTION lake_replication.enforce_journal_admission() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
DECLARE limits journal_limits%ROWTYPE; pending bigint; used_bytes numeric;
 generation bigint:=coalesce(nullif(current_setting('lake_replication.worker_generation',true),''),'0')::bigint;
BEGIN
 NEW.journal_payload_bytes:=octet_length(NEW.payload::text);
 IF generation>0 THEN PERFORM assert_worker(NEW.subscription_id,generation); END IF;
 SELECT * INTO limits FROM journal_limits WHERE subscription_id=NEW.subscription_id;
 IF NOT FOUND OR NEW.state='published' THEN RETURN NEW; END IF;
 -- Serialize every producer with checkpoint/publication changes, not a racy worker-side count.
 PERFORM 1 FROM checkpoints WHERE subscription_id=NEW.subscription_id FOR UPDATE;
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
REVOKE ALL ON FUNCTION lake_replication.enforce_journal_admission() FROM PUBLIC;
CREATE TRIGGER journal_admission BEFORE INSERT OR UPDATE OF payload ON lake_replication.transactions
 FOR EACH ROW EXECUTE FUNCTION lake_replication.enforce_journal_admission();

CREATE FUNCTION lake_replication.try_stage_transaction(bigint,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
BEGIN
 PERFORM stage_transaction($1,$2,$3,$4,$5,$6,$7);
 RETURN true;
EXCEPTION WHEN SQLSTATE 'LR001' THEN RETURN false;
END $$;
CREATE FUNCTION lake_replication.try_commit_stream(bigint,bigint,pg_lsn,pg_lsn,timestamptz)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
BEGIN
 RETURN commit_stream($1,$2,$3,$4,$5);
EXCEPTION WHEN SQLSTATE 'LR001' THEN RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION lake_replication.try_stage_transaction(bigint,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION lake_replication.try_commit_stream(bigint,bigint,pg_lsn,pg_lsn,timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.try_stage_transaction(bigint,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb) TO lake_replication_admin;
GRANT EXECUTE ON FUNCTION lake_replication.try_commit_stream(bigint,bigint,pg_lsn,pg_lsn,timestamptz) TO lake_replication_admin;
CREATE VIEW lake_replication.journal_admission AS
 SELECT p.subscription_id,p.max_transactions,p.max_payload_bytes,
 count(t.transaction_id) AS pending_transactions,coalesce(sum(t.journal_payload_bytes),0) AS pending_payload_bytes
 FROM lake_replication.journal_limits p LEFT JOIN lake_replication.transactions t
 ON t.subscription_id=p.subscription_id AND t.state<>'published'
 GROUP BY p.subscription_id,p.max_transactions,p.max_payload_bytes;
REVOKE ALL ON lake_replication.journal_admission FROM PUBLIC;
GRANT SELECT ON lake_replication.journal_admission TO lake_replication_admin;
COMMENT ON VIEW lake_replication.journal_admission IS
 'Managed committed unpublished journal payload/count admission; excludes uncommitted streamed segments and physical storage overhead';
COMMIT;

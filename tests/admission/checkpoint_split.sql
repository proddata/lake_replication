-- Disposable diagnostic overlay, never an extension upgrade or shipped SQL.
BEGIN;
ALTER TABLE lake_replication.checkpoints RENAME TO checkpoint_ingress;
CREATE TABLE lake_replication.checkpoint_publication (
 subscription_id bigint PRIMARY KEY REFERENCES lake_replication.subscriptions,
 published_lsn pg_lsn NOT NULL DEFAULT '0/0',
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO lake_replication.checkpoint_publication
 SELECT subscription_id,published_lsn,updated_at FROM lake_replication.checkpoint_ingress;
CREATE TRIGGER managed_worker_fence BEFORE INSERT OR UPDATE OR DELETE
 ON lake_replication.checkpoint_publication FOR EACH STATEMENT
 EXECUTE FUNCTION lake_replication.fence_worker_statement();

CREATE VIEW lake_replication.checkpoints AS
 SELECT i.subscription_id,i.received_lsn,i.staged_lsn,p.published_lsn,i.acknowledged_lsn,
 greatest(i.updated_at,p.updated_at) AS updated_at
 FROM lake_replication.checkpoint_ingress i
 JOIN lake_replication.checkpoint_publication p USING(subscription_id);

-- Compatibility for create/drop and stopped initial-copy promotion. Hot-path
-- receiver and publisher routines below use their own physical rows directly.
CREATE FUNCTION lake_replication.prototype_route_checkpoint() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  DELETE FROM checkpoint_publication WHERE subscription_id=OLD.subscription_id;
  DELETE FROM checkpoint_ingress WHERE subscription_id=OLD.subscription_id;
  RETURN OLD;
 END IF;
 NEW.received_lsn:=coalesce(NEW.received_lsn,'0/0');
 NEW.staged_lsn:=coalesce(NEW.staged_lsn,'0/0');
 NEW.published_lsn:=coalesce(NEW.published_lsn,'0/0');
 NEW.acknowledged_lsn:=coalesce(NEW.acknowledged_lsn,'0/0');
 NEW.updated_at:=coalesce(NEW.updated_at,clock_timestamp());
 IF NEW.received_lsn<NEW.staged_lsn OR NEW.staged_lsn<NEW.published_lsn
    OR NEW.staged_lsn<NEW.acknowledged_lsn THEN
  RAISE EXCEPTION 'invalid split checkpoint frontier' USING ERRCODE='23514';
 END IF;
 IF TG_OP='INSERT' THEN
  INSERT INTO checkpoint_ingress(subscription_id,received_lsn,staged_lsn,acknowledged_lsn,updated_at)
   VALUES(NEW.subscription_id,NEW.received_lsn,NEW.staged_lsn,NEW.acknowledged_lsn,NEW.updated_at);
  INSERT INTO checkpoint_publication VALUES(NEW.subscription_id,NEW.published_lsn,NEW.updated_at);
 ELSE
  IF NEW.subscription_id<>OLD.subscription_id THEN RAISE EXCEPTION 'cannot change checkpoint identity'; END IF;
  IF ROW(NEW.received_lsn,NEW.staged_lsn,NEW.acknowledged_lsn)
       IS DISTINCT FROM ROW(OLD.received_lsn,OLD.staged_lsn,OLD.acknowledged_lsn) THEN
   UPDATE checkpoint_ingress SET received_lsn=NEW.received_lsn,staged_lsn=NEW.staged_lsn,
    acknowledged_lsn=NEW.acknowledged_lsn,updated_at=NEW.updated_at WHERE subscription_id=NEW.subscription_id;
  END IF;
  IF NEW.published_lsn IS DISTINCT FROM OLD.published_lsn THEN
   UPDATE checkpoint_publication SET published_lsn=NEW.published_lsn,updated_at=NEW.updated_at
    WHERE subscription_id=NEW.subscription_id;
  END IF;
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION lake_replication.prototype_route_checkpoint() FROM PUBLIC;
CREATE TRIGGER prototype_route_checkpoint INSTEAD OF INSERT OR UPDATE OR DELETE
 ON lake_replication.checkpoints FOR EACH ROW EXECUTE FUNCTION lake_replication.prototype_route_checkpoint();
GRANT SELECT,INSERT,UPDATE,DELETE ON lake_replication.checkpoints,
 lake_replication.checkpoint_publication TO lake_replication_admin;

DO $$ DECLARE f record; definition text; old_lock text;
BEGIN
 FOR f IN SELECT p.oid,p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='lake_replication' AND p.proname IN
 ('stage_transaction','mark_acknowledged','enforce_journal_admission','prepare_batch','finish_batch') LOOP
  definition:=pg_get_functiondef(f.oid);
  IF f.proname='prepare_batch' THEN
   old_lock:='SELECT staged_lsn INTO STRICT v_staged_lsn'||chr(10)||
    '      FROM checkpoints WHERE subscription_id = p_subscription_id FOR UPDATE;';
   IF strpos(definition,old_lock)=0 THEN RAISE EXCEPTION 'prepare_batch lock hook changed'; END IF;
   definition:=replace(definition,old_lock,
    'PERFORM 1 FROM checkpoint_publication WHERE subscription_id=p_subscription_id FOR UPDATE;'||chr(10)||
    '    SELECT staged_lsn INTO STRICT v_staged_lsn FROM checkpoint_ingress WHERE subscription_id=p_subscription_id;');
  ELSIF f.proname='finish_batch' THEN
   definition:=replace(definition,'UPDATE checkpoints','UPDATE checkpoint_publication');
  ELSE
   definition:=replace(definition,'FROM checkpoints','FROM checkpoint_ingress');
   definition:=replace(definition,'UPDATE checkpoints','UPDATE checkpoint_ingress');
  END IF;
  EXECUTE definition;
 END LOOP;
 -- Stored views bind relation OIDs; redirect original checkpoint dependencies
 -- to the combined view rather than the renamed ingress table's stale column.
 FOR f IN SELECT c.oid,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='lake_replication' AND c.relkind='v' AND c.relname<>'checkpoints' LOOP
  definition:=pg_get_viewdef(f.oid,true);
  IF strpos(definition,'checkpoint_ingress')>0 THEN
   EXECUTE format('CREATE OR REPLACE VIEW lake_replication.%I AS %s',f.relname,
    replace(definition,'checkpoint_ingress','checkpoints'));
  END IF;
 END LOOP;
END $$;
COMMIT;

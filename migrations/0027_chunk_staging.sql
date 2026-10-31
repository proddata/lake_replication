BEGIN;

-- Keep old staged rows and their configuration-dump OID. Native appends use
-- chunks; the logical row API reads both stores and supports existing row DML.
ALTER TABLE lake_replication.initial_copy_rows RENAME TO initial_copy_legacy_rows;
CREATE FUNCTION lake_replication.valid_staging_chunk(rows jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SET search_path=pg_catalog AS $$
 SELECT CASE WHEN jsonb_typeof(rows)='array' THEN
  jsonb_array_length(rows)>0 AND NOT EXISTS
   (SELECT 1 FROM jsonb_array_elements(rows) r WHERE jsonb_typeof(r)<>'object')
 ELSE false END
$$;
REVOKE ALL ON FUNCTION lake_replication.valid_staging_chunk(jsonb) FROM PUBLIC;
-- CHECK execution needs this predicate for callers with explicitly granted DML.
GRANT EXECUTE ON FUNCTION lake_replication.valid_staging_chunk(jsonb) TO PUBLIC;
CREATE TABLE lake_replication.initial_copy_chunks (
 subscription_id bigint NOT NULL,
 remote_relation_id oid NOT NULL,
 relation_schema_version bigint NOT NULL,
 row_ordinal bigint NOT NULL CHECK(row_ordinal>=0),
 rows jsonb NOT NULL CHECK(lake_replication.valid_staging_chunk(rows)),
 CHECK(row_ordinal<=9223372036854775807-(jsonb_array_length(rows)-1)),
 row_span int8range GENERATED ALWAYS AS
  (int8range(row_ordinal,CASE WHEN row_ordinal>9223372036854775807-jsonb_array_length(rows)
   THEN NULL ELSE row_ordinal+jsonb_array_length(rows) END,'[)')) STORED,
 PRIMARY KEY(subscription_id,remote_relation_id,relation_schema_version,row_ordinal),
 FOREIGN KEY(subscription_id,remote_relation_id,relation_schema_version)
  REFERENCES lake_replication.initial_copies ON DELETE CASCADE,
 EXCLUDE USING gist(subscription_id WITH =,remote_relation_id WITH =,
  relation_schema_version WITH =,row_span WITH &&)
);
CREATE TRIGGER managed_worker_fence BEFORE INSERT OR UPDATE OR DELETE ON lake_replication.initial_copy_chunks
 FOR EACH STATEMENT EXECUTE FUNCTION lake_replication.fence_worker_statement();

CREATE FUNCTION lake_replication.guard_staging_store() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE parent record;
BEGIN
 -- A real parent version change prevents write skew under repeatable-read as
 -- well as serializing read-committed writers. Lock-only guards are insufficient.
 FOR parent IN SELECT DISTINCT s,r,v FROM (VALUES
  (CASE WHEN TG_OP<>'INSERT' THEN OLD.subscription_id END,CASE WHEN TG_OP<>'INSERT' THEN OLD.remote_relation_id END,CASE WHEN TG_OP<>'INSERT' THEN OLD.relation_schema_version END),
  (CASE WHEN TG_OP<>'DELETE' THEN NEW.subscription_id END,CASE WHEN TG_OP<>'DELETE' THEN NEW.remote_relation_id END,CASE WHEN TG_OP<>'DELETE' THEN NEW.relation_schema_version END)) keys(s,r,v)
  WHERE s IS NOT NULL ORDER BY s,r,v LOOP
  UPDATE initial_copies SET rows_copied=rows_copied
   WHERE subscription_id=parent.s AND remote_relation_id=parent.r AND relation_schema_version=parent.v;
  IF NOT FOUND AND TG_OP<>'DELETE' THEN
   RAISE EXCEPTION 'initial-copy parent is missing' USING ERRCODE='23503';
  END IF;
 END LOOP;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 IF TG_TABLE_NAME='initial_copy_chunks' THEN
  IF EXISTS(SELECT FROM initial_copy_legacy_rows WHERE subscription_id=NEW.subscription_id
   AND remote_relation_id=NEW.remote_relation_id AND relation_schema_version=NEW.relation_schema_version
   AND row_ordinal>=NEW.row_ordinal AND row_ordinal-NEW.row_ordinal<jsonb_array_length(NEW.rows)) THEN
   RAISE EXCEPTION 'initial-copy ordinal overlaps a staged row' USING ERRCODE='23505';
  END IF;
 ELSE
  IF EXISTS(SELECT FROM initial_copy_chunks WHERE subscription_id=NEW.subscription_id
   AND remote_relation_id=NEW.remote_relation_id AND relation_schema_version=NEW.relation_schema_version
   AND row_span @> NEW.row_ordinal) THEN
   RAISE EXCEPTION 'initial-copy ordinal overlaps a staged chunk' USING ERRCODE='23505';
  END IF;
 END IF;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION lake_replication.guard_staging_store() FROM PUBLIC;
CREATE TRIGGER staging_store_guard BEFORE INSERT OR UPDATE ON lake_replication.initial_copy_legacy_rows
 FOR EACH ROW EXECUTE FUNCTION lake_replication.guard_staging_store();
CREATE TRIGGER staging_store_guard BEFORE INSERT OR UPDATE ON lake_replication.initial_copy_chunks
 FOR EACH ROW EXECUTE FUNCTION lake_replication.guard_staging_store();

CREATE VIEW lake_replication.initial_copy_rows AS
 SELECT subscription_id,remote_relation_id,relation_schema_version,row_ordinal,row_data
 FROM lake_replication.initial_copy_legacy_rows
 UNION ALL
 SELECT subscription_id,remote_relation_id,relation_schema_version,
  row_ordinal+(ordinality-1) AS row_ordinal,row_data
 FROM lake_replication.initial_copy_chunks
 CROSS JOIN LATERAL jsonb_array_elements(rows) WITH ORDINALITY AS source(row_data,ordinality);

CREATE FUNCTION lake_replication.route_staging_row() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE parent record; chunk initial_copy_chunks%ROWTYPE; current_data jsonb;
BEGIN
 IF TG_OP='INSERT' THEN
  INSERT INTO initial_copy_legacy_rows VALUES(NEW.subscription_id,NEW.remote_relation_id,
   NEW.relation_schema_version,NEW.row_ordinal,NEW.row_data);
  RETURN NEW;
 END IF;
 FOR parent IN SELECT DISTINCT s,r,v FROM (VALUES
  (OLD.subscription_id,OLD.remote_relation_id,OLD.relation_schema_version),
  (CASE WHEN TG_OP='UPDATE' THEN NEW.subscription_id END,CASE WHEN TG_OP='UPDATE' THEN NEW.remote_relation_id END,CASE WHEN TG_OP='UPDATE' THEN NEW.relation_schema_version END)) keys(s,r,v)
  WHERE s IS NOT NULL ORDER BY s,r,v LOOP
  UPDATE initial_copies SET rows_copied=rows_copied
   WHERE subscription_id=parent.s AND remote_relation_id=parent.r AND relation_schema_version=parent.v;
 END LOOP;
 SELECT row_data INTO current_data FROM initial_copy_rows WHERE subscription_id=OLD.subscription_id
  AND remote_relation_id=OLD.remote_relation_id AND relation_schema_version=OLD.relation_schema_version AND row_ordinal=OLD.row_ordinal;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF current_data IS DISTINCT FROM OLD.row_data THEN
  RAISE EXCEPTION 'staged row changed concurrently; retry the transaction' USING ERRCODE='40001';
 END IF;
 SELECT * INTO chunk FROM initial_copy_chunks WHERE subscription_id=OLD.subscription_id
  AND remote_relation_id=OLD.remote_relation_id AND relation_schema_version=OLD.relation_schema_version
  AND row_span @> OLD.row_ordinal FOR UPDATE;
 IF FOUND THEN
  -- Copy-on-write is only for legacy row edits, never the native append path.
  DELETE FROM initial_copy_chunks WHERE subscription_id=chunk.subscription_id
   AND remote_relation_id=chunk.remote_relation_id AND relation_schema_version=chunk.relation_schema_version AND row_ordinal=chunk.row_ordinal;
  INSERT INTO initial_copy_legacy_rows
   SELECT chunk.subscription_id,chunk.remote_relation_id,chunk.relation_schema_version,
    chunk.row_ordinal+(ordinality-1),row_data FROM jsonb_array_elements(chunk.rows) WITH ORDINALITY source(row_data,ordinality);
 END IF;
 IF TG_OP='DELETE' THEN
  DELETE FROM initial_copy_legacy_rows WHERE subscription_id=OLD.subscription_id
   AND remote_relation_id=OLD.remote_relation_id AND relation_schema_version=OLD.relation_schema_version AND row_ordinal=OLD.row_ordinal;
  RETURN OLD;
 END IF;
 UPDATE initial_copy_legacy_rows SET subscription_id=NEW.subscription_id,remote_relation_id=NEW.remote_relation_id,
  relation_schema_version=NEW.relation_schema_version,row_ordinal=NEW.row_ordinal,row_data=NEW.row_data
  WHERE subscription_id=OLD.subscription_id AND remote_relation_id=OLD.remote_relation_id
   AND relation_schema_version=OLD.relation_schema_version AND row_ordinal=OLD.row_ordinal;
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION lake_replication.route_staging_row() FROM PUBLIC;
CREATE TRIGGER route_staging_row INSTEAD OF INSERT OR UPDATE OR DELETE ON lake_replication.initial_copy_rows
 FOR EACH ROW EXECUTE FUNCTION lake_replication.route_staging_row();
CREATE TRIGGER managed_worker_fence BEFORE INSERT OR UPDATE OR DELETE ON lake_replication.initial_copy_rows
 FOR EACH STATEMENT EXECUTE FUNCTION lake_replication.fence_worker_statement();

CREATE OR REPLACE FUNCTION lake_replication.append_initial_copy_rows(
 p_subscription_id bigint,p_remote_relation_id oid,p_schema_version bigint,p_start_ordinal bigint,p_rows jsonb
) RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE v_state text; v_count bigint;
BEGIN
 IF p_start_ordinal<0 OR jsonb_typeof(p_rows) IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'invalid initial-copy row chunk' USING ERRCODE='22023';
 END IF;
 IF EXISTS(SELECT FROM jsonb_array_elements(p_rows) r WHERE jsonb_typeof(r)<>'object') THEN
  RAISE EXCEPTION 'initial-copy rows must be JSON objects' USING ERRCODE='22023';
 END IF;
 SELECT state INTO STRICT v_state FROM initial_copies WHERE subscription_id=p_subscription_id
  AND remote_relation_id=p_remote_relation_id AND relation_schema_version=p_schema_version FOR UPDATE;
 IF v_state<>'copying' THEN
  RAISE EXCEPTION 'initial copy is not accepting rows (state %)',v_state USING ERRCODE='55000';
 END IF;
 v_count:=jsonb_array_length(p_rows);
 IF v_count>0 THEN
  INSERT INTO initial_copy_chunks(subscription_id,remote_relation_id,relation_schema_version,row_ordinal,rows)
   VALUES(p_subscription_id,p_remote_relation_id,p_schema_version,p_start_ordinal,p_rows);
 END IF;
 UPDATE initial_copies SET rows_copied=rows_copied+v_count WHERE subscription_id=p_subscription_id
  AND remote_relation_id=p_remote_relation_id AND relation_schema_version=p_schema_version;
 RETURN v_count;
END $$;

DO $$ DECLARE a record; target text; who text; col name; BEGIN
 FOR a IN SELECT x.* FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) x
  WHERE c.oid='lake_replication.initial_copy_legacy_rows'::regclass LOOP
  who:=CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END;
  FOREACH target IN ARRAY ARRAY['initial_copy_chunks','initial_copy_rows'] LOOP
   IF a.privilege_type<>'TRUNCATE' OR target<>'initial_copy_rows' THEN
    PERFORM lake_replication.migration_grant(format('GRANT %s ON lake_replication.%I TO %s%s',a.privilege_type,target,who,
     CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END));
   END IF;
  END LOOP;
 END LOOP;
 FOR a IN SELECT attname,x.* FROM pg_attribute CROSS JOIN LATERAL aclexplode(attacl) x
  WHERE attrelid='lake_replication.initial_copy_legacy_rows'::regclass AND attnum>0 AND NOT attisdropped LOOP
  who:=CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) END;
  FOREACH target IN ARRAY ARRAY['initial_copy_chunks','initial_copy_rows'] LOOP
   col:=CASE WHEN target='initial_copy_chunks' AND a.attname='row_data' THEN 'rows' ELSE a.attname END;
   PERFORM lake_replication.migration_grant(format('GRANT %s (%I) ON lake_replication.%I TO %s%s',a.privilege_type,col,target,who,
    CASE WHEN a.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END));
  END LOOP;
 END LOOP;
END $$;
SELECT pg_catalog.pg_extension_config_dump('lake_replication.initial_copy_legacy_rows','');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.initial_copy_chunks','');
COMMIT;

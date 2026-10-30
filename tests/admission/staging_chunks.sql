-- Disposable physical staging experiment. Fresh empty fixtures only.
-- The legacy row view is read-only; this is not an upgrade migration.
BEGIN;
CREATE EXTENSION IF NOT EXISTS btree_gist;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM lake_replication.initial_copy_rows) THEN
  RAISE EXCEPTION 'chunk prototype requires empty staging';
 END IF;
END $$;
ALTER TABLE lake_replication.initial_copy_rows RENAME TO initial_copy_rows_per_row;
ALTER TABLE lake_replication.initial_copy_rows_per_row ADD CONSTRAINT prototype_unused_rows CHECK(false);

CREATE FUNCTION lake_replication.valid_staging_chunk(rows jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SET search_path=pg_catalog AS $$
 SELECT CASE WHEN jsonb_typeof(rows)='array' THEN
  jsonb_array_length(rows) BETWEEN 1 AND 1000 AND NOT EXISTS
   (SELECT 1 FROM jsonb_array_elements(rows) r WHERE jsonb_typeof(r)<>'object')
 ELSE false END
$$;
REVOKE ALL ON FUNCTION lake_replication.valid_staging_chunk(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.valid_staging_chunk(jsonb) TO lake_replication_admin;
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
CREATE VIEW lake_replication.initial_copy_rows AS
 SELECT subscription_id,remote_relation_id,relation_schema_version,
  row_ordinal+(ordinality-1) AS row_ordinal,row_data
 FROM lake_replication.initial_copy_chunks
 CROSS JOIN LATERAL jsonb_array_elements(rows) WITH ORDINALITY AS source(row_data,ordinality);
GRANT SELECT,INSERT,UPDATE,DELETE ON lake_replication.initial_copy_chunks TO lake_replication_admin;
GRANT SELECT ON lake_replication.initial_copy_rows TO lake_replication_admin;
ALTER EXTENSION lake_replication ADD FUNCTION lake_replication.valid_staging_chunk(jsonb);
ALTER EXTENSION lake_replication ADD TABLE lake_replication.initial_copy_chunks;
ALTER EXTENSION lake_replication ADD VIEW lake_replication.initial_copy_rows;

CREATE OR REPLACE FUNCTION lake_replication.append_initial_copy_rows(
 p_subscription_id bigint,p_remote_relation_id oid,p_schema_version bigint,p_start_ordinal bigint,p_rows jsonb
) RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
DECLARE v_state text; v_count bigint;
BEGIN
 IF p_start_ordinal<0 OR jsonb_typeof(p_rows) IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'invalid initial-copy row chunk' USING ERRCODE='22023';
 END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(p_rows) r WHERE jsonb_typeof(r)<>'object') THEN
  RAISE EXCEPTION 'initial-copy rows must be JSON objects' USING ERRCODE='22023';
 END IF;
 SELECT state INTO STRICT v_state FROM initial_copies
  WHERE subscription_id=p_subscription_id AND remote_relation_id=p_remote_relation_id
   AND relation_schema_version=p_schema_version FOR UPDATE;
 IF v_state<>'copying' THEN
  RAISE EXCEPTION 'initial copy is not accepting rows (state %)',v_state USING ERRCODE='55000';
 END IF;
 v_count:=jsonb_array_length(p_rows);
 IF v_count>0 THEN
  INSERT INTO initial_copy_chunks(subscription_id,remote_relation_id,relation_schema_version,row_ordinal,rows)
   VALUES(p_subscription_id,p_remote_relation_id,p_schema_version,p_start_ordinal,p_rows);
 END IF;
 UPDATE initial_copies SET rows_copied=rows_copied+v_count
  WHERE subscription_id=p_subscription_id AND remote_relation_id=p_remote_relation_id
   AND relation_schema_version=p_schema_version;
 RETURN v_count;
END $$;
COMMIT;

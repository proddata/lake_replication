-- Disposable, manually operated prototype. Not installed by release bundles.
BEGIN;
CREATE SCHEMA lake_direct;
REVOKE ALL ON SCHEMA lake_direct FROM PUBLIC;
CREATE TABLE lake_direct.stream (
 id integer PRIMARY KEY CHECK(id=1), slot_name name NOT NULL, publication name NOT NULL,
 system_identifier text NOT NULL, database_oid oid NOT NULL, timeline bigint NOT NULL,
 published_lsn pg_lsn NOT NULL, last_digest text, commits bigint NOT NULL DEFAULT 0, publications bigint NOT NULL DEFAULT 0, replay_lsn pg_lsn,
 last_large_rows bigint,last_large_bytes bigint,last_large_chunks bigint
);
CREATE TABLE lake_direct.mapping (
 source_relation regclass PRIMARY KEY, destination_relation regclass UNIQUE NOT NULL,
 descriptor jsonb NOT NULL, source_shape jsonb NOT NULL, destination_shape jsonb NOT NULL
);
CREATE FUNCTION lake_direct.shape(p_relation regclass) RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT jsonb_build_array(c.relkind,c.relreplident,
  (SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.atttypid,a.atttypmod,
    a.attcollation,a.attnotnull,a.attgenerated,a.attidentity,a.attisdropped,
    pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum)
   FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
   WHERE a.attrelid=c.oid AND a.attnum>0),
  (SELECT jsonb_agg(pg_get_constraintdef(oid) ORDER BY oid) FROM pg_constraint WHERE conrelid=c.oid))
 FROM pg_class c WHERE c.oid=p_relation
$$;
CREATE FUNCTION lake_direct.descriptor(p_relation regclass) RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT jsonb_build_object('namespace',n.nspname,'name',c.relname,'replica_identity',c.relreplident,
 'columns',(SELECT jsonb_agg(jsonb_build_object('name',a.attname,'type_oid',a.atttypid::bigint,
 'type_modifier',a.atttypmod,'flags',CASE WHEN c.relreplident='f' OR EXISTS(
 SELECT FROM pg_index i WHERE i.indrelid=c.oid AND i.indisreplident AND a.attnum=ANY(i.indkey))
 OR (c.relreplident='d' AND EXISTS(SELECT FROM pg_index i WHERE i.indrelid=c.oid AND i.indisprimary AND a.attnum=ANY(i.indkey))) THEN 1 ELSE 0 END)
 ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped))
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=p_relation
$$;
CREATE FUNCTION lake_direct.apply_payload(p_changes jsonb) RETURNS bigint
LANGUAGE plpgsql SET search_path=pg_catalog,lake_direct AS $$
DECLARE
    v_mapping record;
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
    v_group_inserts boolean;

BEGIN
 IF jsonb_typeof(p_changes) IS DISTINCT FROM 'array' OR jsonb_array_length(p_changes)>4096
    OR octet_length(p_changes::text)>8388608 THEN RAISE EXCEPTION 'invalid or oversized payload chunk'; END IF;
    -- Existing native guard locks pg_lake foreign targets (SQL LOCK TABLE
    -- cannot lock a foreign table), and restricts them to qualified local-catalog
    -- scalar targets without defaults/triggers/rules/checks.
    IF NOT lake_replication.lock_insert_group_targets(
        (SELECT array_agg(destination_relation) FROM lake_direct.mapping)) THEN
        RAISE EXCEPTION 'direct prototype target is outside the qualified subset';
    END IF;
    -- Pin source shapes against concurrent DDL before checking them.
    FOR v_mapping IN SELECT * FROM lake_direct.mapping ORDER BY source_relation::oid LOOP
        EXECUTE format('LOCK TABLE %s IN ACCESS SHARE MODE',v_mapping.source_relation);
        IF lake_direct.shape(v_mapping.source_relation) IS DISTINCT FROM v_mapping.source_shape
           OR lake_direct.shape(v_mapping.destination_relation) IS DISTINCT FROM v_mapping.destination_shape THEN
            RAISE EXCEPTION 'frozen schema changed';
        END IF;
    END LOOP;
    IF EXISTS(SELECT FROM jsonb_array_elements(p_changes) c
        LEFT JOIN lake_direct.mapping m ON m.source_relation=(c->>'remote_relation_id')::oid
        WHERE m.source_relation IS NULL OR c->>'operation' NOT IN ('insert','update','delete','truncate')
           OR c->>'operation' IS NULL) THEN RAISE EXCEPTION 'unknown relation or operation'; END IF;
    v_group_inserts := coalesce(current_setting('lake_replication.group_independent_inserts',true),'off')::boolean
        AND NOT EXISTS(SELECT FROM jsonb_array_elements(p_changes) c WHERE c->>'operation'<>'insert');
    FOR v_change IN
        SELECT c->>'operation' AS operation,c->'old_tuple' AS old_tuple,c->'new_tuple' AS new_tuple,
               m.destination_relation FROM jsonb_array_elements(p_changes) WITH ORDINALITY x(c,seq)
        JOIN lake_direct.mapping m ON m.source_relation=(c->>'remote_relation_id')::oid ORDER BY CASE WHEN v_group_inserts THEN m.destination_relation::oid ELSE 0::oid END,seq
    LOOP
        IF v_change.operation = 'truncate' THEN
            IF v_run_operation IS NOT NULL THEN
                v_affected_rows := v_affected_rows + lake_replication.apply_change_run(
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
            v_affected_rows := v_affected_rows + lake_replication.apply_change_run(
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
        v_affected_rows := v_affected_rows + lake_replication.apply_change_run(
            v_run_operation, v_run_destination, v_run_old, v_run_new);
    END IF;

 RETURN v_affected_rows;
END $$;
CREATE FUNCTION lake_direct.publish(p_previous pg_lsn,p_commit pg_lsn,p_end pg_lsn,p_changes jsonb,p_transactions bigint DEFAULT 1)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,lake_direct AS $$
DECLARE
    state lake_direct.stream%ROWTYPE;

BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user) THEN
        RAISE EXCEPTION 'prototype requires an explicit superuser';
    END IF;
    SELECT * INTO STRICT state FROM lake_direct.stream WHERE id=1 FOR UPDATE;
    IF state.system_identifier<>(pg_control_system()).system_identifier::text
       OR state.database_oid<>(SELECT oid FROM pg_database WHERE datname=current_database())
       OR state.timeline<>(pg_control_checkpoint()).timeline_id THEN
        RAISE EXCEPTION 'source identity or timeline changed';
    END IF;
    IF jsonb_typeof(p_changes) IS DISTINCT FROM 'array' OR jsonb_array_length(p_changes)>4096
       OR octet_length(p_changes::text)>8388608 OR p_commit>=p_end OR p_transactions NOT BETWEEN 1 AND 32 THEN
        RAISE EXCEPTION 'invalid or oversized direct batch';
    END IF;
    IF p_end=state.published_lsn AND state.last_digest=md5(p_changes::text) THEN RETURN false; END IF;
    IF p_previous<>state.published_lsn OR p_end<=state.published_lsn THEN
        RAISE EXCEPTION 'stale or conflicting publication boundary';
    END IF;
    PERFORM lake_direct.apply_payload(p_changes);

    UPDATE lake_direct.stream SET published_lsn=p_end,last_digest=md5(p_changes::text),commits=commits+p_transactions,publications=publications+1,replay_lsn=CASE WHEN replay_lsn IS NULL THEN NULL ELSE p_end END WHERE id=1;
    RETURN true;
END $$;
CREATE FUNCTION lake_direct.finish_large(p_previous pg_lsn,p_commit pg_lsn,p_end pg_lsn,
 p_rows bigint,p_bytes bigint,p_chunks bigint,p_digest text) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,lake_direct AS $$
DECLARE state lake_direct.stream%ROWTYPE;
BEGIN
 SELECT * INTO STRICT state FROM lake_direct.stream WHERE id=1 FOR UPDATE;
 IF state.system_identifier<>(pg_control_system()).system_identifier::text
    OR state.database_oid<>(SELECT oid FROM pg_database WHERE datname=current_database())
    OR state.timeline<>(pg_control_checkpoint()).timeline_id
    OR state.published_lsn<>p_previous OR p_commit<p_previous OR p_end<=p_commit
    OR p_rows<0 OR p_bytes<0 OR p_chunks<0 OR p_digest!~'^[0-9a-f]{64}$' THEN
  RAISE EXCEPTION 'invalid large transaction receipt';
 END IF;
 UPDATE lake_direct.stream SET published_lsn=p_end,last_digest='sha256:'||p_digest,
  commits=commits+1,publications=publications+1,last_large_rows=p_rows,last_large_bytes=p_bytes,
  last_large_chunks=p_chunks WHERE id=1;
END $$;
-- Imported logical-slot snapshot, direct heap scan -> Iceberg. This is bounded
-- bootstrap in one transaction, not a general resumable large-copy pipeline.
CREATE TABLE lake_direct.snapshot_tables (
 source_relation oid PRIMARY KEY, rows_copied bigint NOT NULL, input_bytes bigint NOT NULL
);
CREATE TABLE lake_direct.snapshot_state (
 id integer PRIMARY KEY CHECK(id=1),snapshot_name text NOT NULL,consistent_lsn pg_lsn NOT NULL,
 rows_copied bigint NOT NULL,input_bytes bigint NOT NULL,table_count bigint NOT NULL
);
CREATE FUNCTION lake_direct.bootstrap(
 p_slot name,p_publication name,p_target_schema name,p_lsn pg_lsn,p_snapshot text,
 p_system text,p_timeline bigint,p_max_rows bigint,p_max_bytes bigint,p_table_pause_ms bigint
) RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,lake_direct AS $$
DECLARE
 pub pg_publication%ROWTYPE;
 src record;
 slot record;
 columns_sql text;
 copied bigint;
 sized bigint;
 total_rows bigint:=0;
 total_bytes bigint:=0;
 table_count bigint;
 destination regclass;
BEGIN
 IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)
    OR NOT pg_try_advisory_xact_lock(1818321765,1) THEN
  RAISE EXCEPTION 'explicit superuser and exclusive direct ownership required';
 END IF;
 IF p_max_rows NOT BETWEEN 1 AND 100000 OR p_max_bytes NOT BETWEEN 1 AND 67108864
    OR p_table_pause_ms NOT BETWEEN 0 AND 60000 THEN RAISE EXCEPTION 'invalid snapshot bounds'; END IF;
 IF current_setting('transaction_isolation')<>'repeatable read' THEN
  RAISE EXCEPTION 'bootstrap requires an imported repeatable-read snapshot';
 END IF;
 IF EXISTS(SELECT FROM lake_direct.stream) OR EXISTS(SELECT FROM lake_direct.mapping) THEN
  RAISE EXCEPTION 'direct stream is already initialized; refusing replacement';
 END IF;
 IF p_system<>(pg_control_system()).system_identifier::text OR p_timeline<>(pg_control_checkpoint()).timeline_id THEN
  RAISE EXCEPTION 'snapshot source identity changed';
 END IF;
 SELECT * INTO STRICT slot FROM pg_replication_slots WHERE slot_name=p_slot;
 IF slot.plugin<>'pgoutput' OR slot.database<>current_database() OR slot.temporary
    OR slot.invalidation_reason IS NOT NULL OR slot.restart_lsn IS NULL
    OR slot.restart_lsn>p_lsn OR slot.confirmed_flush_lsn>p_lsn THEN
  RAISE EXCEPTION 'snapshot slot lacks required history';
 END IF;
 SELECT * INTO STRICT pub FROM pg_publication WHERE pubname=p_publication;
 IF pub.puballtables OR pub.pubviaroot OR NOT(pub.pubinsert AND pub.pubupdate AND pub.pubdelete AND pub.pubtruncate)
    OR EXISTS(SELECT FROM pg_publication_namespace WHERE pnpubid=pub.oid)
    OR EXISTS(SELECT FROM pg_publication_rel WHERE prpubid=pub.oid AND (prqual IS NOT NULL OR prattrs IS NOT NULL)) THEN
  RAISE EXCEPTION 'bootstrap requires an explicit unfiltered full-operation publication';
 END IF;
 SELECT count(*) INTO table_count FROM pg_publication_rel WHERE prpubid=pub.oid;
 IF table_count NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'invalid snapshot table count'; END IF;
 IF p_target_schema::text LIKE 'pg\_%' ESCAPE '\' OR p_target_schema IN ('lake_direct','lake_replication','information_schema') THEN
  RAISE EXCEPTION 'invalid mirror schema';
 END IF;
 -- Locks live until activation COMMIT and block DDL, while ordinary DML keeps
 -- running. Frozen DDL is a bootstrap contract, not general DDL replication.
 FOR src IN SELECT c.oid,n.nspname,c.relname,c.relkind,c.relrowsecurity,c.relforcerowsecurity,c.relreplident
  FROM pg_publication_rel pr JOIN pg_class c ON c.oid=pr.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE pr.prpubid=pub.oid ORDER BY c.oid LOOP
  EXECUTE format('LOCK TABLE ONLY %I.%I IN ACCESS SHARE MODE',src.nspname,src.relname);
  IF src.nspname IN ('lake_direct','lake_replication','pg_catalog','information_schema') OR src.relkind<>'r' OR src.relrowsecurity OR src.relforcerowsecurity
     OR EXISTS(SELECT FROM pg_inherits WHERE inhrelid=src.oid OR inhparent=src.oid)
     OR src.relreplident NOT IN ('d','i','f')
     OR (src.relreplident='d' AND NOT EXISTS(SELECT FROM pg_index WHERE indrelid=src.oid AND indisprimary)) THEN
   RAISE EXCEPTION 'source table is outside the frozen snapshot subset';
  END IF;
  IF EXISTS(SELECT FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
    WHERE a.attrelid=src.oid AND a.attnum>0 AND NOT a.attisdropped AND
     (a.attgenerated<>'' OR a.attidentity<>'' OR a.attcollation<>t.typcollation OR
      (a.atttypid='numeric'::regtype AND (a.atttypmod<4 OR ((a.atttypmod-4)>>16) NOT BETWEEN 1 AND 38
       OR ((a.atttypmod-4)&2047)>((a.atttypmod-4)>>16))) OR
      a.atttypid<>ALL(ARRAY['bool'::regtype,'int2'::regtype,'int4'::regtype,'int8'::regtype,
        'float4'::regtype,'float8'::regtype,'numeric'::regtype,'text'::regtype,'varchar'::regtype,
        'bpchar'::regtype,'bytea'::regtype,'date'::regtype,'timestamp'::regtype,'timestamptz'::regtype,
        'uuid'::regtype,'jsonb'::regtype]::oid[]))) THEN
   RAISE EXCEPTION 'source column is outside the frozen snapshot subset';
  END IF;
  IF to_regclass(format('%I.%I',p_target_schema,src.relname)) IS NOT NULL THEN
   RAISE EXCEPTION 'snapshot destination already exists';
  END IF;
  EXECUTE format('SELECT count(*),coalesce(sum(pg_column_size(to_jsonb(t))),0) FROM (SELECT * FROM ONLY %I.%I LIMIT $1) t',src.nspname,src.relname)
   INTO copied,sized USING p_max_rows-total_rows+1;
  total_rows:=total_rows+copied;total_bytes:=total_bytes+sized;
  IF total_rows>p_max_rows OR total_bytes>p_max_bytes THEN RAISE EXCEPTION 'snapshot exceeds input budget'; END IF;
  INSERT INTO lake_direct.snapshot_tables VALUES(src.oid,copied,sized);
 END LOOP;
 EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I',p_target_schema);
 FOR src IN SELECT c.oid,n.nspname,c.relname FROM pg_publication_rel pr
  JOIN pg_class c ON c.oid=pr.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE pr.prpubid=pub.oid ORDER BY c.oid LOOP
  SELECT string_agg(format('%I %s%s',a.attname,format_type(a.atttypid,a.atttypmod),
          CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END),',' ORDER BY a.attnum)
   INTO columns_sql FROM pg_attribute a WHERE a.attrelid=src.oid AND a.attnum>0 AND NOT a.attisdropped;
  IF columns_sql IS NULL THEN RAISE EXCEPTION 'empty source row type'; END IF;
  EXECUTE format('CREATE TABLE %I.%I (%s) USING iceberg',p_target_schema,src.relname,columns_sql);
  destination:=to_regclass(format('%I.%I',p_target_schema,src.relname));
  EXECUTE format('INSERT INTO %s SELECT * FROM ONLY %I.%I',destination,src.nspname,src.relname);
  GET DIAGNOSTICS copied=ROW_COUNT;
  IF copied<>(SELECT rows_copied FROM lake_direct.snapshot_tables WHERE source_relation=src.oid) THEN
   RAISE EXCEPTION 'snapshot row count changed';
  END IF;
  INSERT INTO lake_direct.mapping VALUES(src.oid,destination,lake_direct.descriptor(src.oid),
      lake_direct.shape(src.oid),lake_direct.shape(destination));
  RAISE LOG 'direct_snapshot_table_ready relation=% rows=%',src.oid,copied;
  IF p_table_pause_ms>0 THEN PERFORM pg_sleep(p_table_pause_ms/1000.0); END IF;
 END LOOP;
 -- Recheck dynamic slot state immediately before recording activation.
 SELECT * INTO STRICT slot FROM pg_replication_slots WHERE slot_name=p_slot;
 IF slot.invalidation_reason IS NOT NULL OR slot.restart_lsn>p_lsn OR slot.confirmed_flush_lsn>p_lsn THEN
  RAISE EXCEPTION 'snapshot slot changed before activation';
 END IF;
 INSERT INTO lake_direct.stream(id,slot_name,publication,system_identifier,database_oid,timeline,published_lsn)
 VALUES(1,p_slot,p_publication,p_system,(SELECT oid FROM pg_database WHERE datname=current_database()),p_timeline,p_lsn);
 INSERT INTO lake_direct.snapshot_state VALUES(1,p_snapshot,p_lsn,total_rows,total_bytes,table_count);
 RETURN total_rows;
END $$;

REVOKE ALL ON ALL FUNCTIONS IN SCHEMA lake_direct FROM PUBLIC;
COMMIT;

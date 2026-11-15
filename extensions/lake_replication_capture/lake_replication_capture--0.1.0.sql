CREATE SCHEMA lake_capture;
CREATE SCHEMA lake_capture_log;
REVOKE ALL ON SCHEMA lake_capture, lake_capture_log FROM PUBLIC;

CREATE TABLE lake_capture.streams (
    stream_id uuid PRIMARY KEY,
    singleton boolean UNIQUE NOT NULL DEFAULT true CHECK(singleton),
    owner oid NOT NULL,
    origin_system_identifier numeric(20,0) NOT NULL,
    origin_database_oid oid NOT NULL,
    table_a regclass NOT NULL,
    table_b regclass NOT NULL,
    table_a_id uuid NOT NULL,
    table_b_id uuid NOT NULL,
    slot_name name UNIQUE NOT NULL,
    credential_file text,
    object_prefix text NOT NULL,
    log_locations jsonb NOT NULL CHECK(jsonb_typeof(log_locations)='object'),
    desired_state text NOT NULL DEFAULT 'stopped' CHECK(desired_state IN ('stopped','running','drop','reset')),
    config_generation bigint NOT NULL DEFAULT 1,
    capture_generation bigint NOT NULL DEFAULT 1,
    bound_postmaster timestamptz,
    baseline_lsn pg_lsn,
    captured_sequence bigint NOT NULL DEFAULT 0,
    captured_commit_lsn pg_lsn NOT NULL DEFAULT '0/0',
    captured_end_lsn pg_lsn NOT NULL DEFAULT '0/0',
    max_decode_spill_bytes bigint NOT NULL DEFAULT 268435456 CHECK(max_decode_spill_bytes BETWEEN 65536 AND 1073741824),
    decode_work_mem_kb integer NOT NULL DEFAULT 4096 CHECK(decode_work_mem_kb BETWEEN 64 AND 65536),
    transactions_per_read integer NOT NULL DEFAULT 1 CHECK(transactions_per_read BETWEEN 1 AND 32),
    publication_interval_ms integer NOT NULL DEFAULT 0 CHECK(publication_interval_ms BETWEEN 0 AND 1000),
    max_wal_bytes bigint NOT NULL DEFAULT 1073741824 CHECK(max_wal_bytes>0),
    bootstrap_mode text NOT NULL DEFAULT 'empty' CHECK(bootstrap_mode IN ('empty','paused_copy','online_copy')),
    snapshot_state text NOT NULL DEFAULT 'none' CHECK(snapshot_state IN ('none','building','complete')),
    retained_log_bytes bigint NOT NULL DEFAULT 0,
    generation_log_bytes bigint NOT NULL DEFAULT 0,
    pruned_through_sequence bigint NOT NULL DEFAULT 0,
    max_retained_log_bytes bigint NOT NULL DEFAULT 268435456 CHECK(max_retained_log_bytes>0),
    max_generation_log_bytes bigint NOT NULL DEFAULT 1073741824 CHECK(max_generation_log_bytes>0),
    max_generation_batches bigint NOT NULL DEFAULT 10000 CHECK(max_generation_batches>0),
    reserved_upload_bytes bigint NOT NULL DEFAULT 0,
    upload_attempt_count bigint NOT NULL DEFAULT 0,
    maintenance_attempt_count bigint NOT NULL DEFAULT 0,
    max_maintenance_attempts bigint NOT NULL DEFAULT 64 CHECK(max_maintenance_attempts>0),
    max_reserved_upload_bytes bigint NOT NULL DEFAULT 1073741824 CHECK(max_reserved_upload_bytes>0),
    max_upload_attempts bigint NOT NULL DEFAULT 10000 CHECK(max_upload_attempts>0),
    progress_request_count integer NOT NULL DEFAULT 0 CHECK(progress_request_count>=0),
    max_progress_requests integer NOT NULL DEFAULT 1024 CHECK(max_progress_requests BETWEEN 1 AND 10000),
    last_error text,
    capture_failure_count bigint NOT NULL DEFAULT 0 CHECK(capture_failure_count>=0),
    capture_conflict_count bigint NOT NULL DEFAULT 0 CHECK(capture_conflict_count>=0),
    last_failure_at timestamptz,
    last_failure_sqlstate text CHECK(last_failure_sqlstate ~ '^[0-9A-Z]{5}$')
);
ALTER TABLE lake_capture.streams ADD COLUMN schema_auditing boolean NOT NULL DEFAULT false;

CREATE TABLE lake_capture.receipts (
    stream_id uuid REFERENCES lake_capture.streams ON DELETE CASCADE,
    sequence bigint NOT NULL,
    batch_id uuid NOT NULL,
    end_lsn pg_lsn NOT NULL,
    raw_digest text NOT NULL,
    header_hex text NOT NULL,
    digest_hex text NOT NULL,
    payload_bytes bigint NOT NULL,
    log_pruned boolean NOT NULL DEFAULT false,
    table_a_version integer NOT NULL DEFAULT 1 CHECK(table_a_version BETWEEN 1 AND 33),
    table_b_version integer NOT NULL DEFAULT 1 CHECK(table_b_version BETWEEN 1 AND 33),
    PRIMARY KEY(stream_id,sequence),
    UNIQUE(stream_id,batch_id),
    UNIQUE(stream_id,end_lsn)
);
CREATE TABLE lake_capture.progress_requests (
    stream_id uuid NOT NULL REFERENCES lake_capture.streams ON DELETE CASCADE,
    capture_generation bigint NOT NULL,
    request_id uuid PRIMARY KEY,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    captured_sequence bigint CHECK(captured_sequence>0),
    captured_end_lsn pg_lsn,
    CHECK((captured_sequence IS NULL)=(captured_end_lsn IS NULL))
);
CREATE TABLE lake_capture_log.manifests(sequence bigint,header_hex text,digest_hex text) USING iceberg WITH (autovacuum_enabled=false,max_snapshot_age=0);
CREATE TABLE lake_capture_log.events_a(sequence bigint,ordinal integer,event_hex text) USING iceberg WITH (autovacuum_enabled=false,max_snapshot_age=0);
CREATE TABLE lake_capture_log.events_b(sequence bigint,ordinal integer,event_hex text) USING iceberg WITH (autovacuum_enabled=false,max_snapshot_age=0);
CREATE TABLE lake_capture_log.controls(sequence bigint,ordinal integer,event_hex text) USING iceberg WITH (autovacuum_enabled=false,max_snapshot_age=0);
CREATE TABLE lake_capture_log.registrations (
    stream_id uuid REFERENCES lake_capture.streams ON DELETE CASCADE,
    table_id uuid PRIMARY KEY,
    schema_version integer NOT NULL CHECK(schema_version>0),
    source_namespace text NOT NULL,
    source_relation text NOT NULL,
    replica_identity "char" NOT NULL,
    columns jsonb NOT NULL
);
CREATE TABLE lake_capture.consumers (
    stream_id uuid REFERENCES lake_capture.streams ON DELETE CASCADE,
    consumer_id uuid PRIMARY KEY,
    capture_generation bigint NOT NULL,
    owner oid NOT NULL,
    published_sequence bigint NOT NULL DEFAULT 0,
    published_end_lsn pg_lsn NOT NULL DEFAULT '0/0',
    published_digest text,
    supports_schema boolean NOT NULL DEFAULT false
);
CREATE TABLE lake_capture.schema_definitions (
    stream_id uuid NOT NULL REFERENCES lake_capture.streams ON DELETE CASCADE,
    capture_generation bigint NOT NULL CHECK(capture_generation>0),
    table_id uuid NOT NULL REFERENCES lake_capture_log.registrations(table_id) ON DELETE CASCADE,
    schema_version integer NOT NULL CHECK(schema_version BETWEEN 1 AND 33),
    definition_bytes bytea NOT NULL CHECK(octet_length(definition_bytes) BETWEEN 1 AND 1048576),
    definition_json jsonb NOT NULL CHECK(jsonb_typeof(definition_json)='object' AND octet_length(definition_json::text)<=1048576),
    PRIMARY KEY(stream_id,capture_generation,table_id,schema_version)
);
-- The six-field immutable audit shape is consumed directly from historical WAL.
CREATE TABLE lake_capture.schema_events (
    stream_id uuid NOT NULL REFERENCES lake_capture.streams ON DELETE CASCADE,
    capture_generation bigint NOT NULL,
    table_id uuid NOT NULL,
    before_version integer NOT NULL CHECK(before_version BETWEEN 1 AND 32),
    after_version integer NOT NULL CHECK(after_version=before_version+1 AND after_version<=33),
    payload bytea NOT NULL CHECK(octet_length(payload) BETWEEN 1 AND 2097216),
    PRIMARY KEY(stream_id,capture_generation,table_id,after_version),
    FOREIGN KEY(stream_id,capture_generation,table_id,after_version)
        REFERENCES lake_capture.schema_definitions(stream_id,capture_generation,table_id,schema_version) ON DELETE CASCADE
);
CREATE TABLE lake_capture.upload_attempts (
    stream_id uuid REFERENCES lake_capture.streams ON DELETE CASCADE,
    upload_id uuid PRIMARY KEY,
    capture_generation bigint NOT NULL,
    source_end pg_lsn NOT NULL,
    digest text NOT NULL,
    payload_bytes bigint NOT NULL,
    completed boolean NOT NULL DEFAULT false
);

CREATE VIEW lake_capture_log.progress AS SELECT stream_id,origin_system_identifier AS system_identifier,
    origin_database_oid AS database_oid,capture_generation,captured_sequence,pruned_through_sequence
    FROM lake_capture.streams;

CREATE VIEW lake_capture_log.receipt_state AS SELECT sequence,batch_id,end_lsn,digest_hex AS digest
    FROM lake_capture.receipts;
CREATE VIEW lake_capture_log.schema_definitions AS SELECT stream_id,capture_generation,table_id,schema_version,
    definition_bytes,definition_json FROM lake_capture.schema_definitions;

CREATE FUNCTION lake_capture.lock_private_logs(session boolean DEFAULT false) RETURNS jsonb
LANGUAGE C STRICT AS 'MODULE_PATHNAME','lake_capture_lock_private_logs';
CREATE FUNCTION lake_capture.describe_relation(regclass) RETURNS bytea
LANGUAGE C STRICT AS 'MODULE_PATHNAME','lake_capture_describe_relation';
CREATE FUNCTION lake_capture.describe_relation_json(regclass) RETURNS jsonb
LANGUAGE C STRICT AS 'MODULE_PATHNAME','lake_capture_describe_relation_json';

CREATE FUNCTION lake_capture.validate_relation(regclass) RETURNS void
LANGUAGE C STRICT AS 'MODULE_PATHNAME','lake_capture_validate_relation';

CREATE FUNCTION lake_capture.current_log_locations() RETURNS jsonb
LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
    SELECT jsonb_object_agg(c.relname,o.option_value)
    FROM pg_foreign_table f JOIN pg_class c ON c.oid=f.ftrelid
    CROSS JOIN LATERAL pg_options_to_table(f.ftoptions) o
    WHERE f.ftrelid IN ('lake_capture_log.events_a'::regclass,'lake_capture_log.events_b'::regclass,
        'lake_capture_log.manifests'::regclass,'lake_capture_log.controls'::regclass)
      AND o.option_name='location'
$$;

-- The privileged manager connects children as the persisted owner. A trusted
-- control-plane operator must still never rewrite that owner to a superuser.
ALTER TABLE lake_capture.streams ENABLE ROW LEVEL SECURITY;
CREATE POLICY capture_stream_owner ON lake_capture.streams
    USING(owner=current_user::regrole) WITH CHECK(owner=current_user::regrole);

CREATE FUNCTION lake_capture.require_owner(id uuid) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
BEGIN
    -- ACLs supply database-local control-plane access. The selected-table
    -- SELECT grants remain explicit; this does not isolate multiple owners.
    IF NOT (SELECT rolcanlogin AND (rolsuper OR rolreplication) FROM pg_roles WHERE rolname=current_user) THEN
        RAISE EXCEPTION 'capture owner requires LOGIN and REPLICATION privileges' USING ERRCODE='42501';
    END IF;
    IF id IS NOT NULL AND NOT EXISTS(SELECT FROM streams WHERE stream_id=id AND owner=current_user::regrole) THEN
        RAISE EXCEPTION 'capture stream is not owned by this role' USING ERRCODE='42501';
    END IF;
END $$;

CREATE FUNCTION lake_capture.register_stream(a regclass,b regclass,credentials text DEFAULT NULL,bootstrap text DEFAULT 'empty')
RETURNS uuid LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
DECLARE id uuid:=gen_random_uuid(); selected oid; relation record;locations jsonb;prefix text;item record;definition bytea;normalized jsonb;
BEGIN
    PERFORM require_owner(NULL);
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'capture registration requires READ COMMITTED isolation' USING ERRCODE='25001';
    END IF;
    IF bootstrap NOT IN ('empty','paused_copy','online_copy') THEN RAISE EXCEPTION 'unsupported source bootstrap mode'; END IF;
    IF a IS NULL OR b IS NULL OR a=b OR EXISTS(SELECT FROM streams) THEN
        RAISE EXCEPTION 'capture prototype requires one fixed, distinct two-table group';
    END IF;
    -- Serialize registration with source DDL. Once this commits, the event
    -- guard takes over; there is no unprotected catalog-registration window.
    EXECUTE format('LOCK TABLE %s,%s IN SHARE MODE',a,b);
    IF current_setting('server_encoding')<>'UTF8' OR current_setting('wal_level')<>'logical' THEN
        RAISE EXCEPTION 'capture requires UTF8 and logical WAL';
    END IF;
    IF credentials IS NOT NULL AND left(credentials,1)<>'/' THEN
        RAISE EXCEPTION 'capture credential file must be an absolute path';
    END IF;
    FOREACH selected IN ARRAY ARRAY[a::oid,b::oid] LOOP
        -- Native relcache validation remains current even under an old caller snapshot.
        PERFORM validate_relation(selected::regclass);
        SELECT * INTO STRICT relation FROM pg_class WHERE oid=selected;
        IF relation.relrowsecurity OR relation.relforcerowsecurity THEN
            RAISE EXCEPTION 'capture prototype does not support source row-level security';
        END IF;
        IF relation.relkind<>'r' OR relation.relpersistence<>'p' OR relation.relispartition
           OR EXISTS(SELECT FROM pg_inherits WHERE inhrelid=selected OR inhparent=selected)
           OR relation.relnamespace IN ('lake_capture'::regnamespace,'lake_capture_log'::regnamespace)
           OR NOT EXISTS(SELECT FROM pg_index WHERE indrelid=selected AND indisprimary AND indisvalid)
           OR relation.relreplident NOT IN ('d','i')
           OR (relation.relreplident='i' AND NOT EXISTS(SELECT FROM pg_index
                WHERE indrelid=selected AND indisprimary AND indisreplident))
           OR EXISTS(SELECT FROM pg_attribute WHERE attrelid=selected AND attnum>0 AND
                (attisdropped OR attgenerated<>'' OR atttypid NOT IN (16,20,23,25))) THEN
            RAISE EXCEPTION 'capture requires permanent heap tables with supported frozen scalar columns and primary keys';
        END IF;
    END LOOP;
    locations:=lock_private_logs(false);
    IF locations IS NULL OR (SELECT count(*) FROM jsonb_object_keys(locations))<>4 THEN
        RAISE EXCEPTION 'capture requires four qualified private log locations';
    END IF;
    SELECT min(value) INTO prefix FROM jsonb_each_text(locations);
    WHILE EXISTS(SELECT FROM jsonb_each_text(locations) WHERE left(value,length(prefix))<>prefix) LOOP
        prefix:=left(prefix,length(prefix)-1);
    END LOOP;
    prefix:=regexp_replace(prefix,'[^/]*$','');
    IF prefix='' THEN RAISE EXCEPTION 'capture private logs require a common object location';END IF;
    INSERT INTO streams(stream_id,owner,origin_system_identifier,origin_database_oid,
        table_a,table_b,table_a_id,table_b_id,slot_name,credential_file,object_prefix,log_locations,bootstrap_mode)
    VALUES(id,current_user::regrole,(SELECT system_identifier FROM pg_control_system()),
        (SELECT oid FROM pg_database WHERE datname=current_database()),a,b,
        gen_random_uuid(),gen_random_uuid(),'lc_'||replace(id::text,'-',''),credentials,
        prefix,locations,bootstrap);
    INSERT INTO lake_capture_log.registrations
    SELECT s.stream_id,t.table_id,1,n.nspname,c.relname,c.relreplident,
        (SELECT jsonb_agg(jsonb_build_object('name',att.attname,'type_oid',att.atttypid::bigint,
            'type_modifier',att.atttypmod,'flags',CASE WHEN att.attnum=ANY((pk.indkey::smallint[])[0:pk.indnkeyatts-1]) THEN 1 ELSE 0 END)
            ORDER BY att.attnum) FROM pg_attribute att
         JOIN pg_index pk ON pk.indrelid=att.attrelid AND pk.indisprimary
         WHERE att.attrelid=c.oid AND att.attnum>0 AND NOT att.attisdropped)
    FROM streams s CROSS JOIN LATERAL (VALUES(s.table_a::oid,s.table_a_id),(s.table_b::oid,s.table_b_id)) t(relation_id,table_id)
    JOIN pg_class c ON c.oid=t.relation_id JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE s.stream_id=id;
    -- Store version one at the registration boundary, while SHARE locks
    -- protect both tables. Frozen capture also supports some columns outside
    -- the stricter evolution contract; those streams receive no inferred history.
    FOR item IN SELECT * FROM (SELECT table_a::oid AS relation_id,table_a_id AS table_id FROM streams WHERE stream_id=id
        UNION ALL SELECT table_b::oid,table_b_id FROM streams WHERE stream_id=id) t LOOP
        BEGIN
            definition:=describe_relation(item.relation_id::regclass);
            normalized:=describe_relation_json(item.relation_id::regclass);
            INSERT INTO schema_definitions VALUES(id,1,item.table_id,1,definition,normalized);
        EXCEPTION WHEN feature_not_supported THEN NULL;
        END;
    END LOOP;
    RETURN id;
END $$;

-- Expose only the allowlist decision; the capture role does not need access
-- to all superuser-only settings.
CREATE FUNCTION lake_capture.output_plugin_allowed() RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$
    SELECT coalesce(current_setting('output_plugin_libraries',true),'')
        ~ '(^|,)\s*"?lake_replication_capture"?\s*(,|$)'
$$;

CREATE FUNCTION lake_capture.start_stream(id uuid) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
BEGIN
    PERFORM require_owner(id);
    IF NOT output_plugin_allowed() THEN
        RAISE EXCEPTION 'source configuration must explicitly allow lake_replication_capture in output_plugin_libraries';
    END IF;
    IF EXISTS(SELECT FROM streams WHERE stream_id=id AND desired_state IN ('drop','reset')) THEN
        RAISE EXCEPTION 'capture cleanup is pending; wait before starting';
    END IF;
    UPDATE streams SET desired_state='running',config_generation=config_generation+1,
        bound_postmaster=pg_postmaster_start_time(),last_error=NULL
    WHERE stream_id=id AND desired_state NOT IN ('drop','reset')
        AND origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
        AND origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database());
    IF NOT FOUND THEN RAISE EXCEPTION 'copied capture binding requires a new stream/snapshot'; END IF;
END $$;
CREATE FUNCTION lake_capture.stop_stream(id uuid) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
BEGIN
    PERFORM require_owner(id);
    UPDATE streams SET desired_state='stopped',config_generation=config_generation+1 WHERE stream_id=id AND desired_state NOT IN ('drop','reset');
END $$;

CREATE FUNCTION lake_capture.reset_stream(id uuid) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
BEGIN
    PERFORM require_owner(id);
    IF EXISTS(SELECT FROM streams WHERE stream_id=id AND (schema_auditing
        OR EXISTS(SELECT FROM schema_definitions d WHERE d.stream_id=id AND d.schema_version>1))) THEN
        RAISE EXCEPTION 'schema-enabled streams require drop and a new registration for recopy' USING ERRCODE='55000';
    END IF;
    UPDATE streams SET desired_state='reset',config_generation=config_generation+1,bound_postmaster=pg_postmaster_start_time()
    WHERE stream_id=id AND desired_state<>'drop'
        AND origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
        AND origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database());
    IF NOT FOUND THEN RAISE EXCEPTION 'copied source requires a new stream/snapshot'; END IF;
END $$;
CREATE FUNCTION lake_capture.drop_stream(id uuid) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
BEGIN
    PERFORM require_owner(id);
    UPDATE streams SET desired_state='drop',config_generation=config_generation+1,bound_postmaster=pg_postmaster_start_time() WHERE stream_id=id;
END $$;

CREATE FUNCTION lake_capture.assert_capture(id uuid,gen bigint,protect_storage boolean DEFAULT true) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
DECLARE expected_locations jsonb;actual_locations jsonb;
BEGIN
    PERFORM require_owner(id);
    SELECT log_locations INTO expected_locations FROM streams WHERE stream_id=id AND config_generation=gen AND desired_state='running'
        AND bound_postmaster=pg_postmaster_start_time()
        AND origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
        AND origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()) FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'capture worker is stopped, copied or obsolete' USING ERRCODE='55000'; END IF;
    -- Metadata-only reservations use a separate connection during paused copy.
    -- The publishing session already pins all private relations. A second
    -- relation-lock request could queue behind DDL waiting for that parent.
    IF protect_storage THEN actual_locations:=lock_private_logs(false);
    ELSE actual_locations:=current_log_locations();END IF;
    IF expected_locations IS DISTINCT FROM actual_locations THEN
        RAISE EXCEPTION 'capture private log locations changed; controlled rebuild or repair required' USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION lake_capture.verify_schema_history(id uuid) RETURNS boolean
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
DECLARE state streams%ROWTYPE;item record;definition bytea;normalized jsonb;prior schema_definitions%ROWTYPE;
BEGIN
    PERFORM require_owner(id);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id;
    IF state.desired_state<>'stopped' OR state.baseline_lsn IS NULL OR state.snapshot_state='building' THEN
        RAISE EXCEPTION 'schema history verification requires a stopped stream with a complete baseline' USING ERRCODE='55000';
    END IF;
    IF NOT pg_try_advisory_xact_lock(hashtextextended('lake_capture/'||id::text,0)) THEN
        RAISE EXCEPTION 'wait for the capture actor lease before verifying schema history' USING ERRCODE='55000';
    END IF;
    IF state.origin_system_identifier<>(SELECT system_identifier FROM pg_control_system())
       OR state.origin_database_oid<>(SELECT oid FROM pg_database WHERE datname=current_database())
       OR state.bound_postmaster IS DISTINCT FROM pg_postmaster_start_time() THEN
        RAISE EXCEPTION 'copied or unbound capture stream requires a new baseline' USING ERRCODE='55000';
    END IF;
    -- Follow lease -> application locks -> control row. Never hold the control
    -- row while waiting for application writers or DDL to finish.
    EXECUTE format('LOCK TABLE %s,%s IN SHARE MODE',state.table_a,state.table_b);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id AND desired_state='stopped'
        AND bound_postmaster=pg_postmaster_start_time()
        AND origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
        AND origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()) FOR UPDATE;
    IF state.log_locations IS DISTINCT FROM lock_private_logs(false) THEN
        RAISE EXCEPTION 'capture private log locations changed; repair before schema verification' USING ERRCODE='55000';
    END IF;
    FOR item IN SELECT * FROM (VALUES(state.table_a::oid,state.table_a_id),(state.table_b::oid,state.table_b_id)) t(relation_id,table_id) LOOP
        definition:=describe_relation(item.relation_id::regclass);normalized:=describe_relation_json(item.relation_id::regclass);
        SELECT * INTO prior FROM schema_definitions d WHERE d.stream_id=id AND d.capture_generation=state.capture_generation
            AND d.table_id=item.table_id AND d.schema_version=1;
        IF FOUND THEN
            IF (prior.definition_bytes,prior.definition_json) IS DISTINCT FROM (definition,normalized) THEN
                RAISE EXCEPTION 'conflicting initial schema definition' USING ERRCODE='23505';
            END IF;
        ELSE
            RAISE EXCEPTION 'registration has no eligible initial schema definition; register a new stream' USING ERRCODE='55000';
        END IF;
    END LOOP;
    RETURN true;
END $$;

-- Activation is separate from start/stop and requires the decoder lease.
-- SECURITY DEFINER changes only this protected flag after authenticating the
-- original connection owner; application table ownership is never delegated.
CREATE FUNCTION lake_capture.enable_schema_auditing(id uuid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_capture,pg_temp AS $$
DECLARE state streams%ROWTYPE;caller oid:=session_user::regrole;role_info record;item record;prior schema_definitions%ROWTYPE;
BEGIN
    SELECT rolcanlogin,rolreplication,rolsuper INTO STRICT role_info FROM pg_roles WHERE oid=caller;
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id;
    IF NOT role_info.rolcanlogin OR NOT (role_info.rolsuper OR role_info.rolreplication)
        OR (state.owner<>caller AND NOT role_info.rolsuper) THEN
        RAISE EXCEPTION 'schema auditing requires the owning capture LOGIN' USING ERRCODE='42501';
    END IF;
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'schema activation requires READ COMMITTED' USING ERRCODE='25001';
    END IF;
    IF state.desired_state<>'stopped' OR state.baseline_lsn IS NULL OR state.snapshot_state='building'
        OR state.bound_postmaster IS DISTINCT FROM pg_postmaster_start_time()
        OR state.origin_system_identifier<>(SELECT system_identifier FROM pg_control_system())
        OR state.origin_database_oid<>(SELECT oid FROM pg_database WHERE datname=current_database()) THEN
        RAISE EXCEPTION 'schema activation requires a stopped initialized original source binding' USING ERRCODE='55000';
    END IF;
    IF NOT pg_try_advisory_xact_lock(hashtextextended('lake_capture/'||id::text,0)) THEN
        RAISE EXCEPTION 'wait for the capture actor lease before schema activation' USING ERRCODE='55000';
    END IF;
    IF NOT has_table_privilege(caller,state.table_a,'SELECT') OR NOT has_table_privilege(caller,state.table_b,'SELECT')
        OR NOT coalesce((SELECT has_schema_privilege(caller,relnamespace,'USAGE') FROM pg_class WHERE oid=state.table_a::oid),false)
        OR NOT coalesce((SELECT has_schema_privilege(caller,relnamespace,'USAGE') FROM pg_class WHERE oid=state.table_b::oid),false) THEN
        RAISE EXCEPTION 'capture owner needs explicit selected-table SELECT for schema activation' USING ERRCODE='42501';
    END IF;
    EXECUTE format('LOCK TABLE %s,%s IN SHARE MODE',state.table_a,state.table_b);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id AND desired_state='stopped'
        AND bound_postmaster=pg_postmaster_start_time()
        AND origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
        AND origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()) FOR UPDATE;
    IF state.owner<>caller AND NOT role_info.rolsuper THEN
        RAISE EXCEPTION 'schema activation owner changed' USING ERRCODE='42501';
    END IF;
    IF NOT state.schema_auditing AND EXISTS(SELECT FROM consumers WHERE stream_id=id) THEN
        RAISE EXCEPTION 'enable ordered schema before registering consumers; existing mirrors require a new stream' USING ERRCODE='55000';
    END IF;
    IF state.log_locations IS DISTINCT FROM lock_private_logs(false) THEN
        RAISE EXCEPTION 'private log locations changed before schema activation' USING ERRCODE='55000';
    END IF;
    FOR item IN SELECT * FROM (VALUES(state.table_a::oid,state.table_a_id),(state.table_b::oid,state.table_b_id)) t(relation_id,table_id) LOOP
        SELECT * INTO prior FROM schema_definitions d WHERE d.stream_id=id AND d.capture_generation=state.capture_generation
            AND d.table_id=item.table_id ORDER BY schema_version DESC LIMIT 1;
        IF NOT FOUND OR (NOT state.schema_auditing AND prior.schema_version<>1) THEN
            RAISE EXCEPTION 'schema activation requires eligible retained initial definitions' USING ERRCODE='55000';
        END IF;
        IF (prior.definition_bytes,prior.definition_json) IS DISTINCT FROM
            (describe_relation(item.relation_id::regclass),describe_relation_json(item.relation_id::regclass)) THEN
            RAISE EXCEPTION 'live source differs from retained schema history' USING ERRCODE='55000';
        END IF;
    END LOOP;
    IF NOT state.schema_auditing THEN
        UPDATE streams SET schema_auditing=true,config_generation=config_generation+1 WHERE stream_id=id;
    END IF;
END $$;

CREATE FUNCTION lake_capture.request_progress(id uuid) RETURNS uuid
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
DECLARE state streams%ROWTYPE;request uuid:=gen_random_uuid();gen bigint;
BEGIN
    PERFORM require_owner(id);
    SELECT config_generation INTO STRICT gen FROM streams WHERE stream_id=id;
    PERFORM assert_capture(id,gen,false);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id;
    IF state.baseline_lsn IS NULL OR state.snapshot_state='building' THEN
        RAISE EXCEPTION 'capture progress requires a complete baseline' USING ERRCODE='55000';
    END IF;
    IF state.progress_request_count>=state.max_progress_requests THEN
        RAISE EXCEPTION 'capture progress request budget exhausted; controlled generation rebuild is required' USING ERRCODE='LR002';
    END IF;
    INSERT INTO progress_requests(stream_id,capture_generation,request_id) VALUES(id,state.capture_generation,request);
    UPDATE streams SET progress_request_count=progress_request_count+1 WHERE stream_id=id;
    RETURN request;
END $$;

-- A v1 table-less Progress event is ordinal(4), table-tag(1), control-tag(1),
-- payload-length(4), then its 16-byte request UUID. Other controls return NULL.
CREATE FUNCTION lake_capture.progress_event_request(event_hex text) RETURNS uuid
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
    SELECT CASE WHEN length(event_hex)=52 AND substr(event_hex,9,12)='000900000010'
        THEN right(event_hex,32)::uuid END
$$;

CREATE FUNCTION lake_capture.reserve_upload(id uuid,gen bigint,source_end pg_lsn,digest text,bytes bigint)
RETURNS uuid LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
DECLARE state streams%ROWTYPE; reservation uuid:=gen_random_uuid();
BEGIN
    PERFORM assert_capture(id,gen,false);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id;
    IF bytes<=0 OR bytes>134221824 OR digest !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'invalid bounded upload reservation';
    END IF;
    IF state.retained_log_bytes+bytes>state.max_retained_log_bytes
       OR state.generation_log_bytes+bytes>state.max_generation_log_bytes
       OR state.captured_sequence>=state.max_generation_batches
       OR state.reserved_upload_bytes+bytes>state.max_reserved_upload_bytes
       OR state.upload_attempt_count>=state.max_upload_attempts THEN
        RAISE EXCEPTION 'capture log budget exhausted before upload reservation' USING ERRCODE='LR002';
    END IF;
    INSERT INTO upload_attempts(stream_id,upload_id,capture_generation,source_end,digest,payload_bytes)
        VALUES(id,reservation,state.capture_generation,source_end,digest,bytes);
    UPDATE streams SET reserved_upload_bytes=reserved_upload_bytes+bytes,upload_attempt_count=upload_attempt_count+1
        WHERE stream_id=id;
    RETURN reservation;
END $$;

-- Preflight cumulative publication budgets before reserving any member of a
-- complete-transaction group. Per-item reservations alone cannot account for
-- retained/generation bytes that have not yet been published.
CREATE FUNCTION lake_capture.reserve_upload_group(id uuid,gen bigint,ends pg_lsn[],digests text[],sizes bigint[])
RETURNS TABLE(ordinal integer,upload_id uuid) LANGUAGE plpgsql
SET search_path=pg_catalog,lake_capture AS $$
DECLARE state streams%ROWTYPE;items integer;total bigint;position integer;
BEGIN
    PERFORM assert_capture(id,gen,false);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id;
    items:=cardinality(ends);
    IF items IS NULL OR items NOT BETWEEN 1 AND 32 OR cardinality(digests) IS DISTINCT FROM items
       OR cardinality(sizes) IS DISTINCT FROM items OR array_ndims(ends)<>1 OR array_ndims(digests)<>1
       OR array_ndims(sizes)<>1 OR array_lower(ends,1)<>1 OR array_lower(digests,1)<>1 OR array_lower(sizes,1)<>1
       OR EXISTS(SELECT FROM unnest(sizes) n WHERE n IS NULL OR n<=0 OR n>134221824)
       OR EXISTS(SELECT FROM unnest(ends) n WHERE n IS NULL)
       OR EXISTS(SELECT FROM unnest(digests) n WHERE n IS NULL OR n !~ '^[0-9a-f]{64}$') THEN
        RAISE EXCEPTION 'invalid bounded capture upload group';
    END IF;
    SELECT sum(n) INTO total FROM unnest(sizes) n;
    IF state.retained_log_bytes+total>state.max_retained_log_bytes
       OR state.generation_log_bytes+total>state.max_generation_log_bytes
       OR state.captured_sequence+items>state.max_generation_batches
       OR state.reserved_upload_bytes+total>state.max_reserved_upload_bytes
       OR state.upload_attempt_count+items>state.max_upload_attempts THEN
        RAISE EXCEPTION 'capture group log budget exhausted before upload reservation' USING ERRCODE='LR002';
    END IF;
    FOR position IN 1..items LOOP
        ordinal:=position;
        upload_id:=reserve_upload(id,gen,ends[position],digests[position],sizes[position]);
        RETURN NEXT;
    END LOOP;
END $$;

-- Commit this reservation before VACUUM, which cannot run in a SQL transaction.
CREATE FUNCTION lake_capture.reserve_maintenance(id uuid,gen bigint) RETURNS boolean
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
BEGIN
    PERFORM assert_capture(id,gen);
    UPDATE streams SET maintenance_attempt_count=maintenance_attempt_count+1
        WHERE stream_id=id AND maintenance_attempt_count<max_maintenance_attempts;
    RETURN FOUND;
END $$;

-- One physical append per log relation, preserving independent transaction
-- manifests/receipts. All validation and metadata changes share the source TX.
CREATE FUNCTION lake_capture.publish_group(id uuid,gen bigint,batches jsonb)
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
<<group_payload>>
DECLARE state streams%ROWTYPE;receipt receipts%ROWTYPE;item jsonb;position integer;
    accepted integer[]:=ARRAY[]::integer[];bytes bigint;total_bytes bigint:=0;total_events integer:=0;
    seq bigint;batch uuid;source_commit pg_lsn;source_end pg_lsn;raw_hash text;
    header text;digest text;events jsonb;reservation uuid;version_a integer;version_b integer;
BEGIN
    PERFORM assert_capture(id,gen);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id;
    IF jsonb_typeof(batches) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'invalid capture publication group';END IF;
    IF jsonb_array_length(batches) NOT BETWEEN 1 AND 32 THEN RAISE EXCEPTION 'invalid capture group count';END IF;
    FOR item,position IN SELECT value,ordinality::integer FROM jsonb_array_elements(batches) WITH ORDINALITY LOOP
        seq:=(item->>'sequence')::bigint;batch:=(item->>'batch_id')::uuid;
        source_commit:=(item->>'commit_lsn')::pg_lsn;source_end:=(item->>'end_lsn')::pg_lsn;
        raw_hash:=item->>'raw_hash';header:=item->>'header';digest:=item->>'digest';
        events:=item->'events';reservation:=(item->>'reservation')::uuid;
        version_a:=coalesce((item->>'table_a_version')::integer,1);version_b:=coalesce((item->>'table_b_version')::integer,1);
        IF version_a NOT BETWEEN 1 AND 33 OR version_b NOT BETWEEN 1 AND 33
            OR (NOT state.schema_auditing AND (version_a<>1 OR version_b<>1)) THEN
            RAISE EXCEPTION 'invalid captured schema versions';
        END IF;
        IF seq IS NULL OR batch IS NULL OR source_commit IS NULL OR source_end IS NULL
           OR raw_hash IS NULL OR header IS NULL OR digest IS NULL OR events IS NULL OR reservation IS NULL THEN
            RAISE EXCEPTION 'incomplete capture group member';
        END IF;
        SELECT * INTO receipt FROM receipts WHERE stream_id=id AND batch_id=batch;
        IF FOUND THEN
            IF (receipt.sequence,receipt.end_lsn,receipt.raw_digest,receipt.header_hex,receipt.digest_hex,receipt.table_a_version,receipt.table_b_version)
                IS DISTINCT FROM (seq,source_end,raw_hash,header,digest,version_a,version_b) THEN
                RAISE EXCEPTION 'conflicting capture replay' USING ERRCODE='23505';
            END IF;
            CONTINUE;
        END IF;
        IF seq<>state.captured_sequence+1 OR source_end<=state.captured_end_lsn
           OR source_commit<=state.captured_commit_lsn THEN
            RAISE EXCEPTION 'capture sequence gap or backwards source coordinates';
        END IF;
        IF jsonb_typeof(events) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'invalid capture events';END IF;
        IF jsonb_array_length(events) NOT BETWEEN 1 AND 100000
           OR raw_hash !~ '^[0-9a-f]{64}$' OR digest !~ '^[0-9a-f]{64}$'
           OR EXISTS(SELECT FROM jsonb_array_elements(events) e
                     WHERE e->>'table' NOT IN (state.table_a_id::text,state.table_b_id::text,'control')) THEN
            RAISE EXCEPTION 'invalid capture batch';
        END IF;
        SELECT octet_length(header)+octet_length(digest)+4096+
            coalesce(sum(octet_length(e->>'hex')),0) INTO bytes FROM jsonb_array_elements(events) e;
        total_bytes:=total_bytes+bytes;total_events:=total_events+jsonb_array_length(events);
        IF total_bytes>136314880 OR total_events>100000 THEN
            RAISE EXCEPTION 'capture publication group exceeds payload budget';
        END IF;
        IF state.retained_log_bytes+bytes>state.max_retained_log_bytes
           OR state.generation_log_bytes+bytes>state.max_generation_log_bytes
           OR seq>state.max_generation_batches THEN
            RAISE EXCEPTION 'capture log budget exhausted; publication acknowledgement/pruning or a controlled generation rebuild is required'
                USING ERRCODE='LR002';
        END IF;
        IF NOT EXISTS(SELECT FROM upload_attempts a WHERE a.upload_id=reservation AND a.stream_id=id AND a.capture_generation=state.capture_generation
            AND a.source_end=group_payload.source_end AND a.digest=group_payload.digest AND a.payload_bytes=bytes AND NOT a.completed) THEN
            RAISE EXCEPTION 'capture upload requires a durable matching reservation';
        END IF;
        INSERT INTO receipts(stream_id,sequence,batch_id,end_lsn,raw_digest,header_hex,digest_hex,payload_bytes,table_a_version,table_b_version)
            VALUES(id,seq,batch,source_end,raw_hash,header,digest,bytes,version_a,version_b);
        UPDATE upload_attempts a SET completed=true WHERE a.upload_id=reservation;
        IF EXISTS(SELECT FROM jsonb_array_elements(events) e
            WHERE e->>'table'='control' AND progress_event_request(e->>'hex') IS NOT NULL) THEN
            IF EXISTS(SELECT FROM jsonb_array_elements(events) e
                LEFT JOIN progress_requests r ON r.request_id=progress_event_request(e->>'hex')
                WHERE e->>'table'='control' AND progress_event_request(e->>'hex') IS NOT NULL
                    AND (r.request_id IS NULL OR r.stream_id<>id OR r.capture_generation<>state.capture_generation
                        OR r.captured_sequence IS NOT NULL)) THEN
                RAISE EXCEPTION 'invalid or conflicting capture progress request' USING ERRCODE='23505';
            END IF;
            UPDATE progress_requests r SET captured_sequence=seq,captured_end_lsn=group_payload.source_end
                WHERE r.request_id IN (SELECT progress_event_request(e->>'hex')
                    FROM jsonb_array_elements(events) e WHERE e->>'table'='control');
        END IF;
        accepted:=array_append(accepted,position);
        state.captured_sequence:=seq;state.captured_commit_lsn:=source_commit;state.captured_end_lsn:=source_end;
        state.retained_log_bytes:=state.retained_log_bytes+bytes;state.generation_log_bytes:=state.generation_log_bytes+bytes;
        IF state.bootstrap_mode IN ('paused_copy','online_copy') AND seq=1 THEN
            state.baseline_lsn:=source_end;state.snapshot_state:='complete';
        END IF;
    END LOOP;
    IF cardinality(accepted)=0 THEN RETURN 0;END IF;
    INSERT INTO lake_capture_log.events_a
        SELECT (b.value->>'sequence')::bigint,(e->>'ordinal')::integer,e->>'hex'
        FROM jsonb_array_elements(batches) WITH ORDINALITY b(value,n)
        CROSS JOIN LATERAL jsonb_array_elements(b.value->'events') e
        WHERE b.n=ANY(accepted) AND e->>'table'=state.table_a_id::text;
    INSERT INTO lake_capture_log.events_b
        SELECT (b.value->>'sequence')::bigint,(e->>'ordinal')::integer,e->>'hex'
        FROM jsonb_array_elements(batches) WITH ORDINALITY b(value,n)
        CROSS JOIN LATERAL jsonb_array_elements(b.value->'events') e
        WHERE b.n=ANY(accepted) AND e->>'table'=state.table_b_id::text;
    INSERT INTO lake_capture_log.controls
        SELECT (b.value->>'sequence')::bigint,(e->>'ordinal')::integer,e->>'hex'
        FROM jsonb_array_elements(batches) WITH ORDINALITY b(value,n)
        CROSS JOIN LATERAL jsonb_array_elements(b.value->'events') e
        WHERE b.n=ANY(accepted) AND e->>'table'='control';
    INSERT INTO lake_capture_log.manifests
        SELECT (b.value->>'sequence')::bigint,b.value->>'header',b.value->>'digest'
        FROM jsonb_array_elements(batches) WITH ORDINALITY b(value,n) WHERE b.n=ANY(accepted);
    UPDATE streams SET captured_sequence=state.captured_sequence,captured_commit_lsn=state.captured_commit_lsn,
        captured_end_lsn=state.captured_end_lsn,baseline_lsn=state.baseline_lsn,snapshot_state=state.snapshot_state,
        retained_log_bytes=state.retained_log_bytes,generation_log_bytes=state.generation_log_bytes WHERE stream_id=id;
    RETURN cardinality(accepted);
END $$;

CREATE FUNCTION lake_capture.publish_batch(id uuid,gen bigint,seq bigint,batch uuid,
    source_commit pg_lsn,source_end pg_lsn,raw_hash text,header text,digest text,events jsonb,reservation uuid)
RETURNS boolean LANGUAGE sql SET search_path=pg_catalog,lake_capture AS $$
    SELECT publish_group(id,gen,jsonb_build_array(jsonb_build_object('sequence',seq,'batch_id',batch,
        'commit_lsn',source_commit,'end_lsn',source_end,'raw_hash',raw_hash,'header',header,
        'digest',digest,'events',events,'reservation',reservation)))>0
$$;

CREATE FUNCTION lake_capture.register_consumer(id uuid,schema_capable boolean DEFAULT false) RETURNS uuid
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
DECLARE state streams%ROWTYPE; consumer uuid:=gen_random_uuid();
BEGIN
    PERFORM require_owner(id);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id FOR UPDATE;
    IF state.pruned_through_sequence<>0 THEN
        RAISE EXCEPTION 'new consumer requires retained complete history or a new snapshot generation';
    END IF;
    IF state.schema_auditing AND schema_capable IS NOT TRUE THEN
        RAISE EXCEPTION 'ordered-schema streams require an explicitly schema-capable consumer' USING ERRCODE='55000';
    END IF;
    INSERT INTO consumers(stream_id,consumer_id,capture_generation,owner,supports_schema)
        VALUES(id,consumer,state.capture_generation,current_user::regrole,coalesce(schema_capable,false));
    RETURN consumer;
END $$;

CREATE FUNCTION lake_capture.acknowledge_publication(id uuid,consumer uuid,gen bigint,
    seq bigint,source_end pg_lsn,digest text) RETURNS boolean
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
DECLARE state streams%ROWTYPE; current_ack consumers%ROWTYPE;
BEGIN
    PERFORM require_owner(id);
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id FOR SHARE;
    SELECT * INTO STRICT current_ack FROM consumers WHERE stream_id=id AND consumer_id=consumer
        AND owner=current_user::regrole FOR UPDATE;
    IF gen<>state.capture_generation OR gen<>current_ack.capture_generation THEN
        RAISE EXCEPTION 'consumer capture generation mismatch';
    END IF;
    IF seq=current_ack.published_sequence AND source_end=current_ack.published_end_lsn
        AND digest=current_ack.published_digest THEN RETURN false; END IF;
    IF seq<=current_ack.published_sequence OR source_end<=current_ack.published_end_lsn
       OR NOT EXISTS(SELECT FROM receipts WHERE stream_id=id AND sequence=seq
                    AND end_lsn=source_end AND digest_hex=digest) THEN
        RAISE EXCEPTION 'invalid or conflicting publication acknowledgement';
    END IF;
    UPDATE consumers SET published_sequence=seq,published_end_lsn=source_end,published_digest=digest
        WHERE consumer_id=consumer;
    RETURN true;
END $$;

CREATE FUNCTION lake_capture.prune_logs(id uuid,maximum_batches integer DEFAULT 128) RETURNS integer
LANGUAGE plpgsql SET search_path=pg_catalog,lake_capture AS $$
DECLARE state streams%ROWTYPE; frontier bigint; selected bigint[]; bytes bigint; confirmed pg_lsn;
BEGIN
    PERFORM require_owner(id);
    IF maximum_batches<1 OR maximum_batches>10000 THEN RAISE EXCEPTION 'invalid prune batch budget'; END IF;
    SELECT * INTO STRICT state FROM streams WHERE stream_id=id FOR UPDATE;
    IF state.bound_postmaster IS DISTINCT FROM pg_postmaster_start_time()
       OR state.origin_system_identifier<>(SELECT system_identifier FROM pg_control_system())
       OR state.origin_database_oid<>(SELECT oid FROM pg_database WHERE datname=current_database()) THEN
        RAISE EXCEPTION 'source binding requires explicit verification before log maintenance';
    END IF;
    IF state.log_locations IS DISTINCT FROM lock_private_logs(false) THEN
        RAISE EXCEPTION 'capture private log locations changed; controlled rebuild or repair required' USING ERRCODE='55000';
    END IF;
    SELECT min(published_sequence) INTO frontier FROM consumers
        WHERE stream_id=id AND capture_generation=state.capture_generation;
    IF frontier IS NULL THEN RAISE EXCEPTION 'log pruning requires every registered consumer publication acknowledgement'; END IF;
    SELECT confirmed_flush_lsn INTO confirmed FROM pg_replication_slots
        WHERE slot_name=state.slot_name AND plugin='lake_replication_capture' AND database=current_database();
    IF confirmed IS NULL THEN RAISE EXCEPTION 'owned capture slot is missing'; END IF;
    SELECT array_agg(sequence),coalesce(sum(payload_bytes),0) INTO selected,bytes FROM
        (SELECT sequence,payload_bytes FROM receipts WHERE stream_id=id AND sequence<=frontier
         AND NOT log_pruned AND end_lsn<=confirmed ORDER BY sequence LIMIT maximum_batches) eligible;
    IF selected IS NULL THEN RETURN 0; END IF;
    DELETE FROM lake_capture_log.events_a WHERE sequence=ANY(selected);
    DELETE FROM lake_capture_log.events_b WHERE sequence=ANY(selected);
    DELETE FROM lake_capture_log.controls WHERE sequence=ANY(selected);
    DELETE FROM lake_capture_log.manifests WHERE sequence=ANY(selected);
    UPDATE receipts SET log_pruned=true WHERE stream_id=id AND sequence=ANY(selected);
    UPDATE streams SET retained_log_bytes=retained_log_bytes-bytes,
        pruned_through_sequence=greatest(pruned_through_sequence,(SELECT max(v) FROM unnest(selected) v))
        WHERE stream_id=id;
    -- Compact receipts stay for the whole bounded generation: SQL slot
    -- confirmed_flush can rewind after a crash until a checkpoint. Removing
    -- replay receipts based only on that observed pointer would be unsafe.
    RETURN cardinality(selected);
END $$;

-- A fixed schema must be enforced throughout stop/start, while protected WAL
-- can still contain changes. Current-catalog reads cannot repair old schemas.
CREATE FUNCTION lake_capture.guard_ddl_check(event_name text) RETURNS void
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog,lake_capture,pg_temp AS $$
BEGIN
    IF event_name='ddl_command_end' THEN
        IF EXISTS(SELECT FROM streams s JOIN pg_inherits i
            ON i.inhrelid IN (s.table_a::oid,s.table_b::oid) OR i.inhparent IN (s.table_a::oid,s.table_b::oid)
            WHERE s.origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
              AND s.origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())) THEN
            RAISE EXCEPTION 'selected capture table inheritance/partition membership is frozen';
        END IF;
        IF EXISTS(SELECT FROM pg_event_trigger_ddl_commands() c JOIN streams s
            ON (c.classid='pg_class'::regclass AND c.objid IN (s.table_a::oid,s.table_b::oid)
                AND (NOT s.schema_auditing OR current_setting('transaction_isolation')<>'read committed'))
            OR (c.classid='pg_namespace'::regclass AND c.objid IN
                (SELECT relnamespace FROM pg_class WHERE oid IN (s.table_a::oid,s.table_b::oid)))
            WHERE s.origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
              AND s.origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())) THEN
            RAISE EXCEPTION 'selected capture table schema is frozen until the stream is dropped';
        END IF;
    ELSIF event_name='sql_drop' THEN
        IF EXISTS(SELECT FROM pg_event_trigger_dropped_objects() c JOIN streams s
            ON (c.classid='pg_class'::regclass AND c.objid IN (s.table_a::oid,s.table_b::oid))
            OR (c.classid='pg_namespace'::regclass AND c.objid IN
                (SELECT relnamespace FROM pg_class WHERE oid IN (s.table_a::oid,s.table_b::oid)))
            WHERE s.origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
              AND s.origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())) THEN
            RAISE EXCEPTION 'selected capture table cannot be dropped while registered';
        END IF;
    END IF;
END $$;
-- Internal only: the capture actor does not yet enable schema auditing.
-- All writes remain in the application DDL transaction. The fresh-snapshot
-- checker runs first; audited selected DDL is restricted to READ COMMITTED.
CREATE FUNCTION lake_capture.guard_record_schema_ddl(event_name text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_capture,pg_temp AS $$
DECLARE command record;state streams%ROWTYPE;table_uuid uuid;prior schema_definitions%ROWTYPE;
    after_json jsonb;after_bytes bytea;old_columns jsonb;new_columns jsonb;old_column jsonb;new_column jsonb;
    operation integer;renamed integer;position integer;payload bytea;
BEGIN
    IF event_name<>'ddl_command_end' THEN RETURN;END IF;
    FOR command IN SELECT DISTINCT c.objid FROM pg_event_trigger_ddl_commands() c
        JOIN streams s ON c.classid='pg_class'::regclass AND c.objid IN (s.table_a::oid,s.table_b::oid)
        WHERE s.schema_auditing AND s.origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
          AND s.origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()) LOOP
        IF current_setting('transaction_isolation')<>'read committed' THEN
            RAISE EXCEPTION 'audited source DDL requires READ COMMITTED' USING ERRCODE='25001';
        END IF;
        SELECT * INTO STRICT state FROM streams WHERE schema_auditing
            AND command.objid IN (table_a::oid,table_b::oid) FOR UPDATE;
        table_uuid:=CASE WHEN command.objid=state.table_a::oid THEN state.table_a_id ELSE state.table_b_id END;
        SELECT * INTO prior FROM schema_definitions WHERE stream_id=state.stream_id
            AND capture_generation=state.capture_generation AND table_id=table_uuid ORDER BY schema_version DESC LIMIT 1;
        IF NOT FOUND OR prior.schema_version>=33 THEN
            RAISE EXCEPTION 'schema audit requires retained initial history and at most 32 revisions' USING ERRCODE='55000';
        END IF;
        after_bytes:=describe_relation(command.objid::regclass);after_json:=describe_relation_json(command.objid::regclass);
        IF prior.definition_json-'columns' IS DISTINCT FROM after_json-'columns' THEN
            RAISE EXCEPTION 'source schema table or replica identity changed' USING ERRCODE='0A000';
        END IF;
        old_columns:=prior.definition_json->'columns';new_columns:=after_json->'columns';
        operation:=NULL;renamed:=0;
        IF jsonb_array_length(new_columns)=jsonb_array_length(old_columns)+1
            AND new_columns-(jsonb_array_length(new_columns)-1)=old_columns THEN
            new_column:=new_columns->(jsonb_array_length(new_columns)-1);
            IF NOT (new_column->>'key')::boolean AND (new_column->>'nullable')::boolean THEN operation:=0;END IF;
        ELSIF jsonb_array_length(new_columns)=jsonb_array_length(old_columns) THEN
            FOR position IN 0..jsonb_array_length(old_columns)-1 LOOP
                old_column:=old_columns->position;new_column:=new_columns->position;
                IF old_column<>new_column THEN
                    IF old_column-'name' IS DISTINCT FROM new_column-'name' THEN
                        RAISE EXCEPTION 'unsupported combined source schema change' USING ERRCODE='0A000';
                    END IF;
                    renamed:=renamed+1;
                END IF;
            END LOOP;
            IF renamed=1 THEN operation:=1;END IF;
        END IF;
        IF operation IS NULL THEN
            RAISE EXCEPTION 'source schema audit permits one nullable addition or one column rename' USING ERRCODE='0A000';
        END IF;
        payload:=decode('4c4b44444c31','hex')||uuid_send(table_uuid)||int4send(prior.schema_version)
            ||int4send(prior.schema_version+1)||decode(CASE operation WHEN 0 THEN '00' ELSE '01' END,'hex')
            ||int4send(octet_length(prior.definition_bytes))||prior.definition_bytes
            ||int4send(octet_length(after_bytes))||after_bytes;
        INSERT INTO schema_definitions VALUES(state.stream_id,state.capture_generation,table_uuid,
            prior.schema_version+1,after_bytes,after_json);
        INSERT INTO schema_events VALUES(state.stream_id,state.capture_generation,table_uuid,
            prior.schema_version,prior.schema_version+1,payload);
    END LOOP;
END $$;
CREATE FUNCTION lake_capture.guard_ddl() RETURNS event_trigger LANGUAGE C SECURITY DEFINER
SET search_path=pg_catalog,lake_capture,pg_temp
AS 'MODULE_PATHNAME','lake_capture_guard_ddl';
CREATE FUNCTION lake_capture.guard_extension_drop() RETURNS event_trigger LANGUAGE C
AS 'MODULE_PATHNAME','lake_capture_guard_extension_drop';
CREATE EVENT TRIGGER lake_capture_ddl_start ON ddl_command_start WHEN TAG IN
    ('DROP EXTENSION','ALTER TABLE','ALTER FOREIGN TABLE','ALTER SCHEMA','DROP TABLE','DROP FOREIGN TABLE','DROP SCHEMA')
EXECUTE FUNCTION lake_capture.guard_extension_drop();
CREATE EVENT TRIGGER lake_capture_ddl_end ON ddl_command_end EXECUTE FUNCTION lake_capture.guard_ddl();
CREATE EVENT TRIGGER lake_capture_sql_drop ON sql_drop EXECUTE FUNCTION lake_capture.guard_ddl();
CREATE VIEW lake_capture.status AS SELECT s.*,
    CASE WHEN s.desired_state='running' AND s.bound_postmaster IS DISTINCT FROM pg_postmaster_start_time()
         THEN 'needs_rebind' ELSE s.desired_state END AS effective_state,
    r.confirmed_flush_lsn AS decoded_safe_lsn,
    r.restart_lsn,r.wal_status,r.invalidation_reason,
    statistics.spill_bytes AS total_decoder_spill_bytes,
    statistics.spill_count AS decoder_spill_count,
    r.active AS decoder_slot_active,
    pg_wal_lsn_diff(pg_current_wal_lsn(),r.restart_lsn) AS retained_wal_bytes,
    pg_wal_lsn_diff(pg_current_wal_lsn(),coalesce(nullif(s.captured_end_lsn,'0/0'),s.baseline_lsn)) AS source_to_capture_wal_bytes,
    (SELECT count(*) FROM pg_stat_activity a WHERE a.datid=s.origin_database_oid
        AND a.application_name='lake_capture/'||s.stream_id::text||'/'||s.config_generation::text) AS capture_connections,
    (SELECT count(*) FROM lake_capture.upload_attempts a WHERE a.stream_id=s.stream_id AND NOT a.completed) AS incomplete_upload_attempts,
    (SELECT min(c.published_sequence) FROM lake_capture.consumers c WHERE c.stream_id=s.stream_id
        AND c.capture_generation=s.capture_generation) AS slowest_acknowledged_sequence
    FROM lake_capture.streams s
    LEFT JOIN pg_replication_slots r ON r.slot_name=s.slot_name
    LEFT JOIN pg_stat_replication_slots statistics ON statistics.slot_name=s.slot_name;

REVOKE ALL ON ALL TABLES IN SCHEMA lake_capture, lake_capture_log FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA lake_capture FROM PUBLIC;
SELECT pg_extension_config_dump('lake_capture.streams','');
SELECT pg_extension_config_dump('lake_capture.receipts','');
SELECT pg_extension_config_dump('lake_capture_log.registrations','');
SELECT pg_extension_config_dump('lake_capture.consumers','');
SELECT pg_extension_config_dump('lake_capture.upload_attempts','');
SELECT pg_extension_config_dump('lake_capture.progress_requests','');
SELECT pg_extension_config_dump('lake_capture.schema_definitions','');
SELECT pg_extension_config_dump('lake_capture.schema_events','');
CREATE FUNCTION lake_capture.worker(internal) RETURNS internal LANGUAGE C
AS 'MODULE_PATHNAME','lake_capture_launcher';
CREATE FUNCTION lake_capture.start_manager() RETURNS void LANGUAGE C
AS 'MODULE_PATHNAME','lake_capture_start_manager';
REVOKE ALL ON FUNCTION lake_capture.worker(internal) FROM PUBLIC;
REVOKE ALL ON FUNCTION lake_capture.start_manager() FROM PUBLIC;
SELECT extension_base.register_worker('lake_replication capture','lake_capture.worker');

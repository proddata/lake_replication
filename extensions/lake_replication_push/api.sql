
DO $$
BEGIN
    IF current_setting('server_version_num')::integer/10000<>18
       OR (SELECT extversion FROM pg_extension WHERE extname='lake_replication') IS DISTINCT FROM '0.1.0-beta.5'
       OR to_regprocedure('lake_replication.assert_worker(bigint,bigint)') IS NULL
       OR to_regprocedure('lake_replication.source_tls_settings(bigint)') IS NULL THEN
        RAISE EXCEPTION 'optional push prototype requires the qualified PostgreSQL 18 / lake_replication beta.5 worker and TLS interfaces';
    END IF;
END $$;

CREATE VIEW lake_replication.subscription_modes AS
SELECT s.subscription_id,s.subscription_name,
    CASE WHEN b.subscription_id IS NULL THEN 'logical_pull' ELSE 'iceberg_push' END AS source_mode
FROM lake_replication.subscriptions s LEFT JOIN lake_replication.push_bindings b USING(subscription_id);

-- Explicit new subscriptions only. Source configuration/registry are obtained
-- from the source catalog by the administrator; the importer checks both again
-- before admission. All mirror objects are created privately in a new schema.
CREATE FUNCTION lake_replication.create_push_subscription(
    subscription_name name,source_config jsonb,table_registry jsonb,
    target_schema name,credential_file text
) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER
SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE sub bigint;item jsonb;column_definition text;target_name name;
    relation_version bigint;relation_id oid:=0;binding_stream uuid;binding_generation bigint;runtime_role name;ordered boolean;initial_definitions jsonb;
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=session_user)
       AND NOT pg_has_role(session_user,'lake_replication_admin','MEMBER') THEN
        RAISE EXCEPTION 'push subscription management requires lake_replication_admin' USING ERRCODE='42501';
    END IF;
    IF source_config->>'mode' IS DISTINCT FROM 'iceberg_push' THEN
        RAISE EXCEPTION 'explicit iceberg_push source mode required';
    END IF;
    IF credential_file IS NULL OR left(credential_file,1)<>'/'
       OR coalesce(source_config->>'source_address','')='' OR coalesce(source_config->>'source_database','')=''
       OR coalesce(source_config->>'conninfo','')='' THEN
        RAISE EXCEPTION 'push requires an absolute credential file and explicit source transport configuration';
    END IF;
    IF coalesce(source_config->>'schema_mode','frozen') NOT IN ('frozen','ordered') THEN
        RAISE EXCEPTION 'unsupported push schema mode';
    END IF;
    ordered:=coalesce(source_config->>'schema_mode','frozen')='ordered';
    runtime_role:=nullif(source_config->>'destination_user','')::name;
    IF runtime_role IS NULL OR NOT EXISTS(SELECT FROM pg_roles WHERE rolname=runtime_role
        AND rolcanlogin AND (rolsuper OR pg_has_role(oid,'lake_replication_worker','MEMBER'))) THEN
        RAISE EXCEPTION 'push requires an explicit destination LOGIN with lake_replication_worker privileges';
    END IF;
    IF jsonb_typeof(table_registry) IS DISTINCT FROM 'array' OR jsonb_array_length(table_registry)<>2
       OR octet_length(table_registry::text)>2097152 THEN
        RAISE EXCEPTION 'push requires two bounded frozen table registrations';
    END IF;
    IF target_schema IS NULL OR to_regnamespace(target_schema::text) IS NOT NULL THEN
        RAISE EXCEPTION 'push activation requires a new private destination schema';
    END IF;
    binding_stream:=(source_config->>'stream_id')::uuid;
    binding_generation:=(source_config->>'capture_generation')::bigint;
    IF binding_stream IS NULL OR binding_stream='00000000-0000-0000-0000-000000000000'::uuid
       OR binding_generation IS NULL OR binding_generation<=0
       OR (source_config->>'system_identifier')::numeric<=0 OR (source_config->>'database_oid')::oid=0 THEN
        RAISE EXCEPTION 'invalid push source identity or generation';
    END IF;
    sub:=create_subscription((source_config->>'system_identifier')::numeric,
        (source_config->>'database_oid')::oid,coalesce((source_config->>'timeline')::bigint,1),
        subscription_name,('push_'||replace(gen_random_uuid()::text,'-',''))::name,ARRAY['push_transport']);
    INSERT INTO push_bindings(subscription_id,stream_id,system_identifier,database_oid,capture_generation)
        VALUES(sub,binding_stream,(source_config->>'system_identifier')::numeric,
            (source_config->>'database_oid')::oid,binding_generation);
    EXECUTE format('CREATE SCHEMA %I',target_schema);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I',target_schema,runtime_role);
    FOR item IN SELECT value FROM jsonb_array_elements(table_registry) LOOP
        relation_id:=(relation_id::bigint+1)::oid;
        IF (item->>'schema_version')::integer<>1 OR item->>'replica_identity' NOT IN ('d','i')
           OR jsonb_typeof(item->'columns') IS DISTINCT FROM 'array'
           OR jsonb_array_length(item->'columns') NOT BETWEEN 1 AND 1600
           OR NOT EXISTS(SELECT FROM jsonb_array_elements(item->'columns') c WHERE c->>'flags'='1')
           OR EXISTS(SELECT FROM jsonb_array_elements(item->'columns') c
                WHERE (c->>'type_oid')::oid NOT IN (16,20,23,25) OR c->>'flags' NOT IN ('0','1')
                    OR coalesce(c->>'name','')='' OR octet_length(c->>'name')>63) THEN
            RAISE EXCEPTION 'unsupported frozen push table descriptor';
        END IF;
        target_name:=coalesce(item->>'destination_relation',item->>'source_relation')::name;
        SELECT string_agg(format('%I %s%s',c->>'name',CASE (c->>'type_oid')::oid
            WHEN 16 THEN 'boolean' WHEN 20 THEN 'bigint' WHEN 23 THEN 'integer' WHEN 25 THEN 'text' END,
            CASE WHEN c->>'flags'='1' THEN ' NOT NULL' ELSE '' END),',' ORDER BY ordinal)
            INTO column_definition FROM jsonb_array_elements(item->'columns') WITH ORDINALITY f(c,ordinal);
        IF ordered THEN
            IF item->'definition' IS NULL THEN
                RAISE EXCEPTION 'ordered push requires retained initial definitions' USING ERRCODE='22023';
            END IF;
            PERFORM push_encode_schema_definition(item->'definition');
            SELECT string_agg(format('%I %s%s',c->>'name',CASE (c->>'type_oid')::integer
                WHEN 16 THEN 'boolean' WHEN 20 THEN 'bigint' WHEN 23 THEN 'integer' WHEN 25 THEN 'text' END,
                CASE WHEN NOT (c->>'nullable')::boolean THEN ' NOT NULL' ELSE '' END),',' ORDER BY ordinal)
                INTO column_definition FROM jsonb_array_elements(item->'definition'->'columns') WITH ORDINALITY f(c,ordinal);
        END IF;
        EXECUTE format('CREATE TABLE %I.%I(%s) USING iceberg',target_schema,target_name,column_definition);
        EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON %I.%I TO %I',target_schema,target_name,runtime_role);
        relation_version:=register_relation(sub,relation_id,(item->>'source_namespace')::name,
            (item->>'source_relation')::name,(item->>'replica_identity')::"char",item->'columns');
        PERFORM assign_destination(sub,relation_id,relation_version,format('%I.%I',target_schema,target_name)::regclass);
        INSERT INTO push_tables VALUES(sub,(item->>'table_id')::uuid,1,relation_id,relation_version);
    END LOOP;
    -- No pending_cleanup row: capture slots always belong to the source.
    INSERT INTO source_connections(subscription_id,conninfo) VALUES(sub,source_config->>'conninfo');
    INSERT INTO worker_launch_config(subscription_id,slot_name,publication_name,source_database,
        destination_database,source_address,destination_address,destination_schema,credential_file,
        create_slot,desired_state,origin_system_identifier,origin_database_oid)
    SELECT sub,slot_name,'push_transport',source_config->>'source_database',current_database(),
        source_config->>'source_address','127.0.0.1:'||current_setting('port'),target_schema,
        credential_file,false,'stopped',destination_system_identifier(),
        (SELECT oid FROM pg_database WHERE datname=current_database()) FROM subscriptions WHERE subscription_id=sub;
    UPDATE subscriptions SET state='streaming' WHERE subscription_id=sub;
    IF ordered THEN
        SELECT jsonb_agg(jsonb_build_object('table_id',r->>'table_id','definition',r->'definition'))
            INTO initial_definitions FROM jsonb_array_elements(table_registry) r;
        PERFORM initialize_push_schema_history(sub,initial_definitions);
    END IF;
    RETURN sub;
END $$;
REVOKE ALL ON FUNCTION lake_replication.create_push_subscription(name,jsonb,jsonb,name,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.create_push_subscription(name,jsonb,jsonb,name,text) TO lake_replication_admin;
GRANT SELECT ON lake_replication.subscription_modes,lake_replication.push_status,
    lake_replication.push_bindings,lake_replication.push_tables,lake_replication.push_receipts TO lake_replication_admin,lake_replication_worker;
GRANT INSERT ON lake_replication.push_receipts TO lake_replication_worker;
GRANT UPDATE(imported_sequence,imported_commit_lsn,imported_end_lsn,snapshot_end_lsn,
    observed_captured_sequence,source_observed_at)
    ON lake_replication.push_bindings TO lake_replication_worker;
GRANT EXECUTE ON FUNCTION lake_replication.import_push_batch(bigint,uuid,numeric,oid,bigint,bigint,uuid,text,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb,boolean) TO lake_replication_worker;
GRANT EXECUTE ON FUNCTION lake_replication.observe_push_source(bigint,uuid,numeric,oid,bigint,bigint) TO lake_replication_worker;
GRANT EXECUTE ON FUNCTION lake_replication.push_encode_schema_definition(jsonb),
    lake_replication.push_schema_operation(jsonb,jsonb),
    lake_replication.push_encode_schema_change(uuid,integer,integer,jsonb,jsonb)
    TO lake_replication_admin,lake_replication_worker;
SELECT pg_extension_config_dump('lake_replication.push_bindings','');
SELECT pg_extension_config_dump('lake_replication.push_tables','');
SELECT pg_extension_config_dump('lake_replication.push_receipts','');
CREATE FUNCTION lake_replication.guard_push_drop() RETURNS event_trigger LANGUAGE C SECURITY DEFINER
SET search_path=pg_catalog,lake_replication,pg_temp AS 'MODULE_PATHNAME','lake_push_guard_drop';
REVOKE ALL ON FUNCTION lake_replication.guard_push_drop() FROM PUBLIC;
CREATE EVENT TRIGGER lake_push_drop_start ON ddl_command_start WHEN TAG IN ('DROP EXTENSION')
EXECUTE FUNCTION lake_replication.guard_push_drop();

-- Optional ordered-schema metadata. Public activation waits for admission/apply integration.
CREATE TABLE lake_replication.push_schema_tables (
    subscription_id bigint NOT NULL REFERENCES lake_replication.push_bindings ON DELETE CASCADE,
    table_id uuid NOT NULL,
    remote_relation_id oid NOT NULL,
    destination_relation regclass NOT NULL UNIQUE,
    imported_version integer NOT NULL DEFAULT 1 CHECK(imported_version BETWEEN 1 AND 33),
    published_version integer NOT NULL DEFAULT 1 CHECK(published_version BETWEEN 1 AND imported_version),
    PRIMARY KEY(subscription_id,table_id),UNIQUE(subscription_id,remote_relation_id)
);
CREATE TABLE lake_replication.push_schema_versions (
    subscription_id bigint NOT NULL,
    table_id uuid NOT NULL,
    source_schema_version integer NOT NULL CHECK(source_schema_version BETWEEN 1 AND 33),
    definition_bytes bytea NOT NULL CHECK(octet_length(definition_bytes) BETWEEN 1 AND 1048576),
    definition_json jsonb NOT NULL CHECK(jsonb_typeof(definition_json)='object' AND octet_length(definition_json::text)<=1048576),
    remote_relation_id oid NOT NULL,
    relation_schema_version bigint NOT NULL,
    PRIMARY KEY(subscription_id,table_id,source_schema_version),
    UNIQUE(subscription_id,remote_relation_id,relation_schema_version),
    FOREIGN KEY(subscription_id,table_id) REFERENCES lake_replication.push_schema_tables ON DELETE CASCADE,
    FOREIGN KEY(subscription_id,remote_relation_id,relation_schema_version)
        REFERENCES lake_replication.relations(subscription_id,remote_relation_id,schema_version) ON DELETE CASCADE
);
CREATE TABLE lake_replication.push_schema_receipt_versions (
    subscription_id bigint NOT NULL,
    sequence bigint NOT NULL,
    table_id uuid NOT NULL,
    source_schema_version integer NOT NULL,
    PRIMARY KEY(subscription_id,sequence,table_id),
    FOREIGN KEY(subscription_id,sequence) REFERENCES lake_replication.push_receipts ON DELETE CASCADE,
    FOREIGN KEY(subscription_id,table_id,source_schema_version)
        REFERENCES lake_replication.push_schema_versions ON DELETE CASCADE
);
CREATE TABLE lake_replication.push_schema_transactions (
    transaction_id bigint PRIMARY KEY REFERENCES lake_replication.transactions ON DELETE CASCADE,
    subscription_id bigint NOT NULL REFERENCES lake_replication.push_bindings ON DELETE CASCADE,
    applied boolean NOT NULL DEFAULT false
);
REVOKE ALL ON lake_replication.push_schema_tables,lake_replication.push_schema_versions,
    lake_replication.push_schema_receipt_versions,lake_replication.push_schema_transactions FROM PUBLIC;

CREATE FUNCTION lake_replication.push_definition_columns(definition jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE columns jsonb;
BEGIN
    PERFORM push_encode_schema_definition(definition);
    SELECT jsonb_agg(jsonb_build_object('name',c->>'name','type_oid',(c->>'type_oid')::bigint,
        'type_modifier',(c->>'type_modifier')::integer,'flags',CASE WHEN (c->>'key')::boolean THEN 1 ELSE 0 END)
        ORDER BY position) INTO columns FROM jsonb_array_elements(definition->'columns') WITH ORDINALITY t(c,position);
    RETURN columns;
END $$;

CREATE FUNCTION lake_replication.push_check_schema_destination(destination regclass,definition jsonb) RETURNS void
LANGUAGE plpgsql STABLE STRICT SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE actual jsonb;expected jsonb;
BEGIN
    PERFORM push_encode_schema_definition(definition);
    IF NOT EXISTS(SELECT FROM pg_class c JOIN pg_foreign_table f ON f.ftrelid=c.oid
        JOIN pg_foreign_server s ON s.oid=f.ftserver JOIN pg_foreign_data_wrapper w ON w.oid=s.srvfdw
        WHERE c.oid=destination AND c.relkind='f' AND c.relpersistence='p' AND w.fdwname='pg_lake_iceberg'
            AND EXISTS(SELECT FROM pg_options_to_table(f.ftoptions) WHERE option_name='catalog' AND option_value='postgres')) THEN
        RAISE EXCEPTION 'schema publication requires a registered foreign destination' USING ERRCODE='55000';
    END IF;
    SELECT jsonb_agg(jsonb_build_object('name',attname,'type_oid',atttypid::bigint,'type_modifier',atttypmod,'nullable',NOT attnotnull)
        ORDER BY attnum) INTO actual FROM pg_attribute WHERE attrelid=destination AND attnum>0 AND NOT attisdropped;
    SELECT jsonb_agg(jsonb_build_object('name',c->>'name','type_oid',(c->>'type_oid')::bigint,
        'type_modifier',(c->>'type_modifier')::integer,'nullable',(c->>'nullable')::boolean) ORDER BY position) INTO expected
        FROM jsonb_array_elements(definition->'columns') WITH ORDINALITY t(c,position);
    IF actual IS DISTINCT FROM expected OR EXISTS(SELECT FROM pg_attribute
        WHERE attrelid=destination AND attnum>0 AND (attisdropped OR attgenerated<>'' OR attidentity<>'' OR atthasdef)) THEN
        RAISE EXCEPTION 'destination schema differs from retained published definition' USING ERRCODE='55000';
    END IF;
END $$;

CREATE FUNCTION lake_replication.initialize_push_schema_history(sub bigint,definitions jsonb) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE binding push_bindings%ROWTYPE;item jsonb;table_uuid uuid;definition jsonb;registration record;
    columns jsonb;actual_columns jsonb;count_tables integer:=0;
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=session_user)
       AND NOT pg_has_role(session_user,'lake_replication_admin','MEMBER') THEN
        RAISE EXCEPTION 'schema initialization requires lake_replication_admin' USING ERRCODE='42501';
    END IF;
    PERFORM 1 FROM service_status WHERE subscription_id=sub FOR UPDATE NOWAIT;
    PERFORM 1 FROM worker_launch_config WHERE subscription_id=sub FOR UPDATE NOWAIT;
    IF EXISTS(SELECT FROM service_status WHERE subscription_id=sub AND desired_state='running')
        OR EXISTS(SELECT FROM worker_launch_config WHERE subscription_id=sub AND desired_state='running') THEN
        RAISE EXCEPTION 'schema initialization requires a stopped destination service' USING ERRCODE='55000';
    END IF;
    SELECT * INTO STRICT binding FROM push_bindings WHERE subscription_id=sub FOR UPDATE;
    IF binding.imported_sequence<>0 OR binding.schema_enabled
        OR EXISTS(SELECT FROM push_schema_tables WHERE subscription_id=sub)
        OR EXISTS(SELECT FROM transactions WHERE subscription_id=sub)
        OR EXISTS(SELECT FROM push_receipts WHERE subscription_id=sub) THEN
        RAISE EXCEPTION 'schema initialization requires a fresh push binding with no admitted transactions' USING ERRCODE='55000';
    END IF;
    IF jsonb_typeof(definitions) IS DISTINCT FROM 'array' OR jsonb_array_length(definitions)<>2
        OR octet_length(definitions::text)>2097152 THEN
        RAISE EXCEPTION 'schema initialization requires two bounded initial definitions' USING ERRCODE='22023';
    END IF;
    FOR item IN SELECT value FROM jsonb_array_elements(definitions) LOOP
        table_uuid:=(item->>'table_id')::uuid;definition:=item->'definition';
        columns:=push_definition_columns(definition);
        SELECT p.remote_relation_id,p.relation_schema_version,r.source_namespace,r.source_relation,
            r.replica_identity,r.columns,r.destination_relation INTO STRICT registration
        FROM push_tables p JOIN relations r ON r.subscription_id=p.subscription_id
            AND r.remote_relation_id=p.remote_relation_id AND r.schema_version=p.relation_schema_version
        WHERE p.subscription_id=sub AND p.table_id=table_uuid AND p.source_schema_version=1;
        SELECT jsonb_agg(jsonb_build_object('name',c->>'name','type_oid',(c->>'type_oid')::bigint,
            'type_modifier',(c->>'type_modifier')::integer,'flags',(c->>'flags')::integer) ORDER BY position)
            INTO actual_columns FROM jsonb_array_elements(registration.columns) WITH ORDINALITY t(c,position);
        IF (registration.source_namespace::text,registration.source_relation::text,registration.replica_identity::text,actual_columns)
            IS DISTINCT FROM (definition->>'namespace',definition->>'name',definition->>'replica_identity',columns)
            OR registration.destination_relation IS NULL THEN
            RAISE EXCEPTION 'initial schema definition differs from push registration' USING ERRCODE='55000';
        END IF;
        PERFORM push_check_schema_destination(registration.destination_relation,definition);
        INSERT INTO push_schema_tables(subscription_id,table_id,remote_relation_id,destination_relation)
            VALUES(sub,table_uuid,registration.remote_relation_id,registration.destination_relation);
        INSERT INTO push_schema_versions VALUES(sub,table_uuid,1,push_encode_schema_definition(definition),definition,
            registration.remote_relation_id,registration.relation_schema_version);
        count_tables:=count_tables+1;
    END LOOP;
    IF count_tables<>2 OR (SELECT count(*) FROM push_tables WHERE subscription_id=sub)<>2 THEN
        RAISE EXCEPTION 'schema initialization requires exactly two initial table identities';
    END IF;
    UPDATE push_bindings SET schema_enabled=true WHERE subscription_id=sub;
END $$;

-- An ordinary publisher must never mark a schema-enabled transaction published
-- after applying only its row subset. Any preceding DML rolls back with this error.
CREATE FUNCTION lake_replication.guard_push_schema_publication() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication,pg_temp AS $$
BEGIN
    IF NEW.state='published' AND (EXISTS(SELECT FROM push_schema_transactions
        WHERE transaction_id=NEW.transaction_id AND NOT applied)
        OR (EXISTS(SELECT FROM push_bindings WHERE subscription_id=NEW.subscription_id AND schema_enabled)
            AND NOT EXISTS(SELECT FROM push_schema_transactions WHERE transaction_id=NEW.transaction_id AND subscription_id=NEW.subscription_id AND applied))) THEN
        RAISE EXCEPTION 'schema-enabled transaction requires ordered push publication' USING ERRCODE='55000';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER push_schema_publication_guard BEFORE UPDATE OF state ON lake_replication.transactions
FOR EACH ROW EXECUTE FUNCTION lake_replication.guard_push_schema_publication();
ALTER TRIGGER push_schema_publication_guard ON lake_replication.transactions DEPENDS ON EXTENSION lake_replication_push;
REVOKE ALL ON FUNCTION lake_replication.push_definition_columns(jsonb),
    lake_replication.push_check_schema_destination(regclass,jsonb),
    lake_replication.initialize_push_schema_history(bigint,jsonb),
    lake_replication.guard_push_schema_publication() FROM PUBLIC;
GRANT SELECT ON lake_replication.push_schema_tables,lake_replication.push_schema_versions,
    lake_replication.push_schema_receipt_versions,lake_replication.push_schema_transactions
    TO lake_replication_admin,lake_replication_worker;
-- Initialization remains administrator-only while the complete pipeline is being qualified.
GRANT EXECUTE ON FUNCTION lake_replication.initialize_push_schema_history(bigint,jsonb)
    TO lake_replication_admin;
GRANT EXECUTE ON FUNCTION lake_replication.push_definition_columns(jsonb),
    lake_replication.push_check_schema_destination(regclass,jsonb) TO lake_replication_admin,lake_replication_worker;
SELECT pg_extension_config_dump('lake_replication.push_schema_tables','');
SELECT pg_extension_config_dump('lake_replication.push_schema_versions','');
SELECT pg_extension_config_dump('lake_replication.push_schema_receipt_versions','');
SELECT pg_extension_config_dump('lake_replication.push_schema_transactions','');

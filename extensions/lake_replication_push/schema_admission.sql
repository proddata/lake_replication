-- Full ordered payloads share the existing journal admission transaction.
CREATE FUNCTION lake_replication.import_push_schema_batch(
    sub bigint,stream uuid,source_system numeric,source_database oid,capture_gen bigint,
    seq bigint,batch uuid,payload_digest text,source_xid bigint,source_begin pg_lsn,
    source_commit pg_lsn,source_end pg_lsn,source_time timestamptz,actions jsonb,snapshot boolean DEFAULT false
) RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE binding push_bindings%ROWTYPE;receipt push_receipts%ROWTYPE;head push_schema_tables%ROWTYPE;
    prior push_schema_versions%ROWTYPE;
    action jsonb;table_uuid uuid;definition jsonb;before_definition jsonb;
    before_version integer;after_version integer;local_version bigint;canonical bytea;
    normalized jsonb;rows jsonb;transaction bigint;initial_versions jsonb;schema_actions jsonb;
BEGIN
    SELECT * INTO STRICT binding FROM push_bindings WHERE subscription_id=sub FOR UPDATE;
    IF NOT binding.schema_enabled OR
        (binding.stream_id,binding.system_identifier,binding.database_oid,binding.capture_generation)
        IS DISTINCT FROM (stream,source_system,source_database,capture_gen) THEN
        RAISE EXCEPTION 'schema push binding identity, generation or mode mismatch' USING ERRCODE='55000';
    END IF;
    SELECT * INTO receipt FROM push_receipts WHERE subscription_id=sub AND sequence=seq;
    IF FOUND THEN
        IF (receipt.batch_id,receipt.digest,receipt.commit_lsn,receipt.end_lsn)
            IS DISTINCT FROM (batch,payload_digest,source_commit,source_end) THEN
            RAISE EXCEPTION 'conflicting schema push replay' USING ERRCODE='23505';
        END IF;
        IF (SELECT count(*) FROM push_schema_receipt_versions WHERE subscription_id=sub AND sequence=seq)<>2 THEN
            RAISE EXCEPTION 'schema replay receipt has incomplete retained versions' USING ERRCODE='55000';
        END IF;
        RETURN 'replayed';
    END IF;
    IF seq IS NULL OR seq<>binding.imported_sequence+1 OR source_end<=binding.imported_end_lsn
        OR source_commit<=binding.imported_commit_lsn OR source_begin>source_commit THEN
        RAISE EXCEPTION 'schema push manifest gap or backwards source coordinates' USING ERRCODE='22023';
    END IF;
    IF snapshot AND (seq<>1 OR binding.imported_sequence<>0) THEN
        RAISE EXCEPTION 'schema snapshot requires a fresh private destination generation' USING ERRCODE='55000';
    END IF;
    IF jsonb_typeof(actions) IS DISTINCT FROM 'array' OR (jsonb_array_length(actions)>100000 OR (jsonb_array_length(actions)=0 AND NOT snapshot))
        OR octet_length(actions::text)>67108864 THEN
        RAISE EXCEPTION 'ordered journal payload budget exceeded' USING ERRCODE='22023';
    END IF;
    IF EXISTS(SELECT FROM jsonb_array_elements(actions) a WHERE a->>'operation'='progress'
        GROUP BY (a->>'request_id')::uuid HAVING count(*)>1) THEN
        RAISE EXCEPTION 'duplicate ordered progress request' USING ERRCODE='22023';
    END IF;
    -- Lock both heads in one deterministic order, shared with ordered publication.
    -- No subset of new metadata may survive a late journal budget failure.
    BEGIN
        PERFORM 1 FROM push_schema_tables WHERE subscription_id=sub ORDER BY table_id FOR UPDATE;
        IF (SELECT count(*) FROM push_schema_tables WHERE subscription_id=sub)<>2 THEN
            RAISE EXCEPTION 'ordered schema history requires two retained table heads' USING ERRCODE='55000';
        END IF;
        SELECT jsonb_object_agg(table_id::text,imported_version) INTO initial_versions
            FROM push_schema_tables WHERE subscription_id=sub;
        IF EXISTS(SELECT FROM jsonb_array_elements(actions) WITH ORDINALITY t(a,n)
            WHERE jsonb_typeof(a) IS DISTINCT FROM 'object' OR jsonb_typeof(a->'source_ordinal') IS DISTINCT FROM 'number'
                OR (a->>'source_ordinal')::bigint IS DISTINCT FROM n-1
                OR coalesce(a->>'operation','') NOT IN ('schema','progress','insert','update','delete','truncate')
                OR (snapshot AND a->>'operation' IS DISTINCT FROM 'insert')) THEN
            RAISE EXCEPTION 'invalid ordered action or ordinal gap' USING ERRCODE='22023';
        END IF;
        IF EXISTS(SELECT FROM jsonb_array_elements(actions) a WHERE a->>'operation'='progress'
            AND ((a->>'request_id')::uuid IS NULL OR (a->>'request_id')::uuid='00000000-0000-0000-0000-000000000000')) THEN
            RAISE EXCEPTION 'invalid ordered progress request' USING ERRCODE='22023';
        END IF;
        SELECT coalesce(jsonb_agg(a ORDER BY (a->>'source_ordinal')::integer),'[]'::jsonb) INTO schema_actions
            FROM jsonb_array_elements(actions) a WHERE a->>'operation'='schema';
        IF jsonb_array_length(schema_actions)>64 THEN
            RAISE EXCEPTION 'schema revision budget exceeded' USING ERRCODE='22023';
        END IF;
        -- Only bounded schema barriers require sequential metadata writes.
        -- Row validation and JSON assembly remain set based for 100,000 events.
        FOR action IN SELECT value FROM jsonb_array_elements(schema_actions) LOOP
            table_uuid:=(action->>'table_id')::uuid;
            SELECT * INTO STRICT head FROM push_schema_tables WHERE subscription_id=sub AND table_id=table_uuid;
            SELECT * INTO STRICT prior FROM push_schema_versions WHERE subscription_id=sub AND table_id=table_uuid
                AND source_schema_version=head.imported_version;
            before_version:=(action->>'before_version')::integer;after_version:=(action->>'after_version')::integer;
            before_definition:=action->'before_definition';definition:=action->'after_definition';
            IF before_version IS DISTINCT FROM head.imported_version
                OR push_encode_schema_definition(before_definition) IS DISTINCT FROM prior.definition_bytes
                OR before_definition IS DISTINCT FROM prior.definition_json
                OR action->>'payload_hex' IS NULL OR octet_length(action->>'payload_hex')>4194432 THEN
                RAISE EXCEPTION 'schema event does not follow retained imported history' USING ERRCODE='55000';
            END IF;
            canonical:=push_encode_schema_change(table_uuid,before_version,after_version,before_definition,definition);
            IF canonical IS DISTINCT FROM decode(action->>'payload_hex','hex') THEN
                RAISE EXCEPTION 'schema payload differs from normalized definitions' USING ERRCODE='22023';
            END IF;
            local_version:=register_relation(sub,head.remote_relation_id,(definition->>'namespace')::name,
                (definition->>'name')::name,(definition->>'replica_identity')::"char",push_definition_columns(definition));
            INSERT INTO push_schema_versions VALUES(sub,table_uuid,after_version,
                push_encode_schema_definition(definition),definition,head.remote_relation_id,local_version);
            UPDATE push_schema_tables SET imported_version=after_version WHERE subscription_id=sub AND table_id=table_uuid;
        END LOOP;
        IF EXISTS(SELECT FROM jsonb_array_elements(actions) a
            LEFT JOIN push_schema_versions v ON v.subscription_id=sub AND v.table_id=(a->>'table_id')::uuid
                AND v.source_schema_version=(a->>'source_schema_version')::integer
            WHERE a->>'operation' IN ('insert','update','delete','truncate') AND
                (v.subscription_id IS NULL OR (a->>'source_schema_version')::bigint IS DISTINCT FROM
                    (initial_versions->>(((a->>'table_id')::uuid)::text))::bigint+
                    (SELECT count(*) FROM jsonb_array_elements(schema_actions) ddl
                        WHERE (ddl->>'table_id')::uuid=(a->>'table_id')::uuid
                            AND (ddl->>'source_ordinal')::integer<(a->>'source_ordinal')::integer))) THEN
            RAISE EXCEPTION 'row schema version is out of source order' USING ERRCODE='55000';
        END IF;
        WITH items AS MATERIALIZED (
            SELECT (a->>'source_ordinal')::integer AS ordinal,a->>'operation' AS operation,
                CASE a->>'operation'
                WHEN 'schema' THEN jsonb_build_object('operation','schema','source_ordinal',(a->>'source_ordinal')::integer,
                    'table_id',(a->>'table_id')::uuid,'before_version',(a->>'before_version')::integer,
                    'after_version',(a->>'after_version')::integer,'before_definition',a->'before_definition',
                    'after_definition',a->'after_definition','payload_hex',encode(decode(a->>'payload_hex','hex'),'hex'))
                WHEN 'progress' THEN jsonb_build_object('operation','progress','source_ordinal',(a->>'source_ordinal')::integer,
                    'request_id',(a->>'request_id')::uuid)
                ELSE (a-ARRAY['remote_relation_id','relation_schema_version'])||jsonb_build_object(
                    'remote_relation_id',v.remote_relation_id::bigint,'relation_schema_version',v.relation_schema_version) END AS item
            FROM jsonb_array_elements(actions) a LEFT JOIN push_schema_versions v
                ON v.subscription_id=sub AND v.table_id=(a->>'table_id')::uuid
                    AND v.source_schema_version=(a->>'source_schema_version')::integer)
        SELECT coalesce(jsonb_agg(item ORDER BY ordinal),'[]'::jsonb),coalesce(jsonb_agg(item ORDER BY ordinal)
            FILTER(WHERE operation IN ('insert','update','delete','truncate')),'[]'::jsonb)
            INTO normalized,rows FROM items;
        IF octet_length(normalized::text)>67108864 THEN
            RAISE EXCEPTION 'normalized ordered journal payload budget exceeded' USING ERRCODE='22023';
        END IF;
        IF NOT stage_transaction(sub,source_xid,source_begin,source_commit,source_end,source_time,rows) THEN
            RAISE EXCEPTION 'schema journal transaction exists without its push receipt' USING ERRCODE='55000';
        END IF;
        SELECT transaction_id INTO STRICT transaction FROM transactions WHERE subscription_id=sub AND commit_lsn=source_commit;
        -- The core trigger charges Schema/Progress metadata on this UPDATE too.
        UPDATE transactions SET payload=normalized WHERE transaction_id=transaction;
        INSERT INTO push_schema_transactions(transaction_id,subscription_id) VALUES(transaction,sub);
        INSERT INTO push_receipts VALUES(sub,seq,batch,payload_digest,source_commit,source_end);
        INSERT INTO push_schema_receipt_versions
            SELECT sub,seq,table_id,imported_version FROM push_schema_tables WHERE subscription_id=sub;
        UPDATE push_bindings SET imported_sequence=seq,imported_commit_lsn=source_commit,imported_end_lsn=source_end,
            snapshot_end_lsn=CASE WHEN snapshot THEN source_end ELSE snapshot_end_lsn END WHERE subscription_id=sub;
    EXCEPTION WHEN SQLSTATE 'LR001' THEN
        RETURN 'full';
    END;
    RETURN 'imported';
END $$;
REVOKE ALL ON FUNCTION lake_replication.import_push_schema_batch(bigint,uuid,numeric,oid,bigint,bigint,uuid,text,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb,boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.import_push_schema_batch(bigint,uuid,numeric,oid,bigint,bigint,uuid,text,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb,boolean) TO lake_replication_worker;
GRANT INSERT ON lake_replication.push_schema_versions,lake_replication.push_schema_receipt_versions,
    lake_replication.push_schema_transactions TO lake_replication_worker;
GRANT UPDATE(imported_version) ON lake_replication.push_schema_tables TO lake_replication_worker;

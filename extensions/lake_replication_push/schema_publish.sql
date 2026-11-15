CREATE FUNCTION lake_replication.push_lock_schema_targets(regclass[],boolean) RETURNS void
LANGUAGE C STRICT AS 'MODULE_PATHNAME','lake_push_lock_schema_targets';
REVOKE ALL ON FUNCTION lake_replication.push_lock_schema_targets(regclass[],boolean) FROM PUBLIC;

CREATE FUNCTION lake_replication.publish_push_schema_batch(
    sub bigint,batch_key text,first_end pg_lsn,last_end pg_lsn
) RETURNS TABLE(batch_id bigint,published_lsn pg_lsn,inserted_rows bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,lake_replication,pg_temp AS $$
DECLARE batch_number bigint;batch_state text;item record;head record;published_versions jsonb;
    generation bigint:=coalesce(nullif(current_setting('lake_replication.worker_generation',true),''),'0')::bigint;
    targets regclass[];exclusive boolean;operation text;definition jsonb;before_definition jsonb;column_value jsonb;
    ddl_operation text;old_name text;new_name text;affected bigint:=0;
    run_operation text;run_destination regclass;run_old jsonb[]:=ARRAY[]::jsonb[];run_new jsonb[]:=ARRAY[]::jsonb[];
    old_keys text[];new_keys text[];run_old_keys text[];run_new_keys text[];
    groupable boolean;run_groupable boolean:=false;can_append boolean;run_bytes bigint:=0;change_bytes bigint;
BEGIN
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=session_user)
        AND NOT pg_has_role(session_user,'lake_replication_admin','MEMBER') THEN
        IF NOT pg_has_role(session_user,'lake_replication_worker','MEMBER') OR generation<=0
            OR coalesce(nullif(current_setting('lake_replication.worker_subscription',true),''),'0')::bigint<>sub THEN
            RAISE EXCEPTION 'ordered publication requires an active managed worker' USING ERRCODE='42501';
        END IF;
        PERFORM assert_worker(sub,generation);
    END IF;
    IF NOT EXISTS(SELECT FROM push_bindings WHERE subscription_id=sub AND schema_enabled) THEN
        RAISE EXCEPTION 'ordered publication requires a schema-enabled push binding' USING ERRCODE='55000';
    END IF;
    batch_number:=prepare_batch(sub,batch_key,first_end,last_end);
    SELECT state INTO STRICT batch_state FROM batches WHERE batches.batch_id=batch_number;
    IF batch_state='published' THEN RETURN QUERY SELECT batch_number,last_end,0::bigint;RETURN;END IF;
    PERFORM 1 FROM push_schema_tables WHERE subscription_id=sub ORDER BY table_id FOR UPDATE;
    SELECT array_agg(destination_relation ORDER BY destination_relation::oid),
        jsonb_object_agg(table_id::text,published_version) INTO targets,published_versions
        FROM push_schema_tables WHERE subscription_id=sub;
    IF cardinality(targets) IS DISTINCT FROM 2 THEN
        RAISE EXCEPTION 'ordered publication has incomplete retained destinations' USING ERRCODE='55000';
    END IF;
    SELECT EXISTS(SELECT FROM transactions t CROSS JOIN LATERAL jsonb_array_elements(t.payload) a
        WHERE t.batch_id=batch_number AND a->>'operation'='schema') INTO exclusive;
    PERFORM push_lock_schema_targets(targets,exclusive);
    FOR head IN SELECT h.destination_relation,v.definition_json FROM push_schema_tables h
        JOIN push_schema_versions v ON v.subscription_id=h.subscription_id AND v.table_id=h.table_id
            AND v.source_schema_version=h.published_version WHERE h.subscription_id=sub LOOP
        PERFORM push_check_schema_destination(head.destination_relation,head.definition_json);
    END LOOP;
    IF EXISTS(SELECT FROM transactions t LEFT JOIN push_schema_transactions s USING(transaction_id)
        WHERE t.batch_id=batch_number AND (s.subscription_id IS DISTINCT FROM sub OR s.applied)) THEN
        RAISE EXCEPTION 'ordered batch has missing or inconsistent apply markers' USING ERRCODE='55000';
    END IF;
    FOR item IN
        WITH expanded AS MATERIALIZED (
            SELECT t.transaction_id,t.end_lsn,a,n,a->>'operation' AS operation,
                count(*) FILTER(WHERE a->>'operation' IN ('insert','update','delete','truncate'))
                    OVER(PARTITION BY t.transaction_id ORDER BY n)-1 AS row_index
            FROM transactions t CROSS JOIN LATERAL jsonb_array_elements(t.payload) WITH ORDINALITY x(a,n)
            WHERE t.batch_id=batch_number)
        SELECT e.*,h.table_id,h.remote_relation_id,h.destination_relation,v.relation_schema_version,
            v.definition_json AS retained_after,v.remote_relation_id AS retained_relation,b.definition_json AS retained_before,
            c.operation AS row_operation,c.remote_relation_id AS row_relation,c.relation_schema_version AS row_version,
            c.old_tuple AS row_old,c.new_tuple AS row_new
        FROM expanded e LEFT JOIN push_schema_tables h ON h.subscription_id=sub AND h.table_id=(e.a->>'table_id')::uuid
        LEFT JOIN push_schema_versions v ON v.subscription_id=sub AND v.table_id=h.table_id
            AND v.source_schema_version=CASE WHEN e.operation='schema' THEN (e.a->>'after_version')::integer
                ELSE (e.a->>'source_schema_version')::integer END
        LEFT JOIN push_schema_versions b ON b.subscription_id=sub AND b.table_id=h.table_id
            AND b.source_schema_version=(e.a->>'before_version')::integer
        LEFT JOIN changes c ON c.transaction_id=e.transaction_id AND c.change_index=e.row_index
            AND e.operation IN ('insert','update','delete','truncate')
        ORDER BY e.end_lsn,e.n
    LOOP
        operation:=item.operation;
        IF (item.a->>'source_ordinal')::bigint IS DISTINCT FROM item.n-1 THEN
            RAISE EXCEPTION 'ordered journal ordinal gap' USING ERRCODE='55000';
        END IF;
        IF operation='progress' THEN CONTINUE;END IF;
        IF item.table_id IS NULL OR item.retained_after IS NULL OR item.retained_relation IS DISTINCT FROM item.remote_relation_id THEN
            RAISE EXCEPTION 'ordered journal has missing schema metadata' USING ERRCODE='55000';
        END IF;
        IF operation='schema' OR operation='truncate' THEN
            IF run_operation IS NOT NULL THEN
                affected:=affected+apply_change_run(run_operation,run_destination,run_old,run_new);
                run_operation:=NULL;run_old:=ARRAY[]::jsonb[];run_new:=ARRAY[]::jsonb[];run_bytes:=0;
            END IF;
        END IF;
        IF operation='schema' THEN
            before_definition:=item.a->'before_definition';definition:=item.a->'after_definition';
            IF (item.a->>'before_version')::integer IS DISTINCT FROM (published_versions->>(item.table_id::text))::integer
                OR before_definition IS DISTINCT FROM item.retained_before OR definition IS DISTINCT FROM item.retained_after
                OR decode(item.a->>'payload_hex','hex') IS DISTINCT FROM push_encode_schema_change(item.table_id,
                    (item.a->>'before_version')::integer,(item.a->>'after_version')::integer,before_definition,definition) THEN
                RAISE EXCEPTION 'schema publication does not follow retained history' USING ERRCODE='55000';
            END IF;
            PERFORM push_check_schema_destination(item.destination_relation,before_definition);
            ddl_operation:=push_schema_operation(before_definition,definition);
            IF ddl_operation='add_nullable' THEN
                column_value:=definition->'columns'->(jsonb_array_length(definition->'columns')-1);
                EXECUTE format('ALTER FOREIGN TABLE %s ADD COLUMN %I %s',item.destination_relation,column_value->>'name',
                    CASE (column_value->>'type_oid')::integer WHEN 16 THEN 'boolean' WHEN 20 THEN 'bigint'
                        WHEN 23 THEN 'integer' WHEN 25 THEN 'text' END);
            ELSE
                SELECT old_column->>'name',new_column->>'name' INTO STRICT old_name,new_name
                FROM jsonb_array_elements(before_definition->'columns') WITH ORDINALITY o(old_column,n)
                JOIN jsonb_array_elements(definition->'columns') WITH ORDINALITY a(new_column,n) USING(n)
                WHERE old_column->>'name'<>new_column->>'name';
                EXECUTE format('ALTER FOREIGN TABLE %s RENAME COLUMN %I TO %I',item.destination_relation,old_name,new_name);
            END IF;
            PERFORM push_check_schema_destination(item.destination_relation,definition);
            PERFORM assign_destination(sub,item.remote_relation_id,item.relation_schema_version,item.destination_relation);
            UPDATE push_schema_tables SET published_version=(item.a->>'after_version')::integer
                WHERE subscription_id=sub AND table_id=item.table_id;
            published_versions:=jsonb_set(published_versions,ARRAY[item.table_id::text],item.a->'after_version');
            CONTINUE;
        END IF;
        IF operation NOT IN ('insert','update','delete','truncate')
            OR (item.a->>'source_schema_version')::integer IS DISTINCT FROM (published_versions->>(item.table_id::text))::integer
            OR (item.a->>'remote_relation_id')::oid IS DISTINCT FROM item.remote_relation_id
            OR (item.a->>'relation_schema_version')::bigint IS DISTINCT FROM item.relation_schema_version
            OR (item.row_operation,item.row_relation,item.row_version,item.row_old,item.row_new)
                IS DISTINCT FROM (operation,item.remote_relation_id,item.relation_schema_version,item.a->'old_tuple',item.a->'new_tuple') THEN
            RAISE EXCEPTION 'ordered row metadata differs from staged schema history' USING ERRCODE='55000';
        END IF;
        IF operation='truncate' THEN
            EXECUTE format('TRUNCATE %s',item.destination_relation);CONTINUE;
        END IF;
        IF operation='insert' THEN old_keys:=NULL;new_keys:=NULL;
        ELSE
            SELECT array_agg(k ORDER BY k) INTO old_keys FROM jsonb_object_keys(item.a->'old_tuple') AS fields(k);
            IF operation='update' THEN SELECT array_agg(k ORDER BY k) INTO new_keys FROM jsonb_object_keys(item.a->'new_tuple') AS fields(k);
            ELSE new_keys:=NULL;END IF;
        END IF;
        groupable:=operation<>'update' OR (item.a->'new_tuple') @> (item.a->'old_tuple');
        change_bytes:=coalesce(octet_length((item.a->'old_tuple')::text),0)+coalesce(octet_length((item.a->'new_tuple')::text),0);
        can_append:=run_operation=operation AND run_destination=item.destination_relation
            AND run_groupable AND groupable AND run_old_keys IS NOT DISTINCT FROM old_keys AND run_new_keys IS NOT DISTINCT FROM new_keys
            AND greatest(cardinality(run_old),cardinality(run_new))<500 AND run_bytes+change_bytes<=4194304
            AND (operation='insert' OR NOT ((item.a->'old_tuple')=ANY(run_old)));
        IF run_operation IS NOT NULL AND NOT coalesce(can_append,false) THEN
            affected:=affected+apply_change_run(run_operation,run_destination,run_old,run_new);
            run_operation:=NULL;run_old:=ARRAY[]::jsonb[];run_new:=ARRAY[]::jsonb[];run_bytes:=0;
        END IF;
        IF run_operation IS NULL THEN
            run_operation:=operation;run_destination:=item.destination_relation;
            run_old_keys:=old_keys;run_new_keys:=new_keys;run_groupable:=groupable;
        END IF;
        IF operation<>'insert' THEN run_old:=array_append(run_old,item.a->'old_tuple');END IF;
        IF operation<>'delete' THEN run_new:=array_append(run_new,item.a->'new_tuple');END IF;
        run_bytes:=run_bytes+change_bytes;
    END LOOP;
    IF run_operation IS NOT NULL THEN affected:=affected+apply_change_run(run_operation,run_destination,run_old,run_new);END IF;
    UPDATE push_schema_transactions s SET applied=true FROM transactions t
        WHERE s.transaction_id=t.transaction_id AND s.subscription_id=sub AND t.batch_id=batch_number;
    PERFORM finish_batch(batch_number);
    RETURN QUERY SELECT batch_number,last_end,affected;
END $$;
REVOKE ALL ON FUNCTION lake_replication.publish_push_schema_batch(bigint,text,pg_lsn,pg_lsn) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.publish_push_schema_batch(bigint,text,pg_lsn,pg_lsn)
    TO lake_replication_admin,lake_replication_worker;

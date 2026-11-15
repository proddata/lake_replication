-- Bounded canonical schema payloads. These helpers do not enable schema admission.
CREATE FUNCTION lake_replication.push_encode_schema_definition(definition jsonb) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog AS $$
DECLARE result bytea;column_value jsonb;position integer:=0;column_name text;
    seen_names text[]:=ARRAY[]::text[];have_key boolean:=false;identifier text;
BEGIN
    IF jsonb_typeof(definition) IS DISTINCT FROM 'object'
        OR definition-ARRAY['namespace','name','replica_identity','columns']<>'{}'::jsonb
        OR jsonb_typeof(definition->'namespace') IS DISTINCT FROM 'string'
        OR jsonb_typeof(definition->'name') IS DISTINCT FROM 'string'
        OR definition->>'replica_identity' NOT IN ('d','i')
        OR jsonb_typeof(definition->'replica_identity') IS DISTINCT FROM 'string'
        OR jsonb_typeof(definition->'columns') IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'invalid schema definition' USING ERRCODE='22023';
    END IF;
    IF jsonb_array_length(definition->'columns') NOT BETWEEN 1 AND 1600 OR octet_length(definition::text)>1048576 THEN
        RAISE EXCEPTION 'schema definition budget exceeded' USING ERRCODE='22023';
    END IF;
    result:=decode('4c4b44534331','hex');
    FOREACH identifier IN ARRAY ARRAY[definition->>'namespace',definition->>'name'] LOOP
        IF octet_length(convert_to(identifier,'UTF8')) NOT BETWEEN 1 AND 63 THEN
            RAISE EXCEPTION 'invalid schema identifier' USING ERRCODE='22023';
        END IF;
        result:=result||int4send(octet_length(convert_to(identifier,'UTF8')))||convert_to(identifier,'UTF8');
    END LOOP;
    result:=result||convert_to(definition->>'replica_identity','UTF8')||int4send(jsonb_array_length(definition->'columns'));
    FOR column_value IN SELECT value FROM jsonb_array_elements(definition->'columns') LOOP
        position:=position+1;
        IF jsonb_typeof(column_value) IS DISTINCT FROM 'object'
            OR column_value-ARRAY['id','name','type_oid','type_modifier','key','nullable']<>'{}'::jsonb
            OR jsonb_typeof(column_value->'id') IS DISTINCT FROM 'number'
            OR jsonb_typeof(column_value->'name') IS DISTINCT FROM 'string'
            OR jsonb_typeof(column_value->'type_oid') IS DISTINCT FROM 'number'
            OR jsonb_typeof(column_value->'type_modifier') IS DISTINCT FROM 'number'
            OR jsonb_typeof(column_value->'key') IS DISTINCT FROM 'boolean'
            OR jsonb_typeof(column_value->'nullable') IS DISTINCT FROM 'boolean' THEN
            RAISE EXCEPTION 'invalid schema column definition' USING ERRCODE='22023';
        END IF;
        column_name:=column_value->>'name';
        IF (column_value->>'id')::bigint<>position OR (column_value->>'type_oid')::bigint NOT IN (16,20,23,25)
            OR (column_value->>'type_modifier')::bigint<>-1
            OR octet_length(convert_to(column_name,'UTF8')) NOT BETWEEN 1 AND 63
            OR column_name COLLATE "C"=ANY(seen_names)
            OR ((column_value->>'key')::boolean AND (column_value->>'nullable')::boolean) THEN
            RAISE EXCEPTION 'unsupported schema column definition' USING ERRCODE='22023';
        END IF;
        seen_names:=array_append(seen_names,column_name);
        have_key:=have_key OR (column_value->>'key')::boolean;
        result:=result||int4send(position)||int4send(octet_length(convert_to(column_name,'UTF8')))||convert_to(column_name,'UTF8')
            ||int4send((column_value->>'type_oid')::integer)||int4send(-1)
            ||decode(CASE WHEN (column_value->>'key')::boolean THEN '01' ELSE '00' END,'hex')
            ||decode(CASE WHEN (column_value->>'nullable')::boolean THEN '01' ELSE '00' END,'hex');
    END LOOP;
    IF NOT have_key OR octet_length(result)>1048576 THEN
        RAISE EXCEPTION 'invalid or oversized schema definition' USING ERRCODE='22023';
    END IF;
    RETURN result;
END $$;
REVOKE ALL ON FUNCTION lake_replication.push_encode_schema_definition(jsonb) FROM PUBLIC;

CREATE FUNCTION lake_replication.push_schema_operation(before_definition jsonb,after_definition jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog,lake_replication AS $$
DECLARE old_columns jsonb;new_columns jsonb;old_column jsonb;new_column jsonb;position integer;renamed integer:=0;
BEGIN
    PERFORM push_encode_schema_definition(before_definition),push_encode_schema_definition(after_definition);
    IF before_definition-'columns' IS DISTINCT FROM after_definition-'columns' THEN
        RAISE EXCEPTION 'schema table or replica identity changed' USING ERRCODE='22023';
    END IF;
    old_columns:=before_definition->'columns';new_columns:=after_definition->'columns';
    IF jsonb_array_length(new_columns)=jsonb_array_length(old_columns)+1
        AND new_columns-(jsonb_array_length(new_columns)-1)=old_columns THEN
        new_column:=new_columns->(jsonb_array_length(new_columns)-1);
        IF NOT (new_column->>'key')::boolean AND (new_column->>'nullable')::boolean THEN RETURN 'add_nullable';END IF;
        RAISE EXCEPTION 'new schema column must be nullable and non-key' USING ERRCODE='22023';
    END IF;
    IF jsonb_array_length(old_columns)<>jsonb_array_length(new_columns) THEN
        RAISE EXCEPTION 'unsupported schema column lifecycle' USING ERRCODE='22023';
    END IF;
    FOR position IN 0..jsonb_array_length(old_columns)-1 LOOP
        old_column:=old_columns->position;new_column:=new_columns->position;
        IF old_column<>new_column THEN
            IF old_column-'name' IS DISTINCT FROM new_column-'name' OR old_column->>'name'=new_column->>'name' THEN
                RAISE EXCEPTION 'unsupported combined schema change' USING ERRCODE='22023';
            END IF;
            renamed:=renamed+1;
        END IF;
    END LOOP;
    IF renamed<>1 THEN RAISE EXCEPTION 'schema requires exactly one rename' USING ERRCODE='22023';END IF;
    RETURN 'rename';
END $$;
REVOKE ALL ON FUNCTION lake_replication.push_schema_operation(jsonb,jsonb) FROM PUBLIC;

CREATE FUNCTION lake_replication.push_encode_schema_change(table_id uuid,before_version integer,after_version integer,
    before_definition jsonb,after_definition jsonb) RETURNS bytea
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog,lake_replication AS $$
DECLARE operation text;before_bytes bytea;after_bytes bytea;
BEGIN
    IF table_id='00000000-0000-0000-0000-000000000000' OR before_version<1 OR after_version<>before_version::bigint+1
        OR after_version>33 THEN RAISE EXCEPTION 'invalid schema identity or version transition' USING ERRCODE='22023';END IF;
    operation:=push_schema_operation(before_definition,after_definition);
    before_bytes:=push_encode_schema_definition(before_definition);after_bytes:=push_encode_schema_definition(after_definition);
    RETURN decode('4c4b44444c31','hex')||uuid_send(table_id)||int4send(before_version)||int4send(after_version)
        ||decode(CASE operation WHEN 'add_nullable' THEN '00' ELSE '01' END,'hex')
        ||int4send(octet_length(before_bytes))||before_bytes||int4send(octet_length(after_bytes))||after_bytes;
END $$;
REVOKE ALL ON FUNCTION lake_replication.push_encode_schema_change(uuid,integer,integer,jsonb,jsonb) FROM PUBLIC;

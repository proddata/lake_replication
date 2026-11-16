-- Private experiment, never installed by the extension/release bundle.
CREATE FUNCTION lake_direct.lock_target(regclass) RETURNS void
AS '$libdir/pg_lake_table','lake_direct_lock_target' LANGUAGE C STRICT VOLATILE;
REVOKE ALL ON FUNCTION lake_direct.lock_target(regclass) FROM PUBLIC;
ALTER FUNCTION lake_direct.apply_payload(jsonb) RENAME TO uncoordinated_apply_payload;
CREATE FUNCTION lake_direct.apply_payload(p_changes jsonb) RETURNS bigint
LANGUAGE plpgsql SET search_path=pg_catalog,lake_direct AS $$
DECLARE target regclass;
BEGIN
    -- Preserve target DDL fencing and eligibility checks before native locks.
    IF NOT lake_replication.lock_insert_group_targets(
        (SELECT array_agg(destination_relation) FROM lake_direct.mapping)) THEN
        RAISE EXCEPTION 'direct prototype target is outside the qualified subset';
    END IF;
    -- This is the complete frozen mapping even for a streamed chunk touching
    -- just one target. Transaction locks survive every later chunk until COMMIT.
    FOR target IN SELECT DISTINCT destination_relation::oid FROM lake_direct.mapping
                  ORDER BY 1 LOOP
        PERFORM lake_direct.lock_target(target);
    END LOOP;
    RETURN lake_direct.uncoordinated_apply_payload(p_changes);
END $$;

CREATE FUNCTION lake_replication.experiment_admit_engine(boolean)
RETURNS void LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', 'lake_replication_experiment_admit_engine';
REVOKE ALL ON FUNCTION lake_replication.experiment_admit_engine(boolean) FROM PUBLIC;

-- Hold permits through engine commit. Schema helpers admit at DDL, so their
-- PostgreSQL-only ready/existing-table paths do not queue behind a bulk copy.
-- Other engine entry points admit before control-row locks. This late-DDL
-- ordering needs separate deadlock/concurrent-schema qualification before use
-- in production; PostgreSQL still detects and aborts a lock-order cycle.
DO $$
DECLARE
    item record;
    definition text;
    installed integer := 0;
BEGIN
    FOR item IN
        SELECT p.oid, p.proname FROM pg_proc p
        JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname='lake_replication' AND p.proname IN (
            'publish_insert_batch', 'apply_change_run', 'finish_initial_copy',
            'finish_initial_sync_group', 'ensure_iceberg_destination',
            'reconcile_relation_schema', 'promote_subscription_rebuild')
    LOOP
        definition := pg_get_functiondef(item.oid);
        IF definition !~ '\mBEGIN\M' THEN
            RAISE EXCEPTION 'unexpected engine function %', item.proname;
        END IF;
        IF item.proname = 'reconcile_relation_schema' THEN
            -- The ready/no-DDL paths stay outside admission. Acquire before
            -- entering the DDL exception block, so resource contention cannot
            -- be committed as a permanent schema pause.
            IF position('    BEGIN' || chr(10) || '        FOR v_column IN' IN definition) = 0 THEN
                RAISE EXCEPTION 'missing schema DDL block';
            END IF;
            definition := replace(definition,
                '    BEGIN' || chr(10) || '        FOR v_column IN',
                '    IF v_new_count > v_old_count THEN' || chr(10) ||
                '        PERFORM lake_replication.experiment_admit_engine(true);' || chr(10) ||
                '    END IF;' || chr(10) ||
                '    BEGIN' || chr(10) || '        FOR v_column IN');
            IF position('    EXCEPTION WHEN OTHERS THEN' IN definition) = 0 THEN
                RAISE EXCEPTION 'missing schema error handler';
            END IF;
            definition := replace(definition, '    EXCEPTION WHEN OTHERS THEN',
                '    EXCEPTION' || chr(10) ||
                '    WHEN SQLSTATE ''40P01'' OR SQLSTATE ''55P03'' OR SQLSTATE ''40001'' THEN' || chr(10) ||
                '        RAISE;' || chr(10) ||
                '    WHEN OTHERS THEN');
            EXECUTE definition;
        ELSIF item.proname = 'ensure_iceberg_destination' THEN
            IF position('EXECUTE format(' IN definition) = 0 THEN
                RAISE EXCEPTION 'missing DDL site in %', item.proname;
            END IF;
            EXECUTE replace(definition, 'EXECUTE format(',
                'PERFORM lake_replication.experiment_admit_engine(true);' || chr(10) ||
                ' EXECUTE format(');
        ELSE
            EXECUTE regexp_replace(definition, '\mBEGIN\M', 'BEGIN' || chr(10) ||
                format(' PERFORM lake_replication.experiment_admit_engine(%L::boolean);',
                    item.proname NOT IN ('publish_insert_batch', 'apply_change_run')));
        END IF;
        installed := installed + 1;
    END LOOP;
    IF installed <> 7 THEN
        RAISE EXCEPTION 'expected seven engine entry points, found %', installed;
    END IF;
END $$;

CREATE VIEW lake_replication.prototype_writer_admission AS
 SELECT l.pid, a.datname AS database_name,
        CASE l.objid WHEN 2 THEN 'bulk' ELSE 'stream' END AS lane,
        CASE WHEN l.granted THEN 'admitted' ELSE 'waiting_for_writer' END AS admission_state,
        l.waitstart, a.wait_event_type, a.wait_event
 FROM pg_catalog.pg_locks l LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='advisory' AND l.database=0 AND l.classid=1280460112
   AND l.objid IN (1,2) AND l.objsubid=1 AND l.mode='ExclusiveLock';
REVOKE ALL ON lake_replication.prototype_writer_admission FROM PUBLIC;
GRANT SELECT ON lake_replication.prototype_writer_admission TO lake_replication_admin;

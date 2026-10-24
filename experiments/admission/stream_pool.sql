CREATE FUNCTION lake_replication.experiment_admit_stream()
RETURNS void LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME','lake_replication_experiment_admit_stream';
REVOKE ALL ON FUNCTION lake_replication.experiment_admit_stream() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.experiment_admit_stream() TO lake_replication_admin;
DO $$
DECLARE item record; definition text; installed integer := 0;
BEGIN
 FOR item IN SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='lake_replication' AND p.proname IN ('publish_insert_batch','apply_change_run')
 LOOP
  definition := pg_get_functiondef(item.oid);
  IF definition !~ '\mBEGIN\M' THEN RAISE EXCEPTION 'missing stream function body'; END IF;
  EXECUTE regexp_replace(definition,'\mBEGIN\M', 'BEGIN' || chr(10) ||
    '    PERFORM lake_replication.experiment_admit_stream();');
  installed := installed + 1;
 END LOOP;
 IF installed <> 2 THEN RAISE EXCEPTION 'expected two streaming entry points'; END IF;
END $$;
CREATE VIEW lake_replication.prototype_stream_admission AS
 SELECT l.pid,a.datname AS database_name,l.objid::bigint-2 AS slot,
        CASE WHEN l.granted THEN 'admitted' ELSE 'waiting_for_stream' END AS admission_state,
        l.waitstart
 FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='advisory' AND l.database=0 AND l.classid=1280460112
 AND l.objid::bigint BETWEEN 3 AND 6 AND l.objsubid=1 AND l.mode='ExclusiveLock';
REVOKE ALL ON lake_replication.prototype_stream_admission FROM PUBLIC;
GRANT SELECT ON lake_replication.prototype_stream_admission TO lake_replication_admin;

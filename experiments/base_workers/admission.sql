CREATE FUNCTION lake_replication.prototype_admit_writer()
RETURNS void LANGUAGE C VOLATILE PARALLEL UNSAFE
AS 'MODULE_PATHNAME', 'lake_replication_prototype_admit_writer';
REVOKE ALL ON FUNCTION lake_replication.prototype_admit_writer() FROM PUBLIC;

-- The ten native worker entry points already invoke assert_writer first.
-- Admission must precede its ownership row lock. Repeated/nested checks reuse
-- the same transaction permit, held until PostgreSQL ends the transaction.
DO $$
DECLARE definition text;
BEGIN
 definition := pg_get_functiondef('lake_replication.prototype_assert_writer(bigint)'::regprocedure);
 IF definition !~ '\mBEGIN\M' THEN RAISE EXCEPTION 'unexpected writer guard'; END IF;
 EXECUTE regexp_replace(definition,'\mBEGIN\M',
   'BEGIN'||chr(10)||' PERFORM lake_replication.prototype_admit_writer();');
END $$;

CREATE VIEW lake_replication.prototype_writer_admission AS
 SELECT l.pid, a.datname AS database_name,
        CASE WHEN l.granted THEN 'admitted' ELSE 'waiting_for_writer' END AS admission_state,
        l.waitstart, a.wait_event_type, a.wait_event
 FROM pg_catalog.pg_locks l LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='advisory' AND l.database=0
   AND l.classid=1280460112 AND l.objid=1 AND l.objsubid=1
   AND l.mode='ExclusiveLock';
REVOKE ALL ON lake_replication.prototype_writer_admission FROM PUBLIC;
GRANT SELECT ON lake_replication.prototype_writer_admission TO lake_replication_admin;

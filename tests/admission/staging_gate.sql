-- Disposable helper and counters, not an extension migration.
CREATE SCHEMA staging_gate;
REVOKE ALL ON SCHEMA staging_gate FROM PUBLIC;
CREATE FUNCTION staging_gate.admit() RETURNS void
AS '/qualification/staging_gate', 'staging_gate_admit'
LANGUAGE C VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION staging_gate.admit() FROM PUBLIC;
CREATE UNLOGGED TABLE staging_gate.counters (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 chunks bigint NOT NULL DEFAULT 0 CHECK(chunks>=0),
 bytes bigint NOT NULL DEFAULT 0 CHECK(bytes>=0)
);
INSERT INTO staging_gate.counters(singleton) VALUES(true);
CREATE VIEW staging_gate.activity AS
 SELECT l.pid,a.datname AS database_name,
 CASE WHEN l.granted THEN 'admitted' ELSE 'waiting' END AS state,
 l.waitstart,a.wait_event_type,a.wait_event
 FROM pg_catalog.pg_locks l LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid=l.pid
 WHERE l.locktype='advisory' AND l.database=0 AND l.classid=1280460112
 AND l.objid=71 AND l.objsubid=1 AND l.mode='ExclusiveLock';
REVOKE ALL ON staging_gate.activity FROM PUBLIC;

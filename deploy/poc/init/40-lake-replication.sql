\set ON_ERROR_STOP on
-- Install the unified extension once (the
-- extension script's own `LOAD` activates the hook for this session
-- immediately), then session_preload_libraries makes it active for every
-- future connection to this database too.
CREATE EXTENSION lake_replication;

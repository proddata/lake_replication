\set ON_ERROR_STOP on
-- Installed the way pg_lake itself ships: CREATE EXTENSION once (the
-- extension script's own `LOAD` activates the hook for this session
-- immediately), then session_preload_libraries makes it active for every
-- future connection to this database too.
CREATE EXTENSION lake_subscription_sugar;
ALTER DATABASE analytics SET session_preload_libraries = 'lake_subscription_sugar';

\set ON_ERROR_STOP on
CREATE EXTENSION pg_lake CASCADE;
CREATE EXTENSION dblink;
CREATE EXTENSION lake_replication;
ALTER DATABASE analytics SET session_preload_libraries = 'lake_replication';

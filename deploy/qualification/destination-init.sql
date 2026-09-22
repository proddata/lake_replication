\set ON_ERROR_STOP on
CREATE EXTENSION pg_lake CASCADE;
CREATE EXTENSION lake_replication;
CREATE EXTENSION lake_subscription_sugar CASCADE;
ALTER DATABASE analytics SET session_preload_libraries = 'lake_subscription_sugar';

\set ON_ERROR_STOP on
-- POSTGRES_DB=analytics makes docker_setup_db create+connect "analytics"
-- automatically before this file runs; only "application" needs creating
-- here. Extensions are created here (not left to CREATE EXTENSION pg_lake
-- CASCADE alone) so dblink is ready before lake_subscription_sugar loads.
CREATE DATABASE application;
CREATE EXTENSION pg_lake CASCADE;
CREATE EXTENSION dblink;

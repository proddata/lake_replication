\set ON_ERROR_STOP on
CREATE DATABASE application;
CREATE DATABASE analytics;
\connect analytics
CREATE EXTENSION pg_lake CASCADE;
CREATE EXTENSION dblink;
CREATE EXTENSION lake_replication;

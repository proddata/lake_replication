\echo Use "ALTER EXTENSION lake_subscription_sugar UPDATE" to load this file. \quit

ALTER TABLE lake_subscription_sugar.worker_launch_config
    ADD COLUMN credential_file text
        CHECK (credential_file IS NULL OR credential_file LIKE '/%'),
    ADD COLUMN config_generation bigint NOT NULL DEFAULT 1
        CHECK (config_generation > 0);

SELECT pg_catalog.pg_extension_config_dump(
    'lake_subscription_sugar.pending_cleanup', '');
SELECT pg_catalog.pg_extension_config_dump(
    'lake_subscription_sugar.worker_launch_config', '');

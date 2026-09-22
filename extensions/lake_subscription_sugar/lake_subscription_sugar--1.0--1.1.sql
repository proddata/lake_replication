\echo Use "ALTER EXTENSION lake_subscription_sugar UPDATE" to load this file. \quit

SELECT pg_catalog.pg_extension_config_dump(
    'lake_subscription_sugar.pending_cleanup', '');
SELECT pg_catalog.pg_extension_config_dump(
    'lake_subscription_sugar.worker_launch_config', '');

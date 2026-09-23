BEGIN;

-- Tell pg_dump that these extension-owned tables contain operator/runtime
-- state which must survive logical backup and restore. type_mappings is
-- deliberately excluded because the extension recreates its 13 seed rows.
SELECT pg_catalog.pg_extension_config_dump('lake_replication.sources', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.subscriptions', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.relations', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.checkpoints', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.batches', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.transactions', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.changes', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.streamed_transactions', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.streamed_changes', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.initial_copies', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.initial_copy_rows', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.relation_type_contracts', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.rebuild_promotions', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.service_status', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.journal_maintenance_policies', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.journal_maintenance_status', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.pending_cleanup', '');
SELECT pg_catalog.pg_extension_config_dump('lake_replication.worker_launch_config', '');

COMMIT;

-- PoC-only compatibility views, not part of the shipped replication path.
-- Mirrors Crunchy Data Warehouse's
-- documented crunchy_iceberg.stat_subscription / stat_subscription_rel
-- views, built entirely over this project's existing
-- lake_replication.subscriptions/batches/relations/transactions/changes
-- data -- no new instrumentation, no new tables.
--
-- Known gap: Crunchy's stat_subscription_rel also reports cumulative
-- rows_inserted/rows_deleted. This project does not keep a running total
-- once journal rows are reclaimed by Milestone 24's maintenance, so those
-- two columns are intentionally left out rather than faked.

BEGIN;

CREATE SCHEMA IF NOT EXISTS crunchy_iceberg;

CREATE OR REPLACE VIEW crunchy_iceberg.stat_subscription AS
SELECT
    s.subscription_id,
    s.subscription_name AS subname,
    last_batch.published_at AS last_flush_time,
    (last_batch.published_at - last_batch.created_at) AS last_flush_duration,
    last_batch.last_end_lsn AS last_flush_lsn
FROM lake_replication.subscriptions s
LEFT JOIN LATERAL (
    SELECT b.created_at, b.published_at, b.last_end_lsn
      FROM lake_replication.batches b
     WHERE b.subscription_id = s.subscription_id
       AND b.state = 'published'
     ORDER BY b.published_at DESC
     LIMIT 1
) AS last_batch ON true;

COMMENT ON VIEW crunchy_iceberg.stat_subscription IS
    'Experiment: Crunchy-Data-Warehouse-shaped subscription flush status, '
    'derived from lake_replication.batches. Not part of the shipped '
    'replication path.';

CREATE OR REPLACE VIEW crunchy_iceberg.stat_subscription_rel AS
WITH current_relations AS (
    SELECT DISTINCT ON (subscription_id, remote_relation_id)
        subscription_id, remote_relation_id, source_namespace, source_relation
      FROM lake_replication.relations
     ORDER BY subscription_id, remote_relation_id, schema_version DESC
)
SELECT
    r.subscription_id,
    s.subscription_name AS subname,
    r.remote_relation_id,
    r.source_namespace,
    r.source_relation,
    count(*) FILTER (WHERE c.operation = 'insert') AS unflushed_inserts,
    count(*) FILTER (WHERE c.operation = 'update') AS unflushed_updates,
    count(*) FILTER (WHERE c.operation = 'delete') AS unflushed_deletes
FROM current_relations r
JOIN lake_replication.subscriptions s USING (subscription_id)
LEFT JOIN lake_replication.transactions t
       ON t.subscription_id = r.subscription_id
      AND t.state IN ('staged', 'publishing')
LEFT JOIN lake_replication.changes c
       ON c.transaction_id = t.transaction_id
      AND c.remote_relation_id = r.remote_relation_id
GROUP BY r.subscription_id, s.subscription_name, r.remote_relation_id,
         r.source_namespace, r.source_relation;

COMMENT ON VIEW crunchy_iceberg.stat_subscription_rel IS
    'Experiment: Crunchy-Data-Warehouse-shaped per-relation unflushed row '
    'counts, derived from lake_replication.transactions/changes. Cumulative '
    'rows_inserted/rows_deleted are intentionally omitted -- this project '
    'does not keep a running total once Milestone 24 maintenance reclaims '
    'journal rows. Not part of the shipped replication path.';

COMMIT;

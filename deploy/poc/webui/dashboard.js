// Explicit projections keep connection credentials out of the dashboard response.
const QUERIES = {
  subscriptions: ['destination', `
    SELECT s.subscription_id, s.subscription_name, s.state, s.slot_name,
           s.publication_names, COALESCE(s.last_error, svc.last_error) AS last_error,
           svc.supervisor_pid, svc.state AS service_state, svc.heartbeat_at,
           svc.consecutive_failures, svc.next_retry_at,
           c.received_lsn, c.staged_lsn, c.published_lsn, c.acknowledged_lsn, svc.observed_current_lsn,
           GREATEST(pg_wal_lsn_diff(c.received_lsn, c.published_lsn), 0)::text AS journal_lag_bytes,
           pg_wal_lsn_diff(c.staged_lsn, c.published_lsn)::text AS pending_bytes
      FROM lake_replication.subscriptions s
      LEFT JOIN lake_replication.service_status svc USING (subscription_id)
      LEFT JOIN lake_replication.checkpoints c USING (subscription_id)
     ORDER BY s.subscription_id`],
  relations: ['destination', `
    SELECT subname, source_namespace, source_relation,
           unflushed_inserts, unflushed_updates, unflushed_deletes
      FROM crunchy_iceberg.stat_subscription_rel ORDER BY subname, source_relation`],
  batches: ['destination', `
    SELECT b.batch_id, s.subscription_name, b.state, b.last_end_lsn,
           b.created_at, b.published_at,
           round(extract(epoch FROM (b.published_at - b.created_at)) * 1000) AS duration_ms
      FROM lake_replication.batches b
      JOIN lake_replication.subscriptions s USING (subscription_id)
     ORDER BY b.batch_id DESC LIMIT 12`],
  iceberg: ['destination', `
    SELECT table_namespace, table_name, metadata_location
      FROM iceberg_tables ORDER BY table_namespace, table_name LIMIT 50`],
  snapshot: ['destination', `
    WITH demo AS MATERIALIZED (
      SELECT lake_iceberg.metadata(metadata_location) AS metadata
        FROM iceberg_tables
       WHERE table_namespace = 'public' AND table_name = 'orders' LIMIT 1
    )
    SELECT metadata->>'format-version' AS format_version,
           metadata->>'current-schema-id' AS schema_id,
           metadata->>'current-snapshot-id' AS snapshot_id,
           metadata->>'last-sequence-number' AS sequence_number,
           to_timestamp((metadata->>'last-updated-ms')::double precision / 1000) AS updated_at,
           (SELECT snapshot->'summary' FROM jsonb_array_elements(metadata->'snapshots') snapshot
             WHERE snapshot->>'snapshot-id' = metadata->>'current-snapshot-id' LIMIT 1) AS summary
      FROM demo`],
  snapshots: ['destination', `
    WITH demo AS MATERIALIZED (
      SELECT lake_iceberg.metadata(metadata_location) AS metadata FROM iceberg_tables
       WHERE table_namespace = 'public' AND table_name = 'orders' LIMIT 1
    )
    SELECT snapshot->>'snapshot-id' AS snapshot_id,
           to_timestamp((snapshot->>'timestamp-ms')::double precision / 1000) AS committed_at,
           snapshot->'summary'->>'operation' AS operation,
           snapshot->'summary'->>'added-data-files' AS added_files,
           snapshot->'summary'->>'deleted-data-files' AS removed_files,
           snapshot->'summary'->>'added-records' AS added_records,
           snapshot->'summary'->>'deleted-records' AS deleted_records,
           snapshot->'summary' AS summary
      FROM demo, LATERAL jsonb_array_elements(metadata->'snapshots') AS snapshot
     ORDER BY (snapshot->>'timestamp-ms')::bigint DESC LIMIT 8`],
  slots: ['source', `
    SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, wal_status,
           pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::text AS retained_bytes
      FROM pg_replication_slots WHERE database = current_database() ORDER BY slot_name`],
  source: ['source', 'SELECT * FROM public.orders ORDER BY id DESC LIMIT 20'],
  destination: ['destination', 'SELECT * FROM public.orders ORDER BY id DESC LIMIT 20'],
};

async function collectDashboard(pools) {
  const entries = await Promise.all(Object.entries(QUERIES).map(async ([name, [target, sql]]) => {
    try {
      const result = await pools[target].query({ text: sql, query_timeout: 5000 });
      return [name, { rows: result.rows, fields: result.fields.map(field => field.name) }];
    } catch (error) {
      return [name, { rows: [], fields: [], error: error.message }];
    }
  }));
  return { sampledAt: new Date().toISOString(), panels: Object.fromEntries(entries) };
}

module.exports = { collectDashboard, QUERIES };

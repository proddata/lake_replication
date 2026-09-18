// Minimal demo console: two Postgres connections (source "application",
// destination "analytics") and a tiny HTTP API so the static page can run
// arbitrary SQL against either one and show the results side by side.
//
// This intentionally lets the browser run arbitrary SQL -- it is a local
// admin console for demoing replication, not a public app. The compose
// file binds it to 127.0.0.1 only; never expose this beyond localhost.
const express = require('express');
const path = require('path');
const { Pool } = require('pg');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const pools = {
  source: new Pool({
    host: process.env.LAKE_SOURCE_HOST || '127.0.0.1',
    port: Number(process.env.LAKE_SOURCE_PORT || 5432),
    database: process.env.LAKE_SOURCE_DATABASE || 'application',
    user: process.env.LAKE_SOURCE_USER || 'postgres',
    password: process.env.LAKE_SOURCE_PASSWORD || undefined,
    max: 4,
  }),
  destination: new Pool({
    host: process.env.LAKE_DESTINATION_HOST || '127.0.0.1',
    port: Number(process.env.LAKE_DESTINATION_PORT || 5432),
    database: process.env.LAKE_DESTINATION_DATABASE || 'analytics',
    user: process.env.LAKE_DESTINATION_USER || 'postgres',
    password: process.env.LAKE_DESTINATION_PASSWORD || undefined,
    max: 4,
  }),
};

// node-postgres emits 'error' on a pool whenever an idle client's
// connection drops (e.g. the demo Postgres container restarting under a
// kill -9 test) -- with no listener, Node's default EventEmitter behavior
// for an unhandled 'error' event is to throw and crash the whole process.
// Log and let the pool reconnect on the next query instead.
for (const [name, pool] of Object.entries(pools)) {
  pool.on('error', (error) => {
    console.error(`[${name} pool] idle client error (will reconnect on next query):`, error.message);
  });
}

app.post('/api/query', async (req, res) => {
  const { target, sql } = req.body || {};
  const pool = pools[target];
  if (!pool) {
    return res.status(400).json({ error: `unknown target "${target}"; expected source or destination` });
  }
  if (typeof sql !== 'string' || !sql.trim()) {
    return res.status(400).json({ error: 'sql must be a non-empty string' });
  }
  try {
    const result = await pool.query(sql);
    const results = Array.isArray(result) ? result : [result];
    const last = results[results.length - 1];
    res.json({
      fields: (last.fields || []).map((f) => f.name),
      rows: last.rows || [],
      rowCount: last.rowCount,
      command: last.command,
    });
  } catch (error) {
    res.status(400).json({ error: error.message });
  }
});

app.get('/api/status', async (req, res) => {
  try {
    const subscriptions = await pools.destination.query(`
      SELECT s.subscription_id, s.subscription_name, s.state,
             svc.supervisor_pid, svc.state AS service_state,
             svc.last_error
        FROM lake_replication.subscriptions s
        LEFT JOIN lake_replication.service_status svc USING (subscription_id)
       ORDER BY s.subscription_id
    `);
    let relStats = { rows: [] };
    try {
      relStats = await pools.destination.query(`
        SELECT subname, source_relation, unflushed_inserts, unflushed_updates, unflushed_deletes
          FROM crunchy_iceberg.stat_subscription_rel
         ORDER BY subname, source_relation
      `);
    } catch (_) {
      // Monitoring views are optional; ignore if not installed.
    }
    res.json({ subscriptions: subscriptions.rows, relations: relStats.rows });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  console.log(`pg_lake_replication demo console listening on :${port}`);
});

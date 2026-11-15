const { randomUUID } = require('node:crypto');

const ORDER_FIELDS = ['id', 'customer', 'item', 'amount', 'created_at'];
function equalRows(a, b) {
  if (!a || !b) return a === b;
  return ORDER_FIELDS.every(key => String(a[key] ?? '') === String(b[key] ?? ''));
}
function normalize(row) {
  return row ? JSON.parse(JSON.stringify(row)) : null;
}
class Playground {
  constructor({ now = Date.now } = {}) {
    this.now = now;
    this.events = [];
    this.totalVerified = 0;
    this.lastId = 0n;
    this.tracked = [];
    this.history = [];
    this.lastSample = null;
    this.lastHistoryTotal = 0;
    this.sessionId = randomUUID();
    this.destinationSeen = new Set();
  }

  nextId() {
    const candidate = BigInt(this.now()) * 1000n + 500n;
    this.lastId = candidate > this.lastId ? candidate : this.lastId + 1n;
    return this.lastId.toString();
  }

  record({ operation, before, after, xid = null, sql, parameters = [], origin = 'playground', committedAt = this.now() }) {
    const id = String((after || before).id);
    for (const event of this.events) {
      if (event.id === id && event.state === 'pending') event.state = 'superseded';
    }
    const event = { eventId: randomUUID(), id, operation, before: normalize(before), after: normalize(after),
      xid, sql, parameters, origin, committedAt, state: 'pending', visibleAt: null, visibilityMs: null,
      stagedMs: null, batchId: null, publicationMs: null, seenInDestination: this.destinationSeen.has(id) };
    this.events.unshift(event);
    this.events = this.events.slice(0, 200);
    this.tracked = [id, ...this.tracked.filter(value => value !== id)].slice(0, 30);
    return event;
  }

  async mutate(pool, { operation, id }) {
    if (!['insert', 'update', 'delete'].includes(operation)) throw new Error('Choose insert, update, or delete.');
    if (operation !== 'insert' && !/^\d{1,19}$/.test(String(id || ''))) throw new Error('Select an order to update or delete.');
    const client = await pool.connect();
    let event;
    try {
      await client.query('BEGIN');
      const transaction = await client.query('SELECT (pg_current_xact_id()::text::numeric % 4294967296)::text AS xid');
      let before = null;
      if (operation !== 'insert') {
        before = (await client.query('SELECT * FROM public.orders WHERE id = $1 FOR UPDATE', [id])).rows[0];
        if (!before) throw new Error('That order no longer exists. Select another row.');
      }
      const newId = operation === 'insert' ? this.nextId() : String(id);
      const sql = operation === 'insert'
        ? "INSERT INTO public.orders (id, customer, item, amount) VALUES ($1, 'Demo customer', 'demo order', 49.95) RETURNING *"
        : operation === 'update'
          ? 'UPDATE public.orders SET amount = round((amount + 5)::numeric, 2)::double precision WHERE id = $1 RETURNING *'
          : 'DELETE FROM public.orders WHERE id = $1 RETURNING *';
      const result = await client.query(sql, [newId]);
      if (!result.rowCount) throw new Error('The order was not changed. Refresh and try again.');
      await client.query('COMMIT');
      event = this.record({ operation, before: operation === 'delete' ? result.rows[0] : before,
        after: operation === 'delete' ? null : result.rows[0], xid: transaction.rows[0].xid, sql, parameters: [newId] });
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally { client.release(); }
    return event;
  }

  async compare(pools, seedPanels) {
    // Recent mutation IDs stay pinned, including deletions. The same exact ID list
    // is queried on both databases; there is no independent destination LIMIT.
    const seeds = [...(seedPanels.source.rows || []), ...(seedPanels.destination.rows || [])].map(row => String(row.id));
    const ids = [...new Set([...this.tracked, ...seeds])].slice(0, 30);
    this.tracked = ids;
    const read = async pool => {
      try {
        const result = await pool.query({ text: 'SELECT * FROM public.orders WHERE id = ANY($1::bigint[])', values: [ids], query_timeout: 5000 });
        return { rows: result.rows.map(normalize), error: null, observedAt: this.now() };
      } catch (error) { return { rows: [], error: error.message, observedAt: this.now() }; }
    };
    const [source, destination] = await Promise.all([read(pools.source), read(pools.destination)]);
    const xids = this.events.filter(event => (event.state === 'pending' || event.state === 'visible' && !event.batchId) && event.xid).map(event => event.xid);
    let journal = [];
    let journalError = null;
    if (xids.length) {
      try {
        journal = (await pools.destination.query({ text: `
          SELECT t.xid::text, t.state, t.batch_id::text,
                 round(extract(epoch FROM (b.published_at - b.created_at)) * 1000) AS publication_ms
            FROM lake_replication.transactions t
            LEFT JOIN lake_replication.batches b USING (batch_id)
           WHERE t.xid = ANY($1::bigint[]) ORDER BY t.transaction_id DESC`, values: [xids], query_timeout: 5000 })).rows;
      } catch (error) { journalError = error.message; }
    }
    const byId = new Map(destination.rows.map(row => [String(row.id), row]));
    if (!destination.error) {
      for (const id of byId.keys()) this.destinationSeen.add(id);
      this.destinationSeen = new Set([...this.destinationSeen].filter(id => ids.includes(id)));
    }
    const now = this.now();
    const newlyVisible = [];
    for (const event of this.events) {
      if (!['pending', 'visible'].includes(event.state)) continue;
      const transaction = journal.find(row => row.xid === event.xid);
      if (transaction) {
        if (event.stagedMs == null) event.stagedMs = Math.max(0, now - event.committedAt);
        event.batchId = transaction.batch_id;
        event.publicationMs = transaction.publication_ms == null ? null : Number(transaction.publication_ms);
      }
      if (event.state !== 'pending') continue;
      if (!ids.includes(event.id)) { event.state = 'untracked'; continue; }
      if (destination.error || destination.observedAt < event.committedAt) continue;
      const actual = byId.get(event.id) || null;
      if (actual) event.seenInDestination = true;
      const matches = event.operation === 'delete'
        ? !actual && (transaction?.state === 'published' || event.seenInDestination)
        : equalRows(actual, event.after);
      if (matches) {
        event.state = 'visible';
        event.visibleAt = destination.observedAt;
        event.visibilityMs = Math.max(0, event.visibleAt - event.committedAt);
        this.totalVerified++;
        newlyVisible.push(event);
      }
    }
    const sourceMap = new Map(source.rows.map(row => [String(row.id), row]));
    const rows = ids.map(id => {
      const event = this.events.find(candidate => candidate.id === id);
      const left = sourceMap.get(id) || null;
      const right = byId.get(id) || null;
      return { id, source: left, destination: right,
        tombstone: event?.operation === 'delete' ? event.before : null,
        eventId: event?.eventId, operation: event?.operation,
        state: source.error || destination.error ? 'unknown' : equalRows(left, right) ? 'matched' : 'pending' };
    });
    const elapsed = this.lastSample == null ? null : now - this.lastSample;
    const throughput = elapsed > 0 ? (this.totalVerified - this.lastHistoryTotal) * 1000 / elapsed : null;
    const latency = newlyVisible.length ? Math.max(...newlyVisible.map(event => event.visibilityMs)) : null;
    if (this.lastSample == null || elapsed >= 500) {
      this.history.push({ at: now, throughput, latency });
      this.history = this.history.slice(-60);
      this.lastSample = now;
      this.lastHistoryTotal = this.totalVerified;
    }
    return { sessionId: this.sessionId, sampledAt: now, rows, sourceError: source.error, destinationError: destination.error,
      journalError, events: this.events.slice(0, 40), totalVerified: this.totalVerified,
      pending: this.events.filter(event => event.state === 'pending').length,
      history: this.history, throughput: this.history.at(-1)?.throughput ?? null };
  }
}
module.exports = { Playground, equalRows };

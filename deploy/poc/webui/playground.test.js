const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Playground } = require('./playground');
const row = (id = '1', amount = 10) => ({ id, customer: 'Ada', item: 'engine', amount, created_at: '2026-11-11T00:00:00.000Z' });
function fixture() {
  let now = 1000;
  const tracker = new Playground({ now: () => now });
  const data = { source: [row()], destination: [row()], journal: [] };
  const calls = [];
  const pools = Object.fromEntries(['source', 'destination'].map(target => [target, { query: async query => {
    calls.push([target, query]);
    if (query.text.includes('lake_replication.transactions')) return { rows: data.journal };
    if (data[target] instanceof Error) throw data[target];
    return { rows: data[target] };
  } }]));
  const seed = { source: { rows: [row()] }, destination: { rows: [] } };
  return { tracker, data, pools, calls, sample: () => tracker.compare(pools, seed), advance: ms => { now += ms; } };
}
test('both databases query the same IDs, including pinned tombstones', async () => {
  const f = fixture();
  f.tracker.record({ operation: 'delete', before: row('7'), after: null, sql: 'DELETE', xid: '1' });
  const result = await f.sample();
  const reads = f.calls.filter(([, query]) => query.text.includes('ANY($1::bigint[])') && !query.text.includes('transactions'));
  assert.deepEqual(reads[0][1].values, reads[1][1].values);
  assert.deepEqual(reads[0][1].values, [['7', '1']]);
  assert.equal(result.rows[0].tombstone.id, '7');
  assert.equal(result.events[0].state, 'pending', 'absence before ever seeing the row is not proof of deletion');
});
test('insert/update visibility requires exact row values and errors cannot confirm it', async () => {
  const f = fixture();
  f.tracker.record({ operation: 'update', before: row(), after: row('1', 15), sql: 'UPDATE', xid: '22' });
  f.data.source = [row('1', 15)];
  f.data.journal = [{ xid: '22', state: 'published', batch_id: '8', publication_ms: '90' }];
  f.advance(100);
  assert.equal((await f.sample()).totalVerified, 0, 'published WAL alone is not query visibility');
  f.data.destination = new Error('query failed');
  assert.equal((await f.sample()).events[0].state, 'pending');
  f.data.destination = [row('1', 15)];
  f.advance(800);
  const sample = await f.sample();
  assert.equal(sample.events[0].state, 'visible');
  assert.equal(sample.events[0].visibilityMs, 900);
  assert.equal(sample.events[0].stagedMs, 100);
  assert.equal(sample.events[0].publicationMs, 90);
  assert.equal(sample.events[0].batchId, '8');
  assert.equal(sample.totalVerified, 1);
  assert.equal((await f.sample()).totalVerified, 1, 'polling never double counts');
});
test('delete stays in context and verifies after an observed destination row disappears', async () => {
  const f = fixture();
  await f.sample();
  f.tracker.record({ operation: 'delete', before: row(), after: null, sql: 'DELETE' });
  f.data.source = [];
  let result = await f.sample();
  assert.equal(result.rows[0].state, 'pending');
  assert.equal(result.events[0].state, 'pending');
  f.data.destination = [];
  f.advance(400);
  result = await f.sample();
  assert.equal(result.events[0].state, 'visible');
  assert.equal(result.rows[0].tombstone.customer, 'Ada');
  assert.equal(result.rows[0].state, 'matched');
});
test('intermediate versions are superseded rather than counted as replicated', async () => {
  const f = fixture();
  f.tracker.record({ operation: 'insert', after: row(), sql: 'INSERT' });
  f.tracker.record({ operation: 'update', before: row(), after: row('1', 15), sql: 'UPDATE' });
  f.data.source = f.data.destination = [row('1', 15)];
  const result = await f.sample();
  assert.equal(result.totalVerified, 1);
  assert.equal(result.events[1].state, 'superseded');
});
test('mutations use transactions, parameters, returned rows and rollback on failure', async () => {
  const tracker = new Playground();
  const calls = [];
  let released = false;
  const client = { query: async (sql, values) => {
    calls.push([sql, values]);
    if (sql.includes('pg_current_xact_id')) return { rows: [{ xid: '42' }] };
    if (sql.includes('FOR UPDATE')) return { rows: [row()] };
    if (sql.startsWith('UPDATE')) return { rows: [row('1', 15)], rowCount: 1 };
    return { rows: [] };
  }, release: () => { released = true; } };
  const event = await tracker.mutate({ connect: async () => client }, { operation: 'update', id: '1' });
  assert.equal(event.after.amount, 15);
  assert.equal(event.before.amount, 10);
  assert.equal(event.xid, '42');
  assert.ok(calls.find(([sql, values]) => sql.startsWith('UPDATE') && values[0] === '1'));
  assert.equal(calls.at(-1)[0], 'COMMIT');
  assert.ok(released);
  await assert.rejects(tracker.mutate({}, { operation: 'update', id: '1; DROP TABLE orders' }), /Select an order/);
  calls.length = 0;
  client.query = async sql => { calls.push([sql]); if (sql.startsWith('SELECT')) throw new Error('database unavailable'); return { rows: [] }; };
  await assert.rejects(tracker.mutate({ connect: async () => client }, { operation: 'delete', id: '1' }), /database unavailable/);
  assert.equal(calls.at(-1)[0], 'ROLLBACK');
});

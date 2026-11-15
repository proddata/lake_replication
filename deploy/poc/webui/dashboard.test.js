const { test } = require('node:test');
const assert = require('node:assert/strict');
const { collectDashboard, QUERIES } = require('./dashboard');

test('samples both databases and preserves independent panel failures', async () => {
  const calls = [];
  const pool = target => ({ query: async query => {
    calls.push([target, query]);
    if (query.text === QUERIES.relations[1]) throw new Error('monitoring view unavailable');
    if (target === 'source') throw new Error('source offline');
    return { rows: [{ state: 'streaming' }], fields: [{ name: 'state' }] };
  } });
  const sample = await collectDashboard({ source: pool('source'), destination: pool('destination') });
  assert.ok(Number.isFinite(Date.parse(sample.sampledAt)));
  assert.equal(calls.length, Object.keys(QUERIES).length);
  assert.equal(sample.panels.source.error, 'source offline');
  assert.equal(sample.panels.slots.error, 'source offline');
  assert.equal(sample.panels.relations.error, 'monitoring view unavailable');
  assert.deepEqual(sample.panels.subscriptions, { rows: [{ state: 'streaming' }], fields: ['state'] });
  assert.ok(calls.every(([, query]) => query.query_timeout === 5000));
});

test('missing destination table is isolated from healthy source previews', async () => {
  const sample = await collectDashboard({
    source: { query: async () => ({ rows: [{ id: '1' }], fields: [{ name: 'id' }] }) },
    destination: { query: async () => { throw new Error('relation does not exist'); } },
  });
  assert.deepEqual(sample.panels.source.rows, [{ id: '1' }]);
  assert.deepEqual(sample.panels.destination.rows, []);
  assert.equal(sample.panels.destination.error, 'relation does not exist');
});

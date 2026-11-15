const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Simulator } = require('./simulator');

function setup(query = async () => ({ rowCount: 1 })) {
  const timers = new Map();
  const calls = [];
  let clock = 1800000000000;
  const simulator = new Simulator({ query: async (...args) => { calls.push(args); return query(...args); } }, {
    now: () => clock,
    schedule: (fn, delay) => { const key = {}; timers.set(key, { fn, delay }); return key; },
    cancel: key => timers.delete(key),
  });
  return { simulator, timers, calls, advance: ms => { clock += ms; } };
}

test('mixed workload mutates only run-owned IDs and counts committed rows', async () => {
  const { simulator, calls, timers } = setup();
  simulator.start();
  for (let i = 0; i < 5; i++) await simulator.tick();
  assert.deepEqual(simulator.status().counts, { inserts: 3, updates: 1, deletes: 1 });
  assert.equal(calls[2][1][0], calls[1][1][0], 'updates a previously inserted row');
  assert.equal(calls[4][1][0], calls[3][1][0], 'deletes a previously inserted row');
  assert.ok(calls.every(([, values]) => values[1] === calls[0][1][1] && values[1].startsWith('Simulator ')));
  assert.match(calls[2][0], /WHERE id = \$1 AND customer = \$2/);
  assert.match(calls[4][0], /WHERE id = \$1 AND customer = \$2/);
  assert.equal(simulator.status().events.length, 5);
  simulator.stop();
  const count = calls.length;
  await simulator.tick();
  assert.equal(calls.length, count, 'stop prevents new SQL');
  assert.equal(simulator.status().state, 'stopped');
});

test('validates bounds and prevents duplicate runs', () => {
  const { simulator } = setup();
  for (const options of [{ rate: 0 }, { rate: 11 }, { rate: 1.5 }, { duration: 0 }, { duration: 301 }, { rate: '2' }]) {
    assert.throws(() => simulator.start(options), /operations per second/);
  }
  simulator.start({ rate: 10, duration: 300 });
  assert.throws(() => simulator.start(), /already active/);
});

test('stop waits for an in-flight operation and cannot start overlapping work', async () => {
  let release;
  const { simulator, calls } = setup(() => new Promise(resolve => { release = resolve; }));
  simulator.start();
  const pending = simulator.tick();
  await simulator.tick();
  assert.equal(calls.length, 1);
  assert.equal(simulator.stop().state, 'stopping');
  assert.throws(() => simulator.start(), /already active/);
  release({ rowCount: 1 });
  await pending;
  assert.equal(simulator.status().state, 'stopped');
  assert.equal(simulator.status().counts.inserts, 1);
});

test('deadline prevents further writes; query errors stop the run', async () => {
  const { simulator, calls, advance } = setup();
  simulator.start({ duration: 10 });
  advance(10001);
  await simulator.tick();
  assert.equal(simulator.status().state, 'stopped');
  assert.equal(calls.length, 0);
  const failing = setup(async () => { throw new Error('source unavailable'); });
  failing.simulator.start();
  await failing.simulator.tick();
  assert.equal(failing.simulator.status().state, 'error');
  assert.equal(failing.simulator.status().lastError, 'source unavailable');
  assert.equal(failing.simulator.status().counts.inserts, 0);
});

test('activity is bounded and a new run resets counts and ownership', async () => {
  const { simulator, calls } = setup();
  simulator.start();
  for (let i = 0; i < 30; i++) await simulator.tick();
  assert.equal(simulator.status().events.length, 12);
  const previousOwner = calls[0][1][1];
  simulator.stop();
  simulator.start();
  assert.deepEqual(simulator.status().counts, { inserts: 0, updates: 0, deletes: 0 });
  await simulator.tick();
  assert.notEqual(calls.at(-1)[1][1], previousOwner);
  assert.notEqual(calls.at(-1)[1][0], calls[0][1][0], 'IDs stay unique across same-millisecond restarts');
});

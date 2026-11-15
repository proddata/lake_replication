const { randomUUID } = require('node:crypto');

// One shared run per console server. Only IDs inserted by this run are ever
// updated or deleted; the demo's seed orders and manual inserts are untouched.
class Simulator {
  constructor(pool, { now = Date.now, schedule = setTimeout, cancel = clearTimeout, onCommit = () => {} } = {}) {
    this.pool = pool;
    this.onCommit = onCommit;
    this.now = now;
    this.schedule = schedule;
    this.cancel = cancel;
    this.state = 'idle';
    this.counts = { inserts: 0, updates: 0, deletes: 0 };
    this.events = [];
    this.inFlight = false;
    this.lastId = 0n;
  }

  status() {
    return {
      state: this.state, rate: this.rate, duration: this.duration,
      startedAt: this.startedAt, endsAt: this.endsAt, stoppedAt: this.stoppedAt,
      counts: { ...this.counts }, events: this.events.map(event => ({ ...event })),
      lastError: this.lastError,
    };
  }

  start({ rate = 2, duration = 60 } = {}) {
    if (!Number.isInteger(rate) || rate < 1 || rate > 10 ||
        !Number.isInteger(duration) || duration < 10 || duration > 300) {
      throw new Error('Use 1–10 operations per second and a duration of 10–300 seconds.');
    }
    if (this.state === 'running' || this.inFlight) throw new Error('A simulator run is already active.');
    this.rate = rate;
    this.duration = duration;
    this.runId = randomUUID();
    this.ids = [];
    this.step = 0;
    this.counts = { inserts: 0, updates: 0, deletes: 0 };
    this.events = [];
    this.lastError = null;
    this.startedAt = new Date(this.now()).toISOString();
    this.endsAt = new Date(this.now() + duration * 1000).toISOString();
    this.stoppedAt = null;
    this.state = 'running';
    this.deadline = this.schedule(() => this.stop(), duration * 1000);
    this.timer = this.schedule(() => this.tick(), 0);
    return this.status();
  }

  stop() {
    this.cancel(this.timer);
    this.cancel(this.deadline);
    if (this.state === 'running') {
      this.state = this.inFlight ? 'stopping' : 'stopped';
      this.stoppedAt = new Date(this.now()).toISOString();
    }
    return this.status();
  }

  async tick() {
    if (this.state !== 'running' || this.inFlight) return;
    if (this.now() >= Date.parse(this.endsAt)) { this.stop(); return; }
    this.inFlight = true;
    const began = this.now();
    let operation = ['insert', 'insert', 'update', 'insert', 'delete'][this.step++ % 5];
    if (!this.ids.length) operation = 'insert';
    // Time-based IDs sort new simulator orders into the live preview.
    if (operation === 'insert') {
      const candidate = BigInt(this.now()) * 1000n;
      this.lastId = candidate > this.lastId ? candidate : this.lastId + 1n;
    }
    const id = operation === 'insert' ? this.lastId.toString() : this.ids[this.ids.length - 1];
    try {
      let result;
      const owner = `Simulator ${this.runId.slice(0, 8)}`;
      if (operation === 'insert') {
        result = await this.pool.query(
          `INSERT INTO public.orders (id, customer, item, amount)
           VALUES ($1, $2, 'simulated order', round((random() * 500 + 1)::numeric, 2)::double precision) RETURNING *, (pg_current_xact_id()::text::numeric % 4294967296)::text AS demo_xid`,
          [id, owner]);
        if (result.rowCount) this.ids.push(id);
      } else if (operation === 'update') {
        result = await this.pool.query(
          'UPDATE public.orders SET amount = round((amount + 1.25)::numeric, 2)::double precision WHERE id = $1 AND customer = $2 RETURNING *, (pg_current_xact_id()::text::numeric % 4294967296)::text AS demo_xid',
          [id, owner]);
      } else {
        result = await this.pool.query('DELETE FROM public.orders WHERE id = $1 AND customer = $2 RETURNING *, (pg_current_xact_id()::text::numeric % 4294967296)::text AS demo_xid', [id, owner]);
        this.ids.pop();
      }
      if (result.rowCount) {
        this.counts[`${operation}s`] += result.rowCount;
        const row = result.rows?.[0];
        if (row) this.onCommit({ operation, before: operation === 'delete' ? row : null,
          after: operation === 'delete' ? null : row, xid: row.demo_xid,
          sql: operation === 'insert' ? "INSERT INTO public.orders (id, customer, item, amount) VALUES ($1, $2, 'simulated order', round((random() * 500 + 1)::numeric, 2)::double precision)"
            : operation === 'update' ? 'UPDATE public.orders SET amount = round((amount + 1.25)::numeric, 2)::double precision WHERE id = $1 AND customer = $2'
              : 'DELETE FROM public.orders WHERE id = $1 AND customer = $2',
          parameters: [id, owner], origin: 'simulator' });
        this.events.unshift({ operation, id, at: new Date(this.now()).toISOString() });
        this.events = this.events.slice(0, 12);
      }
    } catch (error) {
      this.lastError = error.message;
      this.state = 'error';
      this.stoppedAt = new Date(this.now()).toISOString();
      this.cancel(this.deadline);
    } finally {
      this.inFlight = false;
      if (this.state === 'stopping') this.state = 'stopped';
      if (this.state === 'running') {
        this.timer = this.schedule(() => this.tick(), Math.max(0, 1000 / this.rate - (this.now() - began)));
      }
    }
  }
}

module.exports = { Simulator };

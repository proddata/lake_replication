const PRESETS = {
  source: [
    ['-- pick a preset --', ''],
    ['View demo table', 'SELECT * FROM public.orders ORDER BY id;'],
    ['Insert a new order', "INSERT INTO public.orders (id, customer, item, amount)\nVALUES (floor(random()*1000000)::bigint, 'New Customer', 'demo item', round((random()*500)::numeric, 2)::double precision);"],
    ['Update an order', "UPDATE public.orders SET amount = amount + 1 WHERE id = 1;"],
    ['Delete an order', "DELETE FROM public.orders WHERE id = 3;"],
  ],
  destination: [
    ['-- pick a preset --', ''],
    ['View replicated table', 'SELECT * FROM public.orders ORDER BY id;'],
    ['Create the demo subscription (starts replication)',
      "CREATE SUBSCRIPTION orders_sub\n  CONNECTION 'host=127.0.0.1 port=5432 dbname=application user=postgres'\n  PUBLICATION orders_pub\n  WITH (lake_target = 'iceberg');"],
    ['Drop the demo subscription (stops replication)', 'DROP SUBSCRIPTION orders_sub;'],
    ['Subscription state', 'SELECT * FROM lake_replication.subscriptions;'],
    ['Iceberg files for orders', "SELECT * FROM lake_iceberg.files(\n  (SELECT metadata_location FROM iceberg_tables\n    WHERE table_namespace = 'public' AND table_name = 'orders'));"],
    ['Iceberg metadata for orders', "SELECT lake_iceberg.metadata(\n  (SELECT metadata_location FROM iceberg_tables\n    WHERE table_namespace = 'public' AND table_name = 'orders'));"],
  ],
};

const $ = id => document.getElementById(id);
function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}
function format(value) {
  return value == null ? '—' : typeof value === 'object' ? JSON.stringify(value) : String(value);
}
function time(value) {
  return value ? new Date(value).toLocaleTimeString() : '—';
}
function bytes(value) {
  if (value == null) return '—';
  const number = Number(value);
  if (number < 1024) return `${number.toLocaleString()} B`;
  if (number < 1048576) return `${(number / 1024).toFixed(1)} KB`;
  return `${(number / 1048576).toFixed(1)} MB`;
}
function badge(state) {
  const type = ['streaming', 'running', 'published', 'ready', 'Query-visible in Iceberg'].includes(state) ? ''
    : ['error', 'backoff'].includes(state) ? 'danger'
    : (['starting', 'copying', 'publishing', 'paused'].includes(state) || String(state).includes('waiting for Iceberg')) ? 'warning' : 'neutral';
  return element('span', state || 'No worker', `badge ${type}`);
}
function table(rows, columns) {
  const node = element('table');
  const head = element('thead');
  const header = element('tr');
  columns.forEach(([, label]) => header.append(element('th', label)));
  head.append(header);
  node.append(head);
  const body = element('tbody');
  rows.forEach(row => {
    const tr = element('tr');
    columns.forEach(([key, , transform]) => {
      const td = element('td');
      const value = transform ? transform(row[key]) : format(row[key]);
      if (value instanceof Node) td.append(value);
      else {
        td.textContent = value; td.title = value;
        if (key === 'metadata_location' || key === 'summary') td.className = 'metadata-value';
      }
      tr.append(td);
    });
    body.append(tr);
  });
  node.append(body);
  return node;
}
const emptyMessages = {
  subscriptions: 'Ready when you are. Choose “Create the demo subscription” in the destination console below to start replication.',
  source: 'No orders on the source. Insert an order using the source console below.',
  destination: 'No replicated orders yet. Once replication starts, rows appear here automatically.',
  batches: 'No retained batches yet. Published batches will appear here as changes reach Iceberg.',
  relations: 'No tracked relations yet. Unflushed row counts appear after the subscription discovers its tables.',
  iceberg: 'No Iceberg tables yet. Initial sync creates the destination table and its metadata.',
  snapshot: 'No orders snapshot yet. Snapshot metadata appears after the destination table is created.',
  snapshots: 'No Iceberg snapshots yet. Connect replication and make a change.',
  slots: 'No logical replication slots in the source database yet.',
};
function renderPanel(id, panel, columns) {
  const container = $(id);
  if (panel.error) {
    const message = ['source', 'destination'].includes(id) && /does not exist/.test(panel.error)
      ? `${emptyMessages[id]}\n${panel.error}` : panel.error;
    container.replaceChildren(element('div', message, 'error'));
  } else if (!panel.rows.length) {
    container.replaceChildren(element('div', emptyMessages[id], 'empty'));
  } else {
    container.replaceChildren(table(panel.rows, columns || panel.fields.map(field => [field, field])));
  }
}
function renderSubscriptions(panel) {
  if (panel.error || !panel.rows.length) return renderPanel('subscriptions', panel);
  const cards = panel.rows.map(row => {
    const card = element('article', undefined, 'subscription');
    const heading = element('div', undefined, 'subscription-head');
    heading.append(element('strong', row.subscription_name), badge(row.state), badge(row.service_state));
    card.append(heading);
    card.append(element('div', `Slot: ${row.slot_name} · Publication: ${(row.publication_names || []).join(', ')} · Worker PID: ${format(row.supervisor_pid)} · Heartbeat: ${time(row.heartbeat_at)} · Consecutive failures: ${format(row.consecutive_failures)}${row.next_retry_at ? ` · Retry: ${time(row.next_retry_at)}` : ''}`, 'subscription-meta'));
    const checkpoints = element('div', undefined, 'checkpoints');
    [['received_lsn', 'Received'], ['staged_lsn', 'Staged'], ['published_lsn', 'Published'], ['acknowledged_lsn', 'Acknowledged']].forEach(([key, title]) => {
      const item = element('div', undefined, 'checkpoint');
      item.append(element('span', title), element('code', format(row[key])));
      checkpoints.append(item);
    });
    card.append(checkpoints);
    if (row.last_error) card.append(element('div', `Last reported error: ${row.last_error}`, 'error'));
    return card;
  });
  $('subscriptions').replaceChildren(...cards);
}
let lastComparison;
let selectedId = '';
let focusedEventId = null;
let actionBusy = false;
let lastReceived = null;
let lastPublished = null;
let lastSnapshot = null;
let sessionId = null;
const pageOpenedAt = Date.now();
const observedVersions = new Map();
const rowFlashes = new Map();
const eventVersions = new Map();
const observedSnapshotIds = new Set();

function flash(id, className = 'flow') {
  const node = $(id);
  node.classList.remove(className);
  void node.offsetWidth;
  node.classList.add(className);
}
function duration(value) { return value == null ? '—' : value < 1000 ? `${Math.round(value)} ms` : `${(value / 1000).toFixed(2)} s`; }
function mode(inspect) {
  $('inspect-panel').hidden = !inspect;
  $('demo-mode').setAttribute('aria-pressed', String(!inspect));
  $('inspect-mode').setAttribute('aria-pressed', String(inspect));
  if (inspect) { $('inspect-panel').scrollIntoView({ behavior: 'instant', block: 'start' }); $('inspect-title').focus({ preventScroll: true }); }
}
function drawChart(id, samples, key, color) {
  const svg = $(id);
  svg.replaceChildren();
  const values = samples.map(sample => sample[key]).filter(value => value != null);
  const ns = 'http://www.w3.org/2000/svg';
  const axis = document.createElementNS(ns, 'path');
  axis.setAttribute('d', 'M0 33H300'); axis.setAttribute('stroke', '#e3e9ed');
  svg.append(axis);
  if (!values.length) {
    const label = document.createElementNS(ns, 'text');
    label.setAttribute('x', '0'); label.setAttribute('y', '21'); label.textContent = key === 'latency' ? 'Waiting for a verified change' : 'Collecting samples…'; svg.append(label); return;
  }
  const max = Math.max(1, ...values);
  let path = '';
  let continuing = false;
  samples.forEach((sample, index) => {
    const value = sample[key];
    if (value == null) { continuing = false; return; }
    const x = samples.length === 1 ? 150 : index / (samples.length - 1) * 300;
    const y = 31 - value / max * 26;
    path += `${continuing ? 'L' : 'M'}${x.toFixed(1)} ${y.toFixed(1)} `;
    const point = document.createElementNS(ns, 'circle');
    point.setAttribute('cx', x); point.setAttribute('cy', y); point.setAttribute('r', '2'); point.setAttribute('fill', color);
    const title = document.createElementNS(ns, 'title');
    title.textContent = `${time(sample.at)} · ${key === 'latency' ? duration(value) : value.toFixed(2) + ' changes/s'}`;
    point.append(title); svg.append(point); continuing = true;
  });
  const line = document.createElementNS(ns, 'path');
  line.setAttribute('d', path); line.setAttribute('fill', 'none'); line.setAttribute('stroke', color); line.setAttribute('stroke-width', '1.5'); svg.prepend(line);
}
function renderSnapshots(panel) {
  const container = $('snapshot-timeline');
  if (panel.error || !panel.rows.length) {
    container.replaceChildren(element('span', panel.error ? `Snapshot metadata unavailable: ${panel.error}` : 'No snapshots yet. Initial sync creates the Iceberg table.', 'muted'));
    $('latest-commit').textContent = '—'; $('latest-snapshot').textContent = panel.error ? 'Metadata unavailable' : 'No snapshot yet'; return;
  }
  const latest = panel.rows[0];
  const hasNewCommit = lastSnapshot != null && lastSnapshot !== latest.snapshot_id;
  if (hasNewCommit) { flash('link-iceberg'); flash('stage-iceberg', 'pulse'); }
  lastSnapshot = latest.snapshot_id;
  $('latest-commit').textContent = time(latest.committed_at);
  $('latest-snapshot').textContent = `#${latest.snapshot_id}`;
  $('latest-snapshot').title = new Date(latest.committed_at).toLocaleString();
  const items = panel.rows.map(row => {
    const item = element('article', undefined, 'snapshot-item');
    if (hasNewCommit && !observedSnapshotIds.has(row.snapshot_id)) item.classList.add('new-commit');
    observedSnapshotIds.add(row.snapshot_id);
    item.append(element('div', `${time(row.committed_at)} · ${row.operation || 'commit'}`, 'snapshot-time'), element('code', `#${row.snapshot_id}`));
    item.append(element('small', `Files +${row.added_files ?? '—'} / −${row.removed_files ?? '—'} · records +${row.added_records ?? '—'} / −${row.deleted_records ?? '—'}`));
    item.title = `${new Date(row.committed_at).toLocaleString()}\nRecord counts are Iceberg file metadata, not CDC event counts.\n${JSON.stringify(row.summary || {})}`;
    return item;
  });
  container.replaceChildren(...items);
}
function renderPipeline(panels, comparison) {
  const subscriptions = panels.subscriptions;
  const rows = subscriptions.rows;
  const failed = rows.some(row => row.state === 'error' || ['error', 'backoff'].includes(row.service_state));
  const stale = rows.some(row => row.heartbeat_at && Date.now() - Date.parse(row.heartbeat_at) > 30000);
  const streaming = rows.length > 0 && rows.every(row => row.state === 'streaming' && row.service_state === 'running');
  const pending = comparison?.pending || comparison?.rows.some(row => row.state === 'pending');
  let label = subscriptions.error ? 'Control plane unavailable' : !rows.length ? 'Ready to connect' : failed ? 'Worker needs attention' : stale ? 'Worker heartbeat stale' : !streaming ? 'Starting / paused' : pending ? 'Replicating changes' : 'Streaming';
  const state = $('replication-state');
  state.textContent = label;
  state.className = `badge ${subscriptions.error || failed || stale ? 'danger' : !streaming || pending ? 'warning' : ''}`;
  $('setup-banner').hidden = !!subscriptions.error || rows.length > 0;
  const wal = subscriptions.error || !rows.length || rows.some(row => row.journal_lag_bytes == null) ? null : rows.reduce((sum, row) => sum + Number(row.journal_lag_bytes), 0);
  $('wal-lag').textContent = bytes(wal);
  const received = rows.map(row => `${row.subscription_id}:${row.received_lsn}`).join('|');
  const published = rows.map(row => `${row.subscription_id}:${row.published_lsn}`).join('|');
  if (lastReceived != null && lastReceived !== received && !subscriptions.error) { flash('link-wal'); flash('stage-wal', 'pulse'); }
  if (lastPublished != null && lastPublished !== published && !subscriptions.error) { flash('link-worker'); flash('stage-worker', 'pulse'); }
  if (!subscriptions.error) { lastReceived = received; lastPublished = published; }
}
function comparisonTable(side, comparison) {
  const container = $(side);
  const error = side === 'source' ? comparison.sourceError : comparison.destinationError;
  if (error) {
    container.replaceChildren(element('div', `${side === 'destination' ? 'Waiting for Iceberg visibility. ' : ''}${error}`, 'error')); return;
  }
  if (!comparison.rows.length) { container.replaceChildren(element('div', 'No tracked orders yet. Insert an order to begin.', 'empty')); return; }
  const fields = ['id', 'customer', 'item', 'amount'];
  const node = table([], fields.map(field => [field, field === 'id' ? 'Order ID' : field]));
  const body = node.querySelector('tbody');
  comparison.rows.forEach(row => {
    const current = row[side];
    const key = `${side}:${row.id}`;
    const previous = observedVersions.get(key);
    const version = JSON.stringify(current);
    if (previous !== undefined && previous !== version) {
      const operation = !current ? 'delete' : !JSON.parse(previous) ? 'insert' : 'update';
      rowFlashes.set(key, { until: Date.now() + 2800, operation, previous: JSON.parse(previous) });
    }
    observedVersions.set(key, version);
    const tr = element('tr');
    tr.dataset.orderId = row.id;
    tr.tabIndex = 0;
    tr.setAttribute('aria-label', `Select order ${row.id}`);
    tr.setAttribute('aria-selected', String(row.id === selectedId));
    if (row.id === selectedId) tr.classList.add('selected');
    if (row.state === 'pending') tr.classList.add('pending');
    const flashState = rowFlashes.get(key);
    if (flashState && flashState.until > Date.now()) tr.classList.add(`${flashState.operation}-flash`);
    const display = current || row.tombstone;
    if (!current) tr.classList.add(row.tombstone ? 'tombstone' : 'placeholder');
    fields.forEach(field => {
      const td = element('td', field === 'id' ? row.id : display ? format(display[field]) : '—');
      td.title = td.textContent;
      const other = row[side === 'source' ? 'destination' : 'source'];
      if (current && other && format(current[field]) !== format(other[field])) td.classList.add('changed-cell');
      if (current && flashState?.previous && flashState.until > Date.now() && format(current[field]) !== format(flashState.previous[field])) td.classList.add('changed-cell');
      if (field === 'id' && !current) td.append(element('span', row.tombstone ? 'deleted' : 'not visible', 'row-marker'));
      tr.append(td);
    });
    const select = () => { selectedId = row.id; renderComparison(lastComparison); };
    tr.addEventListener('click', select);
    tr.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); select(); } });
    body.append(tr);
  });
  container.replaceChildren(node);
}
function renderStory(comparison) {
  const event = comparison.events.find(event => event.eventId === focusedEventId) || comparison.events[0];
  if (!event) return;
  const story = $('change-status').parentElement;
  story.classList.toggle('pending-story', event.state === 'pending');
  story.classList.toggle('success-story', event.state === 'visible');
  $('change-status').replaceChildren(element('strong', `${event.operation.toUpperCase()} · order ${event.id}`), badge(event.state === 'visible' ? 'Query-visible in Iceberg' : event.state === 'superseded' ? 'Intermediate version not observed' : event.state === 'untracked' ? 'Outside tracked window · not verified' : 'Committed in PostgreSQL · waiting for Iceberg'));
  $('executed-sql').hidden = false;
  $('action-sql').textContent = event.sql;
  $('action-parameters').textContent = `Parameters: ${JSON.stringify(event.parameters)} · ${event.origin}`;
  $('change-progress').replaceChildren(element('span', `Source committed ${time(event.committedAt)}`), element('span', event.stagedMs == null ? 'WAL staging: not observed yet' : `WAL staging observed ≤ ${duration(event.stagedMs)}`), element('span', event.visibilityMs == null ? 'Iceberg visibility: waiting' : `Iceberg visibility observed ≤ ${duration(event.visibilityMs)}`));
  if (event.publicationMs != null) $('change-progress').append(element('span', `Batch publication: ${duration(event.publicationMs)}`));
  if (event.batchId) {
    const button = element('button', `Batch #${event.batchId}`, 'batch-link');
    button.addEventListener('click', () => { mode(true); $('batch-details').open = true; $('batch-details').scrollIntoView({ block: 'center' }); });
    $('change-progress').append(button);
  }
}
function renderComparison(comparison) {
  lastComparison = comparison;
  if (sessionId !== comparison.sessionId) {
    sessionId = comparison.sessionId;
    eventVersions.clear(); observedVersions.clear(); rowFlashes.clear(); focusedEventId = null;
  }
  const selectable = comparison.rows.filter(row => row.source);
  if (!selectable.some(row => row.id === selectedId)) selectedId = selectable[0]?.id || '';
  const picker = $('selected-order');
  picker.replaceChildren(...(selectable.length ? selectable.map(row => { const option = element('option', `#${row.id}`); option.value = row.id; return option; }) : [element('option', 'Insert an order first')]));
  picker.value = selectedId;
  picker.disabled = actionBusy || !selectable.length || !!comparison.sourceError;
  $('update-order').disabled = actionBusy || !selectedId || !!comparison.sourceError;
  $('delete-order').disabled = actionBusy || !selectedId || !!comparison.sourceError;
  $('insert-order').disabled = actionBusy || !!comparison.sourceError;
  comparisonTable('source', comparison);
  comparisonTable('destination', comparison);
  const state = $('comparison-state');
  const errors = comparison.sourceError || comparison.destinationError;
  const differences = comparison.rows.filter(row => row.state === 'pending').length;
  const pending = comparison.pending || differences;
  state.textContent = errors ? 'Comparison unavailable' : pending ? `${comparison.pending || differences} ${comparison.pending ? 'changes awaiting visibility' : 'records differ'}` : 'Tracked records caught up';
  state.className = `badge ${errors ? 'danger' : pending ? 'warning' : ''}`;
  $('observation-time').textContent = `Observed ${time(comparison.sampledAt)}`;
  $('verified-total').textContent = comparison.totalVerified.toLocaleString();
  $('throughput').textContent = comparison.throughput == null ? '—' : `${comparison.throughput.toFixed(1)} /s`;
  const latestVisible = comparison.events.find(event => event.state === 'visible');
  $('visibility-latency').textContent = latestVisible ? `≤ ${duration(latestVisible.visibilityMs)}` : '—';
  for (const event of comparison.events) {
    if (eventVersions.get(event.eventId) !== event.state) {
      if (event.state === 'visible' && (eventVersions.has(event.eventId) || event.visibleAt >= pageOpenedAt)) { flash('link-iceberg'); flash('stage-iceberg', 'pulse'); }
      eventVersions.set(event.eventId, event.state);
    }
  }
  const activeEvents = new Set(comparison.events.map(event => event.eventId));
  for (const key of eventVersions.keys()) if (!activeEvents.has(key)) eventVersions.delete(key);
  const activeRows = new Set(comparison.rows.flatMap(row => [`source:${row.id}`, `destination:${row.id}`]));
  for (const key of observedVersions.keys()) if (!activeRows.has(key)) { observedVersions.delete(key); rowFlashes.delete(key); }
  drawChart('latency-chart', comparison.history, 'latency', '#3977b5');
  drawChart('throughput-chart', comparison.history, 'throughput', '#087f78');
  const latencies = comparison.history.filter(sample => sample.latency != null);
  $('chart-latency-label').textContent = latencies.length ? `≤ ${duration(latencies.at(-1).latency)}` : '—';
  $('chart-throughput-label').textContent = comparison.throughput == null ? '—' : `${comparison.throughput.toFixed(1)} /s`;
  $('activity-count').textContent = `${comparison.events.length} retained · ${comparison.totalVerified} verified this session`;
  if (comparison.events.length) {
    $('change-events').replaceChildren(table(comparison.events, [['committedAt', 'Source commit', time], ['operation', 'Action'], ['id', 'Order ID'], ['state', 'Observation'], ['visibilityMs', 'Query visibility ≤', duration], ['stagedMs', 'Staging observed ≤', duration], ['batchId', 'Batch']]));
  } else $('change-events').replaceChildren(element('div', 'Real actions and simulator changes appear here after commit.', 'empty'));
  renderStory(comparison);
}
function renderDashboard(data) {
  const panels = data.panels;
  if (data.simulator) renderSimulator(data.simulator);
  if (data.comparison) renderComparison(data.comparison);
  renderPipeline(panels, data.comparison);
  renderSnapshots(panels.snapshots || { rows: [], error: 'Snapshot history not available' });
  renderSubscriptions(panels.subscriptions);
  renderPanel('batches', panels.batches, [['batch_id', 'Batch'], ['subscription_name', 'Subscription'], ['state', 'State', badge], ['last_end_lsn', 'End LSN'], ['created_at', 'Created', time], ['published_at', 'Published', time], ['duration_ms', 'Publication (ms)']]);
  renderPanel('relations', panels.relations, [['subname', 'Subscription'], ['source_namespace', 'Schema'], ['source_relation', 'Table'], ['unflushed_inserts', 'Inserts'], ['unflushed_updates', 'Updates'], ['unflushed_deletes', 'Deletes']]);
  renderPanel('iceberg', panels.iceberg, [['table_namespace', 'Schema'], ['table_name', 'Table'], ['metadata_location', 'Metadata file']]);
  renderPanel('snapshot', panels.snapshot, [['format_version', 'Format version'], ['schema_id', 'Schema ID'], ['snapshot_id', 'Snapshot ID'], ['sequence_number', 'Sequence'], ['updated_at', 'Updated', time], ['summary', 'Snapshot summary']]);
  renderPanel('snapshots', panels.snapshots || { rows: [], error: 'Snapshot history unavailable' }, [['snapshot_id', 'Snapshot ID'], ['committed_at', 'Committed', time], ['operation', 'Operation'], ['added_files', 'Added files'], ['removed_files', 'Removed files'], ['added_records', 'Added file records'], ['deleted_records', 'Deleted file records']]);
  renderPanel('slots', panels.slots, [['slot_name', 'Slot'], ['active', 'Active'], ['wal_status', 'WAL status'], ['restart_lsn', 'Restart LSN'], ['confirmed_flush_lsn', 'Confirmed flush LSN'], ['retained_bytes', 'WAL since confirmed flush', bytes]]);
  const errors = Object.values(panels).filter(panel => panel.error).length;
  setConnection(errors ? `${errors} panels unavailable` : 'Live', errors ? 'warning' : '');
}
async function orderAction(operation) {
  if (actionBusy) return;
  actionBusy = true;
  $('action-error').textContent = '';
  for (const id of ['insert-order', 'update-order', 'delete-order']) $(id).disabled = true;
  try {
    const data = await request('/api/playground/action', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ operation, id: selectedId }) });
    const event = data.event;
    focusedEventId = event.eventId;
    selectedId = event.id;
    // This is the actual row returned by a committed backend operation. Show it
    // immediately; destination rows only change after successful live queries.
    if (lastComparison) {
      const row = lastComparison.rows.find(row => row.id === event.id);
      if (row) Object.assign(row, { source: event.after, tombstone: operation === 'delete' ? event.before : null, operation, state: 'pending' });
      else lastComparison.rows.unshift({ id: event.id, source: event.after, destination: null, tombstone: null, operation, state: 'pending' });
      lastComparison.events = [event, ...lastComparison.events.filter(item => item.eventId !== event.eventId)];
      lastComparison.pending++;
      rowFlashes.set(`source:${event.id}`, { operation, until: Date.now() + 2800, previous: event.before });
      actionBusy = false;
      renderComparison(lastComparison);
    }
    flash('stage-source', 'pulse');
    await refreshDashboard();
  } catch (error) { $('action-error').textContent = error.message; }
  finally {
    actionBusy = false;
    if (lastComparison) renderComparison(lastComparison);
    else $('insert-order').disabled = false;
  }
}
$('insert-order').addEventListener('click', () => orderAction('insert'));
$('update-order').addEventListener('click', () => orderAction('update'));
$('delete-order').addEventListener('click', () => orderAction('delete'));
$('selected-order').addEventListener('change', event => { selectedId = event.target.value; if (lastComparison) renderComparison(lastComparison); });
$('demo-mode').addEventListener('click', () => mode(false));
$('inspect-mode').addEventListener('click', () => mode(true));
$('create-subscription').addEventListener('click', async () => {
  $('create-subscription').disabled = true;
  try {
    await request('/api/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: 'destination', sql: PRESETS.destination[2][1] }) });
    await refreshDashboard();
  } catch (error) { $('action-error').textContent = error.message; }
  finally { $('create-subscription').disabled = false; }
});
function setConnection(message, type) {
  const node = $('connection');
  node.textContent = $('live').checked ? message : 'Paused';
  node.className = `badge ${$('live').checked ? type : 'neutral'}`;
}
async function request(url, options = {}) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(20000) });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}
let simulatorBusy = false;
let simulatorStatus;
function renderSimulator(status) {
  simulatorStatus = status;
  const active = ['running', 'stopping'].includes(status.state);
  const state = $('simulator-state');
  state.textContent = status.state[0].toUpperCase() + status.state.slice(1);
  state.className = `badge ${status.state === 'error' ? 'danger' : status.state === 'running' ? '' : 'neutral'}`;
  $('simulator-start').disabled = active || simulatorBusy;
  $('simulator-stop').disabled = status.state !== 'running' || simulatorBusy;
  $('simulator-rate').disabled = active || simulatorBusy;
  $('simulator-duration').disabled = active || simulatorBusy;
  if (active) {
    $('simulator-rate').value = String(status.rate);
    $('simulator-duration').value = String(status.duration);
  }
  for (const key of ['inserts', 'updates', 'deletes']) $('simulator-' + key).textContent = status.counts[key].toLocaleString();
  $('simulator-timing').textContent = active
    ? `${status.rate} ops/s target · ${status.state === 'stopping' ? 'Finishing current operation…' : `Ends at ${time(status.endsAt)}`}`
    : status.stoppedAt ? `Ended at ${time(status.stoppedAt)}` : 'Ready to start';
  $('simulator-error').textContent = status.lastError || '';
  $('simulator-events').replaceChildren(status.events.length
    ? table(status.events, [['at', 'Time', time], ['operation', 'Operation'], ['id', 'Order ID']])
    : element('div', 'Start a simulation to generate live traffic.', 'empty'));
}
async function controlSimulator(action) {
  if (simulatorBusy) return;
  simulatorBusy = true;
  $('simulator-start').disabled = true;
  $('simulator-stop').disabled = true;
  $('simulator-rate').disabled = true;
  $('simulator-duration').disabled = true;
  try {
    renderSimulator(await request(`/api/simulator/${action}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action === 'start' ? {
        rate: Number($('simulator-rate').value), duration: Number($('simulator-duration').value),
      } : {}),
    }));
    refreshDashboard();
  } catch (error) {
    $('simulator-error').textContent = `Could not ${action} simulation: ${error.message}. Refresh to check its current state.`;
  } finally {
    simulatorBusy = false;
    if (simulatorStatus) {
      const active = ['running', 'stopping'].includes(simulatorStatus.state);
      $('simulator-start').disabled = active;
      $('simulator-stop').disabled = simulatorStatus.state !== 'running';
      $('simulator-rate').disabled = active;
      $('simulator-duration').disabled = active;
    } else {
      $('simulator-start').disabled = false;
      $('simulator-stop').disabled = false;
    }
  }
}
$('simulator-start').addEventListener('click', () => controlSimulator('start'));
$('simulator-stop').addEventListener('click', () => controlSimulator('stop'));

let refreshing = false;
let timer;
async function refreshDashboard() {
  if (refreshing) return;
  clearTimeout(timer);
  refreshing = true;
  $('refresh').disabled = true;
  try {
    renderDashboard(await request('/api/dashboard'));
    $('dashboard-error').textContent = '';
  } catch (error) {
    setConnection('Disconnected', 'danger');
    $('dashboard-error').textContent = `Could not refresh: ${error.message}. Displayed values are from the last successful sample.`;
  } finally {
    refreshing = false;
    $('refresh').disabled = false;
    if ($('live').checked && !document.hidden) timer = setTimeout(refreshDashboard, lastComparison?.pending || simulatorStatus?.state === 'running' ? 700 : 1500);
  }
}
async function runQuery(pane) {
  if (pane.dataset.running === 'true') return;
  const sql = pane.querySelector('.sql').value;
  if (!sql.trim()) return;
  const button = pane.querySelector('.run');
  pane.dataset.running = 'true';
  button.disabled = true;
  pane.querySelector('.error').textContent = '';
  pane.querySelector('.result-meta').textContent = 'Running…';
  const started = performance.now();
  try {
    const data = await request('/api/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ target: pane.dataset.target, sql }) });
    pane.querySelector('.result-meta').textContent = `${data.command} · ${data.rowCount ?? 0} rows · ${Math.round(performance.now() - started)} ms`;
    pane.querySelector('.result').replaceChildren(...(data.rows.length ? [table(data.rows, data.fields.map(field => [field, field]))] : []));
    refreshDashboard();
  } catch (error) {
    pane.querySelector('.error').textContent = error.message;
    pane.querySelector('.result-meta').textContent = 'Query failed';
    pane.querySelector('.result').replaceChildren();
  } finally {
    pane.dataset.running = 'false';
    button.disabled = false;
  }
}
document.querySelectorAll('.pane').forEach(pane => {
  const select = pane.querySelector('.presets');
  const textarea = pane.querySelector('.sql');
  PRESETS[pane.dataset.target].forEach(([label, sql]) => {
    const option = element('option', label);
    option.value = sql;
    select.append(option);
  });
  select.selectedIndex = 1;
  textarea.value = select.value;
  select.addEventListener('change', () => { if (select.value) textarea.value = select.value; });
  pane.querySelector('.run').addEventListener('click', () => runQuery(pane));
  textarea.addEventListener('keydown', event => {
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') { event.preventDefault(); runQuery(pane); }
  });
});
$('refresh').addEventListener('click', refreshDashboard);
$('live').addEventListener('change', () => {
  clearTimeout(timer);
  if ($('live').checked) refreshDashboard();
  else setConnection('Paused', 'neutral');
});
document.addEventListener('visibilitychange', () => {
  clearTimeout(timer);
  if (!document.hidden && $('live').checked) refreshDashboard();
});
refreshDashboard();

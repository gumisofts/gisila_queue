/// The self-contained single-page dashboard served at `GET /`.
///
/// Vanilla HTML/CSS/JS (no build step, no CDN) so the monitor ships as one
/// Dart file. It polls the JSON API and live-tails `GET /api/events` (SSE).
const String dashboardHtml = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>GisilaRose</title>
<style>
  :root {
    --bg: #0f1117; --panel: #181b24; --border: #262b38; --fg: #e6e8ee;
    --muted: #8b93a7; --accent: #6ea8fe;
    --ok: #34d399; --fail: #f87171; --retry: #fbbf24; --run: #60a5fa;
    --pending: #94a3b8; --revoked: #c084fc; --scheduled: #38bdf8;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--fg);
    font: 14px/1.5 system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
  header { display: flex; align-items: center; gap: 12px; padding: 14px 20px;
    border-bottom: 1px solid var(--border); background: var(--panel);
    position: sticky; top: 0; z-index: 5; }
  header h1 { font-size: 16px; margin: 0; letter-spacing: .3px; }
  header .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--fail); }
  header .dot.live { background: var(--ok); box-shadow: 0 0 8px var(--ok); }
  header .spacer { flex: 1; }
  header small { color: var(--muted); }
  main { padding: 20px; max-width: 1200px; margin: 0 auto; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px,1fr));
    gap: 12px; margin-bottom: 22px; }
  .card { background: var(--panel); border: 1px solid var(--border);
    border-radius: 10px; padding: 14px; }
  .card .n { font-size: 26px; font-weight: 650; }
  .card .l { color: var(--muted); font-size: 12px; text-transform: uppercase;
    letter-spacing: .5px; }
  h2 { font-size: 13px; text-transform: uppercase; letter-spacing: .6px;
    color: var(--muted); margin: 26px 0 10px; }
  table { width: 100%; border-collapse: collapse; background: var(--panel);
    border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
  th, td { text-align: left; padding: 9px 12px; border-bottom: 1px solid var(--border);
    font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    max-width: 320px; }
  th { color: var(--muted); font-weight: 600; font-size: 11px;
    text-transform: uppercase; letter-spacing: .5px; }
  tr:last-child td { border-bottom: none; }
  td.mono, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 12px; color: var(--muted); }
  .pill { display: inline-block; padding: 2px 9px; border-radius: 999px;
    font-size: 11px; font-weight: 650; text-transform: uppercase; letter-spacing: .4px; }
  .s-success { background: rgba(52,211,153,.16); color: var(--ok); }
  .s-failure { background: rgba(248,113,113,.16); color: var(--fail); }
  .s-retry { background: rgba(251,191,36,.16); color: var(--retry); }
  .s-started { background: rgba(96,165,250,.16); color: var(--run); }
  .s-pending { background: rgba(148,163,184,.16); color: var(--pending); }
  .s-revoked { background: rgba(192,132,252,.16); color: var(--revoked); }
  .s-scheduled { background: rgba(56,189,248,.16); color: var(--scheduled); }
  .filters { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
  .filters input, .filters select { background: var(--panel); color: var(--fg);
    border: 1px solid var(--border); border-radius: 7px; padding: 6px 9px; font: inherit; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 22px; }
  @media (max-width: 820px) { .grid2 { grid-template-columns: 1fr; } }
  .empty { color: var(--muted); padding: 14px; text-align: center; }
</style>
</head>
<body>
<header>
  <div class="dot" id="live"></div>
  <h1>GisilaRose</h1>
  <small>gisila_queue observability</small>
  <div class="spacer"></div>
  <small id="updated">connecting…</small>
</header>
<main>
  <div class="cards" id="cards"></div>

  <div class="grid2">
    <div>
      <h2>Workers</h2>
      <table id="workers"><tbody><tr><td class="empty">no workers seen</td></tr></tbody></table>
    </div>
    <div>
      <h2>Queues</h2>
      <table id="queues"><tbody><tr><td class="empty">no queue data</td></tr></tbody></table>
    </div>
  </div>

  <h2>Tasks</h2>
  <div class="filters">
    <input id="f-name" placeholder="task name…" />
    <select id="f-state">
      <option value="">all states</option>
      <option>pending</option><option>scheduled</option><option>started</option>
      <option>success</option><option>failure</option><option>retry</option>
      <option>revoked</option>
    </select>
    <input id="f-queue" placeholder="queue…" />
  </div>
  <table id="tasks">
    <thead><tr>
      <th>task</th><th>state</th><th>queue</th><th>worker</th>
      <th>runtime</th><th>retries</th><th>id</th>
    </tr></thead>
    <tbody><tr><td colspan="7" class="empty">no tasks yet</td></tr></tbody>
  </table>
</main>

<script>
const $ = (s) => document.querySelector(s);
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, c =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

function pill(state) {
  return `<span class="pill s-${esc(state)}">${esc(state)}</span>`;
}

async function get(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(r.status);
  return r.json();
}

function renderStats(s) {
  const t = s.totals || {};
  const cards = [
    ['sent', t.sent], ['received', t.received], ['succeeded', t.succeeded],
    ['failed', t.failed], ['retried', t.retried], ['revoked', t.revoked],
    ['workers', `${s.workers.alive}/${s.workers.total}`],
  ];
  $('#cards').innerHTML = cards.map(([l, n]) =>
    `<div class="card"><div class="n">${esc(n ?? 0)}</div><div class="l">${l}</div></div>`
  ).join('');
}

function renderWorkers(ws) {
  const tb = $('#workers tbody');
  if (!ws.length) { tb.innerHTML = '<tr><td class="empty">no workers seen</td></tr>'; return; }
  tb.innerHTML = `<tr><th>worker</th><th>state</th><th>queues</th><th>active</th><th>ok</th><th>fail</th></tr>` +
    ws.map(w => `<tr>
      <td class="mono">${esc(w.hostname || w.id)}</td>
      <td>${pill(w.alive ? 'success' : 'failure')}<span style="font-size:11px;margin-left:4px">${w.alive?'online':'offline'}</span></td>
      <td class="mono">${esc((w.queues||[]).join(', '))}</td>
      <td>${esc(w.active)}</td><td>${esc(w.succeeded)}</td><td>${esc(w.failed)}</td>
    </tr>`).join('');
}

function renderQueues(q) {
  const tb = $('#queues tbody');
  const keys = Object.keys(q || {});
  if (!keys.length) { tb.innerHTML = '<tr><td class="empty">no queue data</td></tr>'; return; }
  tb.innerHTML = `<tr><th>queue</th><th>depth</th></tr>` +
    keys.sort().map(k => `<tr><td class="mono">${esc(k)}</td><td>${esc(q[k])}</td></tr>`).join('');
}

function renderTasks(tasks) {
  const tb = $('#tasks tbody');
  if (!tasks.length) { tb.innerHTML = '<tr><td colspan="7" class="empty">no tasks</td></tr>'; return; }
  tb.innerHTML = tasks.map(t => `<tr>
    <td title="${esc(t.name)}">${esc(t.name)}</td>
    <td>${pill(t.state)}</td>
    <td class="mono">${esc(t.queue || '')}</td>
    <td class="mono" title="${esc(t.worker_id||'')}">${esc((t.worker_id||'').split(':')[0])}</td>
    <td>${t.runtime_ms != null ? esc(t.runtime_ms)+' ms' : ''}</td>
    <td>${esc(t.retries)}/${esc(t.max_retries)}</td>
    <td class="mono" title="${esc(t.id)}">${esc(t.id.slice(0,8))}</td>
  </tr>`).join('');
}

function taskQuery() {
  const p = new URLSearchParams();
  const n = $('#f-name').value.trim(); if (n) p.set('name', n);
  const s = $('#f-state').value; if (s) p.set('state', s);
  const q = $('#f-queue').value.trim(); if (q) p.set('queue', q);
  p.set('limit', '100');
  return '/api/tasks?' + p.toString();
}

async function refresh() {
  try {
    const [stats, workers, tasks] = await Promise.all([
      get('/api/stats'), get('/api/workers'), get(taskQuery()),
    ]);
    renderStats(stats);
    renderWorkers(workers.workers);
    renderQueues(stats.queues);
    renderTasks(tasks.tasks);
    $('#updated').textContent = 'updated ' + new Date().toLocaleTimeString();
  } catch (e) {
    $('#updated').textContent = 'error: ' + e.message;
  }
}

['#f-name', '#f-state', '#f-queue'].forEach(s =>
  $(s).addEventListener('input', refresh));

// Live tail: refresh on every event (debounced), and flag liveness.
let pending = false;
function nudge() {
  if (pending) return; pending = true;
  setTimeout(() => { pending = false; refresh(); }, 300);
}
function connectSSE() {
  const es = new EventSource('/api/events');
  es.onopen = () => $('#live').classList.add('live');
  es.onmessage = nudge;
  es.onerror = () => {
    $('#live').classList.remove('live');
    es.close();
    setTimeout(connectSSE, 3000);
  };
}

refresh();
setInterval(refresh, 5000); // fallback poll
connectSSE();
</script>
</body>
</html>''';

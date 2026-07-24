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
    --bg: #eef1f5;
    --panel: #ffffff;
    --border: #d5dbe6;
    --fg: #1a2332;
    --muted: #5c6b7f;
    --accent: #b44a5a;
    --accent-soft: #f8e8eb;
    --ink: #243044;
    --ok: #1f8a5b;
    --fail: #c0392b;
    --retry: #b8860b;
    --run: #2a6fbf;
    --pending: #6b7c93;
    --revoked: #7a5a9a;
    --scheduled: #0e7490;
    --shadow: 0 1px 2px rgba(26,35,50,.06), 0 8px 24px rgba(26,35,50,.04);
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background:
      radial-gradient(ellipse 80% 50% at 10% -10%, #f3d9de 0%, transparent 55%),
      radial-gradient(ellipse 60% 40% at 100% 0%, #dce8f5 0%, transparent 50%),
      var(--bg);
    color: var(--fg);
    font: 14px/1.5 "Segoe UI", "Helvetica Neue", Candara, Calibri, sans-serif;
    min-height: 100vh;
  }
  header {
    display: flex; align-items: center; gap: 14px; padding: 16px 24px;
    border-bottom: 1px solid var(--border); background: rgba(255,255,255,.88);
    backdrop-filter: blur(8px); position: sticky; top: 0; z-index: 20;
  }
  header h1 {
    font-family: Georgia, "Iowan Old Style", "Palatino Linotype", Palatino, serif;
    font-size: 22px; margin: 0; font-weight: 700; color: var(--accent);
    letter-spacing: -.02em;
  }
  header .dot {
    width: 9px; height: 9px; border-radius: 50%; background: var(--fail);
  }
  header .dot.live { background: var(--ok); box-shadow: 0 0 0 3px rgba(31,138,91,.2); }
  header .spacer { flex: 1; }
  header small { color: var(--muted); }
  header .tag {
    font-size: 11px; font-weight: 650; letter-spacing: .04em; text-transform: uppercase;
    color: var(--accent); background: var(--accent-soft); padding: 4px 9px; border-radius: 6px;
  }
  main { padding: 22px 24px 48px; max-width: 1280px; margin: 0 auto; }
  .cards {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(120px,1fr));
    gap: 12px; margin-bottom: 22px;
  }
  .card {
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 12px; padding: 14px 16px; box-shadow: var(--shadow);
  }
  .card .n { font-size: 26px; font-weight: 700; color: var(--ink); letter-spacing: -.02em; }
  .card .l {
    color: var(--muted); font-size: 11px; text-transform: uppercase;
    letter-spacing: .06em; margin-top: 2px;
  }
  h2 {
    font-size: 12px; text-transform: uppercase; letter-spacing: .08em;
    color: var(--muted); margin: 0 0 10px; font-weight: 700;
  }
  .section-head {
    display: flex; align-items: center; gap: 10px; margin: 26px 0 10px; flex-wrap: wrap;
  }
  .section-head h2 { margin: 0; flex: 1; }
  table {
    width: 100%; border-collapse: collapse; background: var(--panel);
    border: 1px solid var(--border); border-radius: 12px; overflow: hidden;
    box-shadow: var(--shadow);
  }
  th, td {
    text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border);
    font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    max-width: 280px;
  }
  th {
    color: var(--muted); font-weight: 700; font-size: 11px;
    text-transform: uppercase; letter-spacing: .05em; background: #f7f9fc;
  }
  tr:last-child td { border-bottom: none; }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover td { background: #f8fafc; }
  tr.selected td { background: var(--accent-soft); }
  td.mono, .mono {
    font-family: ui-monospace, "Cascadia Mono", "SF Mono", Menlo, monospace;
    font-size: 12px; color: var(--muted);
  }
  .pill {
    display: inline-block; padding: 2px 8px; border-radius: 6px;
    font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .03em;
  }
  .s-success { background: #e6f6ee; color: var(--ok); }
  .s-failure { background: #fdecea; color: var(--fail); }
  .s-retry { background: #fff6e0; color: var(--retry); }
  .s-started { background: #e8f1fc; color: var(--run); }
  .s-pending { background: #eef1f5; color: var(--pending); }
  .s-revoked { background: #f1ebf7; color: var(--revoked); }
  .s-scheduled { background: #e6f6f8; color: var(--scheduled); }
  .filters { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; align-items: center; }
  .filters input, .filters select, .field input, .field textarea {
    background: var(--panel); color: var(--fg);
    border: 1px solid var(--border); border-radius: 8px; padding: 7px 10px; font: inherit;
  }
  .filters input:focus, .filters select:focus, .field input:focus, .field textarea:focus {
    outline: 2px solid rgba(180,74,90,.25); border-color: var(--accent);
  }
  .chip {
    display: inline-flex; align-items: center; gap: 6px;
    background: var(--accent-soft); color: var(--accent); border-radius: 999px;
    padding: 4px 10px; font-size: 12px; font-weight: 650;
  }
  .chip button {
    border: 0; background: transparent; color: var(--accent); cursor: pointer;
    font-size: 14px; line-height: 1; padding: 0;
  }
  .btn {
    border: 1px solid var(--border); background: var(--panel); color: var(--ink);
    border-radius: 8px; padding: 6px 11px; font: inherit; font-size: 12px;
    font-weight: 650; cursor: pointer;
  }
  .btn:hover { border-color: var(--accent); color: var(--accent); }
  .btn:disabled { opacity: .45; cursor: not-allowed; }
  .btn-primary {
    background: var(--accent); color: #fff; border-color: var(--accent);
  }
  .btn-primary:hover { filter: brightness(.95); color: #fff; }
  .btn-danger { color: var(--fail); border-color: #f0c4bf; }
  .btn-danger:hover { background: #fdecea; }
  .btn-row { display: flex; gap: 6px; flex-wrap: wrap; }
  .grid2 { display: grid; grid-template-columns: 1.2fr 1fr; gap: 22px; }
  @media (max-width: 900px) { .grid2 { grid-template-columns: 1fr; } }
  .empty { color: var(--muted); padding: 16px; text-align: center; }
  .hint { color: var(--muted); font-size: 12px; }
  .worker-active {
    margin-top: 10px; padding: 12px 14px; background: var(--panel);
    border: 1px solid var(--border); border-radius: 12px; box-shadow: var(--shadow);
  }
  .worker-active ul { margin: 8px 0 0; padding-left: 18px; }
  .worker-active li { margin: 4px 0; font-size: 13px; }
  .drawer-backdrop {
    position: fixed; inset: 0; background: rgba(26,35,50,.28); z-index: 40;
    opacity: 0; pointer-events: none; transition: opacity .18s ease;
  }
  .drawer-backdrop.open { opacity: 1; pointer-events: auto; }
  .drawer {
    position: fixed; top: 0; right: 0; width: min(420px, 100%); height: 100%;
    background: var(--panel); border-left: 1px solid var(--border);
    box-shadow: -12px 0 40px rgba(26,35,50,.12); z-index: 50;
    transform: translateX(100%); transition: transform .2s ease;
    display: flex; flex-direction: column;
  }
  .drawer.open { transform: translateX(0); }
  .drawer header {
    position: static; border: 0; border-bottom: 1px solid var(--border);
    background: #fff; padding: 16px 18px;
  }
  .drawer h3 {
    margin: 0; font-family: Georgia, "Iowan Old Style", Palatino, serif;
    font-size: 18px; color: var(--ink);
  }
  .drawer .body { padding: 16px 18px; overflow: auto; flex: 1; }
  .kv { display: grid; grid-template-columns: 110px 1fr; gap: 8px 10px; margin: 12px 0; }
  .kv dt { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
  .kv dd { margin: 0; font-size: 13px; word-break: break-word; white-space: pre-wrap; }
  .pre {
    background: #f4f6f9; border: 1px solid var(--border); border-radius: 8px;
    padding: 10px; font-family: ui-monospace, Menlo, monospace; font-size: 12px;
    white-space: pre-wrap; word-break: break-word; max-height: 180px; overflow: auto;
  }
  .modal-backdrop {
    position: fixed; inset: 0; background: rgba(26,35,50,.32); z-index: 60;
    display: none; align-items: center; justify-content: center; padding: 20px;
  }
  .modal-backdrop.open { display: flex; }
  .modal {
    background: var(--panel); border: 1px solid var(--border); border-radius: 14px;
    width: min(480px, 100%); box-shadow: var(--shadow); padding: 18px 20px;
  }
  .modal h3 {
    margin: 0 0 14px; font-family: Georgia, "Iowan Old Style", Palatino, serif;
    font-size: 18px;
  }
  .field { display: flex; flex-direction: column; gap: 5px; margin-bottom: 12px; }
  .field label { font-size: 12px; font-weight: 650; color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }
  .field textarea { min-height: 72px; resize: vertical; font-family: ui-monospace, Menlo, monospace; font-size: 12px; }
  .toast {
    position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
    background: var(--ink); color: #fff; padding: 10px 16px; border-radius: 999px;
    font-size: 13px; z-index: 80; opacity: 0; pointer-events: none; transition: opacity .2s;
  }
  .toast.show { opacity: 1; }
  time { color: var(--muted); font-variant-numeric: tabular-nums; }
</style>
</head>
<body>
<header>
  <div class="dot" id="live"></div>
  <h1>GisilaRose</h1>
  <span class="tag">queue monitor</span>
  <div class="spacer"></div>
  <button class="btn btn-primary" id="btn-fire" type="button">Fire task</button>
  <small id="updated">connecting…</small>
</header>
<main>
  <div class="cards" id="cards"></div>
  <p class="hint" id="cap-hint" hidden></p>

  <div class="grid2">
    <div>
      <div class="section-head"><h2>Workers</h2></div>
      <table id="workers"><tbody><tr><td class="empty">no workers seen</td></tr></tbody></table>
      <div class="worker-active" id="worker-panel" hidden></div>
    </div>
    <div>
      <div class="section-head"><h2>Queues</h2></div>
      <table id="queues"><tbody><tr><td class="empty">no queue data</td></tr></tbody></table>
    </div>
  </div>

  <div class="section-head">
    <h2>Tasks</h2>
    <span class="chip" id="worker-chip" hidden>
      <span id="worker-chip-label"></span>
      <button type="button" id="worker-chip-clear" title="Clear worker filter">×</button>
    </span>
  </div>
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
      <th>task</th><th>state</th><th>time</th><th>queue</th><th>worker</th>
      <th>runtime</th><th>retries</th><th>id</th>
    </tr></thead>
    <tbody><tr><td colspan="8" class="empty">no tasks yet</td></tr></tbody>
  </table>
</main>

<div class="drawer-backdrop" id="drawer-backdrop"></div>
<aside class="drawer" id="drawer" aria-hidden="true">
  <header>
    <h3 id="drawer-title">Task</h3>
    <div class="spacer"></div>
    <button class="btn" type="button" id="drawer-close">Close</button>
  </header>
  <div class="body" id="drawer-body"></div>
</aside>

<div class="modal-backdrop" id="fire-modal">
  <div class="modal">
    <h3>Fire task</h3>
    <div class="field">
      <label for="fire-name">Name</label>
      <input id="fire-name" placeholder="e.g. health.ping" />
    </div>
    <div class="field">
      <label for="fire-queue">Queue</label>
      <input id="fire-queue" placeholder="default" value="default" />
    </div>
    <div class="field">
      <label for="fire-args">Args (JSON array)</label>
      <textarea id="fire-args">[]</textarea>
    </div>
    <div class="field">
      <label for="fire-kwargs">Kwargs (JSON object)</label>
      <textarea id="fire-kwargs">{}</textarea>
    </div>
    <div class="btn-row">
      <button class="btn btn-primary" type="button" id="fire-submit">Enqueue</button>
      <button class="btn" type="button" id="fire-cancel">Cancel</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const $ = (s) => document.querySelector(s);
const esc = (s) => String(s ?? '').replace(/[&<>"]/g, c =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

let caps = { control: false, fire: false, auth: false };
let selectedWorker = null;
let workerCache = {};
let openTaskId = null;

function pill(state) {
  return `<span class="pill s-${esc(state)}">${esc(state)}</span>`;
}

async function get(path) {
  const r = await fetch(path, { credentials: 'same-origin' });
  if (!r.ok) throw new Error(r.status + ' ' + (await r.text()));
  return r.json();
}

async function post(path, body) {
  const r = await fetch(path, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: body == null ? undefined : JSON.stringify(body),
  });
  const text = await r.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch (_) {}
  if (!r.ok) {
    const msg = (json && json.error) || text || r.status;
    throw new Error(msg);
  }
  return json;
}

function toast(msg) {
  const el = $('#toast');
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.classList.remove('show'), 2600);
}

function taskTime(t) {
  return t.finished_at || t.started_at || t.received_at || t.sent_at || t.updated_at || null;
}

function fmtRel(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const sec = Math.round((Date.now() - d.getTime()) / 1000);
  const abs = Math.abs(sec);
  let rel;
  if (abs < 60) rel = (sec >= 0 ? abs + 's ago' : 'in ' + abs + 's');
  else if (abs < 3600) rel = (sec >= 0 ? Math.floor(abs/60) + 'm ago' : 'in ' + Math.floor(abs/60) + 'm');
  else if (abs < 86400) rel = (sec >= 0 ? Math.floor(abs/3600) + 'h ago' : 'in ' + Math.floor(abs/3600) + 'h');
  else rel = (sec >= 0 ? Math.floor(abs/86400) + 'd ago' : 'in ' + Math.floor(abs/86400) + 'd');
  return `<time title="${esc(d.toLocaleString())}">${esc(rel)}</time>`;
}

function renderStats(s) {
  const t = s.totals || {};
  caps = s.capabilities || caps;
  const cards = [
    ['sent', t.sent], ['received', t.received], ['succeeded', t.succeeded],
    ['failed', t.failed], ['retried', t.retried], ['revoked', t.revoked],
    ['workers', `${s.workers.alive}/${s.workers.total}`],
  ];
  $('#cards').innerHTML = cards.map(([l, n]) =>
    `<div class="card"><div class="n">${esc(n ?? 0)}</div><div class="l">${l}</div></div>`
  ).join('');
  const hints = [];
  if (!caps.fire) hints.push('Fire/retry disabled (no broker).');
  if (!caps.control) hints.push('Revoke/pause disabled (no control channel).');
  const hint = $('#cap-hint');
  if (hints.length) {
    hint.hidden = false;
    hint.textContent = hints.join(' ');
  } else {
    hint.hidden = true;
  }
  $('#btn-fire').disabled = !caps.fire;
}

function setWorkerFilter(id, label) {
  selectedWorker = id;
  const chip = $('#worker-chip');
  if (!id) {
    chip.hidden = true;
    $('#worker-panel').hidden = true;
  } else {
    chip.hidden = false;
    $('#worker-chip-label').textContent = 'worker: ' + (label || id);
  }
  refresh();
}

function renderWorkers(ws) {
  const tb = $('#workers tbody');
  workerCache = {};
  if (!ws.length) {
    tb.innerHTML = '<tr><td class="empty">no workers seen</td></tr>';
    return;
  }
  tb.innerHTML = `<tr><th>worker</th><th>state</th><th>queues</th><th>active</th><th>ok</th><th>fail</th></tr>` +
    ws.map(w => {
      workerCache[w.id] = w;
      const label = w.hostname || w.id;
      const sel = selectedWorker === w.id ? ' selected' : '';
      const activeIds = w.active_task_ids || [];
      return `<tr class="clickable${sel}" data-worker="${esc(w.id)}" data-label="${esc(label)}">
        <td class="mono" title="${esc(w.id)}">${esc(label)}</td>
        <td>${pill(w.alive ? 'success' : 'failure')}<span style="font-size:11px;margin-left:4px">${w.alive?'online':'offline'}</span></td>
        <td class="mono">${esc((w.queues||[]).join(', '))}</td>
        <td>${esc(activeIds.length || w.active || 0)}</td>
        <td>${esc(w.succeeded)}</td><td>${esc(w.failed)}</td>
      </tr>`;
    }).join('');
  tb.querySelectorAll('tr[data-worker]').forEach(tr => {
    tr.addEventListener('click', () => {
      const id = tr.getAttribute('data-worker');
      const label = tr.getAttribute('data-label');
      if (selectedWorker === id) setWorkerFilter(null);
      else setWorkerFilter(id, label);
    });
  });
  renderWorkerPanel();
}

function renderWorkerPanel() {
  const panel = $('#worker-panel');
  if (!selectedWorker || !workerCache[selectedWorker]) {
    panel.hidden = true;
    return;
  }
  const w = workerCache[selectedWorker];
  const ids = w.active_task_ids || [];
  panel.hidden = false;
  panel.innerHTML = `<strong>Active on ${esc(w.hostname || w.id)}</strong>
    <div class="hint">${esc(ids.length)} in-flight task(s)</div>
    ${ids.length ? `<ul>${ids.map(id =>
      `<li><a href="#" data-task="${esc(id)}" class="mono">${esc(id.slice(0,8))}…</a></li>`
    ).join('')}</ul>` : '<div class="hint">No started/received tasks right now.</div>'}`;
  panel.querySelectorAll('a[data-task]').forEach(a => {
    a.addEventListener('click', (e) => {
      e.preventDefault();
      openTask(a.getAttribute('data-task'));
    });
  });
}

function renderQueues(q) {
  const tb = $('#queues tbody');
  const keys = Object.keys(q || {});
  if (!keys.length) { tb.innerHTML = '<tr><td class="empty">no queue data</td></tr>'; return; }
  tb.innerHTML = `<tr><th>queue</th><th>depth</th><th>actions</th></tr>` +
    keys.sort().map(k => `<tr>
      <td class="mono">${esc(k)}</td>
      <td>${esc(q[k])}</td>
      <td class="btn-row">
        <button class="btn" type="button" data-pause="${esc(k)}" ${caps.control?'':'disabled'}>Pause</button>
        <button class="btn" type="button" data-resume="${esc(k)}" ${caps.control?'':'disabled'}>Resume</button>
      </td>
    </tr>`).join('');
  tb.querySelectorAll('[data-pause]').forEach(b => b.addEventListener('click', async () => {
    try {
      await post('/api/queues/' + encodeURIComponent(b.getAttribute('data-pause')) + '/pause');
      toast('Paused ' + b.getAttribute('data-pause'));
    } catch (e) { toast('Pause failed: ' + e.message); }
  }));
  tb.querySelectorAll('[data-resume]').forEach(b => b.addEventListener('click', async () => {
    try {
      await post('/api/queues/' + encodeURIComponent(b.getAttribute('data-resume')) + '/resume');
      toast('Resumed ' + b.getAttribute('data-resume'));
    } catch (e) { toast('Resume failed: ' + e.message); }
  }));
}

function renderTasks(tasks) {
  const tb = $('#tasks tbody');
  if (!tasks.length) {
    tb.innerHTML = '<tr><td colspan="8" class="empty">no tasks</td></tr>';
    return;
  }
  tb.innerHTML = tasks.map(t => `<tr class="clickable" data-id="${esc(t.id)}">
    <td title="${esc(t.name)}">${esc(t.name)}</td>
    <td>${pill(t.state)}</td>
    <td>${fmtRel(taskTime(t))}</td>
    <td class="mono">${esc(t.queue || '')}</td>
    <td class="mono" title="${esc(t.worker_id||'')}">${esc((t.worker_id||'').split(':')[0])}</td>
    <td>${t.runtime_ms != null ? esc(t.runtime_ms)+' ms' : ''}</td>
    <td>${esc(t.retries)}/${esc(t.max_retries)}</td>
    <td class="mono" title="${esc(t.id)}">${esc(t.id.slice(0,8))}</td>
  </tr>`).join('');
  tb.querySelectorAll('tr[data-id]').forEach(tr => {
    tr.addEventListener('click', () => openTask(tr.getAttribute('data-id')));
  });
}

function taskQuery() {
  const p = new URLSearchParams();
  const n = $('#f-name').value.trim(); if (n) p.set('name', n);
  const s = $('#f-state').value; if (s) p.set('state', s);
  const q = $('#f-queue').value.trim(); if (q) p.set('queue', q);
  if (selectedWorker) p.set('worker', selectedWorker);
  p.set('limit', '100');
  return '/api/tasks?' + p.toString();
}

function closeDrawer() {
  openTaskId = null;
  $('#drawer').classList.remove('open');
  $('#drawer-backdrop').classList.remove('open');
  $('#drawer').setAttribute('aria-hidden', 'true');
}

async function openTask(id) {
  openTaskId = id;
  try {
    const t = await get('/api/tasks/' + encodeURIComponent(id));
    $('#drawer-title').textContent = t.name || 'Task';
    const times = [
      ['sent', t.sent_at], ['received', t.received_at],
      ['started', t.started_at], ['finished', t.finished_at],
      ['updated', t.updated_at],
    ].filter(([, v]) => v);
    $('#drawer-body').innerHTML = `
      <div class="btn-row" style="margin-bottom:14px">
        <button class="btn btn-primary" type="button" id="act-retry" ${caps.fire?'':'disabled'}>Retry</button>
        <button class="btn btn-danger" type="button" id="act-revoke" ${caps.control?'':'disabled'}>Revoke</button>
      </div>
      <dl class="kv">
        <dt>id</dt><dd class="mono">${esc(t.id)}</dd>
        <dt>state</dt><dd>${pill(t.state)}</dd>
        <dt>queue</dt><dd class="mono">${esc(t.queue || '')}</dd>
        <dt>worker</dt><dd class="mono">${esc(t.worker_id || '')}</dd>
        <dt>retries</dt><dd>${esc(t.retries)} / ${esc(t.max_retries)}</dd>
        <dt>runtime</dt><dd>${t.runtime_ms != null ? esc(t.runtime_ms) + ' ms' : '—'}</dd>
      </dl>
      <h2>Timestamps</h2>
      <dl class="kv">
        ${times.map(([k,v]) => `<dt>${esc(k)}</dt><dd>${fmtRel(v)} <span class="mono">${esc(new Date(v).toLocaleString())}</span></dd>`).join('')}
      </dl>
      <h2>Args</h2><div class="pre">${esc(JSON.stringify(t.args ?? [], null, 2))}</div>
      <h2>Kwargs</h2><div class="pre">${esc(JSON.stringify(t.kwargs ?? {}, null, 2))}</div>
      ${t.error ? `<h2>Error</h2><div class="pre">${esc(t.error)}</div>` : ''}
      ${t.result != null ? `<h2>Result</h2><div class="pre">${esc(JSON.stringify(t.result, null, 2))}</div>` : ''}
      <p class="hint" style="margin-top:14px">Retry enqueues a new task id with the same name/args/kwargs/queue.</p>
    `;
    $('#act-retry')?.addEventListener('click', async () => {
      try {
        const r = await post('/api/tasks/' + encodeURIComponent(id) + '/retry');
        toast('Retried as ' + r.id.slice(0, 8) + '…');
        refresh();
      } catch (e) { toast('Retry failed: ' + e.message); }
    });
    $('#act-revoke')?.addEventListener('click', async () => {
      try {
        await post('/api/tasks/' + encodeURIComponent(id) + '/revoke');
        toast('Revoke sent');
        openTask(id);
        refresh();
      } catch (e) { toast('Revoke failed: ' + e.message); }
    });
    $('#drawer').classList.add('open');
    $('#drawer-backdrop').classList.add('open');
    $('#drawer').setAttribute('aria-hidden', 'false');
  } catch (e) {
    toast('Could not load task: ' + e.message);
  }
}

function openFire() {
  if (!caps.fire) { toast('Fire disabled (no broker)'); return; }
  $('#fire-modal').classList.add('open');
}
function closeFire() { $('#fire-modal').classList.remove('open'); }

async function submitFire() {
  let args, kwargs;
  try { args = JSON.parse($('#fire-args').value || '[]'); }
  catch (e) { toast('Args must be valid JSON'); return; }
  try { kwargs = JSON.parse($('#fire-kwargs').value || '{}'); }
  catch (e) { toast('Kwargs must be valid JSON'); return; }
  const name = $('#fire-name').value.trim();
  if (!name) { toast('Task name required'); return; }
  try {
    const r = await post('/api/tasks/fire', {
      name,
      args,
      kwargs,
      queue: $('#fire-queue').value.trim() || 'default',
    });
    toast('Fired ' + r.id.slice(0, 8) + '…');
    closeFire();
    refresh();
  } catch (e) { toast('Fire failed: ' + e.message); }
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
    if (openTaskId) {
      // keep drawer open; soft-refresh detail occasionally via nudge only
    }
  } catch (e) {
    $('#updated').textContent = 'error: ' + e.message;
  }
}

['#f-name', '#f-state', '#f-queue'].forEach(s =>
  $(s).addEventListener('input', refresh));
$('#worker-chip-clear').addEventListener('click', () => setWorkerFilter(null));
$('#btn-fire').addEventListener('click', openFire);
$('#fire-cancel').addEventListener('click', closeFire);
$('#fire-submit').addEventListener('click', submitFire);
$('#drawer-close').addEventListener('click', closeDrawer);
$('#drawer-backdrop').addEventListener('click', closeDrawer);
$('#fire-modal').addEventListener('click', (e) => {
  if (e.target === $('#fire-modal')) closeFire();
});

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
setInterval(refresh, 5000);
connectSSE();
</script>
</body>
</html>''';

"""A deliberately minimal page at ``/``: fleet summary plus the live alert stream."""

INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FreightPulse Live Alerts</title>
<style>
  :root { --bg:#f7f7f5; --fg:#1d1d1b; --muted:#6b6b66; --card:#fff; --line:#e3e3de;
          --high:#b42318; --med:#b54708; --low:#027a48; --crit:#7a0f0a; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#141413; --fg:#ececea; --muted:#9a9a94; --card:#1e1e1c; --line:#2e2e2b;
            --high:#f97066; --med:#fdb022; --low:#32d583; --crit:#ff4d40; }
  }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--fg);
         font:14px/1.45 ui-sans-serif, system-ui, -apple-system, sans-serif; }
  main { max-width:1100px; margin:0 auto; padding:24px 16px; }
  h1 { font-size:20px; margin:0 0 4px; } p.sub { color:var(--muted); margin:0 0 20px; }
  .tiles { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px;
           margin-bottom:20px; }
  .tile { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:12px; }
  .tile b { display:block; font-size:22px; font-variant-numeric:tabular-nums; }
  .tile span { color:var(--muted); font-size:12px; }
  table { width:100%; border-collapse:collapse; background:var(--card);
          border:1px solid var(--line); border-radius:10px; overflow:hidden; }
  th, td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--line);
           vertical-align:top; }
  th { font-size:12px; color:var(--muted); font-weight:600; }
  td.t { white-space:nowrap; font-variant-numeric:tabular-nums; color:var(--muted); }
  .sev { font-weight:700; font-size:12px; }
  .CRITICAL { color:var(--crit); } .HIGH { color:var(--high); } .MEDIUM { color:var(--med); }
  .LOW, .RESOLVE { color:var(--low); }
  .wrap { overflow-x:auto; }
  a { color:inherit; }
  #status { font-size:12px; color:var(--muted); }
</style>
</head>
<body>
<main>
  <h1>FreightPulse live alerts</h1>
  <p class="sub">Late risk, detention and stale tracking, streamed from <code>freight.alerts</code>
     over <code>/ws/alerts</code>. API docs at <a href="/docs">/docs</a>.
     <span id="status">connecting...</span></p>
  <div class="tiles" id="tiles"></div>
  <div class="wrap"><table>
    <thead><tr><th>Event time</th><th>Type</th><th>Severity</th><th>Shipment</th>
      <th>Message</th></tr></thead>
    <tbody id="rows"><tr><td colspan="5">Waiting for alerts...</td></tr></tbody>
  </table></div>
</main>
<script>
const rows = document.getElementById('rows');
let first = true;
function esc(s) { return String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function addAlert(a) {
  if (first) { rows.innerHTML = ''; first = false; }
  const sev = a.action === 'RESOLVE' ? 'RESOLVE' : a.severity;
  const tr = document.createElement('tr');
  tr.innerHTML = `<td class="t">${esc(a.triggered_at.replace('T',' ').slice(0,16))}</td>
    <td>${esc(a.alert_type)}</td><td class="sev ${sev}">${esc(sev === 'RESOLVE' ? 'resolved' : sev)}</td>
    <td><a href="/shipments/${esc(a.shipment_id)}">${esc(a.shipment_id)}</a></td>
    <td>${esc(a.message)}</td>`;
  rows.prepend(tr);
  while (rows.children.length > 200) rows.lastChild.remove();
}
async function summary() {
  try {
    const s = await (await fetch('/metrics/summary')).json();
    const tiles = [['Fleet time', s.fleet_time.replace('T',' ').slice(0,16)],
      ['Shipments', s.shipments_total], ['In transit', s.in_transit], ['At risk', s.at_risk],
      ['Detained now', s.detained_now],
      ['Detention today', '$' + Math.round(s.detention_cost_today_usd).toLocaleString()],
      ['Delivered today', s.delivered_today]];
    document.getElementById('tiles').innerHTML = tiles.map(([k,v]) =>
      `<div class="tile"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join('');
  } catch (e) {}
}
function connect() {
  const ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws/alerts');
  ws.onopen = () => document.getElementById('status').textContent = '(live)';
  ws.onclose = () => { document.getElementById('status').textContent = '(reconnecting...)'; setTimeout(connect, 2000); };
  ws.onmessage = (ev) => { const a = JSON.parse(ev.data); if (a.alert_type) addAlert(a); };
}
async function backfill() {
  try {
    const r = await (await fetch('/alerts?limit=30')).json();
    r.items.reverse().forEach(a => addAlert({...a, action: a.resolved_at ? 'RESOLVE' : 'TRIGGER'}));
  } catch (e) {}
}
backfill().then(connect); summary(); setInterval(summary, 5000);
</script>
</body>
</html>
"""

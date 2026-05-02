from typing import Dict, Any, List
import glob
import html

from control_plane.entities import ALL_DATASETS, ALL_SOURCES


def _render_table(headers: List[str], rows: List[List[Any]]) -> str:
    header_html = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    row_html = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(cell))}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return (
        "<table>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{row_html}</tbody>"
        "</table>"
    )


def render_dataset_samples(datasets_db: Dict[str, List[Any]]) -> str:
    sections = []
    for source_id, records in datasets_db.items():
        sample_html = "<br/>".join(
            html.escape(str(record)) for record in records[:5]
        ) or "<em>No records yet</em>"
        sections.append(
            f"<div class='card'>"
            f"<h4>{html.escape(source_id)} ({len(records)} records)</h4>"
            f"<div class='sample'>{sample_html}</div>"
            f"</div>"
        )
    return "".join(sections) or "<p>No ingested dataset records available.</p>"


def render_storage_summary() -> str:
    storage_paths = {
        "Ingested": "storage/ingested/*.parquet",
        "Quarantine": "storage/quarantine/*.parquet",
        "CDC Log": "storage/cdc_log/*.parquet",
        "Micro-Batch": "storage/micro_batch/*.parquet",
        "Stream Buffer": "storage/stream_buffer/*.parquet",
        "Checkpoint": "storage/checkpoints/*.json",
        "Detail Logs": "storage/ingested/detail_logs/*.jsonl",
    }
    rows = [[label, len(glob.glob(pattern)), pattern] for label, pattern in storage_paths.items()]
    return _render_table(["Area", "File Count", "Pattern"], rows)


def _source_options() -> str:
    opts = ["<option value='all'>All sources</option>"]
    for src in ALL_SOURCES:
        opts.append(f"<option value='{src.source_id}'>{html.escape(src.name)}</option>")
    return "".join(opts)


def _domain_options() -> str:
    domains = sorted({d.domain for d in ALL_DATASETS})
    opts = ["<option value='all'>All domains</option>"]
    for domain in domains:
        opts.append(f"<option value='{domain}'>{html.escape(domain.title())}</option>")
    return "".join(opts)


DASHBOARD_HTML = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Supply Chain Ingestion Dashboard</title>
<style>
body{{font-family:Arial,sans-serif;background:#0d0f14;color:#eef0f5;margin:0}}
.top{{background:#13161e;padding:12px 16px;display:flex;gap:8px;align-items:center;position:sticky;top:0}}
.content{{padding:16px}}
.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}}
.card,.panel{{background:#13161e;border:1px solid #2a3040;border-radius:8px;padding:12px}}
.k{{font-size:12px;color:#9ba3b8}} .v{{font-size:22px;font-weight:700}}
table{{width:100%;border-collapse:collapse}} th,td{{padding:8px;border-bottom:1px solid #2a3040;text-align:left}}
select,button{{background:#1a1e28;color:#eef0f5;border:1px solid #2a3040;border-radius:6px;padding:6px}}
.warn{{color:#f5a623}} .bad{{color:#f04b4b}} .ok{{color:#3ecf8e}}
</style>
</head>
<body>
  <div class="top">
    <button onclick="refresh()">Refresh</button>
    <label>Source</label><select id="source">{_source_options()}</select>
    <label>Domain</label><select id="domain">{_domain_options()}</select>
    <label>Time</label>
    <select id="time">
      <option value="1h ago" selected>Last 1 hour</option>
      <option value="6h ago">Last 6 hours</option>
      <option value="24h ago">Last 24 hours</option>
      <option value="7d ago">Last 7 days</option>
    </select>
  </div>
  <div class="content">
    <div class="grid" id="kpis"></div>
    <div class="panel" style="margin-top:12px">
      <h3>Error Insights by Source</h3>
      <table><thead><tr><th>Source</th><th>Failed</th><th>Quarantined</th><th>Status</th></tr></thead><tbody id="errors"></tbody></table>
    </div>
    <div class="panel" style="margin-top:12px">
      <h3>Jobs</h3>
      <table><thead><tr><th>Job</th><th>Source</th><th>Status</th><th>Ingested</th><th>Failed</th><th>Quarantined</th></tr></thead><tbody id="jobs"></tbody></table>
    </div>
  </div>
<script>
const API='http://localhost:8000';
const TOK='__API_TOKEN__';
const HDR={{Authorization:`Bearer ${{TOK}}`}};
let state={{}};

function kpi(k,v){{return `<div class="card"><div class="k">${{k}}</div><div class="v">${{v}}</div></div>`;}}
function cls(v){{return v>0?'bad':'ok';}}
function inTime(item, fromTs){{
  const t=item?.telemetry?.start_time || item?.start_time;
  if(!t) return false;
  const ts = new Date(t).getTime();
  const now = Date.now();
  const m = {{'1h ago':3600000,'6h ago':21600000,'24h ago':86400000,'7d ago':604800000}};
  return ts >= now-(m[fromTs]||3600000);
}}
function sourceDomain(sourceId){{
  const s=(state.sources?.sources||[]).find(x=>x.source_id===sourceId);
  return s?.domain||'unknown';
}}
function refresh(){{
  const fromTs=document.getElementById('time').value||'1h ago';
  Promise.all([
    fetch(`${{API}}/metrics/filtered?from_timestamp=${{encodeURIComponent(fromTs)}}`,{{headers:HDR}}).then(r=>r.json()),
    fetch(`${{API}}/dashboard/json`,{{headers:HDR}}).then(r=>r.json()),
    fetch(`${{API}}/source-configurations`,{{headers:HDR}}).then(r=>r.json()),
    fetch(`${{API}}/errors/summary`,{{headers:HDR}}).then(r=>r.json())
  ]).then(([metrics,dashboard,sources,errors])=>{{
    state={{metrics,dashboard,sources,errors}};
    render();
  }});
}}
function render(){{
  const source=document.getElementById('source').value;
  const domain=document.getElementById('domain').value;
  const fromTs=document.getElementById('time').value;
  const jobs=(state.dashboard?.jobs||[]).filter(j=>inTime(j,fromTs)).filter(j=>(source==='all'||j.source_id===source)).filter(j=>(domain==='all'||sourceDomain(j.source_id)===domain));
  const m=state.metrics||{{}};
  document.getElementById('kpis').innerHTML=[kpi('Total Jobs',m.total_jobs||0),kpi('Ingested',m.total_records_ingested||0),kpi('Quarantined',m.total_records_quarantined||0),kpi('Failed',m.total_records_failed||0)].join('');
  document.getElementById('jobs').innerHTML=jobs.map(j=>`<tr><td>${{j.job_id.slice(0,8)}}...</td><td>${{j.source_id}}</td><td>${{j.status}}</td><td>${{j.telemetry?.records_ingested||0}}</td><td class="${{cls(j.telemetry?.records_failed||0)}}">${{j.telemetry?.records_failed||0}}</td><td class="${{cls(j.telemetry?.records_quarantined||0)}}">${{j.telemetry?.records_quarantined||0}}</td></tr>`).join('')||'<tr><td colspan="6">No jobs</td></tr>';
  const errs=state.errors?.ingestion?.by_source||{{}};
  document.getElementById('errors').innerHTML=Object.entries(errs).filter(([sid])=>(source==='all'||sid===source)).filter(([sid])=>(domain==='all'||sourceDomain(sid)===domain)).map(([sid,e])=>`<tr><td>${{sid}}</td><td class="${{cls(e.records_failed)}}">${{e.records_failed}}</td><td class="${{cls(e.records_quarantined)}}">${{e.records_quarantined}}</td><td>${{e.status}}</td></tr>`).join('')||'<tr><td colspan="4">No errors</td></tr>';
}}
document.getElementById('source').addEventListener('change',render);
document.getElementById('domain').addEventListener('change',render);
document.getElementById('time').addEventListener('change',refresh);
refresh();
</script>
</body>
</html>"""
from dashboard_ui.components import (
    domain_options_html,
    render_dataset_samples,
    render_storage_summary,
    source_options_html,
)
from dashboard_ui.templates import BASE_DASHBOARD_HTML


def build_dashboard_html() -> str:
    return (
        BASE_DASHBOARD_HTML
        .replace("__SOURCE_OPTIONS__", source_options_html())
        .replace("__DOMAIN_OPTIONS__", domain_options_html())
    )


DASHBOARD_HTML = build_dashboard_html()

from typing import Dict, Any, List
import glob
import html
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
from control_plane.entities import ALL_DATASETS, ALL_SOURCES

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# region: UI Helpers
# ---------------------------------------------------------------------------
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
        "Ingested":      "storage/ingested/*.parquet",
        "Quarantine":    "storage/quarantine/*.parquet",
        "CDC Log":       "storage/cdc_log/*.parquet",
        "Micro-Batch":   "storage/micro_batch/*.parquet",
        "Stream Buffer": "storage/stream_buffer/*.parquet",
        "Checkpoint":    "storage/checkpoints/*.json",
        "Detail Logs":   "storage/ingested/detail_logs/*.jsonl",
    }
    rows = [[label, len(glob.glob(pattern)), pattern] for label, pattern in storage_paths.items()]
    return _render_table(["Area", "File Count", "Pattern"], rows)


def _source_options_html() -> str:
    options = ["<option value='all'>All sources</option>"]
    for src in ALL_SOURCES:
        options.append(f"<option value='{html.escape(src.source_id)}'>{html.escape(src.name)}</option>")
    return "".join(options)


def _domain_chips_html() -> str:
    domains = sorted({d.domain for d in ALL_DATASETS})
    chips = [
        "<div class=\"chip active\" data-filter=\"domain\" data-val=\"all\" onclick=\"toggleChip('domain','all',this)\">"
        "<div class=\"cdot\" style=\"background:#5a6278\"></div>All domains"
        "</div>"
    ]
    colors = ["#3d8bff", "#00c9a7", "#9b7fff", "#ff7056", "#f5a623", "#3ecf8e"]
    for idx, domain in enumerate(domains):
        color = colors[idx % len(colors)]
        chips.append(
            f"<div class=\"chip\" data-filter=\"domain\" data-val=\"{html.escape(domain)}\" "
            f"onclick=\"toggleChip('domain','{html.escape(domain)}',this)\">"
            f"<div class=\"cdot\" style=\"background:{color}\"></div>{html.escape(domain.title())}"
            f"</div>"
        )
    return "".join(chips)

# ---------------------------------------------------------------------------
# region: Dashboard Template (Frontend)
# ---------------------------------------------------------------------------
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Supply Chain Ingestion Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Syne:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<style>
:root {
  --bg:#0d0f14; --bg2:#13161e; --bg3:#1a1e28;
  --border:rgba(255,255,255,0.07); --border2:rgba(255,255,255,0.12);
  --t1:#eef0f5; --t2:#9ba3b8; --t3:#5a6278;
  --accent:#3d8bff; --green:#3ecf8e; --amber:#f5a623; --red:#f04b4b;
  --purple:#9b7fff; --coral:#ff7056; --teal:#00c9a7;
  --radius:12px; --radius-sm:8px;
  --font-d:'Syne',sans-serif; --font-m:'DM Mono',monospace;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html{background:var(--bg);scroll-behavior:smooth}
body{font-family:var(--font-m);background:var(--bg);color:var(--t1);min-height:100vh;font-size:13px;line-height:1.6}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:var(--bg2)}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}

/* ── Shell ── */
.shell{display:grid;grid-template-columns:220px 1fr;min-height:100vh}

/* ── Sidebar ── */
.sidebar{background:var(--bg2);border-right:1px solid var(--border);padding:24px 0;
  position:sticky;top:0;height:100vh;overflow-y:auto;display:flex;flex-direction:column}
.logo{padding:0 20px 24px;border-bottom:1px solid var(--border);margin-bottom:20px}
.logo-mark{font-family:var(--font-d);font-size:15px;font-weight:700;color:var(--t1);
  letter-spacing:-0.03em;display:flex;align-items:center;gap:8px}
.logo-mark .dot{width:8px;height:8px;border-radius:50%;background:var(--accent);
  box-shadow:0 0 8px var(--accent);animation:breathe 2s ease-in-out infinite}
@keyframes breathe{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(.8)}}
.logo-sub{font-size:10px;color:var(--t3);margin-top:2px;letter-spacing:.05em}

.fsec{padding:0 16px;margin-bottom:20px}
.fsec-lbl{font-size:9px;font-weight:500;color:var(--t3);letter-spacing:.12em;
  text-transform:uppercase;margin-bottom:10px;padding:0 4px}
.chip-grp{display:flex;flex-direction:column;gap:3px}
.chip{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:var(--radius-sm);
  cursor:pointer;transition:all .15s;border:1px solid transparent;
  font-size:11px;color:var(--t2);font-family:var(--font-m);user-select:none}
.chip:hover{background:var(--bg3);color:var(--t1)}
.chip.active{background:rgba(61,139,255,.12);border-color:rgba(61,139,255,.3);color:var(--accent)}
.chip .cdot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.chip .ccnt{margin-left:auto;font-size:9px;background:var(--bg3);padding:1px 5px;
  border-radius:10px;color:var(--t3)}
.fsel{width:100%;background:var(--bg3);border:1px solid var(--border);color:var(--t1);
  border-radius:var(--radius-sm);padding:7px 10px;font-size:11px;font-family:var(--font-m);
  outline:none;cursor:pointer;-webkit-appearance:none;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%235a6278'/%3E%3C/svg%3E");
  background-repeat:no-repeat;background-position:right 10px center;padding-right:28px}
.fsel:focus{border-color:rgba(61,139,255,.4)}
.frange-wrap{display:flex;flex-direction:column;gap:6px}
.frange{width:100%;accent-color:var(--accent);cursor:pointer;height:4px}
.frange-lbls{display:flex;justify-content:space-between;font-size:10px;color:var(--t3)}
.sb-bottom{margin-top:auto;padding:16px;border-top:1px solid var(--border)}
.reset-btn{width:100%;padding:8px;background:transparent;border:1px solid var(--border2);
  color:var(--t2);border-radius:var(--radius-sm);cursor:pointer;
  font-family:var(--font-m);font-size:11px;transition:all .15s}
.reset-btn:hover{background:var(--bg3);color:var(--t1)}

/* ── Main ── */
.main{background:var(--bg);overflow-x:hidden;min-height:100vh}
.page-content{color:var(--t1);min-height:calc(100vh - 52px)}
.page-content .content{background:var(--bg)}
.topbar{background:var(--bg2);border-bottom:1px solid var(--border);padding:14px 28px;
  display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.topbar-l{display:flex;align-items:center;gap:16px}
.page-title{font-family:var(--font-d);font-size:16px;font-weight:600;color:var(--t1);letter-spacing:-0.02em}
.pills{display:flex;gap:6px;flex-wrap:wrap}
.pill{font-size:10px;padding:3px 8px;background:rgba(61,139,255,.15);
  border:1px solid rgba(61,139,255,.25);border-radius:20px;color:var(--accent);
  display:flex;align-items:center;gap:4px;cursor:pointer}
.pill:hover{background:rgba(61,139,255,.25)}
.pill .x{font-size:9px;opacity:.7}
.topbar-r{display:flex;align-items:center;gap:12px}
.live-badge{display:flex;align-items:center;gap:6px;font-size:10px;color:var(--green);letter-spacing:.04em}
.live-dot{width:6px;height:6px;border-radius:50%;background:var(--green);
  box-shadow:0 0 6px var(--green);animation:breathe 1.5s ease-in-out infinite}
.ref-btn{display:flex;align-items:center;gap:6px;padding:6px 14px;background:transparent;
  border:1px solid var(--border2);border-radius:var(--radius-sm);color:var(--t2);
  font-family:var(--font-m);font-size:11px;cursor:pointer;transition:all .15s}
.ref-btn:hover{background:var(--bg3);color:var(--t1)}
.spin{display:inline-block}
@keyframes spin{to{transform:rotate(360deg)}}
.spinning{animation:spin .6s linear infinite}
.last-upd{font-size:10px;color:var(--t3)}

/* ── Content ── */
.content{padding:24px 28px}

/* ── KPI strip ── */
.kpi-strip{display:grid;grid-template-columns:repeat(8,1fr);gap:10px;margin-bottom:20px}
.kpi{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
  padding:14px 16px;position:relative;overflow:hidden;transition:border-color .2s}
.kpi:hover{border-color:var(--border2)}
.kpi-bar{position:absolute;top:0;left:0;right:0;height:2px;border-radius:var(--radius) var(--radius) 0 0}
.kpi-lbl{font-size:9px;color:var(--t3);letter-spacing:.08em;text-transform:uppercase;margin-bottom:8px;margin-top:4px}
.kpi-val{font-family:var(--font-d);font-size:22px;font-weight:700;color:var(--t1);line-height:1}
.kpi-sub{font-size:10px;color:var(--t3);margin-top:6px;display:flex;align-items:center;gap:4px}
.badge{font-size:9px;padding:2px 5px;border-radius:4px}
.b-up{background:rgba(62,207,142,.15);color:var(--green)}
.b-dn{background:rgba(240,75,75,.12);color:var(--red)}
.b-nt{background:rgba(245,166,35,.12);color:var(--amber)}

/* ── Grid rows ── */
.row{display:grid;gap:16px;margin-bottom:16px}
.r-6040{grid-template-columns:1.5fr 1fr}
.r-5050{grid-template-columns:1fr 1fr}
.r-333{grid-template-columns:1fr 1fr 1fr}
.r-full{grid-template-columns:1fr}

/* ── Panel ── */
.panel{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.ph{padding:14px 18px;border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between}
.ph-title{font-family:var(--font-d);font-size:13px;font-weight:600;color:var(--t1)}
.ph-meta{font-size:10px;color:var(--t3)}
.pb{padding:18px}
.pb-np{padding:0}
.scroll-body{max-height:340px;overflow-y:auto}

/* ── Legend ── */
.legend{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:12px}
.leg-item{display:flex;align-items:center;gap:5px;font-size:10px;color:var(--t2)}
.leg-sw{width:8px;height:8px;border-radius:2px;flex-shrink:0}

/* ── SLA ── */
.sla-list{display:flex;flex-direction:column;gap:10px}
.sla-row{display:flex;align-items:center;gap:10px}
.sla-name{font-size:11px;color:var(--t2);width:120px;flex-shrink:0;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sla-track{flex:1;height:5px;background:var(--bg3);border-radius:3px;overflow:hidden}
.sla-fill{height:100%;border-radius:3px;transition:width .6s cubic-bezier(.4,0,.2,1)}
.sla-pct{font-size:10px;color:var(--t2);width:32px;text-align:right}
.sla-badge{font-size:9px;padding:2px 6px;border-radius:10px;width:44px;text-align:center;flex-shrink:0}

/* ── Sources ── */
.src-rows{display:flex;flex-direction:column;gap:6px}
.src-row{display:flex;align-items:center;gap:10px;padding:10px 14px;
  border-radius:var(--radius-sm);background:var(--bg3);border:1px solid transparent;
  transition:border-color .15s;cursor:pointer}
.src-row:hover{border-color:var(--border2)}
.src-icon{width:30px;height:30px;border-radius:7px;display:flex;align-items:center;
  justify-content:center;font-size:12px;flex-shrink:0}
.src-info{flex:1;min-width:0}
.src-name{font-size:11px;font-weight:500;color:var(--t1)}
.src-meta{font-size:9px;color:var(--t3)}
.src-bars{display:flex;flex-direction:column;gap:3px;width:80px}
.src-mini-bar{height:3px;background:var(--bg2);border-radius:2px;overflow:hidden}
.src-mini-fill{height:100%;border-radius:2px}
.src-pct{font-size:10px;color:var(--t2);width:28px;text-align:right}
.cls-pill{font-size:9px;padding:2px 7px;border-radius:10px;flex-shrink:0;font-weight:500}
.cls-pub{background:rgba(62,207,142,.15);color:var(--green);border:1px solid rgba(62,207,142,.2)}
.cls-int{background:rgba(245,166,35,.12);color:var(--amber);border:1px solid rgba(245,166,35,.2)}
.cls-conf{background:rgba(240,75,75,.1);color:var(--red);border:1px solid rgba(240,75,75,.2)}
.cls-res{background:rgba(61,139,255,.12);color:var(--accent);border:1px solid rgba(61,139,255,.2)}
.freq-pill{font-size:9px;padding:2px 6px;border-radius:10px;
  background:var(--bg2);color:var(--t3);border:1px solid var(--border)}

/* ── Alerts ── */
.alert-list{display:flex;flex-direction:column;gap:6px}
.alert-item{display:flex;align-items:flex-start;gap:10px;padding:10px 14px;
  border-radius:var(--radius-sm);border-left:2px solid}
.a-crit{background:rgba(240,75,75,.07);border-color:var(--red)}
.a-warn{background:rgba(245,166,35,.07);border-color:var(--amber)}
.a-info{background:rgba(61,139,255,.07);border-color:var(--accent)}
.a-dot{width:5px;height:5px;border-radius:50%;flex-shrink:0;margin-top:5px}
.a-crit .a-dot{background:var(--red)}
.a-warn .a-dot{background:var(--amber)}
.a-info .a-dot{background:var(--accent)}
.a-text{flex:1;font-size:11px;color:var(--t1)}
.a-sub{font-size:10px;color:var(--t3);margin-top:1px}
.a-time{font-size:9px;color:var(--t3);flex-shrink:0;margin-top:2px}

/* ── Jobs table ── */
.jt{width:100%;border-collapse:collapse}
.jt th{font-size:9px;font-weight:500;color:var(--t3);letter-spacing:.1em;text-transform:uppercase;
  text-align:left;padding:10px 14px;border-bottom:1px solid var(--border);
  position:sticky;top:0;background:var(--bg2)}
.jt td{padding:10px 14px;font-size:11px;color:var(--t2);border-bottom:1px solid var(--border);vertical-align:middle}
.jt tr:last-child td{border-bottom:none}
.jt tr:hover td{background:var(--bg3)}
.jid{font-family:var(--font-m);font-size:10px;color:var(--t3)}
.sp{font-size:9px;padding:3px 8px;border-radius:10px;display:inline-block;font-weight:500}
.s-run{background:rgba(245,166,35,.12);color:var(--amber);border:1px solid rgba(245,166,35,.2)}
.s-ok{background:rgba(62,207,142,.12);color:var(--green);border:1px solid rgba(62,207,142,.2)}
.s-fail{background:rgba(240,75,75,.1);color:var(--red);border:1px solid rgba(240,75,75,.2)}
.ng{color:var(--green)}.nw{color:var(--amber)}.nb{color:var(--red)}

/* ── Gauge ── */
.gauge-wrap{display:flex;align-items:center;gap:20px;padding:8px 0}
.gauge-ring{position:relative;display:inline-flex;align-items:center;justify-content:center}
.gauge-ring svg{display:block}
.gauge-val{position:absolute;text-align:center}
.gauge-num{font-family:var(--font-d);font-size:18px;font-weight:700;color:var(--t1);line-height:1}
.gauge-unit{font-size:9px;color:var(--t3)}
.gauge-stats{display:flex;flex-direction:column;gap:8px;flex:1}
.gstat{display:flex;justify-content:space-between;align-items:center;font-size:11px}
.gstat-l{color:var(--t3)}.gstat-v{color:var(--t1);font-weight:500}

/* ── Storage ── */
.stor-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:8px}
.stor-cell{background:var(--bg3);border:1px solid var(--border);border-radius:var(--radius-sm);
  padding:12px;text-align:center;transition:border-color .15s}
.stor-cell:hover{border-color:var(--border2)}
.stor-cnt{font-family:var(--font-d);font-size:20px;font-weight:700;color:var(--t1)}
.stor-lbl{font-size:9px;color:var(--t3);margin-top:3px;letter-spacing:.03em}
.stor-ico{font-size:16px;margin-bottom:4px}

/* ── Filter summary bar ── */
.fsummary{display:flex;align-items:center;gap:10px;padding:10px 18px;
  background:rgba(61,139,255,.05);border:1px solid rgba(61,139,255,.15);
  border-radius:var(--radius-sm);margin-bottom:16px;font-size:11px;color:var(--t2)}
.fsummary.hidden{display:none}
.fsummary strong{color:var(--accent)}

/* ── Empty ── */
.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}
.ei{font-size:28px;margin-bottom:10px;opacity:.4}

/* ── Animations ── */
@keyframes fadeUp{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}
.ai{animation:fadeUp .4s ease both}
.d1{animation-delay:.05s}.d2{animation-delay:.10s}.d3{animation-delay:.15s}
.d4{animation-delay:.20s}.d5{animation-delay:.25s}
</style>
</head>
<body>
<div class="shell">

<!-- ════════════ SIDEBAR ════════════ -->
<aside class="sidebar">
  <div class="logo">
    <div class="logo-mark"><div class="dot"></div>DataOps</div>
    <div class="logo-sub">Supply Chain Pipeline</div>
  </div>

  <div class="fsec">
    <div class="fsec-lbl">Job Status</div>
    <div class="chip-grp" id="status-chips">
      <div class="chip active" data-filter="status" data-val="all" onclick="toggleChip('status','all',this)">
        <div class="cdot" style="background:#5a6278"></div>All<span class="ccnt" id="cnt-all">—</span>
      </div>
      <div class="chip" data-filter="status" data-val="running" onclick="toggleChip('status','running',this)">
        <div class="cdot" style="background:#f5a623"></div>Running<span class="ccnt" id="cnt-run">—</span>
      </div>
      <div class="chip" data-filter="status" data-val="completed" onclick="toggleChip('status','completed',this)">
        <div class="cdot" style="background:#3ecf8e"></div>Completed<span class="ccnt" id="cnt-ok">—</span>
      </div>
      <div class="chip" data-filter="status" data-val="failed" onclick="toggleChip('status','failed',this)">
        <div class="cdot" style="background:#f04b4b"></div>Failed<span class="ccnt" id="cnt-fail">—</span>
      </div>
    </div>
  </div>

  <div class="fsec">
    <div class="fsec-lbl">Data Source</div>
    <select class="fsel" id="f-source" onchange="applyFilters()">__SOURCE_OPTIONS__</select>
  </div>

  <div class="fsec">
    <div class="fsec-lbl">Domain</div>
    <div class="chip-grp" id="domain-chips">__DOMAIN_CHIPS__</div>
  </div>

  <div class="fsec">
    <div class="fsec-lbl">Classification</div>
    <select class="fsel" id="f-cls" onchange="applyFilters()">
      <option value="all">All classifications</option>
      <option value="public">Public</option>
      <option value="internal">Internal</option>
      <option value="confidential">Confidential</option>
      <option value="restricted">Restricted</option>
    </select>
  </div>

  <div class="fsec">
    <div class="fsec-lbl">Frequency</div>
    <select class="fsel" id="f-freq" onchange="applyFilters()">
      <option value="all">All frequencies</option>
      <option value="real_time">Real-time</option>
      <option value="hourly">Hourly</option>
      <option value="daily">Daily</option>
      <option value="weekly">Weekly</option>
      <option value="on_demand">On demand</option>
    </select>
  </div>

  <div class="fsec">
    <div class="fsec-lbl">Min throughput (rec/s)</div>
    <div class="frange-wrap">
      <input type="range" class="frange" id="f-thr" min="0" max="100" value="0"
        oninput="document.getElementById('thr-val').textContent=this.value+' r/s';applyFilters()"/>
      <div class="frange-lbls"><span>0</span><span id="thr-val">0 r/s</span><span>100</span></div>
    </div>
  </div>

  <div class="fsec">
    <div class="fsec-lbl">Time range</div>
    <select class="fsel" id="f-time" onchange="applyFilters()">
      <option value="all">All time</option>
      <option value="1h" selected>Last 1 hour</option>
      <option value="6h">Last 6 hours</option>
      <option value="24h">Last 24 hours</option>
      <option value="7d">Last 7 days</option>
    </select>
  </div>

  <div class="sb-bottom">
    <button class="reset-btn" onclick="resetFilters()">↺ Reset all filters</button>
  </div>
</aside>

<!-- ════════════ MAIN ════════════ -->
<div class="main">
  <div class="topbar">
    <div class="topbar-l">
      <!-- Page tabs for navigation -->
      <div class="page-tabs" style="display:flex;gap:12px;margin-right:20px;">
        <button class="page-tab active" data-page="ingestion" onclick="switchPage('ingestion')" style="padding:6px 14px;background:transparent;border:none;color:var(--accent);cursor:pointer;font-size:12px;border-bottom:2px solid var(--accent);font-weight:500">Ingestion</button>
        <button class="page-tab" data-page="transformation" onclick="switchPage('transformation')" style="padding:6px 14px;background:transparent;border:none;color:var(--t2);cursor:pointer;font-size:12px;border-bottom:2px solid transparent;font-weight:500">Transformation</button>
        <button class="page-tab" data-page="storage" onclick="switchPage('storage')" style="padding:6px 14px;background:transparent;border:none;color:var(--t2);cursor:pointer;font-size:12px;border-bottom:2px solid transparent;font-weight:500">Storage</button>
      </div>
      <div class="pills" id="active-pills"></div>
    </div>
    <div class="topbar-r">
      <span class="last-upd" id="last-upd">—</span>
      <div class="live-badge"><div class="live-dot"></div>Live</div>
      <button class="ref-btn" onclick="refresh()"><span class="spin" id="spin-icon">↻</span> Refresh</button>
    </div>
  </div>

  <!-- INGESTION PAGE (existing content) -->
  <div id="page-ingestion" class="page-content" style="display:block;">
    <div class="content">

      <!-- filter summary -->
      <div class="fsummary hidden ai" id="fsummary">
        Showing <strong id="f-cnt">—</strong> of <strong id="t-cnt">—</strong> jobs &nbsp;
        <span style="color:var(--t3)" id="f-desc"></span>
      </div>

      <!-- KPI strip -->
      <div class="kpi-strip ai d1">
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#3d8bff,#5ba8ff)"></div>
          <div class="kpi-lbl">Total Jobs</div><div class="kpi-val" id="kv-total">—</div>
          <div class="kpi-sub"><span class="badge b-nt" id="kv-total-s">filtered</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#f5a623,#ffd066)"></div>
          <div class="kpi-lbl">Running</div><div class="kpi-val" id="kv-run">—</div>
          <div class="kpi-sub"><span class="badge b-nt" id="kv-run-s">—</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#3ecf8e,#69f0ae)"></div>
          <div class="kpi-lbl">Completed</div><div class="kpi-val" id="kv-ok">—</div>
          <div class="kpi-sub"><span class="badge b-up" id="kv-ok-s">—</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#f04b4b,#ff7070)"></div>
          <div class="kpi-lbl">Failed</div><div class="kpi-val" id="kv-fail">—</div>
          <div class="kpi-sub"><span class="badge b-dn" id="kv-fail-s">—</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#00c9a7,#4af0d5)"></div>
          <div class="kpi-lbl">Records In</div><div class="kpi-val" id="kv-ing">—</div>
          <div class="kpi-sub"><span class="badge b-up">ingested</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#f5a623,#ffd066)"></div>
          <div class="kpi-lbl">Quarantined</div><div class="kpi-val" id="kv-q">—</div>
          <div class="kpi-sub"><span class="badge b-nt" id="kv-q-s">—</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#ff7056,#ff9a8b)"></div>
          <div class="kpi-lbl">Rec. Failed</div><div class="kpi-val" id="kv-rf">—</div>
          <div class="kpi-sub"><span class="badge b-dn">schema errors</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#9b7fff,#c4aaff)"></div>
          <div class="kpi-lbl">Avg Throughput</div><div class="kpi-val" id="kv-thr">—</div>
          <div class="kpi-sub"><span class="badge b-up">rec/sec</span></div></div>
      </div>

      <!-- Row 1: throughput line + donut -->
      <div class="row r-6040 ai d2">
        <div class="panel">
          <div class="ph"><div class="ph-title">Throughput over time</div><div class="ph-meta" id="thr-meta">rec/sec</div></div>
          <div class="pb">
            <div class="legend">
              <div class="leg-item"><div class="leg-sw" style="background:#3d8bff"></div>Throughput (r/s)</div>
              <div class="leg-item"><div class="leg-sw" style="background:#9b7fff;border:1px dashed #9b7fff"></div>Quarantine %</div>
            </div>
            <div style="position:relative;height:200px">
              <canvas id="c-thr" role="img" aria-label="Throughput and quarantine rate line chart">No data.</canvas>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="ph"><div class="ph-title">Record outcome mix</div><div class="ph-meta">filtered jobs</div></div>
          <div class="pb">
            <div class="legend" id="donut-leg"></div>
            <div style="position:relative;height:190px">
              <canvas id="c-donut" role="img" aria-label="Donut chart of record outcomes">No data.</canvas>
            </div>
          </div>
        </div>
      </div>

      <!-- Row 2: SLA + sources -->
      <div class="row r-5050 ai d3">
        <div class="panel">
          <div class="ph"><div class="ph-title">Data quality SLA</div><div class="ph-meta">ingestion success % per source</div></div>
          <div class="pb"><div class="sla-list" id="sla-panel"><div class="empty"><div class="ei">◎</div>Loading…</div></div></div>
        </div>
        <div class="panel">
          <div class="ph"><div class="ph-title">Source health</div><div class="ph-meta">classification &amp; frequency</div></div>
          <div class="pb-np"><div class="scroll-body" style="padding:12px">
            <div class="src-rows" id="src-rows"><div class="empty"><div class="ei">◎</div>Loading…</div></div>
          </div></div>
        </div>
      </div>

      <!-- Row 3: duration + storage + gauge -->
      <div class="row r-333 ai d4">
        <div class="panel">
          <div class="ph"><div class="ph-title">Job duration</div><div class="ph-meta">seconds per job</div></div>
          <div class="pb">
            <div style="position:relative;height:180px">
              <canvas id="c-dur" role="img" aria-label="Job duration bar chart">No data.</canvas>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="ph"><div class="ph-title">Storage utilisation</div><div class="ph-meta">files per area</div></div>
          <div class="pb"><div class="stor-grid" id="stor-grid"><div class="empty">Loading…</div></div></div>
        </div>
        <div class="panel">
          <div class="ph"><div class="ph-title">Pipeline throughput</div><div class="ph-meta">aggregate performance</div></div>
          <div class="pb">
            <div class="gauge-wrap">
              <div class="gauge-ring">
                <svg width="120" height="120">
                  <circle cx="60" cy="60" r="50" fill="none" stroke="#1a1e28" stroke-width="10"/>
                  <circle id="garc" cx="60" cy="60" r="50" fill="none" stroke="#3d8bff" stroke-width="10"
                    stroke-linecap="round" stroke-dasharray="314" stroke-dashoffset="314"
                    transform="rotate(-90 60 60)" style="transition:stroke-dashoffset .8s ease"/>
                </svg>
                <div class="gauge-val">
                  <div class="gauge-num" id="g-num">—</div>
                  <div class="gauge-unit">rec/s</div>
                </div>
              </div>
              <div class="gauge-stats">
                <div class="gstat"><span class="gstat-l">Peak</span><span class="gstat-v" id="g-peak">—</span></div>
                <div class="gstat"><span class="gstat-l">Min</span><span class="gstat-v" id="g-min">—</span></div>
                <div class="gstat"><span class="gstat-l">P95</span><span class="gstat-v" id="g-p95">—</span></div>
                <div class="gstat"><span class="gstat-l">Jobs</span><span class="gstat-v" id="g-jobs">—</span></div>
                <div class="gstat"><span class="gstat-l">Avg dur.</span><span class="gstat-v" id="g-dur">—</span></div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Row 4: alerts -->
      <div class="row r-full ai d4">
        <div class="panel">
          <div class="ph"><div class="ph-title">Pipeline alerts</div><div class="ph-meta" id="a-cnt">scanning…</div></div>
          <div class="pb"><div class="alert-list" id="alert-list"><div class="empty"><div class="ei">◎</div>Loading…</div></div></div>
        </div>
      </div>

      <!-- Row 5: jobs table -->
      <div class="row r-full ai d5">
        <div class="panel">
          <div class="ph"><div class="ph-title">Active &amp; recent jobs</div><div class="ph-meta" id="j-cnt">—</div></div>
          <div class="pb-np">
            <div class="scroll-body" style="max-height:400px">
              <table class="jt">
                <thead><tr>
                  <th>Job ID</th><th>Source</th><th>Dataset</th><th>Status</th>
                  <th>Ingested</th><th>Failed</th><th>Quarantined</th><th>Throughput</th><th>Duration</th>
                </tr></thead>
                <tbody id="j-body"><tr><td colspan="9" class="empty">Loading…</td></tr></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>

  </div><!-- /page-ingestion -->

  <!-- TRANSFORMATION PAGE -->
  <div id="page-transformation" class="page-content" style="display:none;">
    <div class="content">
      <div class="kpi-strip ai d1">
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#9b7fff,#c4aaff)"></div>
          <div class="kpi-lbl">Total Runs</div><div class="kpi-val" id="txr-total">—</div>
          <div class="kpi-sub"><span class="badge b-nt">all time</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#3d8bff,#5ba8ff)"></div>
          <div class="kpi-lbl">Avg Latency</div><div class="kpi-val" id="txr-latency">—</div>
          <div class="kpi-sub"><span class="badge b-nt">seconds</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#3ecf8e,#69f0ae)"></div>
          <div class="kpi-lbl">Avg Quality</div><div class="kpi-val" id="txr-quality">—</div>
          <div class="kpi-sub"><span class="badge b-up">cleaned %</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#f5a623,#ffd066)"></div>
          <div class="kpi-lbl">Duplicates Removed</div><div class="kpi-val" id="txr-dupes">—</div>
          <div class="kpi-sub"><span class="badge b-nt">%</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#00c9a7,#4af0d5)"></div>
          <div class="kpi-lbl">Late Arrivals</div><div class="kpi-val" id="txr-late">—</div>
          <div class="kpi-sub"><span class="badge b-up">detected</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#ff7056,#ff9a8b)"></div>
          <div class="kpi-lbl">Silver Records</div><div class="kpi-val" id="txr-silver">—</div>
          <div class="kpi-sub"><span class="badge b-up">total</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#00c9a7,#4af0d5)"></div>
          <div class="kpi-lbl">Gold Signals</div><div class="kpi-val" id="txr-gold">—</div>
          <div class="kpi-sub"><span class="badge b-up">replenishment</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#f5a623,#ffd066)"></div>
          <div class="kpi-lbl">Schema Violations</div><div class="kpi-val" id="txr-violations">—</div>
          <div class="kpi-sub"><span class="badge b-dn">count</span></div></div>
      </div>
      
      <div class="row r-5050 ai d2">
        <div class="panel">
          <div class="ph"><div class="ph-title">Transformation KPIs</div><div class="ph-meta">per-run details</div></div>
          <div class="pb-np">
            <div class="scroll-body" style="max-height:350px;padding:12px">
              <table class="jt" style="font-size:10px">
                <thead><tr>
                  <th>Source</th><th>Layer</th><th>Records</th><th>Cleaned</th><th>Latency(s)</th><th>Run Time</th>
                </tr></thead>
                <tbody id="txr-table"><tr><td colspan="6" class="empty">Loading…</td></tr></tbody>
              </table>
            </div>
          </div>
        </div>
        <div class="panel">
          <div class="ph"><div class="ph-title">Data Quality Trend</div><div class="ph-meta">records cleaned %</div></div>
          <div class="pb">
            <div style="position:relative;height:220px">
              <canvas id="c-txr-quality" role="img" aria-label="Quality trend">No data.</canvas>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div><!-- /page-transformation -->

  <!-- STORAGE PAGE -->
  <div id="page-storage" class="page-content" style="display:none;">
    <div class="content">
      <div class="kpi-strip ai d1">
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#3d8bff,#5ba8ff)"></div>
          <div class="kpi-lbl">Total Files</div><div class="kpi-val" id="stor-files">—</div>
          <div class="kpi-sub"><span class="badge b-nt">all tables</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#9b7fff,#c4aaff)"></div>
          <div class="kpi-lbl">Total Storage</div><div class="kpi-val" id="stor-size">—</div>
          <div class="kpi-sub"><span class="badge b-up">MB</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#f5a623,#ffd066)"></div>
          <div class="kpi-lbl">Avg File Size</div><div class="kpi-val" id="stor-avg">—</div>
          <div class="kpi-sub"><span class="badge b-nt">MB</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#f04b4b,#ff7070)"></div>
          <div class="kpi-lbl">Small Files %</div><div class="kpi-val" id="stor-small">—</div>
          <div class="kpi-sub"><span class="badge b-dn">needs compaction</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#3ecf8e,#69f0ae)"></div>
          <div class="kpi-lbl">Tables OK</div><div class="kpi-val" id="stor-healthy">—</div>
          <div class="kpi-sub"><span class="badge b-up">count</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#f04b4b,#ff7070)"></div>
          <div class="kpi-lbl">Needing Compact</div><div class="kpi-val" id="stor-compact">—</div>
          <div class="kpi-sub"><span class="badge b-dn">high priority</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#00c9a7,#4af0d5)"></div>
          <div class="kpi-lbl">Total Snapshots</div><div class="kpi-val" id="stor-snapshots">—</div>
          <div class="kpi-sub"><span class="badge b-up">versions</span></div></div>
        <div class="kpi"><div class="kpi-bar" style="background:linear-gradient(90deg,#ff7056,#ff9a8b)"></div>
          <div class="kpi-lbl">Storage Health</div><div class="kpi-val" id="stor-health">—</div>
          <div class="kpi-sub"><span class="badge b-up" id="stor-health-badge">Good</span></div></div>
      </div>
      
      <div class="row r-full ai d2">
        <div class="panel">
          <div class="ph"><div class="ph-title">Iceberg Tables Health</div><div class="ph-meta">file metrics & compaction status</div></div>
          <div class="pb-np">
            <div class="scroll-body" style="max-height:400px;padding:12px">
              <table class="jt" style="font-size:10px">
                <thead><tr>
                  <th>Table</th><th>Files</th><th>Avg Size</th><th>Small %</th><th>Snapshots</th><th>Status</th>
                </tr></thead>
                <tbody id="stor-table"><tr><td colspan="6" class="empty">Loading…</td></tr></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
      
      <div class="row r-5050 ai d3">
        <div class="panel">
          <div class="ph"><div class="ph-title">Storage Warnings</div><div class="ph-meta">health alerts</div></div>
          <div class="pb"><div class="alert-list" id="stor-warnings"><div class="empty"><div class="ei">✓</div>All tables healthy</div></div></div>
        </div>
        <div class="panel">
          <div class="ph"><div class="ph-title">Recommendations</div><div class="ph-meta">optimization suggestions</div></div>
          <div class="pb"><div class="alert-list" id="stor-recs"><div class="empty"><div class="ei">✓</div>No actions needed</div></div></div>
        </div>
      </div>
    </div>
  </div><!-- /page-storage -->


</div><!-- /main -->
</div><!-- /shell -->

<script>
/* ══ CONFIG ══════════════════════════════════════════════════════════════ */
const API  = window.location.origin || 'http://localhost:8000';
const TOK  = '__API_TOKEN__';      /* injected by FastAPI at request time */
const HDR  = { 'Authorization': `Bearer ${TOK}` };
const CD   = { color:'#9ba3b8', grid:'rgba(255,255,255,0.05)' };

/* ══ STATE ═══════════════════════════════════════════════════════════════ */
let allJobs=[], allTel=[], allSrc=[];
let F = { status:'all', source:'all', domain:'all', cls:'all', freq:'all', thr:0, time:'1h' };

/* ══ CHARTS ══════════════════════════════════════════════════════════════ */
let cDonut, cThr, cDur, cTxr;

function initCharts() {
  cDonut = new Chart(document.getElementById('c-donut'), {
    type:'doughnut',
    data:{ labels:['Ingested','Quarantined','Failed'],
      datasets:[{ data:[0,0,0], backgroundColor:['#3ecf8e','#f5a623','#f04b4b'], borderWidth:0, hoverOffset:6 }] },
    options:{ responsive:true, maintainAspectRatio:false, cutout:'70%',
      plugins:{ legend:{display:false} }, animation:{duration:500} }
  });

  cThr = new Chart(document.getElementById('c-thr'), {
    type:'line',
    data:{ labels:[],
      datasets:[
        { label:'Throughput (r/s)', data:[], borderColor:'#3d8bff', backgroundColor:'rgba(61,139,255,.08)',
          fill:true, tension:.4, pointRadius:3, pointBackgroundColor:'#3d8bff', borderWidth:1.5, yAxisID:'y' },
        { label:'Quarantine %', data:[], borderColor:'#9b7fff', backgroundColor:'transparent',
          fill:false, tension:.4, pointRadius:2, borderWidth:1.5, borderDash:[4,3], yAxisID:'y1' }
      ] },
    options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
      scales:{
        x:{ ticks:{font:{size:9,family:'DM Mono'},color:CD.color}, grid:{color:CD.grid} },
        y:{ ticks:{font:{size:9},color:CD.color}, grid:{color:CD.grid}, beginAtZero:true, position:'left' },
        y1:{ ticks:{font:{size:9},color:'#9b7fff',callback:v=>v+'%'}, grid:{display:false},
          beginAtZero:true, position:'right', max:100 }
      }, animation:{duration:400} }
  });

  cDur = new Chart(document.getElementById('c-dur'), {
    type:'bar',
    data:{ labels:[],
      datasets:[{ label:'Duration (s)', data:[], backgroundColor:'rgba(0,201,167,.25)',
        borderColor:'#00c9a7', borderWidth:1, borderRadius:3 }] },
    options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
      scales:{
        x:{ ticks:{font:{size:9},color:CD.color,maxRotation:30}, grid:{display:false} },
        y:{ ticks:{font:{size:9},color:CD.color}, grid:{color:CD.grid}, beginAtZero:true }
      }, animation:{duration:400} }
  });
}
function initTxrChart() {
  const ctxTxr = document.getElementById('c-txr-quality');
  if (!ctxTxr || cTxr) return;
  cTxr = new Chart(ctxTxr, {
    type:'line',
    data:{ labels:[], datasets:[{
      label:'Quality %', data:[], borderColor:'#3ecf8e', backgroundColor:'rgba(62,207,142,.12)',
      fill:true, tension:.35, pointRadius:3, pointBackgroundColor:'#3ecf8e', borderWidth:1.5
    }]},
    options:{ responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
      scales:{
        x:{ ticks:{font:{size:9,family:'DM Mono'},color:CD.color}, grid:{color:CD.grid} },
        y:{ min:0, max:100, ticks:{font:{size:9},color:CD.color, callback:v=>v+'%'}, grid:{color:CD.grid} }
      }, animation:{duration:400} }
  });
}

/* ══ MOCK DATA ═══════════════════════════════════════════════════════════ */
// function genMock() {
//   const SRCS = [
//     { source_id:'src_sales_history',      name:'Sales History',      source_type:'file',  ingestion_frequency:'daily',    domain:'sales',      classification_level:'internal',      schema_version:'v2' },
//     { source_id:'src_warehouse_master',   name:'Warehouse Master',   source_type:'batch',     ingestion_frequency:'daily',   domain:'inventory',  classification_level:'public',  schema_version:'v1' },
//     { source_id:'src_iot_rfid_stream',    name:'IoT RFID Stream',    source_type:'streaming', ingestion_frequency:'real_time',domain:'logistics',  classification_level:'internal',      schema_version:'v3' },
//     { source_id:'src_weather_api',        name:'Weather API',        source_type:'api',       ingestion_frequency:'every_2_minutes',   domain:'external',   classification_level:'public',        schema_version:'v1' },
//     { source_id:'src_manufacturing_logs', name:'Manufacturing Logs', source_type:'file',      ingestion_frequency:'daily',    domain:'production', classification_level:'restricted',    schema_version:'v2' },
//     { source_id:'src_legacy_trends',      name:'Legacy Trends',      source_type:'batch',     ingestion_frequency:'weekly',   domain:'analytics',  classification_level:'internal',      schema_version:'v1' },
//     { source_id:'src_inventory_transactions',      name:'Inventory Transactions DB',      source_type:'db',     ingestion_frequency:'every_2_minutes',   domain:'analytics',  classification_level:'internal',      schema_version:'v1' },
//   ];
//   const stats=['completed','completed','completed','running','failed'];
//   const tel = SRCS.flatMap((s,si)=>Array.from({length:4},(_,i)=>{
//     const ing=Math.round(100+Math.random()*300), q=Math.round(Math.random()*20), f=Math.round(Math.random()*8);
//     return { job_id:`job-${si}-${i}`, source_id:s.source_id, domain:s.domain,
//       records_ingested:ing, records_quarantined:q, records_failed:f,
//       throughput_rec_sec:parseFloat((5+Math.random()*60).toFixed(2)),
//       duration_seconds:parseFloat((0.3+Math.random()*4).toFixed(2)),
//       status:stats[(si+i)%5], classification_level:s.classification_level,
//       frequency:s.ingestion_frequency, ts:Date.now()-(si*4+i)*3600000 };
//   }));
//   const jobs=tel.map(t=>({ job_id:t.job_id, source_id:t.source_id,
//     dataset_id:t.source_id.replace('src_','ds_'), status:t.status, telemetry:t, domain:t.domain }));
//   return {
//     metrics:{ total_jobs:jobs.length, running_jobs:jobs.filter(j=>j.status==='running').length,
//       completed_jobs:jobs.filter(j=>j.status==='completed').length },
//     telemetry:{ telemetry_records:tel, count:tel.length },
//     sources:{ sources:SRCS },
//     dashboard:{ jobs, storage_summary:{ ingested:12, quarantine:3, cdc_log:5, micro_batch:8,
//       stream_buffer:6, checkpoints:4, details:9 } }
//   };
// }

/* ══ API FETCH ═══════════════════════════════════════════════════════════ */
async function fetchAll() {
  try {
    const timeMap = { "1h":"1h ago", "6h":"6h ago", "24h":"24h ago", "7d":"7d ago" };
    const selectedTime = document.getElementById('f-time')?.value || '1h';
    const fromTs = timeMap[selectedTime];
    const metricsUrl = fromTs ? `${API}/metrics/filtered?from_timestamp=${encodeURIComponent(fromTs)}` : `${API}/metrics`;
    const [m,t,s,d] = await Promise.all([
      fetch(metricsUrl,                   {headers:HDR}).then(r=>r.json()),
      fetch(API+'/telemetry',             {headers:HDR}).then(r=>r.json()),
      fetch(API+'/source-configurations', {headers:HDR}).then(r=>r.json()),
      fetch(API+'/dashboard/json',        {headers:HDR}).then(r=>r.json()),
    ]);
    return { metrics:m, telemetry:t, sources:s, dashboard:d };
  } catch(e) {
    console.warn('API unreachable – using demo data');
    // return genMock();
  }
}

/* ══ FILTERS ═════════════════════════════════════════════════════════════ */
function _timeThreshold() {
  if (F.time==='all') return null;
  const now=Date.now();
  const map={'1h':3600000,'6h':21600000,'24h':86400000,'7d':604800000};
  return now-(map[F.time]||0);
}
function filterJobs(jobs) {
  const cutoff=_timeThreshold();
  return jobs.filter(j=>{
    const tel=j.telemetry||{};
    if (F.status!=='all' && j.status!==F.status) return false;
    if (F.source!=='all' && j.source_id!==F.source) return false;
    if (F.domain!=='all' && j.domain!==F.domain) return false;
    if (F.cls!=='all') { const s=allSrc.find(x=>x.source_id===j.source_id); if(!s||s.classification_level!==F.cls) return false; }
    if (F.freq!=='all') { const s=allSrc.find(x=>x.source_id===j.source_id); if(!s||s.ingestion_frequency!==F.freq) return false; }
    if (F.thr>0 && (tel.throughput_rec_sec||0)<F.thr) return false;
    if (cutoff) { const t=tel.start_time?new Date(tel.start_time).getTime():null; if(!t||t<cutoff) return false; }
    return true;
  });
}
function filterTel(records) {
  const cutoff=_timeThreshold();
  return records.filter(r=>{
    if (F.source!=='all' && r.source_id!==F.source) return false;
    if (F.domain!=='all' && r.domain!==F.domain) return false;
    if (F.cls!=='all') { const s=allSrc.find(x=>x.source_id===r.source_id); if(!s||s.classification_level!==F.cls) return false; }
    if (F.freq!=='all') { const s=allSrc.find(x=>x.source_id===r.source_id); if(!s||s.ingestion_frequency!==F.freq) return false; }
    if (F.thr>0 && (r.throughput_rec_sec||0)<F.thr) return false;
    if (cutoff) { const t=r.start_time?new Date(r.start_time).getTime():null; if(!t||t<cutoff) return false; }
    return true;
  });
}

/* ══ RENDER ══════════════════════════════════════════════════════════════ */
const fmt=n=>{if(n==null)return'—';if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=12000)return(n/12000).toFixed(1)+'k';return Math.round(n)};

function renderKPIs(jobs, tel) {
  const run=jobs.filter(j=>j.status==='running').length;
  const ok=jobs.filter(j=>j.status==='completed').length;
  const fail=jobs.filter(j=>j.status==='failed').length;
  const total=jobs.length;
  const ing=tel.reduce((a,r)=>a+(r.records_ingested||0),0);
  const q=tel.reduce((a,r)=>a+(r.records_quarantined||0),0);
  const rf=tel.reduce((a,r)=>a+(r.records_failed||0),0);
  const all=ing+q+rf||1;
  const avgThr=tel.length?tel.reduce((a,r)=>a+(r.throughput_rec_sec||0),0)/tel.length:0;

  document.getElementById('kv-total').textContent=total;
  document.getElementById('kv-run').textContent=run;
  document.getElementById('kv-run-s').textContent=total?Math.round(run/total*100)+'% of total':'0%';
  document.getElementById('kv-ok').textContent=ok;
  document.getElementById('kv-ok-s').textContent=total?Math.round(ok/total*100)+'% success':'—';
  document.getElementById('kv-fail').textContent=fail;
  document.getElementById('kv-fail-s').textContent=total?Math.round(fail/total*100)+'% fail rate':'—';
  document.getElementById('kv-ing').textContent=fmt(ing);
  document.getElementById('kv-q').textContent=fmt(q);
  document.getElementById('kv-q-s').textContent=Math.round(q/all*100)+'% rate';
  document.getElementById('kv-rf').textContent=fmt(rf);
  document.getElementById('kv-thr').textContent=avgThr.toFixed(1);

  document.getElementById('cnt-all').textContent=total;
  document.getElementById('cnt-run').textContent=run;
  document.getElementById('cnt-ok').textContent=ok;
  document.getElementById('cnt-fail').textContent=fail;
}

function renderDonut(tel) {
  const ing=tel.reduce((a,r)=>a+(r.records_ingested||0),0);
  const q=tel.reduce((a,r)=>a+(r.records_quarantined||0),0);
  const f=tel.reduce((a,r)=>a+(r.records_failed||0),0);
  const total=ing+q+f||1;
  cDonut.data.datasets[0].data=[ing,q,f]; cDonut.update();
  document.getElementById('donut-leg').innerHTML=[
    ['#3ecf8e','Ingested',ing],['#f5a623','Quarantined',q],['#f04b4b','Failed',f]
  ].map(([c,l,v])=>`<div class="leg-item"><div class="leg-sw" style="background:${c}"></div>${l}: ${fmt(v)} (${Math.round(v/total*100)}%)</div>`).join('');
}

function renderThrChart(tel) {
  const sl=tel.slice(-14);
  cThr.data.labels=sl.map((_,i)=>'J'+(i+1));
  cThr.data.datasets[0].data=sl.map(r=>parseFloat((r.throughput_rec_sec||0).toFixed(2)));
  const tots=sl.map(r=>(r.records_ingested||0)+(r.records_quarantined||0)+(r.records_failed||0)||1);
  cThr.data.datasets[1].data=sl.map((r,i)=>Math.round((r.records_quarantined||0)/tots[i]*100));
  cThr.update();
  document.getElementById('thr-meta').textContent=`rec/sec · last ${sl.length} jobs`;
}

function renderDurChart(tel) {
  const sl=tel.slice(-12);
  cDur.data.labels=sl.map((_,i)=>'J'+(i+1));
  cDur.data.datasets[0].data=sl.map(r=>parseFloat((r.duration_seconds||0).toFixed(2)));
  cDur.update();
}

function renderGauge(tel) {
  const vals=tel.map(r=>r.throughput_rec_sec||0).filter(v=>v>0).sort((a,b)=>a-b);
  if(!vals.length) return;
  const avg=vals.reduce((a,v)=>a+v,0)/vals.length;
  const p95=vals[Math.floor(vals.length*.95)]||vals[vals.length-1];
  const avgDur=tel.filter(r=>r.duration_seconds).reduce((a,r,_,arr)=>a+r.duration_seconds/arr.length,0);
  document.getElementById('g-num').textContent=avg.toFixed(1);
  document.getElementById('g-peak').textContent=vals[vals.length-1].toFixed(1)+' r/s';
  document.getElementById('g-min').textContent=vals[0].toFixed(1)+' r/s';
  document.getElementById('g-p95').textContent=p95.toFixed(1)+' r/s';
  document.getElementById('g-jobs').textContent=vals.length;
  document.getElementById('g-dur').textContent=avgDur.toFixed(2)+'s';
  const arc=document.getElementById('garc');
  const pct=Math.min(avg/100,1);
  arc.style.strokeDashoffset=314-pct*314;
  arc.style.stroke=avg<20?'#f04b4b':avg<50?'#f5a623':'#3ecf8e';
}

function renderSLA(tel) {
  const by={};
  tel.forEach(r=>{ if(!by[r.source_id]) by[r.source_id]={ing:0,q:0,f:0};
    by[r.source_id].ing+=r.records_ingested||0; by[r.source_id].q+=r.records_quarantined||0; by[r.source_id].f+=r.records_failed||0; });
  const p=document.getElementById('sla-panel');
  if(!Object.keys(by).length){p.innerHTML='<div class="empty"><div class="ei">◎</div>No telemetry for filter</div>';return;}
  p.innerHTML=Object.entries(by).map(([sid,d])=>{
    const tot=d.ing+d.q+d.f||1, rate=Math.round(d.ing/tot*100);
    const col=rate>=95?'#3ecf8e':rate>=80?'#f5a623':'#f04b4b';
    const bc=rate>=95?'b-up':rate>=80?'b-nt':'b-dn';
    const src=allSrc.find(s=>s.source_id===sid);
    return `<div class="sla-row">
      <div class="sla-name" title="${src?.name||sid}">${src?.name||sid}</div>
      <div class="sla-track"><div class="sla-fill" style="width:${rate}%;background:${col}"></div></div>
      <div class="sla-pct">${rate}%</div>
      <div class="sla-badge badge ${bc}">${rate>=95?'OK':rate>=80?'WARN':'CRIT'}</div>
    </div>`;
  }).join('');
}

function renderSources(sources) {
  const ICONS={batch:'▤',streaming:'⟳',api:'⬡',file:'⊟',database:'⊗',iot:'◈'};
  const COLS=['#3d8bff','#00c9a7','#f5a623','#9b7fff','#ff7056','#3ecf8e'];
  const CLS={public:'cls-pub',internal:'cls-int',confidential:'cls-conf',restricted:'cls-res'};
  const FLB={real_time:'Real-time',hourly:'Hourly',daily:'Daily',weekly:'Weekly',on_demand:'On demand'};
  const ACT={real_time:100,hourly:80,daily:60,weekly:40,on_demand:20};
  const filtered=sources.filter(s=>{
    if(F.domain!=='all' && s.domain!==F.domain) return false;
    if(F.cls!=='all' && s.classification_level!==F.cls) return false;
    if(F.freq!=='all' && s.ingestion_frequency!==F.freq) return false;
    if(F.source!=='all' && s.source_id!==F.source) return false;
    return true;
  });
  const rows=document.getElementById('src-rows');
  if(!filtered.length){rows.innerHTML='<div class="empty"><div class="ei">◎</div>No sources match filter</div>';return;}
  rows.innerHTML=filtered.map((s,i)=>{
    const col=COLS[i%COLS.length], act=ACT[s.ingestion_frequency]||20, cl=s.classification_level||'internal';
    const tel=allTel.filter(t=>t.source_id===s.source_id);
    const ing=tel.reduce((a,r)=>a+(r.records_ingested||0),0);
    const tot=tel.reduce((a,r)=>a+(r.records_ingested||0)+(r.records_quarantined||0)+(r.records_failed||0),0)||1;
    const q=Math.round(ing/tot*100);
    return `<div class="src-row">
      <div class="src-icon" style="background:${col}22;color:${col}">${ICONS[s.source_type]||'◇'}</div>
      <div class="src-info"><div class="src-name">${s.name}</div><div class="src-meta">${s.domain} · ${s.schema_version||'v1'}</div></div>
      <div class="src-bars">
        <div class="src-mini-bar"><div class="src-mini-fill" style="width:${act}%;background:${col}"></div></div>
        <div class="src-mini-bar"><div class="src-mini-fill" style="width:${q}%;background:#3ecf8e"></div></div>
      </div>
      <div class="src-pct">${q}%</div>
      <span class="freq-pill">${FLB[s.ingestion_frequency]||s.ingestion_frequency}</span>
      <span class="cls-pill ${CLS[cl]||'cls-int'}">${cl.charAt(0).toUpperCase()+cl.slice(1,4)}</span>
    </div>`;
  }).join('');
}

function renderStorage(d) {
  const data=[
    {ico:'◼',lbl:'Ingested',    cnt:d?.ingested||0},
    {ico:'◈',lbl:'Quarantine',  cnt:d?.quarantine||0},
    {ico:'◎',lbl:'CDC Log',     cnt:d?.cdc_log||0},
    {ico:'◉',lbl:'Micro-Batch', cnt:d?.micro_batch||0},
    {ico:'◐',lbl:'Stream Buf',  cnt:d?.stream_buffer||0},
    {ico:'◇',lbl:'Checkpoints', cnt:d?.checkpoints||0},
    {ico:'▦',lbl:'Detail Logs', cnt:d?.details||0},
  ];
  const COLS=['#3ecf8e','#f5a623','#3d8bff','#9b7fff','#00c9a7','#ff7056','#f04b4b'];
  document.getElementById('stor-grid').innerHTML=data.map((s,i)=>
    `<div class="stor-cell"><div class="stor-ico" style="color:${COLS[i]}">${s.ico}</div>
    <div class="stor-cnt">${s.cnt}</div><div class="stor-lbl">${s.lbl}</div></div>`
  ).join('');
}

function renderAlerts(jobs, tel, srcs) {
  const q=tel.reduce((a,r)=>a+(r.records_quarantined||0),0);
  const ing=tel.reduce((a,r)=>a+(r.records_ingested||0),0);
  const f=tel.reduce((a,r)=>a+(r.records_failed||0),0);
  const all=q+ing+f||1; const qr=Math.round(q/all*100);
  const alerts=[];
  if(qr>15) alerts.push({t:'crit',txt:`Quarantine rate critical: ${qr}%`,sub:'Exceeds 15% threshold — check contract enforcement',time:'now'});
  else if(qr>5) alerts.push({t:'warn',txt:`Quarantine rate elevated: ${qr}%`,sub:'Threshold: 5% warning · 15% critical',time:'now'});
  if(f>50) alerts.push({t:'crit',txt:`${fmt(f)} records failed validation`,sub:'Schema mismatch or type coercion failures',time:'now'});
  const fj=jobs.filter(j=>j.status==='failed');
  if(fj.length) alerts.push({t:'warn',txt:`${fj.length} job(s) in failed state`,sub:fj.map(j=>j.job_id.slice(0,8)).join(', '),time:'active'});
  const rj=jobs.filter(j=>j.status==='running');
  if(rj.length>3) alerts.push({t:'warn',txt:`${rj.length} concurrent jobs — approaching rate limit (10/min)`,sub:'Slowapi limiter active on /ingest/{source_id}',time:'now'});
  const rt=(srcs||[]).filter(s=>s.ingestion_frequency==='real_time');
  if(rt.length) alerts.push({t:'info',txt:`${rt.length} real-time source(s) streaming continuously`,sub:rt.map(s=>s.name).join(', '),time:'streaming'});
  alerts.push({t:'info',txt:'Contract registry loaded — schemas enforced on ingest',sub:'Using coerce + quarantine mode',time:'startup'});
  if(F.status!=='all'||F.source!=='all'||F.domain!=='all')
    alerts.push({t:'info',txt:'Dashboard view is filtered — metrics reflect a subset',sub:'Reset filters to see full pipeline state',time:'filter'});
  const list=document.getElementById('alert-list');
  list.innerHTML=alerts.map(a=>`<div class="alert-item a-${a.t}"><div class="a-dot"></div>
    <div><div class="a-text">${a.txt}</div><div class="a-sub">${a.sub}</div></div>
    <div class="a-time">${a.time}</div></div>`).join('');
  document.getElementById('a-cnt').textContent=`${alerts.length} alert(s)`;
}

function renderJobs(jobs) {
  document.getElementById('j-cnt').textContent=`${jobs.length} job(s) shown`;
  const tb=document.getElementById('j-body');
  if(!jobs.length){tb.innerHTML='<tr><td colspan="9" class="empty">No jobs match current filters</td></tr>';return;}
  tb.innerHTML=jobs.map(j=>{
    const tel=j.telemetry||{};
    const sc={running:'s-run',completed:'s-ok',failed:'s-fail'}[j.status]||'s-run';
    const q=tel.records_quarantined||0, f=tel.records_failed||0, ing=tel.records_ingested||0;
    return `<tr>
      <td class="jid">${j.job_id.slice(0,8)}…</td>
      <td style="color:var(--t1)">${j.source_id.replace('src_','').replace(/_/g,' ')}</td>
      <td>${j.dataset_id.replace('ds_','').replace(/_/g,' ')}</td>
      <td><span class="sp ${sc}">${j.status}</span></td>
      <td class="ng">${fmt(ing)}</td>
      <td class="${f>10?'nb':f>0?'nw':''}">${fmt(f)}</td>
      <td class="${q>20?'nw':''}">${fmt(q)}</td>
      <td>${tel.throughput_rec_sec?tel.throughput_rec_sec.toFixed(1)+' r/s':'—'}</td>
      <td>${tel.duration_seconds?tel.duration_seconds.toFixed(2)+'s':'—'}</td>
    </tr>`;
  }).join('');
}

function renderFSummary() {
  const has=Object.entries(F).some(([k,v])=>v!=='all'&&v!==0);
  const el=document.getElementById('fsummary');
  if(!has){el.classList.add('hidden');return;}
  el.classList.remove('hidden');
  const fj=filterJobs(allJobs);
  document.getElementById('f-cnt').textContent=fj.length;
  document.getElementById('t-cnt').textContent=allJobs.length;
  const parts=[];
  if(F.status!=='all') parts.push(`status: ${F.status}`);
  if(F.source!=='all') parts.push(`source: ${F.source.replace('src_','').replace(/_/g,' ')}`);
  if(F.domain!=='all') parts.push(`domain: ${F.domain}`);
  if(F.cls!=='all') parts.push(`class: ${F.cls}`);
  if(F.freq!=='all') parts.push(`freq: ${F.freq}`);
  if(F.thr>0) parts.push(`throughput ≥ ${F.thr} r/s`);
  document.getElementById('f-desc').textContent=parts.join(' · ');
}

function renderPills() {
  const pills=[];
  if(F.status!=='all') pills.push(['status',F.status]);
  if(F.source!=='all') pills.push(['source',F.source.replace('src_','').replace(/_/g,' ')]);
  if(F.domain!=='all') pills.push(['domain',F.domain]);
  if(F.cls!=='all') pills.push(['cls',F.cls]);
  if(F.freq!=='all') pills.push(['freq',F.freq]);
  if(F.thr>0) pills.push(['thr','≥'+F.thr+' r/s']);
  document.getElementById('active-pills').innerHTML=pills
    .map(([k,v])=>`<div class="pill" onclick="clearF('${k}')"><span>${v}</span><span class="x">✕</span></div>`).join('');
}

/* ══ FILTER ACTIONS ══════════════════════════════════════════════════════ */
function toggleChip(type, val, el) {
  document.querySelectorAll(`[data-filter="${type}"]`).forEach(c=>c.classList.remove('active'));
  el.classList.add('active'); F[type]=val; applyFilters();
}

function applyFilters() {
  F.source=document.getElementById('f-source').value;
  F.cls=document.getElementById('f-cls').value;
  F.freq=document.getElementById('f-freq').value;
  F.thr=parseInt(document.getElementById('f-thr').value)||0;
  F.time=document.getElementById('f-time').value;
  // F.status and F.domain are managed by toggleChip/clearF/resetFilters — do not overwrite here
  const fj=filterJobs(allJobs), ft=filterTel(allTel);
  renderKPIs(fj,ft); renderDonut(ft); renderThrChart(ft); renderDurChart(ft);
  renderGauge(ft); renderSLA(ft); renderSources(allSrc);
  renderAlerts(fj,ft,allSrc); renderJobs(fj); renderFSummary(); renderPills();
}

function clearF(key) {
  F[key]=key==='thr'?0:'all';
  if(key==='source') document.getElementById('f-source').value='all';
  if(key==='cls')    document.getElementById('f-cls').value='all';
  if(key==='freq')   document.getElementById('f-freq').value='all';
  if(key==='thr')    { document.getElementById('f-thr').value=0; document.getElementById('thr-val').textContent='0 r/s'; }
  if(key==='status'||key==='domain') {
    document.querySelectorAll(`[data-filter="${key}"]`).forEach(c=>c.classList.remove('active'));
    document.querySelector(`[data-filter="${key}"][data-val="all"]`).classList.add('active');
  }
  applyFilters();
}

function resetFilters() {
  F={ status:'all', source:'all', domain:'all', cls:'all', freq:'all', thr:0, time:'1h' };
  ['f-source','f-cls','f-freq'].forEach(id=>{ document.getElementById(id).value='all'; });
  document.getElementById('f-time').value='1h';
  document.getElementById('f-thr').value=0;
  document.getElementById('thr-val').textContent='0 r/s';
  document.querySelectorAll('.chip').forEach(c=>c.classList.remove('active'));
  document.querySelectorAll('[data-val="all"]').forEach(c=>c.classList.add('active'));
  applyFilters();
}

/* ══ PAGE NAVIGATION ═════════════════════════════════════════════════════ */
function switchPage(page) {
  document.querySelectorAll('.page-content').forEach(p=>p.style.display='none');
  const pg=document.getElementById('page-'+page);
  if(pg) pg.style.display='block';
  document.querySelectorAll('.page-tab').forEach(t=>{
    t.classList.remove('active');
    t.style.borderBottom='2px solid transparent';
    t.style.color='var(--t2)';
  });
  const tab=document.querySelector('.page-tab[data-page="'+page+'"]');
  if(tab){
    tab.classList.add('active');
    tab.style.borderBottomColor='var(--accent)';
    tab.style.color='var(--accent)';
  }
  if(page==='transformation') {
    if(!cTxr) initTxrChart();
    loadTransformationData();
    setTimeout(()=>{ if(cTxr) cTxr.resize(); }, 60);
  }
  if(page==='storage') loadStorageData();
}

function loadTransformationData() {
  const qPct=(rr,rc)=>{ const r=Number(rr)||0, c=Number(rc)||0; return r>0 ? Math.min(100, Math.round(100*c/r)) : 0; };

  fetch(API+'/transformation/summary', {headers:HDR})
    .then(r=>{ if(!r.ok) throw new Error('summary '+r.status); return r.json(); })
    .then(d=>{
      const silver=d.silver||{}, gold=d.gold||{};
      document.getElementById('txr-total').textContent=String((silver.run_count||0)+(gold.run_count||0));
      document.getElementById('txr-latency').textContent=(((Number(silver.avg_transformation_latency_sec)||0)+(Number(gold.avg_transformation_latency_sec)||0))/2).toFixed(2);
      document.getElementById('txr-quality').textContent=String(Math.round(((Number(silver.overall_quality_ratio)||0)+(Number(gold.overall_quality_ratio)||0))/2));
      document.getElementById('txr-dupes').textContent=String((silver.total_duplicates_removed||0)+(gold.total_duplicates_removed||0));
      document.getElementById('txr-late').textContent=String(silver.total_late_arrivals||0);
      document.getElementById('txr-silver').textContent=String(silver.total_records_cleaned||0);
      document.getElementById('txr-gold').textContent=String((gold.total_records_cleaned!=null?gold.total_records_cleaned:gold.total_records_processed)||0);
      document.getElementById('txr-violations').textContent='0';
    }).catch(e=>console.error('Error loading transformation data:',e));
  
  fetch(API+'/transformation/kpis?limit=10', {headers:HDR})
    .then(r=>{ if(!r.ok) throw new Error('kpis '+r.status); return r.json(); })
    .then(d=>{
      const kpis=d.kpis||[];
      const tbody=document.getElementById('txr-table');
      if(!tbody) return;
      if(!kpis.length){tbody.innerHTML='<tr><td colspan="6" class="empty">No transformation data</td></tr>'; if(cTxr){ cTxr.data.labels=[]; cTxr.data.datasets[0].data=[]; cTxr.update(); } return;}
      const sid=x=>String(x||'').replace(/^src_/,'');
      tbody.innerHTML=kpis.map(k=>`<tr>
        <td>${sid(k.source_id)}</td>
        <td><span class="sp ${k.layer==='silver'?'s-run':'s-ok'}">${k.layer||'—'}</span></td>
        <td>${k.records_read!=null?k.records_read:'—'}</td>
        <td class="ng">${k.records_cleaned!=null?k.records_cleaned:'—'}</td>
        <td>${(Number(k.transformation_latency_sec)||0).toFixed(2)}</td>
        <td style="font-size:9px;color:var(--t3)">${(k.run_at||'').slice(0,10)}</td>
      </tr>`).join('');
      if(cTxr){
        const slice=kpis.slice().reverse().slice(-12);
        cTxr.data.labels=slice.map((k,i)=>'#'+(i+1));
        cTxr.data.datasets[0].data=slice.map(k=>qPct(k.records_read,k.records_cleaned));
        cTxr.update();
      }
    }).catch(e=>console.error('Error loading KPIs:',e));
}

function loadStorageData() {
  fetch(API+'/storage/summary', {headers:HDR})
    .then(r=>{ if(!r.ok) throw new Error('storage summary '+r.status); return r.json(); })
    .then(d=>{
      const kpis=d.kpis||{}, health=d.health||{};
      const needCompact=health.tables_needing_compaction||[];
      let totalFiles=0, totalMB=0, totalSnapshots=0, smallRatio=0, cnt=0;
      Object.values(kpis).forEach(k=>{
        if(k && !k.error){
          totalFiles+=k.file_count||0;
          totalMB+=Number(k.total_storage_mb)||0;
          totalSnapshots+=k.snapshot_count||0;
          smallRatio+=Number(k.small_file_ratio)||0;
          cnt++;
        }
      });
      const avgSmall=cnt>0?(smallRatio/cnt*100):0;
      const avgSzMb=cnt>0 && totalFiles>0 ? (totalMB/totalFiles) : 0;
      document.getElementById('stor-files').textContent=String(totalFiles);
      document.getElementById('stor-size').textContent=totalMB.toFixed(0);
      document.getElementById('stor-avg').textContent=avgSzMb.toFixed(1);
      document.getElementById('stor-small').textContent=avgSmall.toFixed(0);
      document.getElementById('stor-healthy').textContent=String(Math.max(0, cnt-needCompact.length));
      document.getElementById('stor-compact').textContent=String(needCompact.length);
      document.getElementById('stor-snapshots').textContent=String(totalSnapshots);
      const healthBadge=document.getElementById('stor-health-badge');
      const hstat=health.health_status||'unknown';
      healthBadge.textContent=hstat;
      healthBadge.style.background=(hstat==='warning')?'rgba(240,75,75,.1)':'rgba(62,207,142,.15)';
      document.getElementById('stor-health').textContent=(hstat==='warning')?'⚠️':'✓';
      
      const tbody=document.getElementById('stor-table');
      if(!tbody) return;
      const tables=Object.entries(kpis).filter(([_,k])=>k && !k.error).slice(0,15);
      if(!tables.length){tbody.innerHTML='<tr><td colspan="6" class="empty">No storage data</td></tr>';return;}
      tbody.innerHTML=tables.map(([name,k])=>{
        const avg=Number(k.avg_file_size_mb);
        const sr=Number(k.small_file_ratio)||0;
        const need=k.needs_compaction===true || sr>0.5;
        return `<tr>
        <td style="font-size:9px">${name}</td>
        <td>${k.file_count!=null?k.file_count:'—'}</td>
        <td>${isNaN(avg)?'0.0':avg.toFixed(1)}</td>
        <td class="${sr>0.5?'nb':''}">${(sr*100).toFixed(0)}</td>
        <td>${k.snapshot_count!=null?k.snapshot_count:'—'}</td>
        <td><span class="sp ${need?'s-run':'s-ok'}">${need?'compact':'ok'}</span></td>
      </tr>`;}).join('');
      
      const warns=document.getElementById('stor-warnings');
      warns.innerHTML=(health.warnings||[]).length>0
        ?(health.warnings.map(w=>`<div class="alert-item a-warn"><div class="a-dot"></div><div class="a-text">${String(w)}</div></div>`).join(''))
        :('<div class="empty"><div class="ei">✓</div>All tables healthy</div>');
      
      const recs=document.getElementById('stor-recs');
      recs.innerHTML=(health.recommendations||[]).length>0
        ?(health.recommendations.map(r=>`<div class="alert-item a-info"><div class="a-dot"></div><div class="a-text">${String(r)}</div></div>`).join(''))
        :('<div class="empty"><div class="ei">✓</div>No actions needed</div>');
    }).catch(e=>console.error('Error loading storage data:',e));
}

/* ══ MAIN REFRESH ════════════════════════════════════════════════════════ */
async function refresh() {
  const icon=document.getElementById('spin-icon'); icon.classList.add('spinning');
  try {
    const data=await fetchAll();
    allSrc=data.sources?.sources||[];
    allTel=(data.telemetry?.telemetry_records||[]).map(r=>({
      ...r, domain:allSrc.find(s=>s.source_id===r.source_id)?.domain||'unknown'
    }));
    allJobs=(data.dashboard?.jobs||[]).map(j=>({
      ...j, domain:allSrc.find(s=>s.source_id===j.source_id)?.domain||'unknown'
    }));
    renderStorage(data.dashboard?.storage_summary);
    // Keep secondary tabs warm even before the user clicks them.
    loadTransformationData();
    loadStorageData();
    applyFilters();
    document.getElementById('last-upd').textContent=new Date().toLocaleTimeString();
  } catch(e){ console.error(e); }
  icon.classList.remove('spinning');
}

/* ══ BOOT ════════════════════════════════════════════════════════════════ */
initCharts();
refresh();
</script>
</body>
</html>""".replace("__SOURCE_OPTIONS__", _source_options_html()).replace("__DOMAIN_CHIPS__", _domain_chips_html())
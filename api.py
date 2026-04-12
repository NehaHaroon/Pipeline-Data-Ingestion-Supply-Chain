from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status, Request
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import glob
import html
import logging
from datetime import datetime
import uuid
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import config
from control_plane.entities import ALL_SOURCES, ALL_DATASETS, IngestionJob, ExecutionMode
from control_plane.contracts import CONTRACT_REGISTRY
from observability_plane.telemetry import JobTelemetry

from fastapi.staticfiles import StaticFiles

# Initialize FastAPI app
app = FastAPI()

app.mount("/logs", StaticFiles(directory="logs"), name="logs")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Security
security = HTTPBearer()
API_TOKEN = config.API_TOKEN

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials

# In-memory storage for demo; in production, use database
jobs_db = {}
datasets_db = {}  # dataset_id -> list of records

class IngestRequest(BaseModel):
    records: List[Dict[str, Any]]

class JobStatus(BaseModel):
    job_id: str
    status: str
    telemetry: Optional[Dict[str, Any]] = None

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/metrics")
def metrics():
    """Simple metrics endpoint."""
    total_jobs = len(jobs_db)
    running_jobs = sum(1 for j in jobs_db.values() if j["status"] == "running")
    completed_jobs = total_jobs - running_jobs
    
    # Include historical jobs from telemetry (all are completed)
    persisted_telemetry = JobTelemetry.load_reports()
    historic_completed_jobs = len(persisted_telemetry)
    
    return {
        "total_jobs": total_jobs + historic_completed_jobs,
        "running_jobs": running_jobs,
        "completed_jobs": completed_jobs + historic_completed_jobs
    }

@app.get("/sources")
def list_sources():
    return {"sources": [src.__dict__ for src in ALL_SOURCES]}

@app.get("/datasets")
def list_datasets():
    return {"datasets": [ds.__dict__ for ds in ALL_DATASETS]}

@app.post("/ingest/{source_id}")
@limiter.limit("10/minute")
def ingest_data(request: Request, source_id: str, request_data: IngestRequest, background_tasks: BackgroundTasks, token: str = Depends(verify_token)):
    if source_id not in CONTRACT_REGISTRY:
        raise HTTPException(status_code=404, detail="Source not found")

    contract = CONTRACT_REGISTRY[source_id]
    dataset_id = f"ds_{source_id.replace('src_', '')}"

    # Create a job
    job_id = str(uuid.uuid4())
    job = IngestionJob(
        job_id=job_id,
        source_id=source_id,
        dataset_id=dataset_id,
        execution_mode=ExecutionMode.BATCH
    )
    jobs_db[job_id] = {"job": job, "status": "running", "telemetry": None}

    # Run ingestion in background
    background_tasks.add_task(process_ingestion, job_id, source_id, request_data.records)

    return {"job_id": job_id, "message": "Ingestion started"}

def process_ingestion(job_id: str, source_id: str, records: List[Dict[str, Any]]):
    try:
        # Simulate ingestion
        telemetry = JobTelemetry(job_id=job_id, source_id=source_id)
        telemetry.mark_start()

        for record in records:
            # Enforce contract
            result = CONTRACT_REGISTRY[source_id].enforce(record)
            if result["status"] in ["ok", "coerced"]:
                if source_id not in datasets_db:
                    datasets_db[source_id] = []
                datasets_db[source_id].append(result["record"])
                telemetry.records_ingested += 1
            elif result["status"] == "quarantine":
                telemetry.records_quarantined += 1
            else:
                telemetry.records_failed += 1

        telemetry.mark_end()
        telemetry.save_report()
        jobs_db[job_id]["status"] = "completed"
        jobs_db[job_id]["telemetry"] = telemetry.to_dict()

    except Exception as e:
        log.error(f"Ingestion failed for job {job_id}: {e}")
        jobs_db[job_id]["status"] = "failed"

@app.get("/jobs")
def list_jobs(token: str = Depends(verify_token)):
    """List all jobs with their status and telemetry."""
    return {"jobs": jobs_db}

@app.get("/dashboard-plots")
def get_dashboard_plots(token: str = Depends(verify_token)):
    """Get list of available plot files."""
    plots = _generate_visualizations()
    return {"plots": plots}

@app.get("/storage-summary")
def get_storage_summary(token: str = Depends(verify_token)):
    """Get storage summary HTML."""
    return HTMLResponse(content=_build_storage_summary())

@app.get("/dataset-samples")
def get_dataset_samples(token: str = Depends(verify_token)):
    """Get dataset samples HTML."""
    return HTMLResponse(content=_build_dataset_section())

@app.get("/datasets/{dataset_id}")
def query_dataset(dataset_id: str, limit: int = 100, token: str = Depends(verify_token)):
    source_id = f"src_{dataset_id.replace('ds_', '')}"
    if source_id not in datasets_db:
        return {"records": []}
    records = datasets_db[source_id][:limit]
    return {"records": records}

@app.get("/telemetry")
def get_telemetry(token: str = Depends(verify_token)):
    """Return persisted telemetry job records for ingestion visibility."""
    telemetry_records = JobTelemetry.load_reports()
    return {"telemetry_records": telemetry_records, "count": len(telemetry_records)}

def _generate_visualizations():
    """Generate plots for each dataset if data exists."""
    plots = []
    for dataset in ALL_DATASETS:
        dataset_id = dataset.dataset_id
        source_id = f"src_{dataset_id.replace('ds_', '')}"
        parquet_files = glob.glob(f"storage/ingested/{source_id}*.parquet")
        if not parquet_files:
            continue
        try:
            df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
            if df.empty:
                continue
            plt.figure(figsize=(8, 4))
            if dataset_id == "ds_sales_history" and "sale_timestamp" in df.columns and "units_sold" in df.columns:
                df["sale_timestamp"] = pd.to_datetime(df["sale_timestamp"])
                df = df.sort_values("sale_timestamp")
                plt.plot(df["sale_timestamp"], df["units_sold"])
                plt.title(f"{dataset.name} - Sales Over Time")
                plt.xlabel("Sale Timestamp")
                plt.ylabel("Units Sold")
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                plt.xticks(rotation=45)
            elif dataset_id == "ds_warehouse_master" and "product_id" in df.columns and "reorder_threshold" in df.columns:
                df = df.head(20)  # Limit to 20 products
                plt.bar(df["product_id"], df["reorder_threshold"])
                plt.title(f"{dataset.name} - Reorder Thresholds")
                plt.xlabel("Product ID")
                plt.ylabel("Reorder Threshold")
                plt.xticks(rotation=90)
            elif dataset_id == "ds_manufacturing_logs" and "mfg_timestamp" in df.columns and "quantity_produced" in df.columns:
                df["mfg_timestamp"] = pd.to_datetime(df["mfg_timestamp"])
                df = df.sort_values("mfg_timestamp")
                plt.plot(df["mfg_timestamp"], df["quantity_produced"])
                plt.title(f"{dataset.name} - Production Over Time")
                plt.xlabel("Manufacturing Timestamp")
                plt.ylabel("Quantity Produced")
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                plt.xticks(rotation=45)
            elif dataset_id == "ds_iot_rfid_stream" and "timestamp" in df.columns and "current_stock_on_shelf" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.sort_values("timestamp").tail(100)  # Last 100 points
                plt.plot(df["timestamp"], df["current_stock_on_shelf"])
                plt.title(f"{dataset.name} - Recent Stock Levels")
                plt.xlabel("Timestamp")
                plt.ylabel("Current Stock on Shelf")
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                plt.xticks(rotation=45)
            elif dataset_id == "ds_weather_api" and "timestamp" in df.columns and "temperature" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.sort_values("timestamp")
                plt.plot(df["timestamp"], df["temperature"])
                plt.title(f"{dataset.name} - Temperature Over Time")
                plt.xlabel("Timestamp")
                plt.ylabel("Temperature (°C)")
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
                plt.xticks(rotation=45)
            elif dataset_id == "ds_legacy_trends" and "historical_period" in df.columns and "total_monthly_sales" in df.columns:
                plt.bar(df["historical_period"], df["total_monthly_sales"])
                plt.title(f"{dataset.name} - Monthly Sales by Period")
                plt.xlabel("Historical Period")
                plt.ylabel("Total Monthly Sales")
                plt.xticks(rotation=45)
            else:
                plt.text(0.5, 0.5, f"No specific plot for {dataset.name}", ha='center', va='center')
                plt.title(f"{dataset.name} - Data Sample")
            plot_path = f"logs/{dataset_id}_plot.png"
            os.makedirs("logs", exist_ok=True)
            plt.tight_layout()
            plt.savefig(plot_path)
            plt.close()
            plots.append((dataset.name, plot_path))
        except Exception as e:
            logging.error(f"Error generating plot for {dataset_id}: {e}")
    return plots

@app.get("/dashboard", response_class=HTMLResponse)
# def dashboard(token: str = Depends(verify_token)):
def dashboard():
    persisted_telemetry = JobTelemetry.load_reports()
    total_jobs = len(jobs_db)
    running_jobs = sum(1 for j in jobs_db.values() if j["status"] == "running")
    completed_jobs = total_jobs - running_jobs
    historic_jobs = len(persisted_telemetry)

    job_rows = []
    for job_id, info in jobs_db.items():
        job = info["job"]
        telemetry = info["telemetry"] or {}
        job_rows.append([
            job_id,
            job.source_id,
            job.dataset_id,
            info["status"],
            telemetry.get("records_ingested", 0),
            telemetry.get("records_failed", 0),
            telemetry.get("records_quarantined", 0),
            telemetry.get("throughput_rec_sec", 0.0),
            telemetry.get("duration_seconds", None),
        ])

    persisted_rows = [
        [
            entry.get("job_id"),
            entry.get("source_id"),
            entry.get("records_ingested"),
            entry.get("records_failed"),
            entry.get("records_quarantined"),
            entry.get("throughput_rec_sec"),
            entry.get("duration_seconds"),
            entry.get("storage_summary", {}),
        ]
        for entry in persisted_telemetry[-20:]
    ]

    plots = _generate_visualizations()
    plot_html = ""
    for name, path in plots:
        plot_html += f"<div class='card'><h3>{name}</h3><img src='/{path}' alt='{name} plot' style='max-width:100%; height:auto;'></div>"

    html_body = f"""
<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='UTF-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1.0'>
  <title>Live Ingestion Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; padding: 24px; background: #f7f8fb; color: #111; }}
    h1, h2, h3, h4 {{ margin: 0 0 12px; }}
    .summary {{ display: flex; gap: 12px; flex-wrap: wrap; }}
    .card {{ background: white; border-radius: 10px; padding: 16px; box-shadow: 0 1px 6px rgba(0,0,0,0.08); margin-bottom: 24px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
    th {{ background: #111f3f; color: white; }}
    .small {{ font-size: 0.9rem; color: #555; }}
    .sample {{ background: #f1f5fb; padding: 10px; border-radius: 6px; white-space: pre-wrap; font-family: monospace; font-size: 0.85rem; }}
    .status {{ padding: 4px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold; }}
    .status.running {{ background: #fff3cd; color: #856404; }}
    .status.completed {{ background: #d4edda; color: #155724; }}
    .status.failed {{ background: #f8d7da; color: #721c24; }}
  </style>
</head>
<body>
  <h1>Live Ingestion Dashboard</h1>
  <p class='small'>Real-time updates every 5 seconds. Connected to API at localhost:8000.</p>
  <div class='summary' id='summary'>
    <div class='card'><h3>Total Jobs</h3><p id='total-jobs'>0</p></div>
    <div class='card'><h3>Running</h3><p id='running-jobs'>0</p></div>
    <div class='card'><h3>Completed</h3><p id='completed-jobs'>0</p></div>
    <div class='card'><h3>Historic Telemetry</h3><p id='historic-jobs'>0</p></div>
    <div class='card'><h3>Sources</h3><p>{len(ALL_SOURCES)}</p></div>
    <div class='card'><h3>Datasets</h3><p>{len(ALL_DATASETS)}</p></div>
  </div>

  <div class='card'>
    <h2>Data Visualizations</h2>
    <div id='visualizations'>Loading visualizations...</div>
  </div>

  <div class='card'>
    <h2>Active Ingestion Jobs</h2>
    <table id='jobs-table'>
      <thead><tr><th>Job ID</th><th>Source</th><th>Dataset</th><th>Status</th><th>Ingested</th><th>Failed</th><th>Quarantined</th><th>Throughput</th><th>Duration</th></tr></thead>
      <tbody id='jobs-tbody'><tr><td colspan='9'>Loading...</td></tr></tbody>
    </table>
  </div>

  <div class='card'>
    <h2>Recent Telemetry (last 20)</h2>
    <table id='telemetry-table'>
      <thead><tr><th>Job ID</th><th>Source</th><th>Ingested</th><th>Failed</th><th>Quarantined</th><th>Throughput</th><th>Duration</th><th>Storage Summary</th></tr></thead>
      <tbody id='telemetry-tbody'><tr><td colspan='8'>Loading...</td></tr></tbody>
    </table>
  </div>

  <div class='card'>
    <h2>Storage Summary</h2>
    <div id='storage-summary'>Loading...</div>
  </div>

  <div class='card'>
    <h2>Ingested Dataset Samples</h2>
    <div id='dataset-samples'>Loading...</div>
  </div>

  <script>
    const API_BASE = 'http://localhost:8000';
    const TOKEN = '{config.API_TOKEN}';  // API token for auth

    async function fetchAPI(endpoint) {{
      const response = await fetch(`${{API_BASE}}${{endpoint}}`, {{
        headers: {{ 'Authorization': `Bearer ${{TOKEN}}` }}
      }});
      if (!response.ok) {{
        throw new Error(`API error: ${{response.status}}`);
      }}
      return response.json();
    }}

    async function updateDashboard() {{
      try {{
        // Update metrics
        const metrics = await fetchAPI('/metrics');
        document.getElementById('total-jobs').textContent = metrics.total_jobs;
        document.getElementById('running-jobs').textContent = metrics.running_jobs;
        document.getElementById('completed-jobs').textContent = metrics.completed_jobs;

        // Update telemetry count
        const telemetry = await fetchAPI('/telemetry');
        document.getElementById('historic-jobs').textContent = telemetry.count;

        // Update jobs table
        const jobsResponse = await fetchAPI('/jobs');
        const jobsTbody = document.getElementById('jobs-tbody');
        jobsTbody.innerHTML = '';
        for (const [jobId, info] of Object.entries(jobsResponse.jobs || {{}})) {{
          const job = info.job;
          const telemetry = info.telemetry || {{}};
          const statusClass = `status ${{info.status}}`;
          jobsTbody.innerHTML += `
            <tr>
              <td>${{jobId}}</td>
              <td>${{job.source_id}}</td>
              <td>${{job.dataset_id}}</td>
              <td><span class='${{statusClass}}'>${{info.status}}</span></td>
              <td>${{telemetry.records_ingested || 0}}</td>
              <td>${{telemetry.records_failed || 0}}</td>
              <td>${{telemetry.records_quarantined || 0}}</td>
              <td>${{telemetry.throughput_rec_sec || 0}}</td>
              <td>${{telemetry.duration_seconds || 'N/A'}}</td>
            </tr>
          `;
        }}

        // Update telemetry table
        const telemetryTbody = document.getElementById('telemetry-tbody');
        telemetryTbody.innerHTML = '';
        telemetry.telemetry_records.slice(-20).forEach(record => {{
          telemetryTbody.innerHTML += `
            <tr>
              <td>${{record.job_id}}</td>
              <td>${{record.source_id}}</td>
              <td>${{record.records_ingested}}</td>
              <td>${{record.records_failed}}</td>
              <td>${{record.records_quarantined}}</td>
              <td>${{record.throughput_rec_sec}}</td>
              <td>${{record.duration_seconds}}</td>
              <td>${{JSON.stringify(record.storage_summary).substring(0, 50)}}...</td>
            </tr>
          `;
        }});

        // Update visualizations (regenerate plots)
        const plotsResponse = await fetchAPI('/dashboard-plots');
        const vizDiv = document.getElementById('visualizations');
        vizDiv.innerHTML = plotsResponse.plots.map(([name, path]) => 
          `<div class='card'><h3>${{name}}</h3><img src='${{API_BASE}}/logs/${{path.split('/').pop()}}' alt='${{name}} plot' style='max-width:100%; height:auto;'></div>`
        ).join('');

        // Update storage summary
        const storageResponse = await fetch(`${{API_BASE}}/storage-summary`, {{
          headers: {{ 'Authorization': `Bearer ${{TOKEN}}` }}
        }});
        const storageText = await storageResponse.text();
        document.getElementById('storage-summary').innerHTML = storageText;

        // Update dataset samples
        const datasetsResponse = await fetch(`${{API_BASE}}/dataset-samples`, {{
          headers: {{ 'Authorization': `Bearer ${{TOKEN}}` }}
        }});
        const datasetsText = await datasetsResponse.text();
        document.getElementById('dataset-samples').innerHTML = datasetsText;

      }} catch (error) {{
        console.error('Dashboard update error:', error);
        // Show error in summary
        document.getElementById('summary').innerHTML = '<div class="card"><h3>Error</h3><p>Failed to connect to API. Check if services are running.</p></div>';
      }}
    }}

    // Initial load
    updateDashboard();

    // Update every 5 seconds
    setInterval(updateDashboard, 5000);
  </script>
</body>
</html>"""
    return HTMLResponse(content=html_body)


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


def _build_dataset_section() -> str:
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


def _build_storage_summary() -> str:
    storage_paths = {
        "Ingested": "storage/ingested/*.parquet",
        "Quarantine": "storage/quarantine/*.parquet",
        "CDC Log": "storage/cdc_log/*.parquet",
        "Micro-Batch": "storage/micro_batch/*.parquet",
        "Stream Buffer": "storage/stream_buffer/*.parquet",
        "Checkpoint": "storage/checkpoints/*.json",
        "Detail Logs": "storage/ingested/detail_logs/*.jsonl",
    }
    rows = []
    for label, pattern in storage_paths.items():
        rows.append([label, len(glob.glob(pattern)), pattern])
    return _render_table(["Area", "File Count", "Pattern"], rows)


@app.get("/dashboard/json")
def dashboard_json(token: str = Depends(verify_token)):
    job_list = []
    for job_id, info in jobs_db.items():
        job = info["job"]
        job_list.append({
            "job_id": job_id,
            "source_id": job.source_id,
            "dataset_id": job.dataset_id,
            "status": info["status"],
            "telemetry": info["telemetry"],
        })

    dataset_list = [
        {"source_id": source_id, "record_count": len(records), "sample": records[:5]}
        for source_id, records in datasets_db.items()
    ]

    storage_summary = {
        label: len(glob.glob(pattern))
        for label, pattern in {
            "ingested": "storage/ingested/*.parquet",
            "quarantine": "storage/quarantine/*.parquet",
            "cdc_log": "storage/cdc_log/*.parquet",
            "micro_batch": "storage/micro_batch/*.parquet",
            "stream_buffer": "storage/stream_buffer/*.parquet",
            "checkpoints": "storage/checkpoints/*.json",
            "details": "storage/ingested/detail_logs/*.jsonl",
        }.items()
    }

    return {"jobs": job_list, "datasets": dataset_list, "storage_summary": storage_summary}


@app.get("/inventory/alerts")
def get_inventory_alerts(token: str = Depends(verify_token)):
    """Supply chain specific: Check for low inventory alerts."""
    alerts = []
    if "src_warehouse_master" in datasets_db:
        for record in datasets_db["src_warehouse_master"]:
            if record.get("current_stock", 0) < record.get("reorder_threshold", 100):
                alerts.append({
                    "product_id": record.get("product_id"),
                    "current_stock": record.get("current_stock"),
                    "reorder_threshold": record.get("reorder_threshold"),
                    "alert": "Low stock"
                })
    return {"alerts": alerts}
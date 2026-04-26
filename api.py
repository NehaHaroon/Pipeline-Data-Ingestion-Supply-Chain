from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status, Request
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from slowapi import Limiter
from slowapi.util import get_remote_address
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import glob
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import uuid
import os
import sys

# Setup path and logging
sys.path.insert(0, os.path.dirname(__file__))
from common import setup_logging, ensure_storage_directories

log = setup_logging("api")

import config
from control_plane.entities import ALL_SOURCES, ALL_DATASETS, IngestionJob, ExecutionMode
from control_plane.contracts import CONTRACT_REGISTRY
from observability_plane.telemetry import JobTelemetry
from ui_manager import (
    render_storage_summary, render_dataset_samples, generate_visualizations, DASHBOARD_HTML
)

from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# App bootstrap
# ---------------------------------------------------------------------------
app = FastAPI()

# Allow the dashboard to communicate with the API from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ensure storage and log directories exist
storage_paths = ensure_storage_directories()
app.mount("/logs", StaticFiles(directory=storage_paths.get('logs', 'logs')), name="logs")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# In-memory stores  (use a real DB in production)
# ---------------------------------------------------------------------------
jobs_db: Dict[str, Any] = {}
datasets_db: Dict[str, List[Any]] = {}   # source_id -> list of records


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class IngestRequest(BaseModel):
    records: List[Dict[str, Any]]


class JobStatus(BaseModel):
    job_id: str
    status: str
    telemetry: Optional[Dict[str, Any]] = None


# ===========================================================================
# ENDPOINTS
# ===========================================================================

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
@app.get("/metrics")
def metrics():
    """
    Accurate pipeline metrics endpoint.

    Design notes
    ────────────
    jobs_db        – in-memory dict; holds only the current server session's
                     jobs (running + recently completed/failed).  Resets on
                     every container/process restart.

    persisted_tel  – JSONL/parquet files written by JobTelemetry.log_report()
                     at job completion.  They survive restarts and accumulate
                     across sessions.

    Deduplication  – jobs_db already contains the current session's completed
                     jobs, which are ALSO written to persisted telemetry the
                     moment they finish.  To avoid counting them twice we
                     subtract the current-session completed count from the
                     historic total.  Running jobs have no telemetry record
                     yet, so they're safe to add directly.
    """
    # ── current session (in-memory) ──────────────────────────────────────
    session_total     = len(jobs_db)
    session_running   = sum(1 for j in jobs_db.values() if j["status"] == "running")
    session_completed = sum(1 for j in jobs_db.values() if j["status"] == "completed")
    session_failed    = sum(1 for j in jobs_db.values() if j["status"] == "failed")

    # ── persisted telemetry (all sessions including current) ─────────────
    persisted_telemetry   = JobTelemetry.load_reports()
    persisted_total       = len(persisted_telemetry)

    # Persisted records that belong to THIS session (already counted above).
    # We identify them by job_id membership in jobs_db.
    current_session_ids   = set(jobs_db.keys())
    historic_only         = [
        r for r in persisted_telemetry
        if r.get("job_id") not in current_session_ids
    ]
    historic_total        = len(historic_only)

    # ── aggregate record-level telemetry (de-duped, historic only) ────────
    hist_ingested     = sum(r.get("records_ingested",    0) for r in historic_only)
    hist_quarantined  = sum(r.get("records_quarantined", 0) for r in historic_only)
    hist_failed_recs  = sum(r.get("records_failed",      0) for r in historic_only)

    # Current-session record counts come from jobs_db telemetry payloads
    sess_ingested     = sum(
        (j["telemetry"] or {}).get("records_ingested",    0) for j in jobs_db.values()
    )
    sess_quarantined  = sum(
        (j["telemetry"] or {}).get("records_quarantined", 0) for j in jobs_db.values()
    )
    sess_failed_recs  = sum(
        (j["telemetry"] or {}).get("records_failed",      0) for j in jobs_db.values()
    )

    total_records_ingested    = hist_ingested    + sess_ingested
    total_records_quarantined = hist_quarantined + sess_quarantined
    total_records_failed      = hist_failed_recs + sess_failed_recs

    # ── throughput (average across all completed jobs with telemetry) ─────
    all_thr = [
        r.get("throughput_rec_sec", 0)
        for r in persisted_telemetry
        if r.get("throughput_rec_sec")
    ]
    avg_throughput = round(sum(all_thr) / len(all_thr), 2) if all_thr else 0.0
    peak_throughput = round(max(all_thr), 2) if all_thr else 0.0

    # ── totals (no double-counting) ───────────────────────────────────────
    true_total     = session_running + session_failed + historic_total + session_completed
    # ↑ running + this-session-failed (not in telemetry yet) +
    #   everything persisted (historic + current-session completed)
    true_completed = session_completed + historic_total
    true_failed    = session_failed    # historic failed jobs are also in persisted_telemetry
    # Note: persisted telemetry only contains completed jobs (log_report is
    # called after mark_end).  Failed jobs that crashed before mark_end are
    # only in jobs_db, so session_failed is the correct delta.

    # Per-source breakdown (from persisted telemetry only – most complete)
    source_breakdown: Dict[str, Dict[str, Any]] = {}
    for r in persisted_telemetry:
        sid = r.get("source_id", "unknown")
        if sid not in source_breakdown:
            source_breakdown[sid] = {
                "job_count":          0,
                "records_ingested":   0,
                "records_quarantined":0,
                "records_failed":     0,
                "avg_throughput":     0.0,
                "_thr_samples":       [],
            }
        source_breakdown[sid]["job_count"]           += 1
        source_breakdown[sid]["records_ingested"]    += r.get("records_ingested",    0)
        source_breakdown[sid]["records_quarantined"] += r.get("records_quarantined", 0)
        source_breakdown[sid]["records_failed"]      += r.get("records_failed",      0)
        if r.get("throughput_rec_sec"):
            source_breakdown[sid]["_thr_samples"].append(r["throughput_rec_sec"])

    for sid, d in source_breakdown.items():
        samples = d.pop("_thr_samples", [])
        d["avg_throughput"] = round(sum(samples) / len(samples), 2) if samples else 0.0

    return {
        # ── job counts (no double-counting) ──
        "total_jobs":            true_total,
        "running_jobs":          session_running,
        "completed_jobs":        true_completed,
        "failed_jobs":           true_failed,
        # ── session vs historic split (for debugging) ──
        "session_jobs":          session_total,
        "historic_jobs":         historic_total,
        # ── record-level aggregates ──
        "total_records_ingested":    total_records_ingested,
        "total_records_quarantined": total_records_quarantined,
        "total_records_failed":      total_records_failed,
        # ── throughput ──
        "avg_throughput_rec_sec":  avg_throughput,
        "peak_throughput_rec_sec": peak_throughput,
        # ── per-source breakdown ──
        "source_breakdown": source_breakdown,
        # ── timestamp ──
        "as_of": datetime.utcnow().isoformat() + "Z",
    }


# ---------------------------------------------------------------------------
# Sources / Datasets
# ---------------------------------------------------------------------------
@app.get("/sources")
def list_sources():
    return {"sources": [src.__dict__ for src in ALL_SOURCES]}


@app.get("/datasets")
def list_datasets():
    return {"datasets": [ds.__dict__ for ds in ALL_DATASETS]}


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
@app.post("/ingest/{source_id}")
@limiter.limit("10000/minute")
def ingest_data(
    request: Request,
    source_id: str,
    request_data: IngestRequest,
    background_tasks: BackgroundTasks,
    token: str = Depends(verify_token),
):
    if source_id not in CONTRACT_REGISTRY:
        raise HTTPException(status_code=404, detail="Source not found")

    dataset_id = f"ds_{source_id.replace('src_', '')}"
    job_id = str(uuid.uuid4())
    job = IngestionJob(
        job_id=job_id,
        source_id=source_id,
        dataset_id=dataset_id,
        execution_mode=ExecutionMode.BATCH,
    )
    jobs_db[job_id] = {"job": job, "status": "running", "telemetry": None}
    background_tasks.add_task(process_ingestion, job_id, source_id, request_data.records)
    return {"job_id": job_id, "message": "Ingestion started"}


def process_ingestion(job_id: str, source_id: str, records: List[Dict[str, Any]]):
    try:
        telemetry = JobTelemetry(job_id=job_id, source_id=source_id)
        telemetry.mark_start()

        for idx, record in enumerate(records, start=1):
            result = CONTRACT_REGISTRY[source_id].enforce(record)
            rec_status = result["status"]
            if rec_status in ["ok", "coerced"]:
                datasets_db.setdefault(source_id, []).append(result["record"])
                telemetry.records_ingested += 1
            elif rec_status == "quarantine":
                telemetry.records_quarantined += 1
            else:
                telemetry.records_failed += 1

            log.info(
                f"[API-INGEST] job={job_id} source={source_id} row={idx} status={rec_status} "
                f"records_ingested={telemetry.records_ingested} "
                f"records_quarantined={telemetry.records_quarantined} "
                f"records_failed={telemetry.records_failed}"
            )

        telemetry.mark_end()
        telemetry.log_report()
        jobs_db[job_id]["status"] = "completed"
        jobs_db[job_id]["telemetry"] = telemetry.report()

    except Exception as e:
        log.error(f"Ingestion failed for job {job_id}: {e}")
        jobs_db[job_id]["status"] = "failed"


# ---------------------------------------------------------------------------
# Jobs / Telemetry
# ---------------------------------------------------------------------------
@app.get("/jobs")
def list_jobs(token: str = Depends(verify_token)):
    """List all jobs with their status and telemetry."""
    return {"jobs": jobs_db}


@app.get("/telemetry")
def get_telemetry(token: str = Depends(verify_token)):
    """Return persisted telemetry job records for ingestion visibility."""
    telemetry_records = JobTelemetry.load_reports()
    return {"telemetry_records": telemetry_records, "count": len(telemetry_records)}


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------
@app.get("/datasets/{dataset_id}")
def query_dataset(dataset_id: str, limit: int = 100, token: str = Depends(verify_token)):
    source_id = f"src_{dataset_id.replace('ds_', '')}"
    if source_id not in datasets_db:
        return {"records": []}
    return {"records": datasets_db[source_id][:limit]}


# ---------------------------------------------------------------------------
# Storage / Dataset-sample helpers  (kept for /dashboard/json compatibility)
# ---------------------------------------------------------------------------
@app.get("/dashboard-plots")
def get_dashboard_plots(token: str = Depends(verify_token)):
    """Get list of available plot files (legacy – kept for backward-compat)."""
    plots = generate_visualizations()
    return {"plots": plots}


@app.get("/storage-summary")
def get_storage_summary(token: str = Depends(verify_token)):
    """Get storage summary HTML."""
    return HTMLResponse(content=render_storage_summary())


@app.get("/dataset-samples")
def get_dataset_samples(token: str = Depends(verify_token)):
    """Get dataset samples HTML."""
    return HTMLResponse(content=render_dataset_samples(datasets_db))


# ---------------------------------------------------------------------------
# Dashboard JSON  (used by the JS front-end)
# ---------------------------------------------------------------------------
@app.get("/dashboard/json")
def dashboard_json(token: str = Depends(verify_token)):
    # 1. Get jobs from the current session (in-memory)
    job_list = [
        {
            "job_id":     job_id,
            "source_id":  info["job"].source_id,
            "dataset_id": info["job"].dataset_id,
            "status":     info["status"],
            "telemetry":  info["telemetry"],
        }
        for job_id, info in jobs_db.items()
    ]

    # 2. Merge historic jobs from persisted telemetry records on disk.
    # This ensures "all jobs till date" are visible even after a server restart.
    persisted_telemetry = JobTelemetry.load_reports()
    current_session_ids = set(jobs_db.keys())

    for report in persisted_telemetry:
        jid = report.get("job_id")
        if jid and jid not in current_session_ids:
            sid = report.get("source_id", "unknown")
            job_list.append({
                "job_id":     jid,
                "source_id":  sid,
                "dataset_id": report.get("dataset_id") or f"ds_{sid.replace('src_', '')}",
                "status":     report.get("status", "completed"),
                "telemetry":  report,
            })

    dataset_list = [
        {"source_id": sid, "record_count": len(recs), "sample": recs[:5]}
        for sid, recs in datasets_db.items()
    ]

    storage_summary = {
        label: len(glob.glob(pattern))
        for label, pattern in {
            "ingested":      "storage/ingested/*.parquet",
            "quarantine":    "storage/quarantine/*.parquet",
            "cdc_log":       "storage/cdc_log/*.parquet",
            "micro_batch":   "storage/micro_batch/*.parquet",
            "stream_buffer": "storage/stream_buffer/*.parquet",
            "checkpoints":   "storage/checkpoints/*.json",
            "details":       "storage/ingested/detail_logs/*.jsonl",
        }.items()
    }

    return {"jobs": job_list, "datasets": dataset_list, "storage_summary": storage_summary}


# ---------------------------------------------------------------------------
# Source configurations
# ---------------------------------------------------------------------------
@app.get("/source-configurations")
def get_source_configurations(token: str = Depends(verify_token)):
    """Detailed source configs with classifications and next-ingestion times."""
    from datetime import timezone, timedelta
    import control_plane.entities as entities

    now = datetime.now(timezone.utc)
    source_configs = []

    for source in entities.ALL_SOURCES:
        freq = source.ingestion_frequency.value

        if freq == "real_time":
            next_time = "Continuous"
            next_datetime = None
        elif freq == "hourly":
            next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            remaining = next_hour - now
            next_time = f"{remaining.seconds // 3600}h {(remaining.seconds % 3600) // 60}m"
            next_datetime = next_hour
        elif freq == "daily":
            next_day = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            remaining = next_day - now
            next_time = f"{remaining.days}d {remaining.seconds // 3600}h"
            next_datetime = next_day
        elif freq == "weekly":
            days_to_next = (7 - now.weekday()) % 7 or 7
            next_week = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_to_next)
            remaining = next_week - now
            next_time = f"{remaining.days}d {remaining.seconds // 3600}h"
            next_datetime = next_week
        else:
            next_time = "On Demand"
            next_datetime = None

        dataset = next(
            (d for d in entities.ALL_DATASETS if d.dataset_id == f"ds_{source.source_id[4:]}"),
            None,
        )

        source_configs.append({
            "source_id":             source.source_id,
            "name":                  source.name,
            "source_type":           source.source_type.value,
            "extraction_mode":       source.extraction_mode.value,
            "change_capture_mode":   source.change_capture_mode.value,
            "ingestion_frequency":   freq,
            "next_ingestion":        next_time,
            "next_ingestion_datetime": next_datetime.isoformat() if next_datetime else None,
            "tags":                  source.tags,
            "domain":                dataset.domain if dataset else "unknown",
            "classification_level":  dataset.classification_level.value if dataset else "unknown",
            "retention_policy":      dataset.retention_policy if dataset else "unknown",
            "schema_version":        dataset.schema_version if dataset else "unknown",
            "expected_schema":       source.expected_schema,
        })

    return {"sources": source_configs}


# ---------------------------------------------------------------------------
# Inventory alerts
# ---------------------------------------------------------------------------
@app.get("/inventory/alerts")
def get_inventory_alerts(token: str = Depends(verify_token)):
    """Supply-chain specific: check for low-inventory alerts."""
    alerts = []
    for record in datasets_db.get("src_warehouse_master", []):
        if record.get("current_stock", 0) < record.get("reorder_threshold", 100):
            alerts.append({
                "product_id":        record.get("product_id"),
                "current_stock":     record.get("current_stock"),
                "reorder_threshold": record.get("reorder_threshold"),
                "alert":             "Low stock",
            })
    return {"alerts": alerts}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """
    Production-grade live dashboard.
    - Serves the full dark-themed HTML with slice filters.
    - The API token is injected at request time so the JS can authenticate.
    - No matplotlib images — all charts are rendered in-browser via Chart.js.
    - Falls back to realistic mock data when the backend is unreachable.
    """
    html_body = DASHBOARD_HTML.replace("__API_TOKEN__", config.API_TOKEN)
    return HTMLResponse(content=html_body)
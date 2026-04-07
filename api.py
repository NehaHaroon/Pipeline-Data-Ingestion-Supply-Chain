from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import uuid
import os

from control_plane.entities import ALL_SOURCES, ALL_DATASETS, IngestionJob, ExecutionMode
from control_plane.contracts import CONTRACT_REGISTRY
from data_plane.ingestion.batch_ingest import run_batch_ingestion_for_source
from observability_plane.telemetry import JobTelemetry

app = FastAPI(title="Supply Chain Ingestion API", version="1.0.0")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Security
security = HTTPBearer()
API_TOKEN = os.getenv("API_TOKEN", "default_token_change_in_prod")

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
    return {
        "total_jobs": total_jobs,
        "running_jobs": running_jobs,
        "completed_jobs": total_jobs - running_jobs
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
        telemetry.start()

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

        telemetry.end()
        jobs_db[job_id]["status"] = "completed"
        jobs_db[job_id]["telemetry"] = telemetry.to_dict()

    except Exception as e:
        log.error(f"Ingestion failed for job {job_id}: {e}")
        jobs_db[job_id]["status"] = "failed"

@app.get("/jobs/{job_id}")
def get_job_status(job_id: str, token: str = Depends(verify_token)):
    if job_id not in jobs_db:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs_db[job_id]

@app.get("/datasets/{dataset_id}")
def query_dataset(dataset_id: str, limit: int = 100, token: str = Depends(verify_token)):
    source_id = f"src_{dataset_id.replace('ds_', '')}"
    source_id = f"src_{dataset_id.replace('ds_', '')}"
    if source_id not in datasets_db:
        return {"records": []}
    records = datasets_db[source_id][:limit]
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
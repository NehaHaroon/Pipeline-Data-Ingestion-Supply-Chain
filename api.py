from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import uuid

from control_plane.entities import ALL_SOURCES, ALL_DATASETS, IngestionJob, ExecutionMode
from control_plane.contracts import CONTRACT_REGISTRY
from data_plane.ingestion.batch_ingest import run_batch_ingestion_for_source
from observability_plane.telemetry import JobTelemetry

app = FastAPI(title="Supply Chain Ingestion API", version="1.0.0")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# In-memory storage for demo; in production, use database
jobs_db = {}
datasets_db = {}  # dataset_id -> list of records

class IngestRequest(BaseModel):
    records: List[Dict[str, Any]]

class JobStatus(BaseModel):
    job_id: str
    status: str
    telemetry: Optional[Dict[str, Any]] = None

@app.get("/")
def root():
    return {"message": "Supply Chain Ingestion API"}

@app.get("/sources")
def list_sources():
    return {"sources": [src.__dict__ for src in ALL_SOURCES]}

@app.get("/datasets")
def list_datasets():
    return {"datasets": [ds.__dict__ for ds in ALL_DATASETS]}

@app.post("/ingest/{source_id}")
def ingest_data(source_id: str, request: IngestRequest, background_tasks: BackgroundTasks):
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
    background_tasks.add_task(process_ingestion, job_id, source_id, request.records)

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
def get_job_status(job_id: str):
    if job_id not in jobs_db:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs_db[job_id]

@app.get("/datasets/{dataset_id}")
def query_dataset(dataset_id: str, limit: int = 100):
    source_id = f"src_{dataset_id.replace('ds_', '')}"
    if source_id not in datasets_db:
        return {"records": []}
    records = datasets_db[source_id][:limit]
    return {"records": records}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
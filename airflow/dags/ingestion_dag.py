
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import requests
from requests.exceptions import RequestException
import logging
import math
import time

default_args = {"retries": 3, "retry_delay": timedelta(minutes=2)}
log = logging.getLogger(__name__)


def _sanitize_for_json(value):
    """
    Convert non-JSON-compliant float values (NaN/Inf) to None recursively.
    """
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    return value

def load_and_ingest_source(source_id: str):
    """Load CSV data and send to ingestion API."""
    max_rows_env = os.getenv("INGEST_DAG_MAX_ROWS", "1000").strip()
    try:
        max_rows = int(max_rows_env) if max_rows_env else 0
    except ValueError:
        max_rows = 1000
    # Map source_id to CSV file path
    csv_files = {
        "src_warehouse_master": "/opt/airflow/project/storage/raw/warehouse_master.csv",
        "src_sales_history": "/opt/airflow/project/storage/raw/sales_history.csv",
        "src_manufacturing_logs": "/opt/airflow/project/storage/raw/manufacturing_logs.csv",
        "src_legacy_trends": "/opt/airflow/project/storage/raw/legacy_trends.csv"
    }

    csv_path = csv_files.get(source_id)
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Load CSV data
    df = pd.read_csv(csv_path)
    raw_n = len(df)
    if max_rows > 0 and raw_n > max_rows:
        df = df.iloc[:max_rows].copy()
        log.info(
            "INGEST_DAG_MAX_ROWS=%s: using %s of %s rows for %s",
            max_rows,
            max_rows,
            raw_n,
            source_id,
        )
    df = df.where(pd.notna(df), None)
    records = df.to_dict("records")

    return {"records": records}


def post_to_ingestion_api(source_id: str, records_task_id: str, **context):
    """
    Send records to ingestion API with runtime token from environment.
    Avoid hardcoded token drift that causes 401 Unauthorized.
    """
    import time as _time

    ti = context["ti"]
    records = ti.xcom_pull(task_ids=records_task_id)
    if not isinstance(records, dict) or "records" not in records:
        raise ValueError(f"Invalid XCom payload from {records_task_id}. Expected dict with 'records'.")
    records = _sanitize_for_json(records)

    api_base = os.getenv("INGESTION_API_URL", "http://ingestion-api:8000").rstrip("/")
    api_token = os.getenv("API_TOKEN")
    if not api_token:
        raise ValueError("API_TOKEN is not set in Airflow runtime environment.")

    url = f"{api_base}/ingest/{source_id}"
    record_count = len(records.get("records", []))
    post_timeout = int(os.getenv("INGESTION_POST_TIMEOUT_SECONDS", "600"))
    log.info("Posting %s records to %s (post timeout=%ss)", record_count, url, post_timeout)
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
        json=records,
        timeout=post_timeout,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Ingestion API call failed for {source_id}. status={response.status_code} body={response.text[:500]}"
        )
    payload = response.json()
    job_id = payload.get("job_id")
    if not job_id:
        raise RuntimeError(f"Ingestion API did not return job_id for {source_id}: {payload}")

    # Wait for background ingestion completion so downstream DAGs see Bronze tables.
    status_url = f"{api_base}/jobs/{job_id}/status"
    max_wait_seconds = int(os.getenv("INGESTION_JOB_MAX_WAIT_SECONDS", "1800"))  # Increased to 30 min default for large batches
    poll_interval = int(os.getenv("INGESTION_JOB_POLL_INTERVAL_SECONDS", "5"))
    progress_log_interval = int(os.getenv("INGESTION_POLL_PROGRESS_LOG_SECONDS", "60"))
    waited = 0
    last_progress_log = 0
    while waited < max_wait_seconds:
        try:
            status_resp = requests.get(
                status_url,
                headers={"Authorization": f"Bearer {api_token}"},
                timeout=60,  # Increased from 30 to 60 seconds for better reliability
            )
        except RequestException as exc:
            log.warning(
                "Polling job %s status failed due to transient network error: %s. Retrying in %s seconds...",
                job_id,
                exc,
                poll_interval,
            )
            _time.sleep(poll_interval)
            waited += poll_interval
            continue

        if status_resp.status_code == 404:
            log.warning("Job %s not visible yet, retrying...", job_id)
        elif status_resp.status_code >= 400:
            raise RuntimeError(
                f"Failed polling job {job_id} for {source_id}. status={status_resp.status_code} body={status_resp.text[:500]}"
            )
        else:
            status_payload = status_resp.json()
            status = status_payload.get("status")
            if (
                progress_log_interval > 0
                and waited - last_progress_log >= progress_log_interval
                and status not in ("completed", "failed")
            ):
                last_progress_log = waited
                log.info(
                    "Waiting for ingestion job %s (%s): api_status=%s waited=%ss/%ss",
                    job_id,
                    source_id,
                    status,
                    waited,
                    max_wait_seconds,
                )
            if status == "completed":
                # Only return success after the API reports background processing finished.
                tel = status_payload.get("telemetry") or {}
                result = {
                    "ok": True,
                    "source_id": source_id,
                    "job_id": job_id,
                    "ingestion_status": "completed",
                    "records_submitted": record_count,
                    "records_ingested": tel.get("records_ingested"),
                    "records_failed": tel.get("records_failed"),
                    "records_quarantined": tel.get("records_quarantined"),
                    "duration_seconds": tel.get("duration_seconds"),
                }
                log.info(
                    "Ingestion job completed for %s | job_id=%s | ingested=%s / submitted=%s",
                    source_id,
                    job_id,
                    tel.get("records_ingested"),
                    record_count,
                )
                log.info("Airflow ingest task SUCCESS for %s (job_id=%s)", source_id, job_id)
                return result
            if status == "failed":
                detail = status_payload.get("telemetry") or {}
                raise RuntimeError(
                    f"Ingestion job failed for {source_id} | job_id={job_id} | detail={detail}"
                )

        _time.sleep(poll_interval)
        waited += poll_interval

    raise RuntimeError(
        f"Ingestion job timeout for {source_id} | job_id={job_id} after {max_wait_seconds}s"
    )


with DAG("supply_chain_ingestion", schedule_interval="@hourly",
         start_date=datetime(2026, 1, 1), catchup=False,
         default_args=default_args) as dag:

    for source_id in ["src_warehouse_master", "src_sales_history",
                       "src_manufacturing_logs", "src_legacy_trends"]:

        # Load data task
        load_task = PythonOperator(
            task_id=f"load_{source_id}",
            python_callable=load_and_ingest_source,
            op_kwargs={"source_id": source_id}
        )

        ingest_task = PythonOperator(
            task_id=f"ingest_{source_id}",
            python_callable=post_to_ingestion_api,
            op_kwargs={
                "source_id": source_id,
                "records_task_id": f"load_{source_id}",
            },
        )

        load_task >> ingest_task
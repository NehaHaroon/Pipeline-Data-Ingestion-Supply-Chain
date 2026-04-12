#!/usr/bin/env python3
# run_production.py — Production runner for the supply chain ingestion pipeline.
#
# Usage:  python run_production.py
#
# This script starts the API and optionally the real-time IoT consumer.

import os
import sys
import logging
import threading
import time
import requests
import pandas as pd
import glob
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("run_production")

# Add file logging to root logger to capture all logs
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"run_production_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
file_handler = logging.FileHandler(log_file, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().addHandler(file_handler)

BASE = os.path.dirname(__file__)
sys.path.insert(0, BASE)

from control_plane.entities import ALL_SOURCES, SourceType
from config import API_HOST, API_PORT, API_TOKEN
from observability_plane.telemetry import JobTelemetry

LOCAL_API_HOST = "127.0.0.1"

def banner(title: str, subtitle: str):
    log.info("")
    log.info("=" * 70)
    log.info(f"  {title}: {subtitle}")
    log.info("=" * 70)


def print_dashboard():
    """Print production dashboard with storage audit, ingestion status, source configs, and log summary."""
    banner("PRODUCTION DASHBOARD", "Status & Health Summary")

    # Storage Audit
    log.info("")
    log.info("  -- Storage Audit --")
    dirs = {
        "Ingested (Parquet)": "storage/ingested",
        "Quarantine":         "storage/quarantine",
        "CDC Log":            "storage/cdc_log",
        "Micro-Batch":        "storage/micro_batch",
        "Stream Buffer":      "storage/stream_buffer",
        "Checkpoints":        "storage/checkpoints",
        "Detail Logs (JSONL)":"storage/ingested/detail_logs",
    }

    total_files = 0
    total_bytes = 0
    for label, path in dirs.items():
        if not os.path.exists(path):
            continue
        files = glob.glob(os.path.join(path, "**", "*.*"), recursive=True)
        size  = sum(os.path.getsize(f) for f in files if os.path.isfile(f))
        log.info(f"  {label:<30} {len(files):>4} files  {size/1024:>8.1f} KB")
        total_files += len(files)
        total_bytes += size

    log.info("  " + "-"*55)
    log.info(f"  {'TOTAL':<30} {total_files:>4} files  {total_bytes/1024:>8.1f} KB")

    # Parquet Row Counts
    log.info("")
    log.info("  -- Parquet Row Counts --")
    for label, path in dirs.items():
        for fp in glob.glob(os.path.join(path, "*.parquet")):
            try:
                df = pd.read_parquet(fp)
                log.info(f"  {os.path.basename(fp):<55} {len(df):>6} rows")
            except Exception:
                pass

    # Ingestion Status
    log.info("")
    log.info("  -- Ingestion Status --")
    stream_files = glob.glob(os.path.join("storage/stream_buffer", "*.parquet"))
    if stream_files:
        total_stream_events = 0
        for fp in stream_files:
            try:
                df = pd.read_parquet(fp)
                total_stream_events += len(df)
            except Exception:
                pass
        log.info(f"  Streaming: {len(stream_files)} files, {total_stream_events} events ingested")
    else:
        log.info("  Streaming: No stream buffer files yet")

    # Telemetry summary
    log.info("")
    log.info("  -- Telemetry Summary --")
    reports = JobTelemetry.load_reports()
    log.info(f"  Persisted telemetry jobs: {len(reports)}")
    if reports:
        report_sources = sorted({r.get('source_id') for r in reports})
        log.info(f"  Telemetry sources: {report_sources}")

    # Source Configuration & Next Ingestion
    log.info("")
    log.info("  -- Source Configuration & Next Ingestion --")
    now = datetime.now(timezone.utc)
    for src in ALL_SOURCES:
        freq = src.ingestion_frequency.value
        if freq == "real_time":
            next_time = "Continuous"
        elif freq == "hourly":
            next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            remaining = next_hour - now
            next_time = f"{remaining.seconds // 3600}h {(remaining.seconds % 3600) // 60}m"
        elif freq == "daily":
            next_day = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            remaining = next_day - now
            next_time = f"{remaining.days}d {remaining.seconds // 3600}h"
        elif freq == "weekly":
            days_to_next = (7 - now.weekday()) % 7 or 7
            next_week = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_to_next)
            remaining = next_week - now
            next_time = f"{remaining.days}d {remaining.seconds // 3600}h"
        else:  # on_demand
            next_time = "On Demand"
        log.info(f"  {src.source_id:<25} | {src.source_type.value:<6} | {src.extraction_mode.value:<4} | {freq:<9} | Next: {next_time}")

    # Log File Summary
    log.info("")
    log.info("  -- Log File Summary --")
    if os.path.exists(log_file):
        with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        total_lines = len(lines)
        error_lines = sum(1 for line in lines if '[ERROR]' in line)
        warning_lines = sum(1 for line in lines if '[WARNING]' in line)
        info_lines = sum(1 for line in lines if '[INFO]' in line)
        log.info(f"  Log file: {log_file}")
        log.info(f"  Total lines: {total_lines}")
        log.info(f"  INFO: {info_lines}, WARNING: {warning_lines}, ERROR: {error_lines}")
        if lines:
            log.info("  Last 5 lines:")
            for line in lines[-5:]:
                log.info(f"    {line.strip()}")
    else:
        log.warning("  No log file found")

    log.info("=" * 70)

def periodic_dashboard():
    """Run dashboard periodically every 5 minutes."""
    while True:
        time.sleep(300)  # 5 minutes
        print_dashboard()

def start_api():
    """Start the FastAPI server."""
    banner("PHASE: API SERVER", "FastAPI startup")
    log.info(f"Starting API server on http://{API_HOST}:{API_PORT}")
    import uvicorn
    uvicorn.run("api:app", host=API_HOST, port=API_PORT, log_level="info")

def start_iot_consumer():
    """Start the real-time IoT consumer."""
    banner("PHASE: IOT CONSUMER", "Real-time IoT ingestion")
    log.info("Starting IoT consumer...")
    from data_plane.ingestion.real_time_iot_ingest import run_real_time_ingestion
    run_real_time_ingestion()


def wait_for_api_ready(base_url: str = f"http://{LOCAL_API_HOST}:{API_PORT}", timeout_sec: int = 30) -> bool:
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            response = requests.get(f"{base_url}/health", timeout=5)
            if response.status_code == 200:
                log.info("API health check passed")
                return True
            log.warning(f"API health check returned {response.status_code}; retrying...")
        except requests.exceptions.RequestException as exc:
            log.debug(f"API health not ready: {exc}")
        time.sleep(1)
    log.error("API did not become healthy within timeout")
    return False


def run_weather_api_ingestion():
    """Run a weather API ingestion job to verify external API source paths."""
    banner("PHASE: WEATHER API INGESTION", "Weather API source ingestion")
    log.info("Starting weather API ingestion via internal batch path")
    try:
        from data_plane.ingestion.batch_ingest import run_api_ingestion
        tel = run_api_ingestion("src_weather_api", "ds_weather_api")
        log.info(
            f"  Weather API ingestion completed: ingested={tel.records_ingested} "
            f"failed={tel.records_failed} quarantined={tel.records_quarantined} coerced={tel.records_coerced}"
        )
    except Exception as exc:
        log.error(f"  Weather API ingestion failed: {exc}")


def run_batch_on_startup():
    """Trigger batch ingestion via API automation."""
    banner("PHASE: BATCH INGESTION", "Load raw sources via API")
    batch_sources = [source for source in ALL_SOURCES if source.source_type.value == "file"]
    log.info(f"Preparing batch ingestion for {len(batch_sources)} file-based source(s)")

    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    base_url = f"http://{LOCAL_API_HOST}:{API_PORT}"
    log.info(f"Using local API endpoint for ingestion: {base_url}")

    if not wait_for_api_ready(base_url=base_url):
        log.error("Batch ingestion startup aborted because API health check failed")
        return

    if not batch_sources:
        log.warning("No file-based batch sources configured for startup ingestion")
        return

    for source in batch_sources:
        source_id = source.source_id
        raw_path = f"storage/raw/{source_id.replace('src_', '')}.csv"

        if not os.path.exists(raw_path):
            log.warning(f"Raw file not found: {raw_path} for source {source_id}")
            continue

        log.info(f"Loading data from {raw_path} for {source_id}")
        df = pd.read_csv(raw_path)
        records = df.to_dict('records')
        log.info(f"  {len(records)} rows loaded from raw file")

        # Convert NaN values to None so they serialize as JSON null
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        batch_size = 1000
        requests_sent = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            response = requests.post(
                f"{base_url}/ingest/{source_id}",
                json={"records": batch},
                headers=headers
            )
            requests_sent += 1
            if response.status_code == 200:
                job_id = response.json().get("job_id")
                log.info(f"  Batch ingestion request sent for {source_id} ({len(batch)} records) -> job_id={job_id}")
            else:
                log.error(f"  Failed to start ingestion for {source_id}: {response.status_code} {response.text}")

        log.info(f"Completed batch submission for {source_id}: {requests_sent} request(s)")

    # In production startup, also verify external weather API ingestion
    run_weather_api_ingestion()


def main():
    banner("PRODUCTION STARTUP", "Live API + Dashboard + Batch Ingestion")
    log.info("Starting Supply Chain Ingestion Pipeline (Production Mode)")
    log.info(f"Registered sources: {len(ALL_SOURCES)} | Starting production pipeline with live API and Kafka stream")
    log.info("Production endpoints: http://localhost:8000/health, /telemetry, /storage-summary, /dashboard-plots")

    # Start API in a thread
    api_thread = threading.Thread(target=start_api, daemon=True)
    api_thread.start()

    # Wait a bit for API to start
    time.sleep(2)

    # Print initial dashboard
    print_dashboard()

    # Start periodic dashboard
    dashboard_thread = threading.Thread(target=periodic_dashboard, daemon=True)
    dashboard_thread.start()

    # Start batch ingestion via API automation
    batch_thread = threading.Thread(target=run_batch_on_startup, daemon=True)
    batch_thread.start()

    # Start IoT consumer in its own thread so production startup stays responsive
    iot_thread = threading.Thread(target=start_iot_consumer, daemon=True)
    iot_thread.start()

    log.info("Production startup complete. API, batch ingestion, dashboard, and IoT consumer are running.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down production runner")

if __name__ == "__main__":
    main() 
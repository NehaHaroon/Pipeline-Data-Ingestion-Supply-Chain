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
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("run_production")

# Add file logging to root logger to capture all logs
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"run_production_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))
logging.getLogger().addHandler(file_handler)

BASE = os.path.dirname(__file__)
sys.path.insert(0, BASE)

from control_plane.entities import ALL_SOURCES
from config import API_TOKEN

def print_dashboard():
    """Print production dashboard with storage audit, ingestion status, source configs, and log summary."""
    log.info("")
    log.info("=" * 70)
    log.info("  PRODUCTION DASHBOARD")
    log.info("=" * 70)

    # Storage Audit
    log.info("")
    log.info("  ── Storage Audit ──")
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

    log.info(f"  {'─'*55}")
    log.info(f"  {'TOTAL':<30} {total_files:>4} files  {total_bytes/1024:>8.1f} KB")

    # Parquet Row Counts
    log.info("")
    log.info("  ── Parquet Row Counts ──")
    for label, path in dirs.items():
        for fp in glob.glob(os.path.join(path, "*.parquet")):
            try:
                df = pd.read_parquet(fp)
                log.info(f"  {os.path.basename(fp):<55} {len(df):>6} rows")
            except Exception:
                pass

    # Ingestion Status
    log.info("")
    log.info("  ── Ingestion Status ──")
    stream_files = glob.glob(os.path.join("storage/stream_buffer", "*.parquet"))
    if stream_files:
        total_stream_events = 0
        for fp in stream_files:
            try:
                df = pd.read_parquet(fp)
                total_stream_events += len(df)
            except:
                pass
        log.info(f"  Streaming: {len(stream_files)} files, {total_stream_events} events ingested")
    else:
        log.info("  Streaming: No stream buffer files yet")

    # Source Configuration & Next Ingestion
    log.info("")
    log.info("  ── Source Configuration & Next Ingestion ──")
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
    log.info("  ── Log File Summary ──")
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
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
    log.info("Starting API server...")
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, log_level="info")

def start_iot_consumer():
    """Start the real-time IoT consumer."""
    log.info("Starting IoT consumer...")
    from data_plane.ingestion.real_time_iot_ingest import run_real_time_ingestion
    run_real_time_ingestion()

def run_batch_on_startup():
    """Trigger batch ingestion via API automation."""
    log.info("Starting batch ingestion via API...")
    
    # Wait for API to be ready
    time.sleep(3)
    
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    base_url = "http://localhost:8000"
    
    for source in ALL_SOURCES:
        if source.source_type.value == "file":  # Batch sources are FILE type
            source_id = source.source_id
            raw_path = f"storage/raw/{source_id.replace('src_', '')}.csv"
            
            if os.path.exists(raw_path):
                log.info(f"Loading data from {raw_path} for {source_id}")
                df = pd.read_csv(raw_path)
                records = df.to_dict('records')
                
                # Send in batches if too large
                batch_size = 1000
                for i in range(0, len(records), batch_size):
                    batch = records[i:i+batch_size]
                    response = requests.post(
                        f"{base_url}/ingest/{source_id}",
                        json={"records": batch},
                        headers=headers
                    )
                    if response.status_code == 200:
                        job_id = response.json().get("job_id")
                        log.info(f"Batch ingestion started for {source_id}, job_id: {job_id}")
                    else:
                        log.error(f"Failed to start ingestion for {source_id}: {response.text}")
            else:
                log.warning(f"Raw file not found: {raw_path}")

def main():
    log.info("Starting Supply Chain Ingestion Pipeline (Production Mode)")

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

    # Start IoT consumer
    start_iot_consumer()

    # Start batch ingestion via API automation
    batch_thread = threading.Thread(target=run_batch_on_startup, daemon=True)
    batch_thread.start()

if __name__ == "__main__":
    main() 
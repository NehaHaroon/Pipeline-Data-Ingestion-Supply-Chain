# run_production.py — Production runner for the supply chain ingestion pipeline.

import os
import sys
import threading
import time
import requests
import pandas as pd
import glob
from datetime import datetime, timezone, timedelta

# Setup path and logging
sys.path.insert(0, os.path.dirname(__file__))
from common import setup_logging, ensure_storage_directories, get_ingestion_interval_for_source

log = setup_logging("run_production")

from control_plane.entities import ALL_SOURCES, SourceType
from config import API_HOST, API_PORT, API_TOKEN, LOCAL_API_HOST
from observability_plane.telemetry import JobTelemetry
from db_producer import produce_records

INGESTION_SCALE_FACTOR = int(os.getenv("INGESTION_SCALE_FACTOR", "3"))

# Get storage directories
storage_paths = ensure_storage_directories()

# Use dynamic intervals based on source frequencies
DB_INGESTION_INTERVAL_SECONDS = get_ingestion_interval_for_source("src_inventory_transactions")
WEATHER_INGESTION_INTERVAL_SECONDS = get_ingestion_interval_for_source("src_weather_api")

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

    # DB Producer Status
    log.info("")
    log.info("  -- DB Producer Status --")
    simulated_db_path = "storage/simulated_db/inventory_transactions.jsonl"
    if os.path.exists(simulated_db_path):
        with open(simulated_db_path, 'r', encoding='utf-8') as f:
            record_count = sum(1 for _ in f)
        log.info(f"  Simulated DB records: {record_count}")
        if record_count > 0:
            # Get the latest record timestamp
            import json
            with open(simulated_db_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if lines:
                    latest_record = json.loads(lines[-1])
                    latest_ts = latest_record.get('created_at', 'unknown')
                    log.info(f"  Latest record timestamp: {latest_ts}")
    else:
        log.info("  Simulated DB: File not found")

    # Source Configuration & Next Ingestion
    log.info("")
    log.info("  -- Source Configuration & Next Ingestion --")
    now = datetime.now(timezone.utc)
    for src in ALL_SOURCES:
        freq = src.ingestion_frequency.value
        if freq == "real_time":
            next_time = "Continuous"
        elif freq == "every_2_minutes":
            period_start = now.replace(second=0, microsecond=0)
            minute_bucket = (period_start.minute // 2) * 2
            next_run = period_start.replace(minute=minute_bucket)
            if next_run <= now:
                next_run += timedelta(minutes=2)
            remaining = next_run - now
            next_time = f"{remaining.seconds // 60}m {(remaining.seconds % 60)}s"
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
    log_file = getattr(log, "log_file", None)

    if log_file and os.path.exists(log_file):
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

def wait_for_api_ready(base_url: str = f"http://{LOCAL_API_HOST}:{API_PORT}", timeout_sec: int = 60) -> bool:
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
    """Run one weather API ingestion cycle and return telemetry."""
    log.info("[WEATHER] Starting weather API ingestion cycle")
    cycle_start = time.time()
    try:
        from data_plane.ingestion.batch_ingest import run_api_ingestion
        tel = run_api_ingestion("src_weather_api", "ds_weather_api")
        log.info(
            "[WEATHER] Completed cycle | ingested=%s failed=%s quarantined=%s coerced=%s duration=%.2fs",
            tel.records_ingested,
            tel.records_failed,
            tel.records_quarantined,
            tel.records_coerced,
            time.time() - cycle_start,
        )
        return tel
    except Exception as exc:
        log.error(f"[WEATHER] Ingestion cycle failed: {exc}")
        return None


def run_periodic_weather_ingestion():
    """Continuously ingest weather API data every configured interval."""
    banner("PHASE: WEATHER SCHEDULER", f"Weather ingestion every {WEATHER_INGESTION_INTERVAL_SECONDS}s (from WEATHER_API_SOURCE.ingestion_frequency)")
    run_count = 0
    while True:
        run_count += 1
        log.info(f"[SCHEDULER][WEATHER] Starting run #{run_count}")
        tel = run_weather_api_ingestion()
        if tel and tel.records_ingested == 0 and tel.records_failed > 0:
            log.warning("[SCHEDULER][WEATHER] Run #%s completed with failures and no ingested records", run_count)
        log.info(
            f"[SCHEDULER][WEATHER] Completed run #{run_count}; next run in "
            f"{WEATHER_INGESTION_INTERVAL_SECONDS}s"
        )
        time.sleep(WEATHER_INGESTION_INTERVAL_SECONDS)


def run_periodic_db_ingestion():
    """Continuously execute DB ingestion and emit telemetry every interval."""
    banner("PHASE: DB INGESTION SCHEDULER", f"Database ingestion every {DB_INGESTION_INTERVAL_SECONDS}s (from INVENTORY_TRANSACTIONS_SOURCE.ingestion_frequency)")
    from data_plane.ingestion.db_ingest import ingest_db_source

    run_count = 0
    while True:
        run_count += 1
        log.info(f"[SCHEDULER][DB] Starting DB ingestion run #{run_count}")
        try:
            tel = ingest_db_source()
            log.info(
                "[SCHEDULER][DB] Completed run #%s | ingested=%s failed=%s "
                "quarantined=%s coerced=%s",
                run_count,
                tel.records_ingested,
                tel.records_failed,
                tel.records_quarantined,
                tel.records_coerced,
            )
        except Exception as exc:
            log.error(f"[SCHEDULER][DB] Run #{run_count} failed: {exc}")
        log.info(f"[SCHEDULER][DB] Next run in {DB_INGESTION_INTERVAL_SECONDS}s")
        time.sleep(DB_INGESTION_INTERVAL_SECONDS)


def run_batch_on_startup():
    """Trigger batch ingestion via API automation."""
    banner("PHASE: BATCH INGESTION", "Load raw sources via API")
    batch_sources = [source for source in ALL_SOURCES if source.source_type.value == "file"]
    log.info(f"Preparing batch ingestion for {len(batch_sources)} file-based source(s)")

    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    base_url = f"http://{API_HOST}:{API_PORT}"
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
        if INGESTION_SCALE_FACTOR > 1:
            # Intentional replay-style upscaling for stress/volume testing.
            records = records * INGESTION_SCALE_FACTOR
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

def main():
    banner("PRODUCTION STARTUP", "Batch + Scheduler + Dashboard + DB Producer")

    log.info("Starting Supply Chain Ingestion Pipeline (Batch Runner Mode)")
    log.info(f"Registered sources: {len(ALL_SOURCES)}")
    log.info("API expected at: http://ingestion-api:8000")

    # Start ONLY ONCE
    threading.Thread(target=periodic_dashboard, daemon=True).start()
    threading.Thread(target=run_batch_on_startup, daemon=True).start()
    threading.Thread(target=run_periodic_weather_ingestion, daemon=True).start()
    threading.Thread(target=run_periodic_db_ingestion, daemon=True).start()
    threading.Thread(target=produce_records, daemon=True).start()

    # Initial dashboard
    time.sleep(5)
    print_dashboard()

    log.info("Batch runner started successfully")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down batch runner")

if __name__ == "__main__":
    main() 
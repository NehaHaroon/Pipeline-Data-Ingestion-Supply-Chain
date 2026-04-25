import os
import sys
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from control_plane.entities import OperationType, EventEnvelope
from observability_plane.telemetry import JobTelemetry, Heartbeat
from kafka_consumer import IoTConsumer
from config import API_TOKEN, API_HOST, API_PORT

log = logging.getLogger("real_time_iot_ingest")

# Storage
STREAM_BUFFER_DIR = "storage/stream_buffer"
QUARANTINE_DIR    = "storage/quarantine"
os.makedirs(STREAM_BUFFER_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)

# Config
FLUSH_INTERVAL_EVENTS = 100
TELEMETRY_FLUSH_INTERVAL = 30  # seconds


# ─────────────────────────────────────────────
# API SENDER
# ─────────────────────────────────────────────

def send_to_api(records: List[Dict]):
    if not records:
        return

    try:
        headers = {
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json"
        }

        url = f"http://{API_HOST}:{API_PORT}/ingest/src_iot_rfid_stream"

        response = requests.post(
            url,
            json={"records": records},
            headers=headers,
            timeout=5
        )

        if response.status_code == 200:
            job_id = response.json().get("job_id")
            log.info(f"[IOT-API] Sent {len(records)} records → job_id={job_id}")
        else:
            log.error(f"[IOT-API-FAIL] {response.status_code} {response.text}")

    except Exception as e:
        log.error(f"[IOT-API-ERROR] {e}")


# ─────────────────────────────────────────────
# EVENT PROCESSING
# ─────────────────────────────────────────────

def process_iot_event(event: Dict[str, Any], buffer: List[Dict], tel: JobTelemetry):
    event_id = event.get("event_id", "unknown")

    try:
        # Wrap in envelope (optional but recommended)
        envelope = EventEnvelope(
            payload=event,
            source_id="src_iot_rfid_stream",
            dataset_id="ds_iot_rfid_stream",
            schema_version="v1",
            operation_type=OperationType.INSERT,
            event_timestamp=event["timestamp"],
            source_timestamp=event["timestamp"],
        )

        buffer.append(envelope.to_dict())
        tel.record_ok()

        log.info(
            f"[IOT] {event_id[:8]} | {event.get('product_id')} "
            f"| stock={event.get('current_stock_on_shelf')}"
        )

    except Exception as e:
        tel.record_fail()
        log.error(f"[IOT-ERROR] event_id={event_id} error={e}")


# ─────────────────────────────────────────────
# FLUSH
# ─────────────────────────────────────────────

def flush(buffer: List[Dict], flush_count: int):
    if not buffer:
        return

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    # 1. Send to API
    send_to_api(buffer)

    # 2. Write to data lake (IMPORTANT: include source_id in filename)
    path = os.path.join(
        STREAM_BUFFER_DIR,
        f"src_iot_rfid_stream_{ts}_batch{flush_count:03d}.parquet"
    )

    pd.DataFrame(buffer).to_parquet(path, index=False)
    log.info(f"[IOT-WRITE] {len(buffer)} records → {path}")


# ─────────────────────────────────────────────
# MAIN INGESTION
# ─────────────────────────────────────────────

def run_real_time_ingestion():
    job_id = f"job_iot_realtime_{int(time.time())}"
    tel = JobTelemetry(job_id=job_id, source_id="src_iot_rfid_stream")

    log.info("=" * 70)
    log.info(f"REAL-TIME IOT INGESTION STARTED | job={job_id}")
    log.info("=" * 70)

    tel.mark_start()

    # Start heartbeat
    hb = Heartbeat(job_id, tel)
    hb.start()

    buffer: List[Dict] = []
    flush_count = 0
    last_telemetry_flush = time.time()

    def callback(event: Dict[str, Any]):
        nonlocal buffer, flush_count, last_telemetry_flush

        process_iot_event(event, buffer, tel)

        # Flush by size
        if len(buffer) >= FLUSH_INTERVAL_EVENTS:
            flush_count += 1
            flush(buffer, flush_count)
            buffer.clear()

        #  FIX: periodic telemetry persistence
        if time.time() - last_telemetry_flush > TELEMETRY_FLUSH_INTERVAL:
            tel.save_report()
            log.info("[TELEMETRY] Periodic flush saved")
            last_telemetry_flush = time.time()

    consumer = IoTConsumer()

    try:
        consumer.consume_and_process(callback)

    except KeyboardInterrupt:
        log.info("Stopping IoT ingestion...")

    except Exception as e:
        log.error(f"[FATAL] IoT ingestion crashed: {e}")

    finally:
        # Final flush
        if buffer:
            flush_count += 1
            flush(buffer, flush_count)

        tel.mark_end()
        tel.log_report()

        hb.stop()

        log.info("=" * 70)
        log.info("IOT INGESTION STOPPED")
        log.info("=" * 70)


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    run_real_time_ingestion()
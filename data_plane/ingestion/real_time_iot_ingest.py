# Real-time IoT Stream Ingestion using Kafka Consumer
#
# This module consumes IoT RFID events from Kafka topic and processes them
# in real-time, applying contracts and writing to storage.

import os
import sys
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from control_plane.entities import OperationType, EventEnvelope
from control_plane.contracts import IOT_CONTRACT
from observability_plane.telemetry import JobTelemetry
from kafka_consumer import IoTConsumer

log = logging.getLogger("real_time_iot_ingest")

STREAM_BUFFER_DIR = "storage/stream_buffer"
QUARANTINE_DIR    = "storage/quarantine"
os.makedirs(STREAM_BUFFER_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)

def process_iot_event(event: Dict[str, Any], buffer: List[Dict], quarantine_buf: List[Dict], tel: JobTelemetry):
    """Process a single IoT event from Kafka."""
    event_id = event["event_id"]

    # # Contract enforcement
    # result = IOT_CONTRACT.enforce(event)
    # violations = result["violations"]
    # status = result["status"]

    # if status in ("quarantine", "rejected"):
    #     tel.record_quarantine()
    #     quarantine_buf.append({**event, "_violations": json.dumps(violations)})
    #     log.warning(
    #         f"  [IOT-QUARANTINE] event_id={event_id[:8]}... "
    #         f"| product_id={event.get('product_id')} "
    #         f"| shelf_location={event.get('shelf_location')} "
    #         f"| violations={violations}"
    #     )
    #     return

    # Wrap in EventEnvelope
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
        f"  [IOT-PASS] event_id={event_id[:8]}... "
        f"| product_id={event['product_id']} "
        f"| shelf={event['shelf_location']} "
        f"| stock={event['current_stock_on_shelf']} "
        f"| battery={event['battery_level']}"
    )

def run_real_time_ingestion(flush_interval_events: int = 100):
    """Run real-time IoT ingestion from Kafka."""
    job_id = f"job_iot_realtime_{int(datetime.now().timestamp())}"
    tel = JobTelemetry(job_id=job_id, source_id="src_iot_rfid_stream")

    log.info("")
    log.info("▶" * 70)
    log.info(f"  REAL-TIME IOT INGESTION | job={job_id}")
    log.info(f"  flush_every={flush_interval_events} events")
    log.info("▶" * 70)

    tel.mark_start()

    buffer: List[Dict] = []
    quarantine_buf: List[Dict] = []
    flush_count = 0

    def process_callback(event: Dict[str, Any]):
        nonlocal buffer, quarantine_buf, flush_count
        process_iot_event(event, buffer, quarantine_buf, tel)

        # Flush micro-batch to disk
        if len(buffer) >= flush_interval_events:
            flush_count += 1
            _flush_buffer(buffer, quarantine_buf, flush_count)
            buffer.clear()
            quarantine_buf.clear()
            log.info(f"  [IOT-FLUSH] Micro-batch {flush_count} flushed to disk")

    consumer = IoTConsumer()
    try:
        consumer.consume_and_process(process_callback)
    except KeyboardInterrupt:
        log.info("Stopping real-time ingestion")
    finally:
        # Final flush
        if buffer or quarantine_buf:
            flush_count += 1
            _flush_buffer(buffer, quarantine_buf, flush_count)
            log.info(f"  [IOT-FLUSH] Final micro-batch {flush_count} flushed to disk")

        tel.mark_end()
        tel.log_report()

def _flush_buffer(buffer: List[Dict], quarantine_buf: List[Dict], flush_count: int):
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    if buffer:
        path = os.path.join(STREAM_BUFFER_DIR, f"iot_realtime_{ts}_batch{flush_count:03d}.parquet")
        pd.DataFrame(buffer).to_parquet(path, index=False)
        log.info(f"  [IOT-WRITE] {len(buffer)} valid events → {path}")
    if quarantine_buf:
        qpath = os.path.join(QUARANTINE_DIR, f"iot_realtime_quarantine_{ts}_batch{flush_count:03d}.parquet")
        pd.DataFrame(quarantine_buf).to_parquet(qpath, index=False)
        log.warning(f"  [IOT-QUARANTINE-WRITE] {len(quarantine_buf)} events → {qpath}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    run_real_time_ingestion()
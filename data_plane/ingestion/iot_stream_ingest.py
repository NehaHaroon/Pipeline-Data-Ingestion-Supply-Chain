# Phase 4 (IoT Extension) — Simulated IoT RFID Stream Ingestion
#
# Since we don't have a live Kafka broker, this module simulates the real_time_stream.py
# behavior by generating synthetic IoT events in a loop and flushing them to storage
# in configurable micro-batch windows.
#
# Design mirrors real_time_stream.py exactly:
#   • Generates RFID pings for real product_ids from warehouse_master
#   • ~3% duplicate sensor pings (management policy test)
#   • Applies IoT contract (QUARANTINE policy for invalid zone / stock range)
#   • Flushes every flush_interval_sec into storage/stream_buffer/

import os
import sys
import json
import time
import random
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from control_plane.entities import (
    OperationType, EventEnvelope, IOT_SOURCE, IOT_DATASET
)
from control_plane.contracts import IOT_CONTRACT
from observability_plane.telemetry import JobTelemetry, Heartbeat

log = logging.getLogger("iot_stream_ingest")

STREAM_BUFFER_DIR = "storage/stream_buffer"
QUARANTINE_DIR    = "storage/quarantine"
os.makedirs(STREAM_BUFFER_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)

ZONES = ["ZONE-A", "ZONE-B", "ZONE-C"]


def _generate_rfid_event(product_ids: List[str]) -> Dict[str, Any]:
    """Simulate one RFID ping — mirrors real_time_stream.py exactly."""
    return {
        "event_id":               str(uuid.uuid4()),
        "timestamp":              datetime.now(timezone.utc).isoformat(),
        "product_id":             random.choice(product_ids),
        "shelf_location":         random.choice(ZONES),
        "current_stock_on_shelf": random.randint(5, 150),
        "battery_level":          f"{random.randint(10, 100)}%",
    }


def run_stream_simulation(
    product_ids:         List[str],
    total_events:        int   = 300,
    flush_interval_events: int = 100,
    duplicate_rate:      float = 0.03,
) -> JobTelemetry:
    """
    Simulate IoT RFID stream ingestion.

    Args:
        product_ids          : real product IDs from warehouse_master
        total_events         : total events to generate
        flush_interval_events: how many events per micro-batch flush to disk
        duplicate_rate       : fraction of events that are duplicate sensor pings

    Logs every event with:
      [IOT-PASS]      | valid event written to buffer
      [IOT-DUPLICATE] | duplicate event_id detected, deduplicated
      [IOT-QUARANTINE]| contract violation (bad zone, stock out of range)
    """
    job_id = f"job_iot_stream_{int(time.time())}"
    tel    = JobTelemetry(job_id=job_id, source_id="src_iot_rfid_stream")
    hb     = Heartbeat(job_id, tel)

    log.info("")
    log.info("▶" * 70)
    log.info(f"  IOT STREAM SIMULATION | job={job_id}")
    log.info(f"  total_events={total_events} | flush_every={flush_interval_events} events")
    log.info(f"  duplicate_injection_rate={duplicate_rate:.0%}")
    log.info("▶" * 70)

    tel.mark_start()
    hb.start()

    seen_event_ids: set          = set()
    buffer:         List[Dict]   = []
    quarantine_buf: List[Dict]   = []
    flush_count     = 0
    last_event: Dict[str, Any] = {}

    for i in range(1, total_events + 1):
        # Generate or duplicate
        if last_event and random.random() < duplicate_rate:
            event = dict(last_event)  # exact sensor re-ping (same event_id)
            is_dup = True
        else:
            event   = _generate_rfid_event(product_ids)
            is_dup  = False
            last_event = event

        event_id = event["event_id"]

        # Deduplication
        if event_id in seen_event_ids:
            tel.record_fail()
            log.warning(
                f"  [IOT-DUPLICATE] event={i} | event_id={event_id[:8]}... "
                f"| product_id={event.get('product_id')} "
                f"| reason=duplicate_sensor_ping_policy | discarded=True"
            )
            continue

        seen_event_ids.add(event_id)

        # Contract enforcement
        result     = IOT_CONTRACT.enforce(event)
        violations = result["violations"]
        status     = result["status"]

        if status in ("quarantine", "rejected"):
            tel.record_quarantine()
            quarantine_buf.append({**event, "_violations": json.dumps(violations)})
            log.warning(
                f"  [IOT-QUARANTINE] event={i} | event_id={event_id[:8]}... "
                f"| product_id={event.get('product_id')} "
                f"| shelf_location={event.get('shelf_location')} "
                f"| violations={violations}"
            )
            continue

        # Wrap in EventEnvelope
        envelope = EventEnvelope(
            payload          = event,
            source_id        = "src_iot_rfid_stream",
            dataset_id       = "ds_iot_rfid_stream",
            schema_version   = "v1",
            operation_type   = OperationType.INSERT,
            event_timestamp  = event["timestamp"],
            source_timestamp = event["timestamp"],
        )
        buffer.append(envelope.to_dict())
        tel.record_ok()

        log.info(
            f"  [IOT-PASS] event={i} | event_id={event_id[:8]}... "
            f"| product_id={event['product_id']} "
            f"| shelf={event['shelf_location']} "
            f"| stock={event['current_stock_on_shelf']} "
            f"| battery={event['battery_level']}"
        )

        # Flush micro-batch to disk
        if len(buffer) >= flush_interval_events:
            flush_count += 1
            _flush_buffer(buffer, quarantine_buf, flush_count)
            buffer.clear()
            quarantine_buf.clear()
            log.info(f"  [IOT-FLUSH] Micro-batch {flush_count} flushed to disk")

    # Final flush
    if buffer or quarantine_buf:
        flush_count += 1
        _flush_buffer(buffer, quarantine_buf, flush_count)
        log.info(f"  [IOT-FLUSH] Final micro-batch {flush_count} flushed to disk")

    hb.stop()
    tel.mark_end()

    log.info("")
    log.info(f"  [IOT STREAM COMPLETE] total_flushes={flush_count}")
    tel.log_report()
    return tel


def _flush_buffer(buffer: List[Dict], quarantine_buf: List[Dict], flush_count: int):
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    if buffer:
        path = os.path.join(STREAM_BUFFER_DIR, f"iot_stream_{ts}_batch{flush_count:03d}.parquet")
        pd.DataFrame(buffer).to_parquet(path, index=False)
        log.info(f"  [IOT-WRITE] {len(buffer)} valid events → {path}")
    if quarantine_buf:
        qpath = os.path.join(QUARANTINE_DIR, f"iot_quarantine_{ts}_batch{flush_count:03d}.parquet")
        pd.DataFrame(quarantine_buf).to_parquet(qpath, index=False)
        log.warning(f"  [IOT-QUARANTINE-WRITE] {len(quarantine_buf)} events → {qpath}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    wh_df       = pd.read_csv("storage/raw/warehouse_master.csv")
    product_ids = wh_df["product_id"].tolist()
    run_stream_simulation(product_ids, total_events=300, flush_interval_events=100)

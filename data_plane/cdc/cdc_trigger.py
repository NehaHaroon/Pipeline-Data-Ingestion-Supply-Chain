# Last Updated: 2026-04-05
# Phase 5 — CDC Trigger: data generators emit INSERT / UPDATE / DELETE events.
# Tests two scenarios: Steady Stream (10 rec/s) and Burst (5000 in 1 second).

import os, sys, time, random, logging
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict, Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from control_plane.entities import OperationType, EventEnvelope
from observability_plane.telemetry import JobTelemetry, Heartbeat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

CDC_LOG_DIR = "storage/cdc_log"
os.makedirs(CDC_LOG_DIR, exist_ok=True)


def load_existing_records(parquet_path: str) -> List[Dict]:
    """Load ingested records to simulate existing rows for UPDATE/DELETE."""
    if not os.path.exists(parquet_path):
        return []
    df = pd.read_parquet(parquet_path)
    return df.to_dict("records")


def emit_cdc_event(record: Dict[str, Any], op: OperationType,
                   source_id: str, dataset_id: str) -> Dict[str, Any]:
    """Wrap a change event in an EventEnvelope and return as dict."""
    envelope = EventEnvelope(
        payload=record,
        source_id=source_id,
        dataset_id=dataset_id,
        schema_version="v1",
        operation_type=op,
        event_timestamp=datetime.now(timezone.utc).isoformat(),
    )
    return envelope.to_dict()


def cdc_generator(existing: List[Dict], generator_fn, source_id: str,
                  dataset_id: str) -> Dict[str, Any]:
    """
    Randomly emit INSERT (new record), UPDATE (mutate existing), or DELETE.
    Weight: INSERT=60%, UPDATE=30%, DELETE=10%
    """
    op_choice = random.choices(
        [OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE],
        weights=[60, 30, 10]
    )[0]

    if op_choice == OperationType.INSERT or not existing:
        record = generator_fn()
        record["_cdc_op"] = "INSERT"
        return emit_cdc_event(record, OperationType.INSERT, source_id, dataset_id)

    target = random.choice(existing)

    if op_choice == OperationType.UPDATE:
        # Mutate one numeric field to simulate a real update
        target = dict(target)
        for key in ["quantity_produced", "units_sold", "current_stock_on_shelf", "unit_cost"]:
            if key in target and target[key] is not None:
                try:
                    target[key] = round(float(target[key]) * random.uniform(0.8, 1.2), 2)
                    break
                except (TypeError, ValueError):
                    pass
        target["_cdc_op"] = "UPDATE"
        target["_updated_at"] = datetime.now(timezone.utc).isoformat()
        return emit_cdc_event(target, OperationType.UPDATE, source_id, dataset_id)

    # DELETE
    target = dict(target)
    target["_cdc_op"] = "DELETE"
    target["_deleted_at"] = datetime.now(timezone.utc).isoformat()
    return emit_cdc_event(target, OperationType.DELETE, source_id, dataset_id)


def run_steady_stream(source_id: str, dataset_id: str, existing: List[Dict],
                      generator_fn, duration_sec: int = 10,
                      target_rps: int = 10) -> JobTelemetry:
    """
    Steady Stream Scenario: emit target_rps records/second for duration_sec seconds.
    Tests whether CDC catches every change without lag.
    """
    job_id = f"cdc_steady_{source_id}_{int(time.time())}"
    tel    = JobTelemetry(job_id=job_id, source_id=source_id)
    hb     = Heartbeat(job_id, tel)

    log.info(f"\n{'─'*55}")
    log.info(f"  CDC STEADY STREAM | {source_id} | {target_rps} rec/s for {duration_sec}s")
    log.info(f"{'─'*55}")

    tel.mark_start()
    hb.start()
    start = time.time()

    events = []
    interval = 1.0 / target_rps

    while time.time() - start < duration_sec:
        tick = time.time()
        event = cdc_generator(existing, generator_fn, source_id, dataset_id)
        events.append(event)
        tel.record_ok()
        sleep_time = interval - (time.time() - tick)
        if sleep_time > 0:
            time.sleep(sleep_time)

    hb.stop()
    tel.mark_end()
    end = time.time()

    # Persist CDC log
    out = os.path.join(CDC_LOG_DIR, f"{source_id}_steady_cdc.parquet")
    pd.DataFrame(events).to_parquet(out, index=False)

    log.info(f"[TIMER] CDC Steady | source={source_id} | "
             f"start={datetime.fromtimestamp(start,timezone.utc).isoformat()} | "
             f"end={datetime.fromtimestamp(end,timezone.utc).isoformat()} | "
             f"duration={end-start:.4f}s")
    log.info(f"[CDC STEADY] Emitted {len(events)} events | "
             f"Actual throughput: {len(events)/(end-start):.1f} rec/s → saved to {out}")
    tel.log_report()
    return tel


def run_burst(source_id: str, dataset_id: str, existing: List[Dict],
              generator_fn, burst_count: int = 5000) -> JobTelemetry:
    """
    Burst Scenario: emit 5000 records as fast as possible in ~1 second.
    Tests buffer capacity — does the CDC system drop or process the backlog?
    """
    job_id = f"cdc_burst_{source_id}_{int(time.time())}"
    tel    = JobTelemetry(job_id=job_id, source_id=source_id)
    hb     = Heartbeat(job_id, tel)

    log.info(f"\n{'─'*55}")
    log.info(f"  CDC BURST SCENARIO | {source_id} | {burst_count} records in 1 burst")
    log.info(f"{'─'*55}")

    tel.mark_start()
    hb.start()
    start = time.time()

    events = []
    for _ in range(burst_count):
        event = cdc_generator(existing, generator_fn, source_id, dataset_id)
        events.append(event)
        tel.record_ok()

    hb.stop()
    tel.mark_end()
    end = time.time()

    out = os.path.join(CDC_LOG_DIR, f"{source_id}_burst_cdc.parquet")
    pd.DataFrame(events).to_parquet(out, index=False)

    op_counts = pd.DataFrame(events)["_operation_type"].value_counts().to_dict()
    log.info(f"[TIMER] CDC Burst | source={source_id} | "
             f"start={datetime.fromtimestamp(start,timezone.utc).isoformat()} | "
             f"end={datetime.fromtimestamp(end,timezone.utc).isoformat()} | "
             f"duration={end-start:.4f}s")
    log.info(f"[CDC BURST] Emitted {len(events)} events in {end-start:.3f}s | "
             f"Throughput: {len(events)/(end-start):.0f} rec/s | ops={op_counts}")
    tel.log_report()
    return tel

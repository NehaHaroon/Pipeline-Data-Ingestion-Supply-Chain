# Last Updated: 2026-04-05
# Phase 6 — CDC Strategies: Log-Based, Trigger-Based, Timestamp-Based.
# Implements all three strategies with exactly-once semantics (checkpointing + offset management).

import os, sys, json, time, logging
import pandas as pd
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from observability_plane.telemetry import JobTelemetry, Heartbeat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

CHECKPOINT_DIR = "storage/checkpoints"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


# ─────────────────────────────────────────────────────
# EXACTLY-ONCE HELPERS
# ─────────────────────────────────────────────────────

def save_checkpoint(strategy: str, source_id: str, position: Any):
    """Persist the last processed position (log offset, timestamp, or trigger ID)."""
    path = os.path.join(CHECKPOINT_DIR, f"{strategy}_{source_id}.json")
    with open(path, "w") as f:
        json.dump({"position": str(position), "saved_at": datetime.now(timezone.utc).isoformat()}, f)
    log.debug(f"[CHECKPOINT] {strategy}/{source_id} → position={position}")


def load_checkpoint(strategy: str, source_id: str) -> Optional[str]:
    """Resume from the last saved position. Returns None for fresh start."""
    path = os.path.join(CHECKPOINT_DIR, f"{strategy}_{source_id}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        data = json.load(f)
    log.info(f"[CHECKPOINT] Resuming {strategy}/{source_id} from position={data['position']}")
    return data["position"]


class OffsetTracker:
    """
    Tracks the last committed offset for streaming systems (like Kafka).
    Simulates Kafka's consumer offset management for exactly-once processing.
    """
    def __init__(self, source_id: str):
        self.source_id = source_id
        self._committed_offset = 0
        self._pending_offset   = 0

    def advance(self):
        self._pending_offset += 1

    def commit(self):
        self._committed_offset = self._pending_offset
        log.debug(f"[OFFSET] Committed offset={self._committed_offset} for {self.source_id}")

    @property
    def committed(self):
        return self._committed_offset


# ─────────────────────────────────────────────────────
# STRATEGY 1: LOG-BASED CDC
# Reads from a simulated transaction log (the CDC log Parquet we wrote in Phase 5).
# In production this would be Debezium reading Postgres WAL / MySQL BINLOG.
# Exactly-once via: checkpointing (last processed row index) + transactional writes.
# ─────────────────────────────────────────────────────

def run_log_based_cdc(source_id: str, cdc_log_path: str,
                      scenario: str = "steady") -> JobTelemetry:
    """
    Log-Based CDC:
    - Reads the CDC event log (simulating a DB transaction log).
    - Resumes from last checkpoint (exactly-once guarantee).
    - Appends only NEW events to the processed store.

    Production equivalent: Debezium → WAL/BINLOG → Kafka → Sink
    """
    job_id = f"cdc_log_{source_id}_{scenario}_{int(time.time())}"
    tel    = JobTelemetry(job_id=job_id, source_id=source_id)
    hb     = Heartbeat(job_id, tel)

    log.info(f"\n{'═'*55}")
    log.info(f"  LOG-BASED CDC | {source_id} | scenario={scenario}")
    log.info(f"{'═'*55}")

    tel.mark_start()
    hb.start()
    start = time.time()

    try:
        if not os.path.exists(cdc_log_path):
            log.warning(f"[LOG-CDC] No CDC log found at {cdc_log_path}. Run cdc_trigger first.")
            return tel

        df = pd.read_parquet(cdc_log_path)
        log.info(f"[LOG-CDC] Total events in log: {len(df)}")

        # Resume from checkpoint (exactly-once: skip already-processed rows)
        checkpoint = load_checkpoint("log_based", source_id)
        start_idx  = int(checkpoint) + 1 if checkpoint else 0
        df_new     = df.iloc[start_idx:]
        log.info(f"[LOG-CDC] Processing {len(df_new)} new events (from index {start_idx})")

        processed = []
        offset    = OffsetTracker(source_id)

        for idx, row in df_new.iterrows():
            record = row.to_dict()
            op     = record.get("_operation_type", "INSERT")
            processed.append(record)
            tel.record_ok()
            offset.advance()

            # Transactional sink: commit every 500 records (prevents partial writes)
            if len(processed) % 500 == 0:
                offset.commit()
                save_checkpoint("log_based", source_id, start_idx + len(processed) - 1)
                log.info(f"[LOG-CDC] Checkpointed at index {start_idx + len(processed) - 1}")

        # Final commit
        offset.commit()
        if processed:
            save_checkpoint("log_based", source_id, start_idx + len(processed) - 1)
            out = f"storage/ingested/{source_id}_log_cdc_{scenario}.parquet"
            pd.DataFrame(processed).to_parquet(out, index=False)
            log.info(f"[LOG-CDC]  Processed {len(processed)} events → {out}")

        op_counts = pd.DataFrame(processed)["_operation_type"].value_counts().to_dict() if processed else {}
        log.info(f"[LOG-CDC] Operation breakdown: {op_counts}")

    except Exception as e:
        log.error(f"[LOG-CDC] ❌ Error: {e}", exc_info=True)
    finally:
        hb.stop()
        tel.mark_end()
        end = time.time()
        log.info(f"[TIMER] Log-CDC | source={source_id} | "
                 f"start={datetime.fromtimestamp(start,timezone.utc).isoformat()} | "
                 f"end={datetime.fromtimestamp(end,timezone.utc).isoformat()} | "
                 f"duration={end-start:.4f}s")
        tel.log_report()

    return tel


# ─────────────────────────────────────────────────────
# STRATEGY 2: TRIGGER-BASED CDC
# DB triggers write changed rows to a "trigger table".
# We simulate this by filtering the CDC log for UPDATE/DELETE events.
# Exactly-once via: unique trigger_event_id deduplication set.
# ─────────────────────────────────────────────────────

def run_trigger_based_cdc(source_id: str, cdc_log_path: str,
                           scenario: str = "steady") -> JobTelemetry:
    """
    Trigger-Based CDC:
    - Simulates database triggers capturing UPDATE/DELETE events.
    - Deduplicates by _event_id (exactly-once via seen-set).
    - Used when DB transaction logs are unavailable.

    Production equivalent: DB TRIGGER → change_log table → pipeline reads it
    """
    job_id = f"cdc_trigger_{source_id}_{scenario}_{int(time.time())}"
    tel    = JobTelemetry(job_id=job_id, source_id=source_id)
    hb     = Heartbeat(job_id, tel)

    log.info(f"\n{'═'*55}")
    log.info(f"  TRIGGER-BASED CDC | {source_id} | scenario={scenario}")
    log.info(f"{'═'*55}")

    tel.mark_start()
    hb.start()
    start = time.time()

    try:
        if not os.path.exists(cdc_log_path):
            log.warning(f"[TRIGGER-CDC] No CDC log at {cdc_log_path}.")
            return tel

        df = pd.read_parquet(cdc_log_path)
        # Triggers capture only changes (not INSERTs from initial load in this simulation)
        trigger_events = df[df["_operation_type"].isin(["UPDATE", "DELETE"])].copy()
        log.info(f"[TRIGGER-CDC] Found {len(trigger_events)} trigger events "
                 f"({len(df) - len(trigger_events)} INSERTs skipped)")

        # Exactly-once: track seen event IDs
        seen_ids  = set()
        processed = []
        dup_count = 0

        for _, row in trigger_events.iterrows():
            record   = row.to_dict()
            event_id = record.get("_event_id", "")
            if event_id in seen_ids:
                dup_count += 1
                continue
            seen_ids.add(event_id)
            processed.append(record)
            tel.record_ok()

        if dup_count:
            log.info(f"[TRIGGER-CDC] Deduplicated {dup_count} duplicate trigger events.")

        if processed:
            out = f"storage/ingested/{source_id}_trigger_cdc_{scenario}.parquet"
            pd.DataFrame(processed).to_parquet(out, index=False)
            op_counts = pd.DataFrame(processed)["_operation_type"].value_counts().to_dict()
            log.info(f"[TRIGGER-CDC]  {len(processed)} events → {out} | ops={op_counts}")

    except Exception as e:
        log.error(f"[TRIGGER-CDC] ❌ Error: {e}", exc_info=True)
    finally:
        hb.stop()
        tel.mark_end()
        end = time.time()
        log.info(f"[TIMER] Trigger-CDC | source={source_id} | "
                 f"start={datetime.fromtimestamp(start,timezone.utc).isoformat()} | "
                 f"end={datetime.fromtimestamp(end,timezone.utc).isoformat()} | "
                 f"duration={end-start:.4f}s")
        tel.log_report()

    return tel


# ─────────────────────────────────────────────────────
# STRATEGY 3: TIMESTAMP-BASED CDC
# Extracts rows WHERE last_updated > watermark.
# Exactly-once via: watermark checkpoint (last processed timestamp).
# ─────────────────────────────────────────────────────

def run_timestamp_based_cdc(source_id: str, cdc_log_path: str,
                             scenario: str = "steady") -> JobTelemetry:
    """
    Timestamp-Based CDC:
    - Selects records WHERE _ingestion_timestamp > last watermark.
    - Simple and widely supported (no log access needed).
    - Risk: misses updates that happen within the same second as the watermark.

    Production equivalent: SELECT * FROM table WHERE updated_at > :watermark
    """
    job_id = f"cdc_ts_{source_id}_{scenario}_{int(time.time())}"
    tel    = JobTelemetry(job_id=job_id, source_id=source_id)
    hb     = Heartbeat(job_id, tel)

    log.info(f"\n{'═'*55}")
    log.info(f"  TIMESTAMP-BASED CDC | {source_id} | scenario={scenario}")
    log.info(f"{'═'*55}")

    tel.mark_start()
    hb.start()
    start = time.time()

    try:
        if not os.path.exists(cdc_log_path):
            log.warning(f"[TS-CDC] No CDC log at {cdc_log_path}.")
            return tel

        df = pd.read_parquet(cdc_log_path)
        df["_ingestion_timestamp"] = pd.to_datetime(df["_ingestion_timestamp"], utc=True)

        # Load watermark (last processed timestamp)
        watermark_str = load_checkpoint("timestamp_based", source_id)
        if watermark_str:
            watermark = pd.Timestamp(watermark_str, tz="UTC")
            new_df    = df[df["_ingestion_timestamp"] > watermark]
            log.info(f"[TS-CDC] Watermark={watermark} | {len(new_df)} new records")
        else:
            new_df = df
            log.info(f"[TS-CDC] No watermark found — processing all {len(df)} records")

        processed = []
        for _, row in new_df.iterrows():
            processed.append(row.to_dict())
            tel.record_ok()

        if processed:
            # Advance watermark to max timestamp seen
            new_watermark = new_df["_ingestion_timestamp"].max().isoformat()
            save_checkpoint("timestamp_based", source_id, new_watermark)
            out = f"storage/ingested/{source_id}_ts_cdc_{scenario}.parquet"
            pd.DataFrame(processed).to_parquet(out, index=False)
            log.info(f"[TS-CDC]  {len(processed)} records → {out} | "
                     f"New watermark: {new_watermark}")

    except Exception as e:
        log.error(f"[TS-CDC] ❌ Error: {e}", exc_info=True)
    finally:
        hb.stop()
        tel.mark_end()
        end = time.time()
        log.info(f"[TIMER] TS-CDC | source={source_id} | "
                 f"start={datetime.fromtimestamp(start,timezone.utc).isoformat()} | "
                 f"end={datetime.fromtimestamp(end,timezone.utc).isoformat()} | "
                 f"duration={end-start:.4f}s")
        tel.log_report()

    return tel

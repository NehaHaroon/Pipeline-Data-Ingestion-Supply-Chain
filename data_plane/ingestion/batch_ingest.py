# Phase 4 — Batch Ingestion: Initial Load + Micro-Batch
#
# Two ingestion modes:
#   1. run_all_batch_ingestion()  — full initial load of all 4 CSV sources
#   2. run_micro_batch_ingestion()— time-windowed micro-batches (simulates near-real-time)
#
# Every record produces a detailed log line:
#   [PASS] | [QUARANTINE] | [REJECT] | [COERCE] with exact field-level reasons.

import os
import sys
import time
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

# Resolve project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from control_plane.entities import (
    OperationType, EventEnvelope,
    WAREHOUSE_SOURCE, MANUFACTURING_SOURCE, SALES_SOURCE, LEGACY_SOURCE,
    WAREHOUSE_DATASET, MANUFACTURING_DATASET, SALES_DATASET, LEGACY_DATASET
)
from control_plane.contracts import CONTRACT_REGISTRY, ViolationPolicy
from observability_plane.telemetry import JobTelemetry, Heartbeat

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("batch_ingest")

# ── Storage directories ────────────────────────────────────────────────────────
OUTPUT_DIR      = "storage/ingested"
QUARANTINE_DIR  = "storage/quarantine"
MICRO_BATCH_DIR = "storage/micro_batch"
DETAIL_LOG_DIR  = "storage/ingested/detail_logs"

for d in [OUTPUT_DIR, QUARANTINE_DIR, MICRO_BATCH_DIR, DETAIL_LOG_DIR]:
    os.makedirs(d, exist_ok=True)

# ── Deduplication keys per source ─────────────────────────────────────────────
DEDUP_KEY_MAP = {
    "src_sales_history":      "receipt_id",
    "src_manufacturing_logs": "production_batch_id",
    "src_warehouse_master":   "product_id",
    "src_legacy_trends":      "old_product_code",   # pre-normalization name
}

# ── Normalization log ──────────────────────────────────────────────────────────
_NORMALIZATION_LOG: List[Dict] = []


# ──────────────────────────────────────────────────────────────────────────────
# RECORD NORMALIZATION
# ──────────────────────────────────────────────────────────────────────────────

def normalize_record(record: Dict[str, Any], source_id: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Apply normalization rules before contract enforcement.
    Returns (normalized_record, list_of_change_descriptions).

    Rules:
      • All ID fields (product_id, article_id, batch_id, old_product_code) → UPPERCASE
      • All timestamp fields → ISO-8601 UTC
      • Manufacturing defect_count null → 0.0 (imputation)
      • Legacy old_product_code → product_id (schema migration)
    """
    changes: List[str] = []

    # Uppercase ID fields
    id_fields = ["product_id", "article_id", "production_batch_id", "old_product_code"]
    for key in id_fields:
        val = record.get(key)
        if isinstance(val, str) and val != val.upper():
            record[key] = val.upper()
            changes.append(f"NORMALIZE | field={key} | '{val}' → '{record[key]}' | reason=uppercase_policy")

    # ISO-8601 timestamp normalization
    ts_fields = ["mfg_timestamp", "sale_timestamp", "timestamp", "historical_timestamp"]
    for key in ts_fields:
        val = record.get(key)
        if val is not None:
            try:
                parsed = pd.to_datetime(val, utc=True)
                iso    = parsed.isoformat()
                if iso != val:
                    changes.append(f"NORMALIZE | field={key} | '{val}' → '{iso}' | reason=iso8601_utc")
                record[key] = iso
            except Exception as e:
                changes.append(f"NORMALIZE | field={key} | parse_error='{e}' | value_kept='{val}'")

    # Imputation: manufacturing null defect_count → 0.0
    if source_id == "src_manufacturing_logs":
        val = record.get("defect_count")
        if val is None or (isinstance(val, float) and pd.isna(val)):
            record["defect_count"] = 0.0
            changes.append("IMPUTE | field=defect_count | null → 0.0 | reason=manufacturing_imputation_policy")

    # Schema migration: legacy old_product_code → product_id
    if source_id == "src_legacy_trends" and "old_product_code" in record:
        old_val = record.pop("old_product_code")
        record["product_id"] = old_val
        changes.append(f"SCHEMA_MIGRATE | old_product_code → product_id | value='{old_val}'")

    if changes:
        _NORMALIZATION_LOG.append({"source_id": source_id, "changes": changes})

    return record, changes


# ──────────────────────────────────────────────────────────────────────────────
# CORE RECORD-LEVEL LOGGING
# ──────────────────────────────────────────────────────────────────────────────

def _log_record_result(
    source_id: str,
    row_num: int,
    record_key: str,
    status: str,
    norm_changes: List[str],
    violations: List[str],
    contract_id: str,
    policy: str,
):
    """
    Emit one structured log line per record.

    Status codes:
      PASS       — record valid, written to ingested store
      COERCED    — record had type mismatches; auto-corrected, written
      QUARANTINE — contract violated; record written to quarantine store
      REJECT     — contract violated with REJECT policy; record discarded
      DUPLICATE  — dedup key seen before; record skipped
    """
    norm_summary = f"norm_changes={len(norm_changes)}"
    if norm_changes:
        norm_summary += " | " + " | ".join(norm_changes[:3])
        if len(norm_changes) > 3:
            norm_summary += f" (+{len(norm_changes)-3} more)"

    if status in ("PASS", "COERCED"):
        log.info(
            f"[{status}] source={source_id} | row={row_num} | key={record_key} "
            f"| contract={contract_id} | policy={policy} | {norm_summary}"
        )
    elif status == "DUPLICATE":
        log.warning(
            f"[DUPLICATE] source={source_id} | row={row_num} | key={record_key} "
            f"| reason=dedup_key_already_seen | skipped=True"
        )
    else:  # QUARANTINE / REJECT
        viol_str = " | ".join(violations[:5])
        if len(violations) > 5:
            viol_str += f" ... (+{len(violations)-5} more violations)"
        log.warning(
            f"[{status}] source={source_id} | row={row_num} | key={record_key} "
            f"| contract={contract_id} | policy={policy} | violations=[{viol_str}] "
            f"| {norm_summary}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# SINGLE-SOURCE INGESTION
# ──────────────────────────────────────────────────────────────────────────────

def ingest_source(
    source_id:      str,
    raw_path:       str,
    dataset_id:     str,
    schema_version: str = "v1",
    df_override:    Optional[pd.DataFrame] = None,
    output_suffix:  str = "",
) -> JobTelemetry:
    """
    Ingest one CSV source through the full pipeline:
      1. Load CSV (or use df_override for micro-batch slices)
      2. Normalize each record
      3. Deduplicate by primary key
      4. Enforce data contract
      5. Wrap in EventEnvelope
      6. Write PASS → Parquet; QUARANTINE → quarantine Parquet
      7. Emit detailed log per record and summary telemetry report
    """
    job_id = f"job_{source_id}_{int(time.time())}"
    tel    = JobTelemetry(job_id=job_id, source_id=source_id)
    hb     = Heartbeat(job_id, tel)

    tel.mark_start()
    hb.start()
    wall_start = time.time()

    contract   = CONTRACT_REGISTRY.get(source_id)
    contract_id = contract.contract_id if contract else "no_contract"
    policy      = contract.violation_policy.value if contract else "n/a"
    dedup_col   = DEDUP_KEY_MAP.get(source_id)
    seen_ids    = set()
    dup_count   = 0

    good_records:      List[Dict] = []
    quarantine_records: List[Dict] = []
    detail_log:        List[Dict] = []

    log.info(f"")
    log.info(f"{'═'*70}")
    log.info(f"  INGESTION START | source={source_id} | job={job_id}")
    log.info(f"  contract={contract_id} | violation_policy={policy}")
    log.info(f"  dedup_key={dedup_col} | output_suffix='{output_suffix}'")
    log.info(f"{'═'*70}")

    try:
        if df_override is not None:
            df = df_override
            log.info(f"  [LOAD] Micro-batch slice: {len(df)} rows provided via df_override")
        else:
            try:
                df = pd.read_csv(raw_path)
                log.info(f"  [LOAD] Loaded {len(df)} rows from {raw_path}")
                log.info(f"  [SCHEMA] Columns: {list(df.columns)}")
            except FileNotFoundError:
                log.error(f"  [LOAD] File not found: {raw_path}")
                tel.record_fail()
                return tel
            except pd.errors.EmptyDataError:
                log.error(f"  [LOAD] Empty CSV file: {raw_path}")
                tel.record_fail()
                return tel
            except Exception as e:
                log.error(f"  [LOAD] Error loading CSV: {e}")
                tel.record_fail()
                return tel

        for row_num, (_, row) in enumerate(df.iterrows(), start=1):
            try:
                record      = row.where(pd.notna(row), None).to_dict()
                record_key  = str(record.get(dedup_col, f"row_{row_num}")) if dedup_col else f"row_{row_num}"

                # ── Step 1: Normalize ────────────────────────────────────────
                record, norm_changes = normalize_record(record, source_id)

                # ── Step 2: Deduplicate ──────────────────────────────────────
                if dedup_col:
                    dedup_val = record.get(dedup_col) or record.get("product_id")
                    if dedup_val and dedup_val in seen_ids:
                        dup_count += 1
                        _log_record_result(source_id, row_num, record_key,
                                           "DUPLICATE", norm_changes, [], contract_id, policy)
                        detail_log.append({"row": row_num, "key": record_key, "status": "DUPLICATE"})
                        continue
                    if dedup_val:
                        seen_ids.add(dedup_val)

                # ── Step 3: Contract enforcement ─────────────────────────────
                if contract:
                    result     = contract.enforce(record)
                    status_raw = result["status"]
                    violations = result["violations"]

                    if status_raw == "rejected":
                        tel.record_fail()
                        _log_record_result(source_id, row_num, record_key,
                                           "REJECT", norm_changes, violations, contract_id, policy)
                        detail_log.append({"row": row_num, "key": record_key,
                                           "status": "REJECT", "violations": violations})
                        continue

                    elif status_raw == "quarantine":
                        tel.record_quarantine()
                        _log_record_result(source_id, row_num, record_key,
                                           "QUARANTINE", norm_changes, violations, contract_id, policy)
                        record["_violations"] = json.dumps(violations)
                        record["_quarantine_reason"] = "; ".join(violations)
                        quarantine_records.append(record)
                        detail_log.append({"row": row_num, "key": record_key,
                                           "status": "QUARANTINE", "violations": violations})
                        continue

                    elif status_raw == "coerced":
                        tel.record_coerce()
                        record = result["record"]
                        _log_record_result(source_id, row_num, record_key,
                                           "COERCED", norm_changes, violations, contract_id, policy)
                        detail_log.append({"row": row_num, "key": record_key,
                                           "status": "COERCED", "coercions": violations})
                    else:
                        _log_record_result(source_id, row_num, record_key,
                                           "PASS", norm_changes, [], contract_id, policy)
                        detail_log.append({"row": row_num, "key": record_key, "status": "PASS"})
                else:
                    _log_record_result(source_id, row_num, record_key,
                                       "PASS (no contract)", norm_changes, [], contract_id, policy)
                    detail_log.append({"row": row_num, "key": record_key, "status": "PASS"})

                # ── Step 4: Wrap in EventEnvelope ────────────────────────────
                envelope = EventEnvelope(
                    payload         = record,
                    source_id       = source_id,
                    dataset_id      = dataset_id,
                    schema_version  = schema_version,
                    operation_type  = OperationType.SNAPSHOT,
                    event_timestamp = (
                        record.get("mfg_timestamp") or
                        record.get("sale_timestamp") or
                        record.get("timestamp")
                    ),
                    source_timestamp = (
                        record.get("mfg_timestamp") or
                        record.get("sale_timestamp")
                    ),
                )
                good_records.append(envelope.to_dict())
                tel.record_ok()
            except Exception as e:
                log.error(f"  [RECORD ERROR] Row {row_num}: {e}")
                tel.record_fail()

        # ── Write outputs ────────────────────────────────────────────────
        suffix = f"_{output_suffix}" if output_suffix else ""

        try:
            if good_records:
                out_path = os.path.join(OUTPUT_DIR, f"{source_id}{suffix}.parquet")
                pd.DataFrame(good_records).to_parquet(out_path, index=False)
                tel.file_count_per_partition += 1
                tel.snapshot_count += 1
                log.info(f"  [WRITE] ✅ {len(good_records)} records → {out_path}")
        except Exception as e:
            log.error(f"  [WRITE] Failed to write good records: {e}")
            tel.record_fail()

        try:
            if quarantine_records:
                q_path = os.path.join(QUARANTINE_DIR, f"{source_id}{suffix}_quarantine.parquet")
                pd.DataFrame(quarantine_records).to_parquet(q_path, index=False)
                log.warning(f"  [QUARANTINE] ⚠  {len(quarantine_records)} records → {q_path}")
        except Exception as e:
            log.error(f"  [QUARANTINE] Failed to write quarantine records: {e}")

        try:
            # Write per-record detail log as JSON Lines
            if detail_log:
                dl_path = os.path.join(DETAIL_LOG_DIR, f"{source_id}{suffix}_record_log.jsonl")
                with open(dl_path, "w") as f:
                    for entry in detail_log:
                        f.write(json.dumps(entry) + "\n")
                log.info(f"  [DETAIL LOG] {len(detail_log)} record entries → {dl_path}")
        except Exception as e:
            log.error(f"  [DETAIL LOG] Failed to write detail log: {e}")

        if dup_count:
            log.info(f"  [DEDUP] Dropped {dup_count} duplicate rows (dedup_key={dedup_col})")

    except Exception as exc:
        log.error(f"  [FATAL] ❌ source={source_id} | error={exc}", exc_info=True)
        tel.record_fail()

    finally:
        hb.stop()
        tel.mark_end()
        wall_end = time.time()
        log.info(f"")
        log.info(f"  [TIMER] source={source_id} | "
                 f"start={datetime.fromtimestamp(wall_start, timezone.utc).isoformat()} | "
                 f"end={datetime.fromtimestamp(wall_end, timezone.utc).isoformat()} | "
                 f"duration={wall_end - wall_start:.4f}s")
        tel.log_report()

    return tel


# ──────────────────────────────────────────────────────────────────────────────
# FULL BATCH INITIAL LOAD
# ──────────────────────────────────────────────────────────────────────────────

BATCH_SOURCES = [
    ("src_warehouse_master",   "storage/raw/warehouse_master.csv",   "ds_warehouse_master"),
    ("src_manufacturing_logs", "storage/raw/manufacturing_logs.csv", "ds_manufacturing_logs"),
    ("src_sales_history",      "storage/raw/sales_history.csv",      "ds_sales_history"),
    ("src_legacy_trends",      "storage/raw/legacy_trends.csv",      "ds_legacy_trends"),
]


def run_all_batch_ingestion() -> List[JobTelemetry]:
    """
    Phase 4 — Initial Load:
    Ingests all 4 CSV sources in sequence. Full file per source.
    Each record logged with PASS / QUARANTINE / REJECT / DUPLICATE.
    """
    log.info("")
    log.info("█" * 70)
    log.info("  PHASE 4 — BATCH INGESTION: INITIAL LOAD")
    log.info("█" * 70)

    total_start  = time.time()
    all_telemetry: List[JobTelemetry] = []

    for source_id, path, dataset_id in BATCH_SOURCES:
        if not os.path.exists(path):
            log.error(f"  [SKIP] Source file not found: {path}")
            continue
        tel = ingest_source(source_id, path, dataset_id)
        all_telemetry.append(tel)

    # API ingestion demo
    tel_api = run_api_ingestion("src_weather_api", "ds_weather_api")
    all_telemetry.append(tel_api)

    total_end = time.time()
    _print_batch_summary(all_telemetry, total_end - total_start)

    return all_telemetry


def _print_batch_summary(telemetry: List[JobTelemetry], total_sec: float):
    log.info("")
    log.info("█" * 70)
    log.info("  BATCH INGESTION SUMMARY")
    log.info("█" * 70)
    log.info(f"  {'Source':<40} {'OK':>6} {'Fail':>6} {'Quar':>6} {'Coerce':>7} {'RPS':>8}")
    log.info(f"  {'─'*40} {'─'*6} {'─'*6} {'─'*6} {'─'*7} {'─'*8}")
    for t in telemetry:
        log.info(
            f"  {t.source_id:<40} "
            f"{t.records_ingested:>6} "
            f"{t.records_failed:>6} "
            f"{t.records_quarantined:>6} "
            f"{t.records_coerced:>7} "
            f"{t.throughput:>8.1f}"
        )
    log.info(f"  {'─'*40} {'─'*6} {'─'*6} {'─'*6} {'─'*7} {'─'*8}")
    log.info(f"  Total wall time: {total_sec:.2f}s")
    log.info(f"  Normalization changes applied: {len(_NORMALIZATION_LOG)} records")
    log.info("█" * 70)


# ──────────────────────────────────────────────────────────────────────────────
# MICRO-BATCH INGESTION
# ──────────────────────────────────────────────────────────────────────────────

def run_micro_batch_ingestion(
    source_id:    str,
    raw_path:     str,
    dataset_id:   str,
    batch_size:   int = 500,
    max_batches:  int = 5,
) -> List[JobTelemetry]:
    """
    Phase 4 (extension) — Micro-Batch:
    Splits the source CSV into time-windowed slices of `batch_size` rows.
    Each slice is ingested independently with its own job_id, telemetry, and logs.

    Simulates near-real-time ingestion without a streaming broker.
    Each micro-batch is written to storage/micro_batch/{source_id}_batch_{n}.parquet
    """
    log.info("")
    log.info("▓" * 70)
    log.info(f"  MICRO-BATCH INGESTION | source={source_id} | batch_size={batch_size}")
    log.info("▓" * 70)

    if not os.path.exists(raw_path):
        log.error(f"  [MICRO-BATCH] File not found: {raw_path}")
        return []

    df      = pd.read_csv(raw_path)
    total   = len(df)
    batches = [df.iloc[i:i+batch_size] for i in range(0, total, batch_size)]
    batches = batches[:max_batches]  # cap for demo

    log.info(f"  Total rows: {total} | Slices to process: {len(batches)} of {len(df)//batch_size + 1}")

    all_tel: List[JobTelemetry] = []
    for idx, slice_df in enumerate(batches, start=1):
        log.info(f"")
        log.info(f"  ── Micro-Batch {idx}/{len(batches)} | rows {(idx-1)*batch_size}–{(idx-1)*batch_size+len(slice_df)-1} ──")
        tel = ingest_source(
            source_id     = source_id,
            raw_path      = raw_path,
            dataset_id    = dataset_id,
            df_override   = slice_df,
            output_suffix = f"microbatch_{idx:03d}",
        )
        # Move output to micro_batch dir
        src = os.path.join(OUTPUT_DIR, f"{source_id}_microbatch_{idx:03d}.parquet")
        dst = os.path.join(MICRO_BATCH_DIR, f"{source_id}_microbatch_{idx:03d}.parquet")
        if os.path.exists(src):
            if os.path.exists(dst):
                os.remove(dst)
            os.rename(src, dst)
            log.info(f"  [MICRO-BATCH] Moved → {dst}")
        all_tel.append(tel)
        time.sleep(0.1)  # Simulate processing interval

    log.info("")
    log.info(f"  [MICRO-BATCH COMPLETE] {len(all_tel)} batches processed for {source_id}")
    return all_tel


def run_api_ingestion(source_id: str, dataset_id: str) -> JobTelemetry:
    """Ingest from REST API (demonstrates extraction_mode: PULL)."""
    log.info(f"  ── API Ingestion for {source_id} ──")

    from control_plane.entities import WEATHER_API_SOURCE

    source = WEATHER_API_SOURCE

    url = source.connection_info["url"]

    params = source.connection_info["params"]

    # For demo, use httpbin.org to simulate API call (since OpenWeatherMap requires API key)
    try:
        response = requests.get("https://httpbin.org/json", timeout=10)
        response.raise_for_status()
        api_data = response.json()
    except requests.RequestException as e:
        log.error(f"API call failed: {e}")
        return JobTelemetry(source_id, dataset_id, 0, 0, 0, 0, 0, 0, 0)

    # Transform API response to expected schema
    record = {
        "city": "Lahore",  # Mock city
        "temperature": 25.0,  # Mock temp
        "humidity": 60,  # Mock humidity
        "weather_description": "clear sky",  # Mock desc
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # Ingest as DataFrame
    df = pd.DataFrame([record])
    tel = ingest_source(source_id=source_id, raw_path=None, dataset_id=dataset_id, df_override=df)
    log.info(f"  [API] Ingested 1 record from {url}")
    return tel


if __name__ == "__main__":
    run_all_batch_ingestion()

    # Demo micro-batch on sales (most interesting for near-RT)
    log.info("")
    log.info("  ── Running Micro-Batch Demo on Sales History ──")
    run_micro_batch_ingestion(
        source_id  = "src_sales_history",
        raw_path   = "storage/raw/sales_history.csv",
        dataset_id = "ds_sales_history",
        batch_size = 200,
        max_batches= 3,
    )

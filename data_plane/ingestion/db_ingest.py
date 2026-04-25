# Phase 4 — DB Ingestion: Production-Grade Continuous Database Source
#
# Simulates reading from an operational database with:
#   • CDC checkpoint tracking (timestamp-based capture)
#   • Continuous ingestion without polling overhead
#   • Schema validation via data contracts
#   • Detailed per-record logging and telemetry
#
# In production, this would read from PostgreSQL, MySQL, or Oracle.
# For this simulation, we use a JSON-based simulated DB.

import os
import sys
import time
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from control_plane.entities import (
    OperationType, EventEnvelope, INVENTORY_TRANSACTIONS_SOURCE, INVENTORY_TRANSACTIONS_DATASET
)
from control_plane.contracts import CONTRACT_REGISTRY
from observability_plane.telemetry import JobTelemetry, Heartbeat

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("db_ingest")

# ── Storage directories ────────────────────────────────────────────────────────
OUTPUT_DIR      = "storage/ingested"
QUARANTINE_DIR  = "storage/quarantine"
DETAIL_LOG_DIR  = "storage/ingested/detail_logs"
CHECKPOINT_DIR  = "storage/checkpoints"
SIM_DB_DIR      = "storage/simulated_db"

for d in [OUTPUT_DIR, QUARANTINE_DIR, DETAIL_LOG_DIR, CHECKPOINT_DIR, SIM_DB_DIR]:
    os.makedirs(d, exist_ok=True)

# ── Deduplication key ──────────────────────────────────────────────────────────
DEDUP_KEY = "transaction_id"

# ── Normalization log ──────────────────────────────────────────────────────────
_NORMALIZATION_LOG: List[Dict] = []

# ── Simulated DB storage ───────────────────────────────────────────────────────
SIMULATED_DB_PATH = os.path.join(SIM_DB_DIR, "inventory_transactions.jsonl")


# ──────────────────────────────────────────────────────────────────────────────
# SIMULATED DATABASE OPERATIONS
# ──────────────────────────────────────────────────────────────────────────────

def write_to_simulated_db(record: Dict[str, Any]) -> None:
    """
    Write a transaction record to simulated DB (append to JSONL file).
    In production, this would be an INSERT to PostgreSQL, MySQL, etc.
    """
    with open(SIMULATED_DB_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")


def read_from_simulated_db(checkpoint_timestamp: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Read all transaction records from simulated DB created after checkpoint.
    Simulates: SELECT * FROM inventory_transactions WHERE created_at > checkpoint ORDER BY created_at
    In production, this would be a parameterized query.
    """
    if not os.path.exists(SIMULATED_DB_PATH):
        return []

    records = []
    with open(SIMULATED_DB_PATH, "r") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)
            if checkpoint_timestamp:
                try:
                    record_ts = datetime.fromisoformat(record.get("created_at", "").replace("Z", "+00:00"))
                    checkpoint_ts = datetime.fromisoformat(checkpoint_timestamp.replace("Z", "+00:00"))
                    if record_ts <= checkpoint_ts:
                        continue
                except Exception:
                    pass
            records.append(record)

    return records


def get_checkpoint(source_id: str) -> Optional[str]:
    """
    Load the last processed timestamp (CDC checkpoint) for a source.
    Returns ISO timestamp or None if first run.
    """
    checkpoint_file = os.path.join(CHECKPOINT_DIR, f"db_{source_id}_checkpoint.json")
    if not os.path.exists(checkpoint_file):
        return None
    with open(checkpoint_file, "r") as f:
        data = json.load(f)
        return data.get("last_processed_timestamp")


def save_checkpoint(source_id: str, timestamp: str) -> None:
    """
    Save the latest processed timestamp (CDC checkpoint) for a source.
    """
    checkpoint_file = os.path.join(CHECKPOINT_DIR, f"db_{source_id}_checkpoint.json")
    with open(checkpoint_file, "w") as f:
        json.dump({"last_processed_timestamp": timestamp, "saved_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)


# ──────────────────────────────────────────────────────────────────────────────
# RECORD NORMALIZATION
# ──────────────────────────────────────────────────────────────────────────────

def normalize_record(record: Dict[str, Any], source_id: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Apply normalization rules before contract enforcement.
    Returns (normalized_record, list_of_change_descriptions).

    Rules:
      • All ID fields → UPPERCASE
      • All timestamp fields → ISO-8601 UTC
      • Warehouse location → UPPERCASE
      • Transaction type → UPPERCASE
    """
    changes: List[str] = []

    # Uppercase ID fields
    id_fields = ["transaction_id", "product_id", "reference_order_id"]
    for key in id_fields:
        val = record.get(key)
        if isinstance(val, str) and val and val != val.upper():
            record[key] = val.upper()
            changes.append(f"NORMALIZE | field={key} | '{val}' → '{record[key]}' | reason=uppercase_policy")

    # Uppercase warehouse and transaction type
    if "warehouse_location" in record:
        val = record["warehouse_location"]
        if isinstance(val, str) and val and val != val.upper():
            record["warehouse_location"] = val.upper()
            changes.append(f"NORMALIZE | field=warehouse_location | '{val}' → '{record['warehouse_location']}'")

    if "transaction_type" in record:
        val = record["transaction_type"]
        if isinstance(val, str) and val and val != val.upper():
            record["transaction_type"] = val.upper()
            changes.append(f"NORMALIZE | field=transaction_type | '{val}' → '{record['transaction_type']}'")

    # ISO-8601 timestamp normalization
    ts_fields = ["timestamp", "created_at"]
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

    if changes:
        _NORMALIZATION_LOG.append({"source_id": source_id, "changes": changes})

    return record, changes


# ──────────────────────────────────────────────────────────────────────────────
# CORE RECORD-LEVEL LOGGING
# ──────────────────────────────────────────────────────────────────────────────

def _log_record_result(
    source_id: str,
    record_num: int,
    record_key: str,
    status: str,
    norm_changes: List[str],
    violations: List[str],
    contract_id: str,
    policy: str,
):
    """Emit one structured log line per record."""
    norm_summary = f"norm_changes={len(norm_changes)}"
    if norm_changes:
        norm_summary += " | " + " | ".join(norm_changes[:2])
        if len(norm_changes) > 2:
            norm_summary += f" (+{len(norm_changes)-2} more)"

    if status in ("PASS", "COERCED"):
        log.info(
            f"[{status}] source={source_id} | record={record_num} | key={record_key} "
            f"| contract={contract_id} | policy={policy} | {norm_summary}"
        )
    elif status == "DUPLICATE":
        log.warning(
            f"[DUPLICATE] source={source_id} | record={record_num} | key={record_key} "
            f"| reason=dedup_key_already_seen | skipped=True"
        )
    else:  # QUARANTINE / REJECT
        viol_str = " | ".join(violations[:3])
        if len(violations) > 3:
            viol_str += f" ... (+{len(violations)-3} more violations)"
        log.warning(
            f"[{status}] source={source_id} | record={record_num} | key={record_key} "
            f"| contract={contract_id} | policy={policy} | violations=[{viol_str}] "
            f"| {norm_summary}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# DB INGESTION WITH CDC
# ──────────────────────────────────────────────────────────────────────────────

def ingest_db_source(
    source_id: str = "src_inventory_transactions",
    dataset_id: str = "ds_inventory_transactions",
    schema_version: str = "v1",
) -> JobTelemetry:
    """
    Ingest records from simulated DB with CDC checkpoint tracking.

    Flow:
      1. Load checkpoint (last processed timestamp)
      2. Query simulated DB for records created after checkpoint
      3. Normalize, deduplicate, enforce contracts
      4. Write PASS → Parquet; QUARANTINE → quarantine store
      5. Save new checkpoint
      6. Emit telemetry
    """
    job_id = f"job_{source_id}_{int(time.time())}"
    tel = JobTelemetry(job_id=job_id, source_id=source_id)
    hb = Heartbeat(job_id, tel)

    tel.mark_start()
    hb.start()
    wall_start = time.time()

    contract = CONTRACT_REGISTRY.get(source_id)
    contract_id = contract.contract_id if contract else "no_contract"
    policy = contract.violation_policy.value if contract else "n/a"
    seen_ids = set()
    dup_count = 0
    new_checkpoint = None

    good_records: List[Dict] = []
    quarantine_records: List[Dict] = []
    detail_log: List[Dict] = []

    log.info(f"")
    log.info(f"{'═'*70}")
    log.info(f"  DB INGESTION START (CDC) | source={source_id} | job={job_id}")
    log.info(f"  contract={contract_id} | violation_policy={policy}")
    log.info(f"{'═'*70}")

    try:
        # ── Load checkpoint ──────────────────────────────────────────────
        checkpoint = get_checkpoint(source_id)
        log.info(f"  [CDC] Loading checkpoint: {checkpoint or 'FIRST RUN (no checkpoint)'}")

        # ── Query simulated DB ───────────────────────────────────────────
        db_records = read_from_simulated_db(checkpoint)
        log.info(f"  [DB] Retrieved {len(db_records)} records from simulated database")

        if not db_records:
            log.info(f"  [DB] No new records to ingest. Checkpoint already up-to-date.")
            tel.mark_end()
            return tel

        # ── Process records ──────────────────────────────────────────────
        for record_num, record in enumerate(db_records, start=1):
            try:
                record_key = str(record.get(DEDUP_KEY, f"record_{record_num}"))

                # Step 1: Normalize
                record, norm_changes = normalize_record(record, source_id)

                # Step 2: Deduplicate
                if record_key in seen_ids:
                    dup_count += 1
                    _log_record_result(source_id, record_num, record_key,
                                       "DUPLICATE", norm_changes, [], contract_id, policy)
                    detail_log.append({"record": record_num, "key": record_key, "status": "DUPLICATE"})
                    continue
                seen_ids.add(record_key)

                # Step 3: Contract enforcement
                if contract:
                    result = contract.enforce(record)
                    status_raw = result["status"]
                    violations = result["violations"]

                    if status_raw == "rejected":
                        tel.record_fail()
                        _log_record_result(source_id, record_num, record_key,
                                           "REJECT", norm_changes, violations, contract_id, policy)
                        detail_log.append({"record": record_num, "key": record_key,
                                           "status": "REJECT", "violations": violations})
                        continue

                    elif status_raw == "quarantine":
                        tel.record_quarantine()
                        _log_record_result(source_id, record_num, record_key,
                                           "QUARANTINE", norm_changes, violations, contract_id, policy)
                        record["_violations"] = json.dumps(violations)
                        record["_quarantine_reason"] = "; ".join(violations)
                        quarantine_records.append(record)
                        detail_log.append({"record": record_num, "key": record_key,
                                           "status": "QUARANTINE", "violations": violations})
                        continue

                    elif status_raw == "coerced":
                        tel.record_coerce()
                        record = result["record"]
                        _log_record_result(source_id, record_num, record_key,
                                           "COERCED", norm_changes, violations, contract_id, policy)
                        detail_log.append({"record": record_num, "key": record_key,
                                           "status": "COERCED", "coercions": violations})
                    else:  # ok
                        _log_record_result(source_id, record_num, record_key,
                                           "PASS", norm_changes, [], contract_id, policy)
                        detail_log.append({"record": record_num, "key": record_key, "status": "PASS"})
                else:
                    _log_record_result(source_id, record_num, record_key,
                                       "PASS (no contract)", norm_changes, [], contract_id, policy)
                    detail_log.append({"record": record_num, "key": record_key, "status": "PASS"})

                # Step 4: Wrap in EventEnvelope
                envelope = EventEnvelope(
                    payload=record,
                    source_id=source_id,
                    dataset_id=dataset_id,
                    schema_version=schema_version,
                    operation_type=OperationType.SNAPSHOT,
                    event_timestamp=record.get("created_at"),
                    source_timestamp=record.get("created_at"),
                )
                good_records.append(envelope.to_dict())
                tel.record_ok()

                # Update checkpoint
                new_checkpoint = record.get("created_at")

            except Exception as e:
                log.error(f"  [RECORD ERROR] Record {record_num}: {e}")
                tel.record_fail()

        # ── Write outputs ────────────────────────────────────────────────
        try:
            if good_records:
                ts_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
                out_path = os.path.join(OUTPUT_DIR, f"{source_id}_cdc_{ts_suffix}.parquet")
                pd.DataFrame(good_records).to_parquet(out_path, index=False)
                tel.file_count_per_partition += 1
                tel.snapshot_count += 1
                log.info(f"  [WRITE]  {len(good_records)} records → {out_path}")
        except Exception as e:
            log.error(f"  [WRITE] Failed to write good records: {e}")
            tel.record_fail()

        try:
            if quarantine_records:
                q_path = os.path.join(QUARANTINE_DIR, f"{source_id}_quarantine.parquet")
                pd.DataFrame(quarantine_records).to_parquet(q_path, index=False)
                log.warning(f"  [QUARANTINE] ⚠  {len(quarantine_records)} records → {q_path}")
        except Exception as e:
            log.error(f"  [QUARANTINE] Failed to write quarantine records: {e}")

        try:
            if detail_log:
                dl_path = os.path.join(DETAIL_LOG_DIR, f"{source_id}_record_log.jsonl")
                with open(dl_path, "w") as f:
                    for entry in detail_log:
                        f.write(json.dumps(entry) + "\n")
                log.info(f"  [DETAIL LOG] {len(detail_log)} record entries → {dl_path}")
        except Exception as e:
            log.error(f"  [DETAIL LOG] Failed to write detail log: {e}")

        if dup_count:
            log.info(f"  [DEDUP] Dropped {dup_count} duplicate records")

        # ── Save checkpoint ──────────────────────────────────────────────
        if new_checkpoint:
            save_checkpoint(source_id, new_checkpoint)
            log.info(f"  [CDC] Checkpoint saved: {new_checkpoint}")

    except Exception as exc:
        log.error(f"  [FATAL] ❌ source={source_id} | error={exc}", exc_info=True)
        tel.record_fail()

    finally:
        hb.stop()
        tel.mark_end()
        wall_end = time.time()
        log.info(f"")
        log.info(f"  [TIMER] Duration: {wall_end - wall_start:.4f}s")
        tel.log_report()

    return tel


if __name__ == "__main__":
    # For testing: generate some records in simulated DB and ingest them
    from data_plane.generators.source_generators import InventoryTransactionsGenerator
    from data_plane.generators.base_generator import BaseGenerator

    log.info("Seeding simulated DB with test records...")
    gen = InventoryTransactionsGenerator()

    # Write 50 test records to simulated DB
    for _ in range(50):
        record = gen.generate_one()
        write_to_simulated_db(record)

    log.info(f"Ingesting from simulated DB...")
    ingest_db_source()

#!/usr/bin/env python3
# run_all.py — Master runner for all 7 phases of the supply chain ingestion pipeline.
#
# Usage:  python run_all.py
#
# Phases:
#   Phase 1+2  — Control Plane: entities, contracts, violation policies
#   Phase 3    — Data generators fitted to real distributions
#   Phase 4    — Batch initial load + micro-batch + IoT stream simulation
#   Phase 5    — CDC trigger: INSERT / UPDATE / DELETE (steady + burst)
#   Phase 6    — CDC strategies: log-based, trigger-based, timestamp-based
#   Phase 7    — Observability: telemetry across all phases

import os
import sys
import time
import logging
from datetime import datetime
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("run_all")

# Add file logging to root logger to capture all logs
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"run_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))
logging.getLogger().addHandler(file_handler)

BASE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(BASE, ".."))

import pandas as pd


def banner(phase: str, title: str):
    log.info("")
    log.info("=" * 70)
    log.info(f"  {phase}: {title}")
    log.info("=" * 70)


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 1 + 2: Control Plane verification
# ──────────────────────────────────────────────────────────────────────────────

def run_phase_1_2():
    banner("PHASE 1+2", "Control Plane — Entities & Contracts")

    from control_plane.entities import (
        DataSource, Dataset, IngestionJob, EventEnvelope,
        SourceType, ExtractionMode, ChangeCaptureMode, IngestionFrequency,
        ClassificationLevel, ExecutionMode, OperationType,
        ALL_SOURCES, ALL_DATASETS
    )
    from control_plane.contracts import (
        CONTRACT_REGISTRY, ViolationPolicy
    )

    log.info(f"  Sources registered: {len(ALL_SOURCES)}")
    for src in ALL_SOURCES:
        log.info(f"    {src}")

    log.info(f"  Datasets registered: {len(ALL_DATASETS)}")
    for ds in ALL_DATASETS:
        log.info(f"    {ds}")

    log.info(f"  Contracts registered: {len(CONTRACT_REGISTRY)}")
    for sid, c in CONTRACT_REGISTRY.items():
        log.info(f"    {c.contract_id} | policy={c.violation_policy.value} | "
                 f"required_fields={c.required_fields}")

    # Quick contract smoke-test
    wh_contract = CONTRACT_REGISTRY["src_warehouse_master"]
    good = {"product_id": "ART-1001-MID-M", "article_id": "ART-1001",
            "product_name": "Slim Fit Tee", "color": "Midnight Black",
            "size": "M", "reorder_threshold": 100, "max_capacity": 500, "unit_cost": 29.99}
    bad  = {"product_id": None, "reorder_threshold": None, "max_capacity": 500, "unit_cost": -5.0}

    r_good = wh_contract.enforce(good)
    r_bad  = wh_contract.enforce(bad)

    log.info(f"  Contract smoke-test (good record): status={r_good['status']}")
    log.info(f"  Contract smoke-test (bad record):  status={r_bad['status']} | "
             f"violations={r_bad['violations']}")


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 3: Generators — profile real data, generate synthetic records
# ──────────────────────────────────────────────────────────────────────────────

def run_phase_3() -> list:
    """Returns list of real product_ids for use in later phases."""
    banner("PHASE 3", "Data Generators — Distribution Profiling + Synthetic Generation")

    from data_plane.generators.source_generators import (
        WarehouseMasterGenerator, ManufacturingLogsGenerator,
        SalesHistoryGenerator, LegacyTrendsGenerator, IoTStreamGenerator,
        WeatherAPIGenerator
    )

    wh_df  = pd.read_csv("storage/raw/warehouse_master.csv")
    mfg_df = pd.read_csv("storage/raw/manufacturing_logs.csv")
    sal_df = pd.read_csv("storage/raw/sales_history.csv")
    leg_df = pd.read_csv("storage/raw/legacy_trends.csv")
    pids   = wh_df["product_id"].tolist()

    generators = {
        "WarehouseMaster":   WarehouseMasterGenerator(wh_df),
        "ManufacturingLogs": ManufacturingLogsGenerator(mfg_df, pids),
        "SalesHistory":      SalesHistoryGenerator(sal_df, pids),
        "LegacyTrends":      LegacyTrendsGenerator(leg_df, pids),
        "IoTStream":         IoTStreamGenerator(pids),
        "WeatherAPI":        WeatherAPIGenerator(),
    }

    for name, gen in generators.items():
        if name == "IoTStream":
            samples = [gen.generate_one() for _ in range(5)]
        else:
            samples = gen.generate(5)
        log.info(f"  [{name}] Generated 5 synthetic records. Sample[0] keys: {list(samples[0].keys())}")

    log.info(f"  Phase 3 complete. All generators profiled and verified.")
    return pids


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 4: Batch + Micro-batch + IoT Stream
# ──────────────────────────────────────────────────────────────────────────────

def run_phase_4(product_ids: list):
    banner("PHASE 4", "Ingestion — Batch Initial Load + Micro-Batch + IoT Stream")

    from data_plane.ingestion.batch_ingest import (
        run_all_batch_ingestion, run_micro_batch_ingestion
    )
    from data_plane.ingestion.iot_stream_ingest import (
        run_stream_simulation
    )

    # ── 4a: Full batch initial load ─────────────────────────────────────
    log.info("")
    log.info("  ── 4a: Full Batch Initial Load ──")
    batch_tels = run_all_batch_ingestion()

    # ── 4b: Micro-batch demo on sales (most interesting) ────────────────
    log.info("")
    log.info("  ── 4b: Micro-Batch on Sales History (3 batches × 200 rows) ──")
    run_micro_batch_ingestion(
        source_id  = "src_sales_history",
        raw_path   = "storage/raw/sales_history.csv",
        dataset_id = "ds_sales_history",
        batch_size = 200,
        max_batches= 3,
    )

    # ── 4c: Micro-batch demo on manufacturing ────────────────────────────
    log.info("")
    log.info("  ── 4c: Micro-Batch on Manufacturing Logs (3 batches × 200 rows) ──")
    run_micro_batch_ingestion(
        source_id  = "src_manufacturing_logs",
        raw_path   = "storage/raw/manufacturing_logs.csv",
        dataset_id = "ds_manufacturing_logs",
        batch_size = 200,
        max_batches= 3,
    )

    # ── 4d: IoT Stream simulation ────────────────────────────────────────
    log.info("")
    log.info("  ── 4d: IoT RFID Stream Simulation (300 events, flush every 100) ──")
    run_stream_simulation(
        product_ids          = product_ids,
        total_events         = 300,
        flush_interval_events= 100,
        duplicate_rate       = 0.03,
    )


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 5: CDC Trigger — INSERT / UPDATE / DELETE
# ──────────────────────────────────────────────────────────────────────────────

def run_phase_5(product_ids: list):
    banner("PHASE 5", "CDC Trigger — INSERT / UPDATE / DELETE (Steady + Burst)")

    from data_plane.generators.source_generators import (
        SalesHistoryGenerator, ManufacturingLogsGenerator
    )
    from data_plane.cdc.cdc_trigger import (
        run_steady_stream, run_burst, load_existing_records
    )

    mfg_df  = pd.read_csv("storage/raw/manufacturing_logs.csv")
    sal_df  = pd.read_csv("storage/raw/sales_history.csv")

    sal_gen = SalesHistoryGenerator(sal_df, product_ids)
    mfg_gen = ManufacturingLogsGenerator(mfg_df, product_ids)

    # Load existing ingested records for UPDATE/DELETE targets
    existing_sales = load_existing_records("storage/ingested/src_sales_history.parquet")
    existing_mfg   = load_existing_records("storage/ingested/src_manufacturing_logs.parquet")

    log.info("  ── 5a: Steady stream — Sales (10 rec/s × 10s) ──")
    run_steady_stream(
        source_id  = "src_sales_history",
        dataset_id = "ds_sales_history",
        existing   = existing_sales,
        generator_fn = sal_gen.generate_one,
        duration_sec = 10,
        target_rps   = 10,
    )

    log.info("  ── 5b: Burst — Manufacturing (5000 records) ──")
    run_burst(
        source_id    = "src_manufacturing_logs",
        dataset_id   = "ds_manufacturing_logs",
        existing     = existing_mfg,
        generator_fn = mfg_gen.generate_one,
        burst_count  = 5000,
    )


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 6: CDC Strategies
# ──────────────────────────────────────────────────────────────────────────────

def run_phase_6():
    banner("PHASE 6", "CDC Strategies — Log-Based, Trigger-Based, Timestamp-Based")

    from data_plane.cdc.cdc_strategies import (
        run_log_based_cdc, run_trigger_based_cdc, run_timestamp_based_cdc
    )

    sources_and_scenarios = [
        ("src_sales_history",      "storage/cdc_log/src_sales_history_steady_cdc.parquet",      "steady"),
        ("src_manufacturing_logs", "storage/cdc_log/src_manufacturing_logs_burst_cdc.parquet",   "burst"),
    ]

    for source_id, cdc_path, scenario in sources_and_scenarios:
        if not os.path.exists(cdc_path):
            log.warning(f"  [SKIP] CDC log not found: {cdc_path} — run Phase 5 first")
            continue

        log.info(f"  ── Log-Based CDC | {source_id} | {scenario} ──")
        run_log_based_cdc(source_id, cdc_path, scenario)

        log.info(f"  ── Trigger-Based CDC | {source_id} | {scenario} ──")
        run_trigger_based_cdc(source_id, cdc_path, scenario)

        log.info(f"  ── Timestamp-Based CDC | {source_id} | {scenario} ──")
        run_timestamp_based_cdc(source_id, cdc_path, scenario)


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 7: Observability summary
# ──────────────────────────────────────────────────────────────────────────────

def run_phase_7():
    banner("PHASE 7", "Observability — Pipeline Summary & Storage Audit")

    import glob

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

    log.info("")
    log.info("  ── Parquet Row Counts ──")
    for label, path in dirs.items():
        for fp in glob.glob(os.path.join(path, "*.parquet")):
            try:
                df = pd.read_parquet(fp)
                log.info(f"  {os.path.basename(fp):<55} {len(df):>6} rows")
            except Exception:
                pass

    log.info("")
    log.info("  ── Simple Dashboard ──")
    # Mock throughput data for demo (in real scenario, collect from telemetry)
    throughput_data = [100, 200, 150, 300, 250]  # Example values
    plt.figure(figsize=(8,4))
    plt.plot(throughput_data, marker='o')
    plt.title('Sample Ingestion Throughput Over Time')
    plt.xlabel('Batch')
    plt.ylabel('Records/sec')
    plt.grid(True)
    plt.savefig('logs/dashboard_throughput.png')
    log.info("  Dashboard plot saved to logs/dashboard_throughput.png")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    pipeline_start = time.time()

    run_phase_1_2()
    product_ids = run_phase_3()
    run_phase_4(product_ids)
    run_phase_5(product_ids)
    run_phase_6()
    run_phase_7()

    pipeline_end = time.time()
    log.info("")
    log.info("=" * 70)
    log.info("  ALL 7 PHASES COMPLETE")
    log.info(f"  Total pipeline duration: {pipeline_end - pipeline_start:.2f}s")
    log.info("  Check storage/ for Parquet files, quarantine/, stream_buffer/, etc.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()

# #!/usr/bin/env python3
# # run_all.py — Master runner for all 7 phases of the supply chain ingestion pipeline.
# #
# # Usage:  python run_all.py
# #
# # Phases:
# #   Phase 1+2  — Control Plane: entities, contracts, violation policies
# #   Phase 3    — Data generators fitted to real distributions
# #   Phase 4    — Batch initial load + micro-batch + IoT stream simulation
# #   Phase 5    — CDC trigger: INSERT / UPDATE / DELETE (steady + burst)
# #   Phase 6    — CDC strategies: log-based, trigger-based, timestamp-based
# #   Phase 7    — Observability: telemetry across all phases

# import os
# import sys
# import time
# import logging
# from datetime import datetime, timezone, timedelta
# import matplotlib.pyplot as plt

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
#     datefmt="%Y-%m-%dT%H:%M:%S"
# )
# log = logging.getLogger("run_all")

# # Add file logging to root logger to capture all logs
# log_dir = "logs"
# os.makedirs(log_dir, exist_ok=True)
# log_file = os.path.join(log_dir, f"run_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
# file_handler = logging.FileHandler(log_file)
# file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))
# logging.getLogger().addHandler(file_handler)

# BASE = os.path.dirname(__file__)
# sys.path.insert(0, os.path.join(BASE, ".."))

# import pandas as pd

# LOAD_MULTIPLIER = int(os.getenv("INGESTION_LOAD_MULTIPLIER", "3"))

# def banner(phase: str, title: str):
#     log.info("")
#     log.info("=" * 70)
#     log.info(f"  {phase}: {title}")
#     log.info("=" * 70)


# def describe_generator(name: str, gen) -> None:
#     cols = len(gen._profiles)
#     types = {val: list(gen._col_types.values()).count(val) for val in set(gen._col_types.values())}
#     log.info(f"  [{name}] profile summary: columns={cols} type_counts={types}")
#     for col, profile in gen._profiles.items():
#         if profile["type"] == "categorical":
#             sample_values = list(zip(profile["values"][:3], profile["probs"][:3]))
#             log.info(f"    - {col}: categorical values={len(profile['values'])} sample={sample_values}")
#         elif profile["type"] == "numeric":
#             log.info(f"    - {col}: numeric mean={profile['mean']:.2f} std={profile['std']:.2f} min={profile['min']:.2f} max={profile['max']:.2f}")
#         elif profile["type"] == "timestamp":
#             log.info(f"    - {col}: timestamp range={datetime.fromtimestamp(profile['min'], timezone.utc).isoformat()} → {datetime.fromtimestamp(profile['max'], timezone.utc).isoformat()}")
#         else:
#             log.info(f"    - {col}: type={profile['type']}")


# # ──────────────────────────────────────────────────────────────────────────────
# # PHASE 1 + 2: Control Plane verification
# # ──────────────────────────────────────────────────────────────────────────────

# def run_phase_1_2():
#     banner("PHASE 1+2", "Control Plane — Entities & Contracts")

#     from control_plane.entities import (
#         DataSource, Dataset, IngestionJob, EventEnvelope,
#         SourceType, ExtractionMode, ChangeCaptureMode, IngestionFrequency,
#         ClassificationLevel, ExecutionMode, OperationType,
#         ALL_SOURCES, ALL_DATASETS
#     )
#     from control_plane.contracts import (
#         CONTRACT_REGISTRY, ViolationPolicy
#     )

#     log.info(f"  Sources registered: {len(ALL_SOURCES)}")
#     for src in ALL_SOURCES:
#         log.info(f"    {src}")

#     log.info(f"  Datasets registered: {len(ALL_DATASETS)}")
#     for ds in ALL_DATASETS:
#         log.info(f"    {ds}")

#     log.info(f"  Contracts registered: {len(CONTRACT_REGISTRY)}")
#     for sid, c in CONTRACT_REGISTRY.items():
#         log.info(f"    {c.contract_id} | policy={c.violation_policy.value} | "
#                  f"required_fields={c.required_fields}")

#     # Quick contract smoke-test
#     wh_contract = CONTRACT_REGISTRY["src_warehouse_master"]
#     good = {"product_id": "ART-1001-MID-M", "article_id": "ART-1001",
#             "product_name": "Slim Fit Tee", "color": "Midnight Black",
#             "size": "M", "reorder_threshold": 100, "max_capacity": 500, "unit_cost": 29.99}
#     bad  = {"product_id": None, "reorder_threshold": None, "max_capacity": 500, "unit_cost": -5.0}

#     r_good = wh_contract.enforce(good)
#     r_bad  = wh_contract.enforce(bad)

#     log.info(f"  Contract smoke-test (good record): status={r_good['status']}")
#     log.info(f"  Contract smoke-test (bad record):  status={r_bad['status']} | "
#              f"violations={r_bad['violations']}")


# # ──────────────────────────────────────────────────────────────────────────────
# # PHASE 3: Generators — profile real data, generate synthetic records
# # ──────────────────────────────────────────────────────────────────────────────

# def run_phase_3() -> list:
#     """Returns list of real product_ids for use in later phases."""
#     banner("PHASE 3", "Data Generators — Distribution Profiling + Synthetic Generation")

#     from data_plane.generators.source_generators import (
#         WarehouseMasterGenerator, ManufacturingLogsGenerator,
#         SalesHistoryGenerator, LegacyTrendsGenerator, IoTStreamGenerator,
#         WeatherAPIGenerator
#     )

#     wh_df  = pd.read_csv("storage/raw/warehouse_master.csv")
#     mfg_df = pd.read_csv("storage/raw/manufacturing_logs.csv")
#     sal_df = pd.read_csv("storage/raw/sales_history.csv")
#     leg_df = pd.read_csv("storage/raw/legacy_trends.csv")
#     pids   = wh_df["product_id"].tolist()

#     generators = {
#         "WarehouseMaster":   WarehouseMasterGenerator(wh_df),
#         "ManufacturingLogs": ManufacturingLogsGenerator(mfg_df, pids),
#         "SalesHistory":      SalesHistoryGenerator(sal_df, pids),
#         "LegacyTrends":      LegacyTrendsGenerator(leg_df, pids),
#         "IoTStream":         IoTStreamGenerator(pids),
#         "WeatherAPI":        WeatherAPIGenerator(),
#     }

#     for name, gen in generators.items():
#         if name == "IoTStream":
#             samples = [gen.generate_one() for _ in range(5)]
#         else:
#             samples = gen.generate(5)
#         log.info(f"  [{name}] Generated 5 synthetic records. Sample[0] keys: {list(samples[0].keys())}")
#         describe_generator(name, gen)

#     log.info(f"  Phase 3 complete. All generators profiled, synthetic generation verified, and distribution fingerprints captured.")
#     return pids


# # ──────────────────────────────────────────────────────────────────────────────
# # PHASE 4: Batch + Micro-batch + IoT Stream
# # ──────────────────────────────────────────────────────────────────────────────

# def run_phase_4(product_ids: list):
#     banner("PHASE 4", "Ingestion — Batch Initial Load + Micro-Batch + IoT Stream")

#     from data_plane.ingestion.batch_ingest import (
#         run_all_batch_ingestion, run_micro_batch_ingestion
#     )
#     from data_plane.ingestion.iot_stream_ingest import (
#         run_stream_simulation
#     )

#     # ── 4a: Full batch initial load ─────────────────────────────────────
#     log.info("")
#     log.info("  ── 4a: Full Batch Initial Load ──")
#     batch_tels = run_all_batch_ingestion()

#     total_ingested = sum(t.records_ingested for t in batch_tels)
#     total_failed   = sum(t.records_failed for t in batch_tels)
#     total_quar     = sum(t.records_quarantined for t in batch_tels)
#     total_coerce   = sum(t.records_coerced for t in batch_tels)
#     log.info(f"  [PHASE 4 SUMMARY] batch ingestion | ingested={total_ingested} failed={total_failed} quarantined={total_quar} coerced={total_coerce}")

#     # ── 4b: Micro-batch demo on sales (most interesting) ────────────────
#     log.info("")
#     micro_batch_size = 200 * LOAD_MULTIPLIER
#     micro_batches = 3 * LOAD_MULTIPLIER
#     log.info(f"  ── 4b: Micro-Batch on Sales History ({micro_batches} batches × {micro_batch_size} rows) ──")
#     run_micro_batch_ingestion(
#         source_id  = "src_sales_history",
#         raw_path   = "storage/raw/sales_history.csv",
#         dataset_id = "ds_sales_history",
#         batch_size = micro_batch_size,
#         max_batches= micro_batches,
#     )

#     # ── 4c: Micro-batch demo on manufacturing ────────────────────────────
#     log.info("")
#     log.info(f"  ── 4c: Micro-Batch on Manufacturing Logs ({micro_batches} batches × {micro_batch_size} rows) ──")
#     run_micro_batch_ingestion(
#         source_id  = "src_manufacturing_logs",
#         raw_path   = "storage/raw/manufacturing_logs.csv",
#         dataset_id = "ds_manufacturing_logs",
#         batch_size = micro_batch_size,
#         max_batches= micro_batches,
#     )

#     # ── 4d: IoT Stream simulation ────────────────────────────────────────
#     log.info("")
#     iot_total_events = 300 * LOAD_MULTIPLIER
#     iot_flush = 100 * LOAD_MULTIPLIER
#     log.info(f"  ── 4d: IoT RFID Stream Simulation ({iot_total_events} events, flush every {iot_flush}) ──")
#     run_stream_simulation(
#         product_ids          = product_ids,
#         total_events         = iot_total_events,
#         flush_interval_events= iot_flush,
#         duplicate_rate       = 0.03,
#     )


# # ──────────────────────────────────────────────────────────────────────────────
# # PHASE 5: CDC Trigger — INSERT / UPDATE / DELETE
# # ──────────────────────────────────────────────────────────────────────────────

# def run_phase_5(product_ids: list):
#     banner("PHASE 5", "CDC Trigger — INSERT / UPDATE / DELETE (Steady + Burst)")

#     from data_plane.generators.source_generators import (
#         SalesHistoryGenerator, ManufacturingLogsGenerator
#     )
#     from data_plane.cdc.cdc_trigger import (
#         run_steady_stream, run_burst, load_existing_records
#     )

#     mfg_df  = pd.read_csv("storage/raw/manufacturing_logs.csv")
#     sal_df  = pd.read_csv("storage/raw/sales_history.csv")

#     sal_gen = SalesHistoryGenerator(sal_df, product_ids)
#     mfg_gen = ManufacturingLogsGenerator(mfg_df, product_ids)

#     # Load existing ingested records for UPDATE/DELETE targets
#     existing_sales = load_existing_records("storage/ingested/src_sales_history.parquet")
#     existing_mfg   = load_existing_records("storage/ingested/src_manufacturing_logs.parquet")

#     steady_rps = 10 * LOAD_MULTIPLIER
#     steady_duration = 10 * LOAD_MULTIPLIER
#     burst_count = 5000 * LOAD_MULTIPLIER
#     log.info(f"  ── 5a: Steady stream — Sales ({steady_rps} rec/s × {steady_duration}s) ──")
#     run_steady_stream(
#         source_id  = "src_sales_history",
#         dataset_id = "ds_sales_history",
#         existing   = existing_sales,
#         generator_fn = sal_gen.generate_one,
#         duration_sec = steady_duration,
#         target_rps   = steady_rps,
#     )

#     log.info(f"  ── 5b: Burst — Manufacturing ({burst_count} records) ──")
#     run_burst(
#         source_id    = "src_manufacturing_logs",
#         dataset_id   = "ds_manufacturing_logs",
#         existing     = existing_mfg,
#         generator_fn = mfg_gen.generate_one,
#         burst_count  = burst_count,
#     )


# # ──────────────────────────────────────────────────────────────────────────────
# # PHASE 6: CDC Strategies
# # ──────────────────────────────────────────────────────────────────────────────

# def run_phase_6():
#     banner("PHASE 6", "CDC Strategies — Log-Based, Trigger-Based, Timestamp-Based")

#     from data_plane.cdc.cdc_strategies import (
#         run_log_based_cdc, run_trigger_based_cdc, run_timestamp_based_cdc
#     )

#     sources_and_scenarios = [
#         ("src_sales_history",      "storage/cdc_log/src_sales_history_steady_cdc.parquet",      "steady"),
#         ("src_manufacturing_logs", "storage/cdc_log/src_manufacturing_logs_burst_cdc.parquet",   "burst"),
#     ]

#     for source_id, cdc_path, scenario in sources_and_scenarios:
#         if not os.path.exists(cdc_path):
#             log.warning(f"  [SKIP] CDC log not found: {cdc_path} — run Phase 5 first")
#             continue

#         log.info(f"  ── Log-Based CDC | {source_id} | {scenario} ──")
#         run_log_based_cdc(source_id, cdc_path, scenario)

#         log.info(f"  ── Trigger-Based CDC | {source_id} | {scenario} ──")
#         run_trigger_based_cdc(source_id, cdc_path, scenario)

#         log.info(f"  ── Timestamp-Based CDC | {source_id} | {scenario} ──")
#         run_timestamp_based_cdc(source_id, cdc_path, scenario)


# # ──────────────────────────────────────────────────────────────────────────────
# # PHASE 7: Observability summary
# # ──────────────────────────────────────────────────────────────────────────────

# def run_phase_7():
#     banner("PHASE 7", "Observability — Pipeline Summary & Storage Audit")

#     import glob

#     log.info("  ── Storage Audit ──")
#     dirs = {
#         "Ingested (Parquet)": "storage/ingested",
#         "Quarantine":         "storage/quarantine",
#         "CDC Log":            "storage/cdc_log",
#         "Micro-Batch":        "storage/micro_batch",
#         "Stream Buffer":      "storage/stream_buffer",
#         "Checkpoints":        "storage/checkpoints",
#         "Detail Logs (JSONL)":"storage/ingested/detail_logs",
#     }

#     total_files = 0
#     total_bytes = 0
#     for label, path in dirs.items():
#         if not os.path.exists(path):
#             continue
#         files = glob.glob(os.path.join(path, "**", "*.*"), recursive=True)
#         size  = sum(os.path.getsize(f) for f in files if os.path.isfile(f))
#         log.info(f"  {label:<30} {len(files):>4} files  {size/1024:>8.1f} KB")
#         total_files += len(files)
#         total_bytes += size

#     log.info(f"  {'─'*55}")
#     log.info(f"  {'TOTAL':<30} {total_files:>4} files  {total_bytes/1024:>8.1f} KB")

#     log.info("")
#     log.info("  ── Parquet Row Counts ──")
#     for label, path in dirs.items():
#         for fp in glob.glob(os.path.join(path, "*.parquet")):
#             try:
#                 df = pd.read_parquet(fp)
#                 log.info(f"  {os.path.basename(fp):<55} {len(df):>6} rows")
#             except Exception:
#                 pass

#     log.info("")
#     log.info("  ── Simple Dashboard ──")
#     # Mock throughput data for demo (in real scenario, collect from telemetry)
#     throughput_data = [100, 200, 150, 300, 250]  # Example values
#     plt.figure(figsize=(8,4))
#     plt.plot(throughput_data, marker='o')
#     plt.title('Sample Ingestion Throughput Over Time')
#     plt.xlabel('Batch')
#     plt.ylabel('Records/sec')
#     plt.grid(True)
#     plt.savefig('logs/dashboard_throughput.png')
#     log.info("  Dashboard plot saved to logs/dashboard_throughput.png")

#     log.info("")
#     log.info("  ── Log File Summary ──")
#     if os.path.exists(log_file):
#         with open(log_file, 'r') as f:
#             lines = f.readlines()
#         total_lines = len(lines)
#         error_lines = sum(1 for line in lines if '[ERROR]' in line)
#         warning_lines = sum(1 for line in lines if '[WARNING]' in line)
#         info_lines = sum(1 for line in lines if '[INFO]' in line)
#         log.info(f"  Log file: {log_file}")
#         log.info(f"  Total lines: {total_lines}")
#         log.info(f"  INFO: {info_lines}, WARNING: {warning_lines}, ERROR: {error_lines}")
#         if lines:
#             log.info("  Last 5 lines:")
#             for line in lines[-5:]:
#                 log.info(f"    {line.strip()}")
#     else:
#         log.warning("  No log file found")

#     from observability_plane.telemetry import JobTelemetry
#     reports = JobTelemetry.load_reports()
#     log.info("")
#     log.info("  ── Telemetry Audit ──")
#     log.info(f"  Telemetry reports written: {len(reports)}")
#     if reports:
#         sources = sorted({r.get('source_id') for r in reports})
#         log.info(f"  Sources covered by telemetry: {sources}")

#     log.info("")
#     log.info("  ── Ingestion Status ──")
#     stream_files = glob.glob(os.path.join("storage/stream_buffer", "*.parquet"))
#     if stream_files:
#         total_stream_events = 0
#         for fp in stream_files:
#             try:
#                 df = pd.read_parquet(fp)
#                 total_stream_events += len(df)
#             except:
#                 pass
#         log.info(f"  Streaming: {len(stream_files)} files, {total_stream_events} events ingested")
#     else:
#         log.info("  Streaming: No stream buffer files found (run Phase 4d or production mode)")

#     log.info("")
#     log.info("  ── Source Configuration & Next Ingestion ──")
#     from control_plane.entities import ALL_SOURCES
#     now = datetime.now(timezone.utc)
#     for src in ALL_SOURCES:
#         freq = src.ingestion_frequency.value
#         if freq == "real_time":
#             next_time = "Continuous"
#         elif freq == "every_2_minutes":
#             period_start = now.replace(second=0, microsecond=0)
#             minute_bucket = (period_start.minute // 2) * 2
#             next_run = period_start.replace(minute=minute_bucket)
#             if next_run <= now:
#                 next_run += timedelta(minutes=2)
#             remaining = next_run - now
#             next_time = f"{remaining.seconds // 60}m {(remaining.seconds % 60)}s"
#         elif freq == "hourly":
#             next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
#             remaining = next_hour - now
#             next_time = f"{remaining.seconds // 3600}h {(remaining.seconds % 3600) // 60}m"
#         elif freq == "daily":
#             next_day = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
#             remaining = next_day - now
#             next_time = f"{remaining.days}d {remaining.seconds // 3600}h"
#         elif freq == "weekly":
#             days_to_next = (7 - now.weekday()) % 7 or 7
#             next_week = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_to_next)
#             remaining = next_week - now
#             next_time = f"{remaining.days}d {remaining.seconds // 3600}h"
#         else:  # on_demand
#             next_time = "On Demand"
#         log.info(f"  {src.source_id:<25} | {src.source_type.value:<6} | {src.extraction_mode.value:<4} | {freq:<9} | Next: {next_time}")


# # ──────────────────────────────────────────────────────────────────────────────
# # MAIN
# # ──────────────────────────────────────────────────────────────────────────────

# def main():
#     pipeline_start = time.time()

#     run_phase_1_2()
#     product_ids = run_phase_3()
#     run_phase_4(product_ids)
#     run_phase_5(product_ids)
#     run_phase_6()
#     run_phase_7()

#     pipeline_end = time.time()
#     log.info("")
#     log.info("=" * 70)
#     log.info("  ALL 7 PHASES COMPLETE")
#     log.info(f"  Total pipeline duration: {pipeline_end - pipeline_start:.2f}s")
#     log.info("  Check storage/ for Parquet files, quarantine/, stream_buffer/, etc.")
#     log.info("=" * 70)


# if __name__ == "__main__":
#     main()

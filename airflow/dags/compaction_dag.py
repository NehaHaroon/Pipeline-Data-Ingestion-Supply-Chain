# """
# Compaction Pipeline DAG: Iceberg table maintenance

# This DAG runs scheduled compaction on Iceberg tables to:
# 1. Consolidate small files into optimally-sized files (128 MB target)
# 2. Improve query performance (fewer files = fewer seeks)
# 3. Reduce metadata overhead
# 4. Reclaim storage space from deleted records

# Schedule:
# - Bronze streaming tables (IoT, CDC): Every 30 minutes
# - Bronze batch tables: Daily at 2 AM
# - Silver: Every 4 hours
# - Gold: Every 2 hours (most read traffic)

# Compaction strategy: Bin-packing (combine small files, preserve large files)
# Compaction monitoring: Track files_before/after, duration, and compaction lag

# Design notes:
# - Compaction is a maintenance task (low priority, can be delayed)
# - Idempotent: re-running compaction is safe
# - Runs post-transformation to consolidate fresh data
# - Respects CompactionPolicy from storage_plane/iceberg_entities.py
# """

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import json
import logging
import os
import subprocess
import sys

from control_plane.service_registry import StorageLayer, table_name_for_layer

# Add project root to Python path (tasks that import project code directly)
sys.path.insert(0, "/opt/airflow/project")

log = logging.getLogger(__name__)

PROJECT_ROOT = os.environ.get("PIPELINE_PROJECT_ROOT", "/opt/airflow/project")
ICEBERG_SCRIPT = os.path.join(PROJECT_ROOT, "scripts", "run_iceberg_task.py")
ICEBERG_PY = os.environ.get("ICEBERG_TOOLKIT_PYTHON", "/opt/airflow/iceberg-toolkit/bin/python")


def _invoke_iceberg_toolkit(args: list[str], timeout_sec: int = 7200) -> dict:
    """
    Run Iceberg work in the toolkit venv (SQLAlchemy 2.x). Airflow's interpreter stays on SQLAlchemy 1.4.
    """
    if not os.path.isfile(ICEBERG_SCRIPT):
        raise AirflowException(f"Missing {ICEBERG_SCRIPT} — mount project into the Airflow container.")
    if not os.path.isfile(ICEBERG_PY):
        raise AirflowException(
            f"Missing Iceberg toolkit at {ICEBERG_PY}. Rebuild Dockerfile.airflow (iceberg-toolkit venv)."
        )
    env = {**os.environ, "PYTHONPATH": PROJECT_ROOT}
    cmd = [ICEBERG_PY, ICEBERG_SCRIPT, *args]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        cwd=PROJECT_ROOT,
        env=env,
    )
    if proc.returncode != 0:
        raise AirflowException(
            f"Iceberg toolkit failed ({proc.returncode}): {proc.stderr or proc.stdout}"
        )
    line = (proc.stdout or "").strip().splitlines()
    payload = line[-1] if line else "{}"
    return json.loads(payload)


# Same Silver sources as transformation_dag (avoid importing PyIceberg at DAG parse time).
_SILVER_SOURCE_IDS = [
    "src_warehouse_master",
    "src_sales_history",
    "src_manufacturing_logs",
    "src_legacy_trends",
    "src_iot_rfid_stream",
    "src_weather_api",
]
SILVER_TABLES = [
    table_name_for_layer(StorageLayer.SILVER, sid) for sid in _SILVER_SOURCE_IDS
]

# ═══════════════════════════════════════════════════════════════════════════════════
# DAG CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════════

default_args = {
    "retries": 1,  # Compaction failures are not critical
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2026, 1, 1),
    "catchup": False,
    "owner": "data-engineering",
}

dag = DAG(
    "supply_chain_iceberg_compaction",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Run daily at 2 AM
    description="Iceberg table compaction and maintenance",
    tags=["compaction", "maintenance", "iceberg"],
    catchup=False,
)

# ═══════════════════════════════════════════════════════════════════════════════════
# COMPACTION TASKS
# ═══════════════════════════════════════════════════════════════════════════════════

# """
# Run bin-pack compaction on an Iceberg table.

# Args:
#     table_name: Fully qualified table name (e.g., "bronze.iot_rfid_stream")

# Returns:
#     Dictionary with compaction metrics:
#     - table: table name
#     - files_before: number of files before
#     - files_after: number of files after
#     - files_compacted: how many files were merged
#     - duration_sec: how long compaction took

# Raises:
#     AirflowException: If compaction fails
# """
def compact_table(table_name: str) -> dict:
    import time

    try:
        log.info("Starting compaction for %s (iceberg toolkit)", table_name)
        t0 = time.time()
        result = _invoke_iceberg_toolkit(["compact", table_name])
        duration = time.time() - t0
        result["duration_sec"] = round(duration, 2)

        if result.get("skipped"):
            log.info("Skipped compaction for %s: %s", table_name, result.get("reason"))
        else:
            log.info(
                "Compaction completed for %s: %s → %s files, duration=%ss",
                table_name,
                result.get("files_before", 0),
                result.get("files_after", 0),
                duration,
            )
        return result
    except AirflowException:
        raise
    except Exception as e:
        log.error("Compaction failed for %s: %s", table_name, e, exc_info=True)
        raise AirflowException(f"Compaction failed for {table_name}: {e}") from e


# """
# Collect and log compaction metrics for monitoring.
# """
def collect_compaction_metrics(**context) -> dict:
    try:
        summary = _invoke_iceberg_toolkit(["compaction-health"], timeout_sec=600)
        n = summary.get("tables_needing_compaction", 0)
        log.info("Compaction health check: %s tables need attention (small_file_ratio > 0.5)", n)
        for table_name, ratio in summary.get("tables", []) or []:
            log.warning(
                "Table %s has small_file_ratio=%s — consider compaction",
                table_name,
                ratio,
            )
        return summary
    except Exception as e:
        log.error("Failed to collect compaction metrics: %s", e)
        return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════════
# TABLE CONFIGURATIONS FOR COMPACTION
# ═══════════════════════════════════════════════════════════════════════════════════

# Streaming tables (more frequent compaction due to small files)
STREAMING_TABLES = [
    "bronze.iot_rfid_stream",
    "bronze.inventory_transactions",
]

# Batch tables (less frequent)
BATCH_TABLES = [
    "bronze.sales_history",
    "bronze.warehouse_master",
    "bronze.manufacturing_logs",
    "bronze.legacy_trends",
    "bronze.weather_api",
]

# Silver: module-level SILVER_TABLES (same sources as transformation_dag).
GOLD_TABLES = [
    "gold.replenishment_signals",
]

# ═══════════════════════════════════════════════════════════════════════════════════
# TASK GROUPS & DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════════

# Compaction metrics check
# """
# Compact all Bronze tables. Bronze accumulates files from:
# - Batch ingestion (daily)
# - Streaming ingestion (continuous)

# Streaming Bronze tables are more prone to small files due to high event volume.
# """
metrics_task = PythonOperator(
    task_id="check_compaction_health",
    python_callable=collect_compaction_metrics,
    provide_context=True
)

with dag:
    with TaskGroup(
        "compact_bronze_tables",
        tooltip="Bin-pack consolidation for Bronze tables"
    ) as bronze_compact_tasks:
        for table_name in STREAMING_TABLES + BATCH_TABLES:
            PythonOperator(
                task_id=f"compact_{table_name.replace('.', '_')}",
                python_callable=compact_table,
                op_kwargs={"table_name": table_name},
            )

# Silver table compaction group
# """
# Silver tables grow as Silver transformations append deduplicated data.
# Less prone to small files than Bronze, but still need periodic compaction.
# """
# Auto-detect Silver tables from catalog
with dag:
    with TaskGroup(
        "compact_silver_tables",
        tooltip="Consolidation for Silver tables"   
    ) as silver_compact_tasks:
        for table_name in SILVER_TABLES:
            PythonOperator(
                task_id=f"compact_{table_name.replace('.', '_')}",
                python_callable=compact_table,
                op_kwargs={"table_name": table_name},
            )

# Gold table compaction group
# """
# Gold tables are the primary read tables (dashboards, APIs).
# Frequent compaction ensures fast query performance.
# """
with dag:
    with TaskGroup(
        "compact_gold_tables",
        tooltip="Consolidation for Gold tables"
    ) as gold_compact_tasks:
        for table_name in GOLD_TABLES:
            PythonOperator(
                task_id=f"compact_{table_name.replace('.', '_')}",
                python_callable=compact_table,
                op_kwargs={"table_name": table_name},
            )

# ═══════════════════════════════════════════════════════════════════════════════════
# TASK DEPENDENCIES
# ═══════════════════════════════════════════════════════════════════════════════════

metrics_task >> [bronze_compact_tasks, silver_compact_tasks, gold_compact_tasks]

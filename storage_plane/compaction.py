
# from pyiceberg.catalog import Catalog
# from storage_plane.iceberg_catalog import get_catalog
# import logging

# log = logging.getLogger(__name__)

# class CompactionRunner:
#     """
#     Runs Iceberg bin-pack compaction on tables that exceed the
#     min_files_to_compact threshold in their CompactionPolicy.
#     Called by compaction_dag.py on schedule.
#     """

#     def run_table(self, table_name: str) -> dict:
#         catalog = get_catalog()
#         if not catalog.table_exists(table_name):
#             return {"skipped": True, "reason": "table does not exist"}

#         t = catalog.load_table(table_name)
#         files_before = len(t.inspect.files().to_pydict().get("file_path", []))
#         # PyIceberg 0.7+ exposes table.rewrite_data_files()
#         result = t.rewrite_data_files(
#             strategy="binpack",
#             options={"target-file-size-bytes": str(128 * 1024 * 1024)}
#         )
#         files_after = len(t.inspect.files().to_pydict().get("file_path", []))
#         log.info(f"Compacted {table_name}: {files_before} → {files_after} files")
#         return {
#             "table": table_name,
#             "files_before": files_before,
#             "files_after": files_after,
#             "files_compacted": files_before - files_after,
#         }

"""
Compaction DAG: Iceberg file compaction (bin-pack)

This DAG periodically compacts small files in Iceberg tables
to improve query performance and reduce metadata overhead.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging
import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from storage_plane.iceberg_catalog import get_catalog

log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════════

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
    "owner": "data-engineering",
}

TABLES_TO_COMPACT = [
    "bronze_sales_history",
    "bronze_inventory_transactions",
    "silver_sales_history",
    "gold_replenishment_signals",
]

# ═══════════════════════════════════════════════════════════════════════════════════
# TASK LOGIC
# ═══════════════════════════════════════════════════════════════════════════════════

def compact_table(table_name: str) -> dict:
    try:
        catalog = get_catalog()

        if not catalog.table_exists(table_name):
            log.warning(f"{table_name} does not exist")
            return {"skipped": True}

        table = catalog.load_table(table_name)

        files_before = len(
            table.inspect.files().to_pydict().get("file_path", [])
        )

        table.rewrite_data_files(
            strategy="binpack",
            options={"target-file-size-bytes": str(128 * 1024 * 1024)}
        )

        files_after = len(
            table.inspect.files().to_pydict().get("file_path", [])
        )

        log.info(f"{table_name}: {files_before} → {files_after}")

        return {
            "table": table_name,
            "files_before": files_before,
            "files_after": files_after,
            "compacted": files_before - files_after,
        }

    except Exception as e:
        log.error(f"Compaction failed for {table_name}: {e}", exc_info=True)
        raise


# ═══════════════════════════════════════════════════════════════════════════════════
# DAG (FIXED)
# ═══════════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="iceberg_compaction",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["compaction", "iceberg"],
) as dag:

    # Task group for parallel compaction
    with TaskGroup(
        group_id="compact_tables",
        tooltip="Compact Iceberg tables"
    ) as compaction_group:

        for table_name in TABLES_TO_COMPACT:
            PythonOperator(
                task_id=f"compact_{table_name}",
                python_callable=compact_table,
                op_kwargs={"table_name": table_name},
            )
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

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.task_group import TaskGroup
# from airflow.exceptions import AirflowException
# from datetime import datetime, timedelta
# import logging
# import sys
# import os


# log = logging.getLogger(__name__)

# # ═══════════════════════════════════════════════════════════════════════════════════
# # region DAG CONFIGURATION
# # ═══════════════════════════════════════════════════════════════════════════════════

# default_args = {
#     "retries": 1,  # Compaction failures are not critical
#     "retry_delay": timedelta(minutes=10),
#     "start_date": datetime(2026, 1, 1),
#     "catchup": False,
#     "owner": "data-engineering",
# }

# with DAG(
#     "supply_chain_iceberg_compaction",
#     default_args=default_args,
#     schedule_interval="0 2 * * *",  # Run daily at 2 AM
#     description="Iceberg table compaction and maintenance",
#     tags=["compaction", "maintenance", "iceberg"],
#     catchup=False,
# ) as dag:

# # ═══════════════════════════════════════════════════════════════════════════════════
# # region COMPACTION TASKS
# # ═══════════════════════════════════════════════════════════════════════════════════

# def compact_table(table_name: str) -> dict:
#     """
#     Run bin-pack compaction on an Iceberg table.
    
#     Args:
#         table_name: Fully qualified table name (e.g., "bronze.iot_rfid_stream")
    
#     Returns:
#         Dictionary with compaction metrics:
#         - table: table name
#         - files_before: number of files before
#         - files_after: number of files after
#         - files_compacted: how many files were merged
#         - duration_sec: how long compaction took
    
#     Raises:
#         AirflowException: If compaction fails
#     """
#     try:
#         from storage_plane.compaction import CompactionRunner
#         import time
        
#         log.info(f"Starting compaction for {table_name}")
#         t0 = time.time()
        
#         runner = CompactionRunner()
#         result = runner.run_table(table_name)
        
#         duration = time.time() - t0
#         result["duration_sec"] = round(duration, 2)
        
#         if result.get("skipped"):
#             log.info(f"Skipped compaction for {table_name}: {result.get('reason')}")
#         else:
#             log.info(
#                 f"Compaction completed for {table_name}: "
#                 f"{result.get('files_before', 0)} → {result.get('files_after', 0)} files, "
#                 f"duration={duration:.1f}s"
#             )
        
#         return result
    
#     except Exception as e:
#         log.error(f"Compaction failed for {table_name}: {e}", exc_info=True)
#         raise AirflowException(f"Compaction failed for {table_name}: {e}")


# def collect_compaction_metrics(**context) -> dict:
#     """
#     Collect and log compaction metrics for monitoring.
#     """
#     from storage_plane.iceberg_catalog import get_catalog
#     from storage_plane.storage_kpis import get_all_tables_kpis
    
#     try:
#         all_kpis = get_all_tables_kpis()
        
#         # Find tables that need compaction (small_file_ratio > 0.5)
#         tables_needing_compaction = [
#             (name, kpis.get("small_file_ratio", 0))
#             for name, kpis in all_kpis.items()
#             if not "error" in kpis and kpis.get("small_file_ratio", 0) > 0.5
#         ]
        
#         log.info(
#             f"Compaction health check: {len(tables_needing_compaction)} tables "
#             f"need attention (small_file_ratio > 0.5)"
#         )
        
#         for table_name, ratio in tables_needing_compaction:
#             log.warning(
#                 f"Table {table_name} has small_file_ratio={ratio:.1%}, "
#                 f"consider running compaction"
#             )
        
#         return {
#             "total_tables": len(all_kpis),
#             "tables_needing_compaction": len(tables_needing_compaction),
#             "tables": tables_needing_compaction,
#         }
    
#     except Exception as e:
#         log.error(f"Failed to collect compaction metrics: {e}")
#         return {"error": str(e)}


# # ═══════════════════════════════════════════════════════════════════════════════════
# # region TABLE CONFIGURATIONS FOR COMPACTION
# # ═══════════════════════════════════════════════════════════════════════════════════

# # Streaming tables (more frequent compaction due to small files)
# STREAMING_TABLES = [
#     "bronze.iot_rfid_stream",
#     "bronze.inventory_transactions",
# ]

# # Batch tables (less frequent)
# BATCH_TABLES = [
#     "bronze.sales_history",
#     "bronze.warehouse_master",
#     "bronze.manufacturing_logs",
#     "bronze.legacy_trends",
#     "bronze.weather_api",
# ]

# # Silver and Gold (already relatively well-formed)
# SILVER_TABLES = []  # Will be auto-detected
# GOLD_TABLES = [
#     "gold.replenishment_signals",
# ]

# # ═══════════════════════════════════════════════════════════════════════════════════
# # region TASK GROUPS & DEFINITIONS
# # ═══════════════════════════════════════════════════════════════════════════════════

# # Compaction metrics check
# metrics_task = PythonOperator(
#     task_id="check_compaction_health",
#     python_callable=collect_compaction_metrics,
#     provide_context=True,
# )

# # Bronze table compaction group
# with TaskGroup(
#     "compact_bronze_tables",
#     tooltip="Bin-pack consolidation for Bronze tables"
# ) as bronze_compact_tasks:
#     """
#     Compact all Bronze tables. Bronze accumulates files from:
#     - Batch ingestion (daily)
#     - Streaming ingestion (continuous)
    
#     Streaming Bronze tables are more prone to small files due to high event volume.
#     """
#     for table_name in STREAMING_TABLES + BATCH_TABLES:
#         PythonOperator(
#             task_id=f"compact_{table_name.replace('.', '_')}",
#             python_callable=compact_table,
#             op_kwargs={"table_name": table_name},
#         )

# # Silver table compaction group
# with TaskGroup(
#     "compact_silver_tables",
#     tooltip="Consolidation for Silver tables"
# ) as silver_compact_tasks:
#     """
#     Silver tables grow as Silver transformations append deduplicated data.
#     Less prone to small files than Bronze, but still need periodic compaction.
#     """
#     # Auto-detect Silver tables from catalog
#     try:
#         from storage_plane.iceberg_catalog import get_catalog
#         catalog = get_catalog()
#         silver_table_ids = catalog.list_tables("silver")
#         SILVER_TABLES = [f"silver.{tid[1]}" for tid in silver_table_ids]
#     except Exception as e:
#         log.warning(f"Could not auto-detect Silver tables: {e}")
    
#     for table_name in SILVER_TABLES:
#         PythonOperator(
#             task_id=f"compact_{table_name.replace('.', '_')}",
#             python_callable=compact_table,
#             op_kwargs={"table_name": table_name},
#         )

# # Gold table compaction group
# with TaskGroup(
#     "compact_gold_tables",
#     tooltip="Consolidation for Gold tables"
# ) as gold_compact_tasks:
#     """
#     Gold tables are the primary read tables (dashboards, APIs).
#     Frequent compaction ensures fast query performance.
#     """
#     for table_name in GOLD_TABLES:
#         PythonOperator(
#             task_id=f"compact_{table_name.replace('.', '_')}",
#             python_callable=compact_table,
#             op_kwargs={"table_name": table_name},
#         )

# # ═══════════════════════════════════════════════════════════════════════════════════
# # region TASK DEPENDENCIES
# # ═══════════════════════════════════════════════════════════════════════════════════

# metrics_task >> [bronze_compact_tasks, silver_compact_tasks, gold_compact_tasks]

"""
Compaction Pipeline DAG: Iceberg table maintenance

This DAG runs scheduled compaction on Iceberg tables to:
1. Consolidate small files into optimally-sized files (128 MB target)
2. Improve query performance (fewer files = fewer seeks)
3. Reduce metadata overhead
4. Reclaim storage space from deleted records

Schedule:
- Bronze streaming tables (IoT, CDC): Every 30 minutes
- Bronze batch tables: Daily at 2 AM
- Silver: Every 4 hours
- Gold: Every 2 hours (most read traffic)

Compaction strategy: Bin-packing (combine small files, preserve large files)
Compaction monitoring: Track files_before/after, duration, and compaction lag

Design notes:
- Compaction is a maintenance task (low priority, can be delayed)
- Idempotent: re-running compaction is safe
- Runs post-transformation to consolidate fresh data
- Respects CompactionPolicy from storage_plane/iceberg_entities.py
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import sys
import os

log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════════
# region DAG CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════════

default_args = {
    "retries": 1,  # Compaction failures are not critical
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2026, 1, 1),
    "catchup": False,
    "owner": "data-engineering",
}

with DAG(
    "supply_chain_iceberg_compaction",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Run daily at 2 AM
    description="Iceberg table compaction and maintenance",
    tags=["compaction", "maintenance", "iceberg"],
    catchup=False,
) as dag:  # ✅ FIX: added colon

# ═══════════════════════════════════════════════════════════════════════════════════
# region COMPACTION TASKS
# ═══════════════════════════════════════════════════════════════════════════════════

    def compact_table(table_name: str) -> dict:
        """
        Run bin-pack compaction on an Iceberg table.
        
        Args:
            table_name: Fully qualified table name (e.g., "bronze.iot_rfid_stream")
        
        Returns:
            Dictionary with compaction metrics:
            - table: table name
            - files_before: number of files before
            - files_after: number of files after
            - files_compacted: how many files were merged
            - duration_sec: how long compaction took
        
        Raises:
            AirflowException: If compaction fails
        """
        try:
            from storage_plane.compaction import CompactionRunner
            import time
            
            log.info(f"Starting compaction for {table_name}")
            t0 = time.time()
            
            runner = CompactionRunner()
            result = runner.run_table(table_name)
            
            duration = time.time() - t0
            result["duration_sec"] = round(duration, 2)
            
            if result.get("skipped"):
                log.info(f"Skipped compaction for {table_name}: {result.get('reason')}")
            else:
                log.info(
                    f"Compaction completed for {table_name}: "
                    f"{result.get('files_before', 0)} → {result.get('files_after', 0)} files, "
                    f"duration={duration:.1f}s"
                )
            
            return result
        
        except Exception as e:
            log.error(f"Compaction failed for {table_name}: {e}", exc_info=True)
            raise AirflowException(f"Compaction failed for {table_name}: {e}")


    def collect_compaction_metrics(**context) -> dict:
        """
        Collect and log compaction metrics for monitoring.
        """
        from storage_plane.iceberg_catalog import get_catalog
        from storage_plane.storage_kpis import get_all_tables_kpis
        
        try:
            all_kpis = get_all_tables_kpis()
            
            # Find tables that need compaction (small_file_ratio > 0.5)
            tables_needing_compaction = [
                (name, kpis.get("small_file_ratio", 0))
                for name, kpis in all_kpis.items()
                if not "error" in kpis and kpis.get("small_file_ratio", 0) > 0.5
            ]
            
            log.info(
                f"Compaction health check: {len(tables_needing_compaction)} tables "
                f"need attention (small_file_ratio > 0.5)"
            )
            
            for table_name, ratio in tables_needing_compaction:
                log.warning(
                    f"Table {table_name} has small_file_ratio={ratio:.1%}, "
                    f"consider running compaction"
                )
            
            return {
                "total_tables": len(all_kpis),
                "tables_needing_compaction": len(tables_needing_compaction),
                "tables": tables_needing_compaction,
            }
        
        except Exception as e:
            log.error(f"Failed to collect compaction metrics: {e}")
            return {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════════════
# region TABLE CONFIGURATIONS FOR COMPACTION
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

    # Silver and Gold (already relatively well-formed)
    SILVER_TABLES = []  # Will be auto-detected
    GOLD_TABLES = [
        "gold.replenishment_signals",
    ]

# ═══════════════════════════════════════════════════════════════════════════════════
# region TASK GROUPS & DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════════

    # Compaction metrics check
    metrics_task = PythonOperator(
        task_id="check_compaction_health",
        python_callable=collect_compaction_metrics,
    )

    # Bronze table compaction group
    with TaskGroup(
        "compact_bronze_tables",
        tooltip="Bin-pack consolidation for Bronze tables"
    ) as bronze_compact_tasks:
        """
        Compact all Bronze tables. Bronze accumulates files from:
        - Batch ingestion (daily)
        - Streaming ingestion (continuous)
        
        Streaming Bronze tables are more prone to small files due to high event volume.
        """
        bronze_tasks = []
        for table_name in STREAMING_TABLES + BATCH_TABLES:
            task = PythonOperator(
                task_id=f"compact_{table_name.replace('.', '_')}",
                python_callable=compact_table,
                op_kwargs={"table_name": table_name},
            )
            bronze_tasks.append(task)

    # Silver table compaction group
    with TaskGroup(
        "compact_silver_tables",
        tooltip="Consolidation for Silver tables"
    ) as silver_compact_tasks:
        """
        Silver tables grow as Silver transformations append deduplicated data.
        Less prone to small files than Bronze, but still need periodic compaction.
        """
        # Auto-detect Silver tables from catalog
        try:
            from storage_plane.iceberg_catalog import get_catalog
            catalog = get_catalog()
            silver_table_ids = catalog.list_tables("silver")
            SILVER_TABLES = [f"silver.{tid[1]}" for tid in silver_table_ids]
        except Exception as e:
            log.warning(f"Could not auto-detect Silver tables: {e}")
        
        silver_tasks = []
        for table_name in SILVER_TABLES:
            task = PythonOperator(
                task_id=f"compact_{table_name.replace('.', '_')}",
                python_callable=compact_table,
                op_kwargs={"table_name": table_name},
            )
            silver_tasks.append(task)

    # Gold table compaction group
    with TaskGroup(
        "compact_gold_tables",
        tooltip="Consolidation for Gold tables"
    ) as gold_compact_tasks:
        """
        Gold tables are the primary read tables (dashboards, APIs).
        Frequent compaction ensures fast query performance.
        """
        gold_tasks = []
        for table_name in GOLD_TABLES:
            task = PythonOperator(
                task_id=f"compact_{table_name.replace('.', '_')}",
                python_callable=compact_table,
                op_kwargs={"table_name": table_name},
            )
            gold_tasks.append(task)

# ═══════════════════════════════════════════════════════════════════════════════════
# region TASK DEPENDENCIES
# ═══════════════════════════════════════════════════════════════════════════════════

metrics_task >> [bronze_compact_tasks, silver_compact_tasks, gold_compact_tasks]
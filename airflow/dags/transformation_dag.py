"""
Transformation Pipeline DAG: Bronze → Silver → Gold

This DAG orchestrates the data transformation layers:
1. Silver Layer: Data cleaning, schema validation, deduplication, type casting
2. Gold Layer: Aggregations, business metrics, replenishment signals

Schedule: Runs 30 min after ingestion completes (via external trigger or sensor)
Idempotency: Transformation tasks use Iceberg snapshot IDs to avoid reprocessing

Design notes:
- Each source has its own Bronze→Silver task
- Silver→Gold is a single aggregation step (uses all Silver tables)
- Error handling: Failed tasks are retried 2x before alerting
- KPIs are logged to storage/ingested/detail_logs/transformation_kpis.jsonl
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════════
# DAG CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════════

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
    "catchup": False,
    "owner": "data-engineering",
}

dag = DAG(
    "supply_chain_transformation",
    default_args=default_args,
    schedule_interval="@hourly",  # Run every hour
    description="Bronze → Silver → Gold transformation pipeline",
    tags=["transformation", "supply-chain", "iceberg"],
    catchup=False,
)

# ═══════════════════════════════════════════════════════════════════════════════════
# SILVER LAYER TRANSFORMATIONS (one task per source)
# ═══════════════════════════════════════════════════════════════════════════════════

def transform_silver(source_id: str) -> dict:
    """
    Execute Silver transformation for a single source.
    
    Args:
        source_id: Source identifier (e.g., "src_sales_history")
    
    Returns:
        Dictionary with transformation results and KPIs
    
    Raises:
        AirflowException: If transformation fails
    """
    try:
        from data_plane.transformation.silver_transformer import SilverTransformer
        from data_plane.transformation.transformation_kpis import TransformationKPILogger
        
        log.info(f"Starting Silver transformation for {source_id}")
        
        transformer = SilverTransformer(source_id)
        result = transformer.transform()
        
        log.info(
            f"Silver transformation completed for {source_id}: "
            f"read={result.records_read}, cleaned={result.records_cleaned}, "
            f"rejected={result.records_rejected}, latency={result.transformation_latency_sec}s"
        )
        
        return {
            "source_id": source_id,
            "layer": "silver",
            "records_read": result.records_read,
            "records_cleaned": result.records_cleaned,
            "records_rejected": result.records_rejected,
            "latency_sec": result.transformation_latency_sec,
            "status": "success",
        }
    
    except Exception as e:
        log.error(f"Silver transformation failed for {source_id}: {e}", exc_info=True)
        raise AirflowException(f"Silver transformation failed for {source_id}: {e}")


def transform_gold() -> dict:
    """
    Execute Gold layer aggregations (joins Silver tables).
    
    Gold operations:
    - Daily sales aggregations
    - 7-day rolling defect rates
    - Current shelf stock (latest per product)
    - Replenishment urgency scoring
    - Weather risk flagging
    
    Returns:
        Dictionary with aggregation results
    
    Raises:
        AirflowException: If aggregation fails
    """
    try:
        from data_plane.transformation.gold_aggregator import GoldAggregator
        from data_plane.transformation.transformation_kpis import TransformationKPILogger
        
        log.info("Starting Gold aggregation")
        
        aggregator = GoldAggregator()
        result = aggregator.run()
        
        log.info(
            f"Gold aggregation completed: "
            f"products_checked={result.get('products_checked', 0)}, "
            f"replenishment_signals={result.get('records_written', 0)}, "
            f"weather_risk={result.get('weather_risk_active', False)}"
        )
        
        return {
            "layer": "gold",
            "products_checked": result.get("products_checked", 0),
            "replenishment_signals": result.get("records_written", 0),
            "weather_risk_active": result.get("weather_risk_active", False),
            "status": "success",
        }
    
    except Exception as e:
        log.error(f"Gold aggregation failed: {e}", exc_info=True)
        raise AirflowException(f"Gold aggregation failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════════
# TASK GROUPS & TASK DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════════

# Define all sources that need Silver transformation
SOURCES_FOR_SILVER = [
    "src_sales_history",
    "src_warehouse_master",
    "src_manufacturing_logs",
    "src_iot_rfid_stream",
    "src_inventory_transactions",
    "src_legacy_trends",
    "src_weather_api",
]

with TaskGroup(
    "silver_transformations",
    tooltip="Bronze → Silver for all sources",
    parent_dag=dag,
) as silver_tasks:
    """
    Parallel Silver transformations, one per source.
    Each task reads from Bronze, applies cleaning rules, writes to Silver.
    Tasks run in parallel since they're independent.
    """
    silver_results = {}
    for source_id in SOURCES_FOR_SILVER:
        task = PythonOperator(
            task_id=f"transform_silver_{source_id.replace('src_', '')}",
            python_callable=transform_silver,
            op_kwargs={"source_id": source_id},
            provide_context=True,
            pool="default_pool",
            pool_slots=1,
        )
        silver_results[source_id] = task


# Gold aggregation task (depends on all Silver tasks)
gold_task = PythonOperator(
    task_id="transform_gold",
    python_callable=transform_gold,
    provide_context=True,
    pool="default_pool",
)

# ═══════════════════════════════════════════════════════════════════════════════════
# TASK DEPENDENCIES
# ═══════════════════════════════════════════════════════════════════════════════════

# All Silver tasks must complete before Gold aggregation
silver_tasks >> gold_task

# Optional: Emit summary metrics
def emit_transformation_summary(**context) -> dict:
    """
    Emit a summary of the transformation run for monitoring/alerting.
    """
    from data_plane.transformation.transformation_kpis import TransformationKPILogger
    
    silver_stats = TransformationKPILogger.get_aggregate_stats("silver")
    gold_stats = TransformationKPILogger.get_aggregate_stats("gold")
    
    summary = {
        "run_date": context["execution_date"].isoformat(),
        "silver": silver_stats,
        "gold": gold_stats,
    }
    
    log.info(f"Transformation summary: {summary}")
    return summary


summary_task = PythonOperator(
    task_id="emit_summary",
    python_callable=emit_transformation_summary,
    provide_context=True,
    trigger_rule="all_done",  # Run even if some tasks fail
)

gold_task >> summary_task

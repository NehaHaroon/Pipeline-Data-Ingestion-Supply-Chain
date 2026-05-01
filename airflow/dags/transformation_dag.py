# """
# Transformation Pipeline DAG: Bronze → Silver → Gold
# """

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import sys
import os
import requests

# Add project root to Python path
sys.path.insert(0, '/opt/airflow/project')

log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════════

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
    "owner": "data-engineering"
}

SOURCES_FOR_SILVER = [
    "src_warehouse_master",
    "src_sales_history",
    "src_manufacturing_logs",
    "src_legacy_trends"
]

# ═══════════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════════

def transform_silver(source_id: str) -> dict:
    """Call the transform service to run Silver transformation."""
    try:
        url = "http://transform-service:8001/transform/silver/{}".format(source_id)
        log.info(f"Starting Silver transformation for {source_id} via {url}")

        response = requests.post(url, timeout=300)  # 5 minute timeout
        response.raise_for_status()

        result = response.json()
        log.info(
            f"Silver done for {source_id}: "
            f"read={result.get('records_read', 0)}, cleaned={result.get('records_cleaned', 0)}, "
            f"rejected={result.get('records_rejected', 0)}"
        )

        return result

    except Exception as e:
        log.error(f"Silver failed for {source_id}: {e}", exc_info=True)
        raise AirflowException(str(e))

def transform_gold() -> dict:
    """Call the transform service to run Gold aggregation."""
    try:
        url = "http://transform-service:8001/transform/gold"
        log.info("Starting Gold aggregation via {}".format(url))

        response = requests.post(url, timeout=300)  # 5 minute timeout
        response.raise_for_status()

        result = response.json()
        log.info(f"Gold completed: {result}")

        return result

    except Exception as e:
        log.error(f"Gold failed: {e}", exc_info=True)
        raise AirflowException(str(e))


# ═══════════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS FOR DAG
# ═══════════════════════════════════════════════════════════════════════════════════

def trigger_silver(source_id: str) -> dict:
    """Trigger Silver transformation via the transform service."""
    try:
        url = "http://transform-service:8001/transform/silver/{}".format(source_id)
        log.info(f"Triggering Silver transformation for {source_id}")
        
        response = requests.post(url, timeout=300)
        response.raise_for_status()
        
        result = response.json()
        log.info(f"Silver triggered for {source_id}: {result}")
        return result
    
    except Exception as e:
        log.error(f"Silver trigger failed for {source_id}: {e}", exc_info=True)
        raise AirflowException(str(e))

def trigger_gold():
    url = "http://transform-service:8001/transform/gold"
    response = requests.post(url)

    if response.status_code != 200:
        raise Exception(f"Gold failed: {response.text}")

    return response.json()

def emit_transformation_summary(**context):
    from data_plane.transformation.transformation_kpis import TransformationKPILogger

    silver_stats = TransformationKPILogger.get_aggregate_stats("silver")
    gold_stats = TransformationKPILogger.get_aggregate_stats("gold")

    summary = {
        "run_date": context["ds"],
        "silver": silver_stats,
        "gold": gold_stats,
    }

    log.info(f"Transformation summary: {summary}")
    return summary


# ═══════════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION (IMPORTANT FIX)
# ═══════════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="supply_chain_transformation",
    default_args=default_args,
    schedule_interval="@hourly",
    description="Bronze → Silver → Gold transformation pipeline",
    catchup=False,
    tags=["transformation", "supply-chain"],
) as dag:

    # ─────────────────────────────────────────────────────────────
    # WAIT FOR INGESTION
    # ─────────────────────────────────────────────────────────────
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="supply_chain_ingestion",
        external_task_id=None,  # Wait for entire DAG
        timeout=3600,  # 1 hour timeout
        poke_interval=60,  # Check every minute
        mode="reschedule"
    )

    # ─────────────────────────────────────────────────────────────
    # SILVER TASK GROUP
    # ─────────────────────────────────────────────────────────────
    with TaskGroup(
        group_id="silver_transformations",
        tooltip="Bronze → Silver"
    ) as silver_tasks:

        silver_results = {}

        for source_id in SOURCES_FOR_SILVER:
            task = PythonOperator(
                task_id=f"trigger_silver_{source_id.replace('src_', '')}",
                python_callable=trigger_silver,
                op_kwargs={"source_id": source_id},
            )
            silver_results[source_id] = task

    # ─────────────────────────────────────────────────────────────
    # GOLD TASK
    # ─────────────────────────────────────────────────────────────
    gold_task = PythonOperator(
        task_id="trigger_gold",
        python_callable=trigger_gold,
    )

    # ─────────────────────────────────────────────────────────────
    # SUMMARY TASK
    # ─────────────────────────────────────────────────────────────
    summary_task = PythonOperator(
        task_id="emit_summary",
        python_callable=emit_transformation_summary,
        trigger_rule="all_done",
    )

    # ─────────────────────────────────────────────────────────────
    # DEPENDENCIES
    # ─────────────────────────────────────────────────────────────
    wait_for_ingestion >> silver_tasks >> gold_task >> summary_task
    # """
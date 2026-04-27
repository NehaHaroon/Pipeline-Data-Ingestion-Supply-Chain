from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {"retries": 3, "retry_delay": timedelta(minutes=2)}
SOURCES = [
    "src_warehouse_master",
    "src_sales_history",
    "src_manufacturing_logs",
    "src_legacy_trends",
]

with DAG(
    dag_id="supply_chain_ingestion",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion"],
) as dag:

    tasks = []

    for source_id in SOURCES:
        task = SimpleHttpOperator(
            task_id=f"ingest_{source_id}",
            http_conn_id="ingestion_api",
            endpoint=f"/ingest/{source_id}",
            method="POST",
            headers={
                "Authorization": "Bearer {{ var.value.API_TOKEN }}",
                "Content-Type": "application/json",
            },
            data={"trigger": True},
        )
        tasks.append(task)

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
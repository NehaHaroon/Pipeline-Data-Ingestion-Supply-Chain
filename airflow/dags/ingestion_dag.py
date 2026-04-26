
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta

default_args = {"retries": 3, "retry_delay": timedelta(minutes=2)}

with DAG("supply_chain_ingestion", schedule_interval="@daily",
         start_date=datetime(2026, 1, 1), catchup=False,
         default_args=default_args) as dag:

    for source_id in ["src_warehouse_master", "src_sales_history",
                       "src_manufacturing_logs", "src_legacy_trends"]:
        SimpleHttpOperator(
            task_id=f"ingest_{source_id}",
            http_conn_id="ingestion_api",   # set in Airflow Connections UI
            endpoint=f"/ingest/{source_id}",
            method="POST",
            headers={"Authorization": "Bearer {{ var.value.API_TOKEN }}"},
            data='{"records": []}',         # trigger-only; actual data loading
        )                                   # happens inside your existing ingest code

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os

default_args = {"retries": 3, "retry_delay": timedelta(minutes=2)}

def load_and_ingest_source(source_id: str):
    """Load CSV data and send to ingestion API."""
    # Map source_id to CSV file path
    csv_files = {
        "src_warehouse_master": "/opt/airflow/project/storage/raw/warehouse_master.csv",
        "src_sales_history": "/opt/airflow/project/storage/raw/sales_history.csv",
        "src_manufacturing_logs": "/opt/airflow/project/storage/raw/manufacturing_logs.csv",
        "src_legacy_trends": "/opt/airflow/project/storage/raw/legacy_trends.csv"
    }

    csv_path = csv_files.get(source_id)
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Load CSV data
    df = pd.read_csv(csv_path)
    records = df.to_dict('records')

    # Send to API in batches to avoid payload size limits
    batch_size = 1000
    # for i in range(0, len(records), batch_size):
    #     batch = records[i:i + batch_size]
        # API call will be made by the HTTP operator
        # return json.dumps({"records": batch})
        # return {"records": batch}
    return {"records": records}


with DAG("supply_chain_ingestion", schedule_interval="@hourly",
         start_date=datetime(2026, 1, 1), catchup=False,
         default_args=default_args) as dag:

    for source_id in ["src_warehouse_master", "src_sales_history",
                       "src_manufacturing_logs", "src_legacy_trends"]:

        # Load data task
        load_task = PythonOperator(
            task_id=f"load_{source_id}",
            python_callable=load_and_ingest_source,
            op_kwargs={"source_id": source_id}
        )

        # Ingest task
        ingest_task = SimpleHttpOperator(
            task_id=f"ingest_{source_id}",
            http_conn_id="ingestion-api",
            endpoint=f"/ingest/{source_id}",
            method="POST",
            headers={
                "Authorization": "Bearer ee910d618e617c559f1ca41a3a48c3c7",
                "Content-Type": "application/json"},
            # data="{{ task_instance.xcom_pull(task_ids='load_{}') }}".format(source_id),
            data="{{ ti.xcom_pull(task_ids='load_" + source_id + "') | tojson }}",
            log_response=True
        )

        load_task >> ingest_task
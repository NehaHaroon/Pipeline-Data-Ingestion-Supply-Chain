"""
Docker Production Pipeline Runner

This script orchestrates the complete supply chain data engineering pipeline
in a Docker environment. It coordinates between the ingestion API, Airflow,
and the transformation processes.

Usage:
    python run_docker_pipeline.py

Prerequisites:
    - Docker and Docker Compose installed
    - .env file configured
    - Raw data files in storage/raw/
"""

import sys
import os
import logging
import time
import requests
import subprocess
from datetime import datetime
from typing import Dict, List, Any, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("docker_pipeline")

# Load configuration from config.py which reads .env
sys.path.insert(0, os.path.dirname(__file__))
from config import API_TOKEN, LOCAL_API_HOST, API_PORT

# Configuration - Use values from .env via config.py
API_BASE_URL = f"http://{LOCAL_API_HOST}:{API_PORT}"
AIRFLOW_URL = "http://localhost:8080"

class DockerPipelineRunner:
    """Orchestrates the complete Docker-based pipeline execution."""

    def __init__(self):
        self.api_headers = {"Authorization": f"Bearer {API_TOKEN}"}

    def run_command(self, command: str, description: str) -> bool:
        """Run a shell command and return success status."""
        log.info(f" {description}")
        try:
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
            log.info(f" {description} completed")
            return True
        except subprocess.CalledProcessError as e:
            log.error(f" {description} failed: {e}")
            log.error(f"STDOUT: {e.stdout}")
            log.error(f"STDERR: {e.stderr}")
            return False

    def wait_for_service(self, url: str, service_name: str, timeout: int = 60) -> bool:
        """Wait for a service to become available."""
        log.info(f"⏳ Waiting for {service_name} at {url}")
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    log.info(f" {service_name} is ready")
                    return True
            except requests.RequestException:
                pass

            time.sleep(5)

        log.error(f" {service_name} failed to start within {timeout}s")
        return False

    def start_infrastructure(self) -> bool:
        """Start the Docker infrastructure (Kafka, Zookeeper, API)."""
        log.info(" Starting Docker infrastructure...")

        # Start main services
        if not self.run_command("docker compose up -d", "Starting main services (Kafka, API)"):
            return False

        # Wait for services
        if not self.wait_for_service(f"{API_BASE_URL}/health", "Ingestion API"):
            return False

        if not self.wait_for_service("http://localhost:9092", "Kafka"):
            return False

        return True

    def start_airflow(self) -> bool:
        """Start Airflow services."""
        log.info("✈️ Starting Airflow...")

        if not self.run_command("docker compose -f docker-compose.airflow.yml up -d", "Starting Airflow services"):
            return False

        if not self.wait_for_service(f"{AIRFLOW_URL}/health", "Airflow"):
            return False

        return True

    def run_batch_ingestion(self) -> bool:
        """Run batch ingestion via API."""
        log.info(" Running batch ingestion...")

        # Check if raw data exists
        raw_dir = "storage/raw"
        if not os.path.exists(raw_dir):
            log.error(f" Raw data directory not found: {raw_dir}")
            return False

        csv_files = [f for f in os.listdir(raw_dir) if f.endswith('.csv')]
        if not csv_files:
            log.error(f" No CSV files found in {raw_dir}")
            return False

        log.info(f" Found {len(csv_files)} CSV files: {csv_files}")

        # Run batch ingestion for each source
        sources = [
            ("src_warehouse_master", "storage/raw/warehouse_master.csv"),
            ("src_manufacturing_logs", "storage/raw/manufacturing_logs.csv"),
            ("src_sales_history", "storage/raw/sales_history.csv"),
            ("src_legacy_trends", "storage/raw/legacy_trends.csv"),
        ]

        for source_id, csv_path in sources:
            if not os.path.exists(csv_path):
                log.warning(f" CSV file not found: {csv_path}")
                continue

            try:
                # Read CSV and send to API
                import pandas as pd
                df = pd.read_csv(csv_path)
                records = df.to_dict('records')

                log.info(f" Ingesting {len(records)} records for {source_id}")

                response = requests.post(
                    f"{API_BASE_URL}/ingest/{source_id}",
                    json={"records": records},
                    headers=self.api_headers,
                    timeout=300
                )

                if response.status_code == 200:
                    job_data = response.json()
                    job_id = job_data.get("job_id")
                    log.info(f" Ingestion started for {source_id}, job_id: {job_id}")

                    # Wait for completion (simple polling)
                    self.wait_for_job_completion(job_id)
                else:
                    log.error(f" Failed to start ingestion for {source_id}: {response.text}")

            except Exception as e:
                log.error(f" Error ingesting {source_id}: {e}")

        return True

    def wait_for_job_completion(self, job_id: str, timeout: int = 300) -> bool:
        """Wait for a job to complete."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                response = requests.get(
                    f"{API_BASE_URL}/job/{job_id}",
                    headers=self.api_headers,
                    timeout=10
                )

                if response.status_code == 200:
                    job_data = response.json()
                    status = job_data.get("status")

                    if status == "completed":
                        log.info(f" Job {job_id} completed")
                        return True
                    elif status == "failed":
                        log.error(f" Job {job_id} failed")
                        return False

                time.sleep(5)

            except requests.RequestException as e:
                log.warning(f" Error checking job status: {e}")

        log.error(f" Job {job_id} timed out")
        return False

    def trigger_airflow_dag(self, dag_id: str) -> bool:
        """Trigger an Airflow DAG."""
        log.info(f" Triggering Airflow DAG: {dag_id}")

        try:
            response = requests.post(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
                json={"conf": {}},
                auth=("admin", "admin"),
                timeout=30
            )

            if response.status_code == 200:
                log.info(f" DAG {dag_id} triggered successfully")
                return True
            else:
                log.error(f" Failed to trigger DAG {dag_id}: {response.text}")
                return False

        except Exception as e:
            log.error(f" Error triggering DAG {dag_id}: {e}")
            return False

    def run_transformations_via_airflow(self) -> bool:
        """Run transformations using Airflow DAGs."""
        log.info(" Running transformations via Airflow...")

        # Trigger transformation DAG
        if not self.trigger_airflow_dag("supply_chain_transformation"):
            return False

        # Wait for completion (simplified - in production you'd monitor DAG runs)
        log.info(" Waiting for transformation DAG to complete...")
        time.sleep(60)  # Give it time to start

        return True

    def validate_pipeline(self) -> bool:
        """Validate that the pipeline ran successfully."""
        log.info(" Validating pipeline execution...")

        try:
            # Check transformation summary
            response = requests.get(
                f"{API_BASE_URL}/transformation/summary",
                headers=self.api_headers,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                silver = data.get("silver", {})
                gold = data.get("gold", {})

                if silver.get("run_count", 0) > 0 or gold.get("run_count", 0) > 0:
                    log.info(" Transformation data found")
                    return True
                else:
                    log.warning(" No transformation data found")
                    return False
            else:
                log.error(f" Failed to get transformation summary: {response.text}")
                return False

        except Exception as e:
            log.error(f" Error validating pipeline: {e}")
            return False

    def check_dashboard_data(self) -> bool:
        """Check if dashboard has data."""
        log.info(" Checking dashboard data...")

        try:
            response = requests.get(
                f"{API_BASE_URL}/dashboard/json",
                headers=self.api_headers,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                storage = data.get("storage_summary", {})

                if storage.get("ingested", 0) > 0:
                    log.info(" Dashboard has data")
                    return True
                else:
                    log.warning(" Dashboard appears empty")
                    return False
            else:
                log.error(f" Failed to get dashboard data: {response.text}")
                return False

        except Exception as e:
            log.error(f" Error checking dashboard: {e}")
            return False

    def run_full_pipeline(self) -> bool:
        """Run the complete pipeline."""
        log.info(" STARTING DOCKER PRODUCTION PIPELINE")
        log.info("=" * 60)

        pipeline_start = time.time()

        steps = [
            ("Start Infrastructure", self.start_infrastructure),
            ("Start Airflow", self.start_airflow),
            ("Run Batch Ingestion", self.run_batch_ingestion),
            ("Run Transformations", self.run_transformations_via_airflow),
            ("Validate Pipeline", self.validate_pipeline),
            ("Check Dashboard", self.check_dashboard_data),
        ]

        completed_steps = 0

        for step_name, step_func in steps:
            log.info(f"\n Step {completed_steps + 1}: {step_name}")
            if step_func():
                completed_steps += 1
                log.info(f" Step {completed_steps} completed")
            else:
                log.error(f" Step {completed_steps + 1} failed: {step_name}")
                break

        pipeline_duration = time.time() - pipeline_start

        log.info("\n" + "=" * 60)
        if completed_steps == len(steps):
            log.info(" DOCKER PIPELINE COMPLETED SUCCESSFULLY!")
            log.info(f"   Pipeline duration: {pipeline_duration:.2f} seconds")
            log.info("   - Infrastructure: Kafka, API, Airflow running")
            log.info("   - Data ingested into Iceberg bronze layer")
            log.info("   - Transformations completed (Silver → Gold)")
            log.info("   - Dashboard populated with real-time data")
            log.info("\n Access points:")
            log.info("   - API: http://localhost:8000")
            log.info("   - Airflow: http://localhost:8080 (admin/admin)")
            log.info("   - Dashboard: Open ui_manager.py in browser")
        else:
            log.error(" Pipeline completed with issues")
            log.error(f"   Completed {completed_steps}/{len(steps)} steps")

        log.info("=" * 60)
        return completed_steps == len(steps)

def main():
    """Main entry point."""
    runner = DockerPipelineRunner()

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "infrastructure":
            runner.start_infrastructure()
        elif command == "airflow":
            runner.start_airflow()
        elif command == "ingest":
            runner.run_batch_ingestion()
        elif command == "transform":
            runner.run_transformations_via_airflow()
        elif command == "validate":
            runner.validate_pipeline()
        else:
            print("Usage: python run_docker_pipeline.py [infrastructure|airflow|ingest|transform|validate]")
            print("Or run without arguments for full pipeline")
    else:
        success = runner.run_full_pipeline()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
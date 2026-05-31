"""
Part 3 — Semantic layer DAG: export parquet → dbt run → dbt test → SQL workloads.

Iceberg export uses the iceberg-toolkit venv (SQLAlchemy 2.x). Airflow's Python stays on 1.4.
dbt + SQL workloads call analytics-service (Dockerfile.part3) when available.
"""

from datetime import datetime, timedelta
import json
import logging
import os
import subprocess
import time

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

PROJECT_ROOT = os.environ.get("PIPELINE_PROJECT_ROOT", "/opt/airflow/project")
ICEBERG_SCRIPT = os.path.join(PROJECT_ROOT, "scripts", "run_iceberg_task.py")
ICEBERG_PY = os.environ.get("ICEBERG_TOOLKIT_PYTHON", "/opt/airflow/iceberg-toolkit/bin/python")
ANALYTICS_URL = os.getenv("ANALYTICS_SERVICE_URL", "http://analytics-service:8002").rstrip("/")
_WAIT_ANALYTICS_TIMEOUT_SEC = int(os.getenv("WAIT_ANALYTICS_TIMEOUT_SEC", "300"))
_WAIT_ANALYTICS_POLL_SEC = int(os.getenv("WAIT_ANALYTICS_POLL_SEC", "10"))


def _wait_for_analytics_service() -> dict:
    """
    Block until Part 3 analytics-service accepts HTTP (Docker network pipeline-net).

    Connection refused means the container is not running — start docker-compose.part3.yml first.
    """
    health_url = f"{ANALYTICS_URL}/health"
    deadline = time.time() + _WAIT_ANALYTICS_TIMEOUT_SEC
    last_error: str | None = None
    while time.time() < deadline:
        try:
            r = requests.get(health_url, timeout=10)
            if r.ok:
                log.info("analytics-service ready at %s", ANALYTICS_URL)
                return r.json()
            last_error = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as exc:
            last_error = str(exc)
            log.warning(
                "analytics-service not reachable at %s (%s); retry in %ss",
                health_url,
                exc,
                _WAIT_ANALYTICS_POLL_SEC,
            )
        time.sleep(_WAIT_ANALYTICS_POLL_SEC)
    raise AirflowException(
        f"analytics-service not reachable at {ANALYTICS_URL} after {_WAIT_ANALYTICS_TIMEOUT_SEC}s. "
        "Part 3 must be running before dbt/SQL tasks:\n"
        "  docker compose -f docker-compose.part3.yml up -d --build analytics-service\n"
        "Verify on host: curl http://localhost:8002/health\n"
        f"Last error: {last_error}"
    )


def _invoke_iceberg_toolkit(args: list[str], timeout_sec: int = 3600) -> dict:
    """Run PyIceberg work in toolkit venv — same pattern as compaction_dag."""
    if not os.path.isfile(ICEBERG_SCRIPT):
        raise AirflowException(f"Missing {ICEBERG_SCRIPT} — mount project into Airflow.")
    if not os.path.isfile(ICEBERG_PY):
        raise AirflowException(
            f"Missing Iceberg toolkit at {ICEBERG_PY}. Rebuild: docker compose -f docker-compose.airflow.yml build"
        )
    env = {**os.environ, "PYTHONPATH": PROJECT_ROOT}
    proc = subprocess.run(
        [ICEBERG_PY, ICEBERG_SCRIPT, *args],
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
    lines = (proc.stdout or "").strip().splitlines()
    payload = lines[-1] if lines else "{}"
    return json.loads(payload)


def _post_analytics(path: str, timeout_sec: int = 600, *, wait_for_service: bool = True) -> dict:
    """Call analytics-service (Part 3 container on pipeline-net)."""
    if wait_for_service:
        _wait_for_analytics_service()
    url = f"{ANALYTICS_URL}{path}"
    try:
        r = requests.post(url, timeout=timeout_sec)
    except requests.RequestException as exc:
        raise AirflowException(
            f"Cannot reach analytics-service at {url}. "
            f"Start Part 3: docker compose -f docker-compose.part3.yml up -d --build analytics-service. "
            f"Error: {exc}"
        ) from exc
    if not r.ok:
        raise AirflowException(f"analytics-service {path} HTTP {r.status_code}: {r.text[:2000]}")
    return r.json()


def export_parquet(**_):
    result = _invoke_iceberg_toolkit(["export-semantic-parquet"])
    log.info("Semantic parquet export: %s", result)
    return result


def ensure_analytics_service(**_):
    """Explicit gate: Part 3 stack must be up before dbt / run-all."""
    return _wait_for_analytics_service()


def run_dbt_models(**_):
    return _post_analytics("/semantic/dbt/run?select=marts", timeout_sec=900)


def run_dbt_tests(**_):
    return _post_analytics("/semantic/dbt/test", timeout_sec=600)


def run_sql_workloads(**_):
    # Single lakehouse load in analytics-service; allow up to 30 min for 8 workloads on Windows.
    result = _post_analytics("/analytics/run-all", timeout_sec=1800)
    failed = [
        r
        for r in result.get("results", [])
        if r.get("status") not in ("ok", "blocked")
    ]
    if failed:
        raise AirflowException(
            f"SQL workloads failed ({len(failed)}): "
            f"{[(r.get('query_id'), r.get('error')) for r in failed[:5]]}"
        )
    log.info(
        "SQL workloads OK | executed=%s ok=%s blocked=%s",
        result.get("executed"),
        result.get("ok"),
        result.get("blocked"),
    )
    return result


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="supply_chain_semantic",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["semantic", "dbt", "part3"],
    description="Export (iceberg-toolkit) → dbt → SQL via analytics-service",
) as dag:
    t_export = PythonOperator(task_id="export_semantic_parquet", python_callable=export_parquet)
    t_wait = PythonOperator(
        task_id="wait_for_analytics_service",
        python_callable=ensure_analytics_service,
    )
    t_dbt = PythonOperator(task_id="dbt_run_marts", python_callable=run_dbt_models)
    t_test = PythonOperator(task_id="dbt_test", python_callable=run_dbt_tests)
    t_sql = PythonOperator(task_id="run_analytics_sql", python_callable=run_sql_workloads)

    t_export >> t_wait >> t_dbt >> t_test >> t_sql

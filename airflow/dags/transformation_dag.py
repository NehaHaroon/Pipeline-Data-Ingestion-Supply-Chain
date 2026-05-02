# """
# Transformation Pipeline DAG: Bronze → Silver → Gold
# """

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import json
import logging
import sys
import os
import traceback
import requests

# Add project root to Python path
sys.path.insert(0, '/opt/airflow/project')

log = logging.getLogger(__name__)

# Silver/Gold POST must cover pandas work plus waiting on the Iceberg SQLite catalog lock.
_TRANSFORM_HTTP_TIMEOUT = int(os.getenv("TRANSFORM_POST_TIMEOUT_SECONDS", "3600"))


def wait_for_ingestion_completed(**context):
    """
    Wait until ``supply_chain_ingestion`` has a **successful** DagRun whose logical date
    falls in a UTC window around this run's ``logical_date``.

    ``ExternalTaskSensor`` + exact logical-date equality breaks for manual triggers and for
    Airflow 2 hourly timetables (ingestion run may be keyed at H or H-1). This polls the
    metadata DB instead.

    Env:
      WAIT_INGESTION_TIMEOUT_SEC (default 3600)
      WAIT_INGESTION_POLL_SEC (default 60)
      WAIT_INGESTION_WINDOW_BEFORE_HOURS — window start = floor(logical_date to hour) minus this (default 3)
      WAIT_INGESTION_WINDOW_AFTER_HOURS — window end = floor + this (default 2)
    """
    import time

    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState

    try:
        import pendulum
    except ImportError:
        pendulum = None

    logical = context.get("logical_date")
    if logical is None:
        raise AirflowException("logical_date missing from task context")

    timeout_sec = int(os.getenv("WAIT_INGESTION_TIMEOUT_SEC", "3600"))
    poll_sec = int(os.getenv("WAIT_INGESTION_POLL_SEC", "60"))
    before_h = int(os.getenv("WAIT_INGESTION_WINDOW_BEFORE_HOURS", "3"))
    after_h = int(os.getenv("WAIT_INGESTION_WINDOW_AFTER_HOURS", "2"))

    if pendulum:
        ref = pendulum.instance(logical).in_timezone("UTC")
        window_lo = ref.start_of("hour").subtract(hours=before_h)
        window_hi = ref.start_of("hour").add(hours=after_h)
    else:
        from datetime import timedelta, timezone as tz

        dt = logical
        if getattr(dt, "tzinfo", None) is not None:
            dt = dt.astimezone(tz.utc)
        floored = dt.replace(minute=0, second=0, microsecond=0)
        window_lo = floored - timedelta(hours=before_h)
        window_hi = floored + timedelta(hours=after_h)

    deadline = time.time() + timeout_sec

    while time.time() < deadline:
        with create_session() as session:
            recent = (
                session.query(DagRun)
                .filter(DagRun.dag_id == "supply_chain_ingestion")
                .filter(DagRun.state == DagRunState.SUCCESS)
                .order_by(DagRun.execution_date.desc())
                .limit(50)
                .all()
            )
            for dr in recent:
                ld = getattr(dr, "logical_date", None) or getattr(dr, "execution_date", None)
                if ld is None:
                    continue
                if pendulum:
                    ldp = pendulum.instance(ld).in_timezone("UTC")
                    if window_lo <= ldp < window_hi:
                        log.info(
                            "Ingestion gate OK | run_id=%s logical_date=%s window=[%s,%s)",
                            dr.run_id,
                            ldp,
                            window_lo,
                            window_hi,
                        )
                        return {
                            "ingestion_run_id": dr.run_id,
                            "ingestion_logical_date": str(ldp),
                            "window_lo": str(window_lo),
                            "window_hi": str(window_hi),
                        }
                else:
                    if window_lo <= ld < window_hi:
                        log.info(
                            "Ingestion gate OK | run_id=%s logical_date=%s",
                            dr.run_id,
                            ld,
                        )
                        return {
                            "ingestion_run_id": dr.run_id,
                            "ingestion_logical_date": str(ld),
                        }

        log.info(
            "No successful supply_chain_ingestion DagRun in [%s, %s); sleeping %ss",
            window_lo,
            window_hi,
            poll_sec,
        )
        time.sleep(poll_sec)

    raise AirflowException(
        f"Timed out after {timeout_sec}s waiting for successful supply_chain_ingestion "
        f"with logical_date in [{window_lo}, {window_hi}). "
        "Confirm ingestion DAG finished green and widen WAIT_INGESTION_WINDOW_*_HOURS if needed."
    )


def _transform_service_error_text(response: requests.Response) -> str:
    """Parse FastAPI/Starlette error body so task logs show the real failure (not just '500')."""
    try:
        data = response.json()
    except Exception:
        return (response.text or "")[:4000]
    detail = data.get("detail")
    if detail is None:
        return json.dumps(data)[:4000]
    if isinstance(detail, list):
        return json.dumps(detail)[:4000]
    return str(detail)[:4000]


def _post_transform_or_fail(url: str, *, what: str, timeout: int = 3600) -> dict:
    try:
        response = requests.post(url, timeout=timeout)
    except requests.RequestException as exc:
        log.error(
            "%s request failed before HTTP response | url=%s | error=%s: %s\n%s",
            what,
            url,
            type(exc).__name__,
            exc,
            traceback.format_exc(),
        )
        raise AirflowException(
            f"{what}: connection/request error ({type(exc).__name__}: {exc})"
        ) from exc

    if not response.ok:
        err = _transform_service_error_text(response)
        log.error(
            "%s failed | url=%s | http_status=%s | response_body=%s",
            what,
            url,
            response.status_code,
            err,
        )
        raise AirflowException(f"{what} HTTP {response.status_code}: {err}")

    try:
        return response.json()
    except json.JSONDecodeError as exc:
        body_preview = (response.text or "")[:2000]
        log.error(
            "Transform service returned non-JSON | url=%s | status=%s | preview=%s\n%s",
            url,
            response.status_code,
            body_preview,
            traceback.format_exc(),
        )
        raise AirflowException(
            f"{what}: invalid JSON response ({type(exc).__name__}: {exc}); body preview: {body_preview}"
        ) from exc

# ═══════════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════════

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
    "owner": "data-engineering"
}

# Gold replenishment joins warehouse + IoT shelf stock (+ sales + weather). Include all dependencies here
# or Gold exits early with records_written=0 (see gold_aggregator gate on warehouse + iot).
SOURCES_FOR_SILVER = [
    "src_warehouse_master",
    "src_sales_history",
    "src_manufacturing_logs",
    "src_legacy_trends",
    "src_iot_rfid_stream",
    "src_weather_api",
]

# ═══════════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════════

def transform_silver(source_id: str) -> dict:
    """Call the transform service to run Silver transformation."""
    try:
        url = "http://transform-service:8001/transform/silver/{}".format(source_id)
        log.info(f"Starting Silver transformation for {source_id} via {url}")
        result = _post_transform_or_fail(
            url, what=f"Silver transform ({source_id})", timeout=_TRANSFORM_HTTP_TIMEOUT
        )
        log.info(
            f"Silver done for {source_id}: "
            f"read={result.get('records_read', 0)}, cleaned={result.get('records_cleaned', 0)}, "
            f"rejected={result.get('records_rejected', 0)}"
        )
        return result
    except AirflowException:
        raise
    except Exception as e:
        log.error(f"Silver failed for {source_id}: {e}", exc_info=True)
        raise AirflowException(str(e))

def transform_gold() -> dict:
    """Call the transform service to run Gold aggregation."""
    try:
        url = "http://transform-service:8001/transform/gold"
        log.info("Starting Gold aggregation via {}".format(url))
        result = _post_transform_or_fail(url, what="Gold transform", timeout=_TRANSFORM_HTTP_TIMEOUT)
        log.info(f"Gold completed: {result}")
        return result
    except AirflowException:
        raise
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
        result = _post_transform_or_fail(
            url, what=f"Silver transform ({source_id})", timeout=_TRANSFORM_HTTP_TIMEOUT
        )
        log.info(f"Silver triggered for {source_id}: {result}")
        return result
    except AirflowException:
        raise
    except Exception as e:
        log.error(f"Silver trigger failed for {source_id}: {e}", exc_info=True)
        raise AirflowException(str(e))


def assert_bronze_ready(source_id: str, min_records: int = 1) -> dict:
    """
    Hard gate for normal Silver path:
    fail task if Bronze table is missing or still empty.
    """
    bronze_table = f"bronze.{source_id.replace('src_', '')}"
    ingestion_api = os.getenv("INGESTION_API_URL", "http://ingestion-api:8000").rstrip("/")
    api_token = os.getenv("API_TOKEN")
    if not api_token:
        raise AirflowException("API_TOKEN is not configured in Airflow environment.")
    try:
        response = requests.get(
            f"{ingestion_api}/storage/iceberg-kpis",
            params={"table_name": bronze_table},
            headers={"Authorization": f"Bearer {api_token}"},
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()
        kpis = payload.get("kpis", {})
        if not isinstance(kpis, dict) or kpis.get("error"):
            raise AirflowException(
                f"Bronze table '{bronze_table}' is not queryable yet: {kpis.get('error', payload)}"
            )
        records = int(kpis.get("record_count", 0) or 0)
        if records < min_records:
            raise AirflowException(
                f"Bronze table '{bronze_table}' exists but has {records} records; required >= {min_records}."
            )
        log.info(f"Bronze readiness OK for {source_id}: table={bronze_table} records={records}")
        return {"source_id": source_id, "bronze_table": bronze_table, "records": records}
    except Exception as exc:
        log.error(f"Bronze readiness check failed for {source_id}: {exc}", exc_info=True)
        raise AirflowException(str(exc))

def trigger_gold():
    url = "http://transform-service:8001/transform/gold"
    return _post_transform_or_fail(url, what="Gold transform", timeout=_TRANSFORM_HTTP_TIMEOUT)

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
    # WAIT FOR INGESTION (DB poll — avoids ExternalTaskSensor logical-date mismatch)
    # ─────────────────────────────────────────────────────────────
    wait_for_ingestion = PythonOperator(
        task_id="wait_for_ingestion",
        python_callable=wait_for_ingestion_completed,
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
            bronze_ready_task = PythonOperator(
                task_id=f"check_bronze_{source_id.replace('src_', '')}",
                python_callable=assert_bronze_ready,
                op_kwargs={"source_id": source_id, "min_records": 1},
            )

            task = PythonOperator(
                task_id=f"trigger_silver_{source_id.replace('src_', '')}",
                python_callable=trigger_silver,
                op_kwargs={"source_id": source_id},
            )
            bronze_ready_task >> task
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
"""Shared Airflow wait helpers for cross-compose service readiness."""

from __future__ import annotations

import logging
import os
import time

import requests
from airflow.exceptions import AirflowException
from requests.exceptions import RequestException

log = logging.getLogger(__name__)

INGESTION_API_URL = os.getenv("INGESTION_API_URL", "http://ingestion-api:8000").rstrip("/")
TRANSFORM_SERVICE_URL = os.getenv("TRANSFORM_SERVICE_URL", "http://transform-service:8001").rstrip("/")
_WAIT_INGESTION_API_TIMEOUT_SEC = int(os.getenv("WAIT_INGESTION_API_TIMEOUT_SEC", "300"))
_WAIT_INGESTION_API_POLL_SEC = int(os.getenv("WAIT_INGESTION_API_POLL_SEC", "10"))
_WAIT_TRANSFORM_TIMEOUT_SEC = int(os.getenv("WAIT_TRANSFORM_TIMEOUT_SEC", "300"))
_WAIT_TRANSFORM_POLL_SEC = int(os.getenv("WAIT_TRANSFORM_POLL_SEC", "10"))


def wait_for_ingestion_api() -> dict:
    """Part 1 ingestion-api must be up on pipeline-net before HTTP calls to :8000."""
    health_url = f"{INGESTION_API_URL}/health"
    deadline = time.time() + _WAIT_INGESTION_API_TIMEOUT_SEC
    last_error: str | None = None
    while time.time() < deadline:
        try:
            response = requests.get(health_url, timeout=10)
            if response.ok:
                log.info("ingestion-api ready at %s", INGESTION_API_URL)
                return response.json()
            last_error = f"HTTP {response.status_code}: {response.text[:200]}"
        except RequestException as exc:
            last_error = str(exc)
            log.warning(
                "ingestion-api not reachable at %s (%s); retry in %ss",
                health_url,
                exc,
                _WAIT_INGESTION_API_POLL_SEC,
            )
        time.sleep(_WAIT_INGESTION_API_POLL_SEC)
    raise AirflowException(
        f"ingestion-api not reachable at {INGESTION_API_URL} after {_WAIT_INGESTION_API_TIMEOUT_SEC}s. "
        "Start Part 1:\n"
        "  docker compose -f docker-compose.yml up -d ingestion-api\n"
        "Verify on host: curl http://localhost:8000/health\n"
        f"Last error: {last_error}"
    )


def ensure_ingestion_api(**_) -> dict:
    return wait_for_ingestion_api()


def wait_for_transform_service() -> dict:
    """Transform service must be up before Silver/Gold HTTP POSTs."""
    health_url = f"{TRANSFORM_SERVICE_URL}/health"
    deadline = time.time() + _WAIT_TRANSFORM_TIMEOUT_SEC
    last_error: str | None = None
    while time.time() < deadline:
        try:
            response = requests.get(health_url, timeout=10)
            if response.ok:
                log.info("transform-service ready at %s", TRANSFORM_SERVICE_URL)
                return response.json()
            last_error = f"HTTP {response.status_code}: {response.text[:200]}"
        except RequestException as exc:
            last_error = str(exc)
            log.warning(
                "transform-service not reachable at %s (%s); retry in %ss",
                health_url,
                exc,
                _WAIT_TRANSFORM_POLL_SEC,
            )
        time.sleep(_WAIT_TRANSFORM_POLL_SEC)
    raise AirflowException(
        f"transform-service not reachable at {TRANSFORM_SERVICE_URL} after {_WAIT_TRANSFORM_TIMEOUT_SEC}s. "
        "Start Part 2 transform service:\n"
        "  docker compose -f docker-compose.airflow.yml up -d --build transform-service\n"
        "Verify on host: curl http://localhost:8001/health\n"
        f"Last error: {last_error}"
    )


def ensure_transform_service(**_) -> dict:
    return wait_for_transform_service()

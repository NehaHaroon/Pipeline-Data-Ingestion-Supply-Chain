import logging
import traceback
from dataclasses import asdict
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from data_plane.transformation.silver_transformer import SilverTransformer
from data_plane.transformation.gold_aggregator import GoldAggregator
from control_plane.service_registry import StorageLayer
from observability_plane.structured_logging import get_logger, log_pipeline_event

app = FastAPI()
log = get_logger("transform_service")

_HTTP_DETAIL_MAX = 8000


def _json_safe(value: Any) -> Any:
    """Convert numpy/pandas scalars and nested structures to JSON-serializable Python types."""
    try:
        import numpy as np
    except ImportError:
        np = None  # type: ignore[assignment]

    if np is not None:
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, np.ndarray):
            return value.tolist()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(x) for x in value]
    return value


def _exception_report(exc: BaseException) -> tuple[str, str]:
    tb = traceback.format_exc()
    short = f"{type(exc).__name__}: {exc}"
    return short, tb


def _detail_for_client(short: str, tb: str) -> str:
    body = f"{short}\n\nFull traceback:\n{tb}"
    if len(body) > _HTTP_DETAIL_MAX:
        return body[: _HTTP_DETAIL_MAX] + "\n... (truncated for HTTP response; see transform-service container logs for full traceback)"
    return body


def _error_json_response(short: str, tb: str) -> JSONResponse:
    """Always JSON so Airflow logs parse ``detail`` instead of plain 'Internal Server Error'."""
    return JSONResponse(status_code=500, content={"detail": _detail_for_client(short, tb)})


@app.post("/transform/silver/{source_id}")
def run_silver(source_id: str):
    try:
        log_pipeline_event(
            log,
            "info",
            "Starting Silver transformation request",
            layer=StorageLayer.SILVER.value,
            source_id=source_id,
        )
        transformer = SilverTransformer(source_id)
        result = transformer.transform()
        log_pipeline_event(
            log,
            "info",
            "Silver transformation request completed",
            layer=StorageLayer.SILVER.value,
            source_id=source_id,
            records_cleaned=result.records_cleaned,
        )
        return JSONResponse(content=_json_safe(asdict(result)))
    except Exception as e:
        short, tb = _exception_report(e)
        try:
            log_pipeline_event(
                log,
                "error",
                f"Silver transformation failed: {short}",
                layer=StorageLayer.SILVER.value,
                source_id=source_id,
                exception_type=type(e).__name__,
                exception_message=str(e),
            )
            log.logger.error("%s\n%s", short, tb)
        except Exception:
            logging.getLogger("transform_service").exception("Silver transformation failed")
        return _error_json_response(short, tb)


@app.post("/transform/gold")
def run_gold():
    try:
        log_pipeline_event(log, "info", "Starting Gold aggregation request", layer=StorageLayer.GOLD.value)
        aggregator = GoldAggregator()
        result = aggregator.run()
        log_pipeline_event(
            log,
            "info",
            "Gold aggregation request completed",
            layer=StorageLayer.GOLD.value,
            records_written=result.get("records_written", 0),
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as e:
        short, tb = _exception_report(e)
        try:
            log_pipeline_event(
                log,
                "error",
                f"Gold aggregation failed: {short}",
                layer=StorageLayer.GOLD.value,
                exception_type=type(e).__name__,
                exception_message=str(e),
            )
            log.logger.error("%s\n%s", short, tb)
        except Exception:
            logging.getLogger("transform_service").exception("Gold aggregation failed")
        return _error_json_response(short, tb)

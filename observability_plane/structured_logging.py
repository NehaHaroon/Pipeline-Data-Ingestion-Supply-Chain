import logging
import json
from typing import Any, Dict, Optional

import config


_LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | "
    "layer=%(layer)s source=%(source_id)s job=%(job_id)s | %(message)s"
)


def configure_root_logger() -> None:
    if logging.getLogger().handlers:
        return

    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
        format=_LOG_FORMAT,
    )


class _PipelineAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: Dict[str, Any]):
        extra = kwargs.setdefault("extra", {})
        base = {
            "layer": extra.get("layer", "n/a"),
            "source_id": extra.get("source_id", "n/a"),
            "job_id": extra.get("job_id", "n/a"),
        }
        extra.update(base)
        return msg, kwargs


def get_logger(name: str) -> logging.LoggerAdapter:
    configure_root_logger()
    return _PipelineAdapter(logging.getLogger(name), {})


def log_pipeline_event(
    logger: logging.LoggerAdapter,
    level: str,
    message: str,
    *,
    layer: Optional[str] = None,
    source_id: Optional[str] = None,
    job_id: Optional[str] = None,
    **kwargs: Any,
) -> None:
    payload = {"layer": layer or "n/a", "source_id": source_id or "n/a", "job_id": job_id or "n/a"}
    payload.update(kwargs)
    details = {k: v for k, v in payload.items() if k not in ("layer", "source_id", "job_id")}
    details_str = f" | details={json.dumps(details, default=str)}" if details else ""
    logger.log(getattr(logging, level.upper(), logging.INFO), f"{message}{details_str}", extra=payload)

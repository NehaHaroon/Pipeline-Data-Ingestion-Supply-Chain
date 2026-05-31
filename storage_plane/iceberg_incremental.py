"""
Incremental Bronze reads for CDC Silver transforms (PyIceberg 0.6.x).

PyIceberg does not yet expose IncrementalAppendScan; we diff data-file paths between
two snapshot plans and read only newly appended Parquet files.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from config import STORAGE_CHECKPOINTS
from observability_plane.structured_logging import get_logger, log_pipeline_event

log = get_logger("iceberg_incremental")

CHECKPOINT_DIR = os.getenv("STORAGE_CHECKPOINTS", STORAGE_CHECKPOINTS)


def _checkpoint_path(source_id: str) -> str:
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    return os.path.join(CHECKPOINT_DIR, f"silver_bronze_{source_id}.json")


def load_bronze_silver_watermark(source_id: str) -> dict[str, Any] | None:
    path = _checkpoint_path(source_id)
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    log_pipeline_event(
        log,
        "info",
        "Loaded Silver Bronze watermark",
        source_id=source_id,
        bronze_snapshot_id=data.get("bronze_snapshot_id"),
    )
    return data


def save_bronze_silver_watermark(source_id: str, bronze_snapshot_id: int) -> None:
    path = _checkpoint_path(source_id)
    payload = {
        "source_id": source_id,
        "bronze_snapshot_id": int(bronze_snapshot_id),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    log_pipeline_event(
        log,
        "info",
        "Saved Silver Bronze watermark",
        source_id=source_id,
        bronze_snapshot_id=bronze_snapshot_id,
    )


def clear_bronze_silver_watermark(source_id: str) -> None:
    path = _checkpoint_path(source_id)
    if os.path.isfile(path):
        os.remove(path)


def _plan_file_paths(table, snapshot_id: int) -> set[str]:
    scan = table.scan(snapshot_id=snapshot_id)
    return {task.file.file_path for task in scan.plan_files()}


def _read_parquet_tasks(table, tasks) -> pd.DataFrame:
    io = table.io
    frames: list[pd.DataFrame] = []
    for task in tasks:
        path = task.file.file_path
        try:
            inp = io.new_input(path)
            with inp as f:
                arrow_table = pq.read_table(f)
            if arrow_table.num_rows:
                frames.append(arrow_table.to_pandas())
        except Exception as exc:
            log_pipeline_event(
                log,
                "warning",
                f"Failed reading incremental data file {path}: {exc}",
                file_path=path,
                exception_type=type(exc).__name__,
            )
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def read_bronze_append_delta(table, start_snapshot_id: int | None) -> tuple[pd.DataFrame, int | None, str]:
    """
    Read Bronze rows appended since ``start_snapshot_id``.

    Returns:
        (delta_dataframe, current_snapshot_id, mode)
        mode: ``none`` | ``delta`` | ``bootstrap_full``
    """
    current = table.current_snapshot()
    if current is None:
        return pd.DataFrame(), None, "none"

    end_id = int(current.snapshot_id)

    if start_snapshot_id is None:
        log_pipeline_event(
            log,
            "info",
            "Silver CDC bootstrap — no watermark, reading full Bronze once",
            bronze_snapshot_id=end_id,
        )
        return table.scan().to_pandas(), end_id, "bootstrap_full"

    start_id = int(start_snapshot_id)
    if start_id == end_id:
        log_pipeline_event(
            log,
            "info",
            "Silver CDC delta — no new Bronze snapshot since watermark",
            bronze_snapshot_id=end_id,
        )
        return pd.DataFrame(), end_id, "none"

    try:
        prev_paths = _plan_file_paths(table, start_id)
    except Exception as exc:
        log_pipeline_event(
            log,
            "warning",
            f"Watermark snapshot {start_id} unavailable ({exc}); bootstrap full Bronze read",
            exception_type=type(exc).__name__,
        )
        return table.scan().to_pandas(), end_id, "bootstrap_full"

    cur_scan = table.scan(snapshot_id=end_id)
    new_tasks = [t for t in cur_scan.plan_files() if t.file.file_path not in prev_paths]
    if not new_tasks:
        log_pipeline_event(
            log,
            "info",
            "Silver CDC delta — snapshot advanced but no new data files",
            from_snapshot=start_id,
            to_snapshot=end_id,
        )
        return pd.DataFrame(), end_id, "none"

    delta = _read_parquet_tasks(table, new_tasks)
    log_pipeline_event(
        log,
        "info",
        "Silver CDC delta read complete",
        from_snapshot=start_id,
        to_snapshot=end_id,
        new_files=len(new_tasks),
        delta_rows=len(delta),
    )
    return delta, end_id, "delta"

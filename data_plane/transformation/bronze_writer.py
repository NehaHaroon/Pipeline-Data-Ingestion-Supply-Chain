
import math
from typing import Any, Dict, List, Optional

import pyarrow as pa
from storage_plane.iceberg_catalog import get_catalog
from storage_plane.iceberg_session_lock import iceberg_catalog_session, retry_catalog_mutation
from control_plane.entities import EventEnvelope
from control_plane.service_registry import StorageLayer, table_name_for_layer
from observability_plane.structured_logging import get_logger, log_pipeline_event


def _summary_get(summary: Any, key: str, default: int = 0) -> int:
    """Read Iceberg snapshot summary counter across PyIceberg versions."""
    if summary is None:
        return default
    try:
        if hasattr(summary, "get"):
            raw = summary.get(key, default)
        else:
            raw = getattr(summary, key.replace("-", "_"), default)
        if raw is None:
            return default
        return int(raw)
    except (TypeError, ValueError):
        return default


def _sanitize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert numpy/pandas scalars to Python natives for PyArrow."""
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
            continue
        if hasattr(v, "item"):
            try:
                v = v.item()
            except Exception:
                pass
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            out[k] = None
        else:
            out[k] = v
    return out


class BronzeWriter:
    """
    Unified Iceberg Bronze sink for supply-chain ingestion.

    Used by:
      • REST `/ingest/{source_id}` (EventEnvelope list)
      • Batch CSV / micro-batch (`batch_ingest`)
      • Streaming simulations (`iot_stream_ingest`, `real_time_iot_ingest` fallback)
      • Operational DB ingest (`db_ingest`)
      • CDC strategy processors (`cdc_strategies`) — append CDC envelope-shaped rows

    All validated rows land in `bronze.<source_table>` for downstream Silver/Gold.
    """

    def __init__(self, source_id: str):
        self.source_id = source_id
        self.table_name = table_name_for_layer(StorageLayer.BRONZE, source_id)
        self.catalog = get_catalog()
        self.log = get_logger("bronze_writer")

    def _drop_all_null_columns(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return rows
        all_keys = set().union(*(row.keys() for row in rows))
        all_null_keys = [
            key for key in all_keys
            if all(row.get(key) is None for row in rows)
        ]
        if not all_null_keys:
            return rows
        cleaned = []
        for row in rows:
            r = {k: v for k, v in row.items() if k not in all_null_keys}
            cleaned.append(r)
        log_pipeline_event(
            self.log,
            "warning",
            "Dropped all-null columns from Bronze batch",
            layer=StorageLayer.BRONZE.value,
            source_id=self.source_id,
            dropped_columns=all_null_keys,
        )
        return cleaned

    def append_flat_records(self, rows: List[Dict[str, Any]]) -> dict:
        """
        Append rows that are already flattened (e.g. EventEnvelope.to_dict() or
        batch ingest envelope dicts). Sanitizes numpy types for PyArrow.
        """
        if not rows:
            return {"snapshot_id": None, "files_added": 0, "records_written": 0}

        sanitized = [_sanitize_row(r) for r in rows]
        sanitized = self._drop_all_null_columns(sanitized)

        arrow_table = pa.Table.from_pylist(sanitized)
        log_pipeline_event(
            self.log,
            "info",
            "Appending records to Bronze Iceberg table",
            layer=StorageLayer.BRONZE.value,
            source_id=self.source_id,
            records=len(sanitized),
            table_name=self.table_name,
        )

        def _bronze_append():
            try:
                tbl = self.catalog.load_table(self.table_name)
            except Exception:
                tbl = self.catalog.create_table(
                    self.table_name,
                    schema=arrow_table.schema,
                )
            tbl.append(arrow_table)
            return tbl.current_snapshot()

        with iceberg_catalog_session():
            snap = retry_catalog_mutation(_bronze_append)

        files_added = _summary_get(snap.summary if snap else None, "added-data-files", 0)
        return {
            "snapshot_id": snap.snapshot_id if snap else None,
            "files_added": files_added,
            "records_written": len(sanitized),
        }

    def write_batch(self, envelopes: List[EventEnvelope]) -> dict:
        """Append from EventEnvelope objects (REST API and batch ingestion)."""
        rows = [e.to_dict() for e in envelopes]
        return self.append_flat_records(rows)

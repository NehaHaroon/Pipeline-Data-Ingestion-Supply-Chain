"""
Export Silver/Gold Iceberg tables to parquet for dbt-duckdb sources.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from control_plane.service_registry import StorageLayer, table_name_for_layer
from storage_plane.iceberg_catalog import get_catalog

EXPORT_DIR = Path(os.getenv("SEMANTIC_PARQUET_DIR", "storage/semantic/parquet"))

SILVER_SOURCES = [
    "src_warehouse_master",
    "src_sales_history",
    "src_manufacturing_logs",
    "src_legacy_trends",
    "src_iot_rfid_stream",
    "src_weather_api",
    "src_inventory_transactions",
]


def export_table(table_name: str, out_name: str) -> int:
    catalog = get_catalog()
    try:
        df = catalog.load_table(table_name).scan().to_pandas()
    except Exception:
        return 0
    if df.empty:
        return 0
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORT_DIR / f"{out_name}.parquet"
    df.to_parquet(path, index=False)
    return len(df)


def export_all() -> dict:
    counts = {}
    for sid in SILVER_SOURCES:
        tbl = table_name_for_layer(StorageLayer.SILVER, sid)
        short = sid.replace("src_", "")
        n = export_table(tbl, f"silver_{short}")
        counts[tbl] = n
    counts["gold.replenishment_signals"] = export_table(
        "gold.replenishment_signals", "gold_replenishment_signals"
    )
    return counts


if __name__ == "__main__":
    import json

    print(json.dumps(export_all(), indent=2))

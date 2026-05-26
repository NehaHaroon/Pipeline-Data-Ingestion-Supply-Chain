"""
DuckDB analytics engine: registers Iceberg-backed tables as views for SQL workloads.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

from control_plane.service_registry import StorageLayer, table_name_for_layer
from observability_plane.structured_logging import get_logger, log_pipeline_event

log = get_logger("duckdb_engine")

# Logical table aliases used in SQL workload files
SILVER_SOURCES = [
    "src_warehouse_master",
    "src_sales_history",
    "src_manufacturing_logs",
    "src_legacy_trends",
    "src_iot_rfid_stream",
    "src_weather_api",
    "src_inventory_transactions",
]


def _iceberg_table_to_df(table_name: str) -> pd.DataFrame:
    from storage_plane.iceberg_catalog import get_catalog

    try:
        tbl = get_catalog().load_table(table_name)
        return tbl.scan().to_pandas()
    except Exception as exc:
        log_pipeline_event(
            log,
            "warning",
            f"Could not load Iceberg table {table_name}: {exc}",
            table_name=table_name,
        )
        return pd.DataFrame()


def register_lakehouse_views(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Register silver.* and gold.* as DuckDB views; return row counts."""
    counts: Dict[str, int] = {}

    for source_id in SILVER_SOURCES:
        tbl = table_name_for_layer(StorageLayer.SILVER, source_id)
        short = source_id.replace("src_", "")
        df = _iceberg_table_to_df(tbl)
        view_name = f"silver_{short}"
        if df.empty:
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM (SELECT 1 WHERE 1=0)")
            counts[view_name] = 0
        else:
            con.register(f"_tmp_{view_name}", df)
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM _tmp_{view_name}")
            con.unregister(f"_tmp_{view_name}")
            counts[view_name] = len(df)

    gold_df = _iceberg_table_to_df("gold.replenishment_signals")
    if gold_df.empty:
        con.execute(
            "CREATE OR REPLACE VIEW gold_replenishment_signals AS SELECT * FROM (SELECT 1 WHERE 1=0)"
        )
        counts["gold_replenishment_signals"] = 0
    else:
        con.register("_tmp_gold", gold_df)
        con.execute("CREATE OR REPLACE VIEW gold_replenishment_signals AS SELECT * FROM _tmp_gold")
        con.unregister("_tmp_gold")
        counts["gold_replenishment_signals"] = len(gold_df)

    return counts


@contextmanager
def lakehouse_connection():
    con = duckdb.connect(database=":memory:")
    try:
        register_lakehouse_views(con)
        yield con
    finally:
        con.close()


def execute_sql(
    sql: str,
    *,
    max_wall_sec: float = 120.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Execute SQL and return (rows as dicts, execution_stats).
    """
    t0 = time.perf_counter()
    with lakehouse_connection() as con:
        rel = con.execute(sql)
        df = rel.fetchdf() if rel is not None else pd.DataFrame()
    elapsed = time.perf_counter() - t0
    if elapsed > max_wall_sec:
        raise TimeoutError(f"Query exceeded max_wall_sec={max_wall_sec}")
    stats = {
        "elapsed_sec": round(elapsed, 4),
        "row_count": len(df),
        "column_count": len(df.columns) if not df.columns.empty else 0,
    }
    rows = df.to_dict(orient="records") if not df.empty else []
    return rows, stats

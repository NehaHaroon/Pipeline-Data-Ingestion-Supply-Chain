"""
DuckDB analytics engine: registers Iceberg-backed tables as views for SQL workloads.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from pathlib import Path
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

PARQUET_DIR = Path(os.getenv("SEMANTIC_PARQUET_DIR", "storage/semantic/parquet"))


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


def _parquet_fallback(duckdb_table_name: str) -> pd.DataFrame:
    path = PARQUET_DIR / f"{duckdb_table_name}.parquet"
    if not path.is_file():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
        log_pipeline_event(
            log,
            "info",
            f"Loaded {duckdb_table_name} from parquet fallback",
            path=str(path),
            rows=len(df),
        )
        return df
    except Exception as exc:
        log_pipeline_event(log, "warning", f"Parquet read failed for {path}: {exc}")
        return pd.DataFrame()


def _load_table_df(iceberg_table: str, duckdb_name: str) -> pd.DataFrame:
    df = _iceberg_table_to_df(iceberg_table)
    if not df.empty:
        return df
    return _parquet_fallback(duckdb_name)


def _materialize_dataframe(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
) -> int:
    """
    Copy dataframe into a DuckDB TABLE (not a VIEW over a temp register name).

    DuckDB views created as ``SELECT * FROM _tmp_*`` keep referencing ``_tmp_*``;
    unregistering the temp relation breaks all downstream SQL (the run-all errors).
    """
    if df.empty and len(df.columns) == 0:
        log_pipeline_event(
            log,
            "warning",
            f"Table {table_name} has no Iceberg/parquet data — registering empty placeholder",
            table_name=table_name,
        )
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0"
        )
        return 0

    tmp = f"__mat_{table_name}"
    con.register(tmp, df)
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {tmp}")
    try:
        con.unregister(tmp)
    except Exception:
        pass
    return len(df)


def register_lakehouse_views(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Register silver.* and gold.* as DuckDB tables; return row counts."""
    counts: Dict[str, int] = {}

    for source_id in SILVER_SOURCES:
        tbl = table_name_for_layer(StorageLayer.SILVER, source_id)
        short = source_id.replace("src_", "")
        view_name = f"silver_{short}"
        df = _load_table_df(tbl, view_name)
        counts[view_name] = _materialize_dataframe(con, view_name, df)

    gold_df = _load_table_df("gold.replenishment_signals", "gold_replenishment_signals")
    counts["gold_replenishment_signals"] = _materialize_dataframe(
        con, "gold_replenishment_signals", gold_df
    )

    log_pipeline_event(log, "info", "DuckDB lakehouse tables registered", **counts)
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
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Execute SQL and return (rows as dicts, execution_stats).

    When ``con`` is provided (e.g. run-all batch), lakehouse tables are not reloaded.
    """
    t0 = time.perf_counter()

    def _run(connection: duckdb.DuckDBPyConnection) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        rel = connection.execute(sql)
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

    if con is not None:
        return _run(con)
    with lakehouse_connection() as connection:
        return _run(connection)

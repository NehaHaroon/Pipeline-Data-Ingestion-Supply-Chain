import time
from dataclasses import replace

import pandas as pd
import pyarrow as pa
from common.arrow_iceberg_utils import prepare_arrow_table_for_iceberg
from storage_plane.iceberg_catalog import get_catalog
from storage_plane.iceberg_session_lock import iceberg_catalog_session, retry_catalog_mutation
from control_plane.service_registry import StorageLayer, table_name_for_layer
from data_plane.transformation.transformation_kpis import TransformationKPILogger, TransformationKPITracker
from observability_plane.structured_logging import get_logger, log_pipeline_event

class GoldAggregator:
    """
    Reads from Silver tables, computes the Gold replenishment signals table.
    Joins batch Silver (warehouse_master, sales_history) with
    streaming Silver (iot_rfid_stream) and weather Silver.
    """

    def __init__(self):
        self.catalog = get_catalog()
        self.log = get_logger("gold_aggregator")

    def _load_silver(self, source_id: str) -> pd.DataFrame:
        tbl_name = table_name_for_layer(StorageLayer.SILVER, source_id)
        try:
            return self.catalog.load_table(tbl_name).scan().to_pandas()
        except:
            return pd.DataFrame()

    def run(self) -> dict:
        t0 = time.time()
        kpi = TransformationKPITracker("gold_replenishment_signals", layer="gold")
        kpi.start_time = t0

        log_pipeline_event(
            self.log,
            "info",
            "Starting Gold aggregation run",
            layer=StorageLayer.GOLD.value,
        )
        warehouse = self._load_silver("src_warehouse_master")
        iot       = self._load_silver("src_iot_rfid_stream")
        sales     = self._load_silver("src_sales_history")
        weather   = self._load_silver("src_weather_api")

        if warehouse.empty or iot.empty:
            reason = (
                "silver_warehouse_master_empty"
                if warehouse.empty
                else "silver_iot_rfid_stream_empty"
            )
            log_pipeline_event(
                self.log,
                "warning",
                f"Gold skipped — need non-empty Silver warehouse and IoT ({reason}). "
                "Ensure transformation DAG runs Silver for src_iot_rfid_stream (and Bronze has IoT data).",
                layer=StorageLayer.GOLD.value,
                skip_reason=reason,
                warehouse_rows=len(warehouse),
                iot_rows=len(iot),
            )
            kpi.end_time = time.time()
            TransformationKPILogger.log_kpi(
                replace(
                    kpi.finalize(),
                    status="skipped",
                    error_message=reason,
                )
            )
            return {
                "records_written": 0,
                "skipped": True,
                "skip_reason": reason,
                "silver_warehouse_rows": int(len(warehouse)),
                "silver_iot_rows": int(len(iot)),
            }

        # Latest shelf stock per product (streaming)
        latest_stock = (iot.sort_values("timestamp")
                           .drop_duplicates("product_id", keep="last")
                           [["product_id", "current_stock_on_shelf", "shelf_location"]])

        # 7-day rolling sales velocity
        if not sales.empty and "sale_timestamp" in sales.columns:
            sales["sale_timestamp"] = pd.to_datetime(sales["sale_timestamp"], utc=True, errors="coerce")
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
            recent_sales = (sales[sales["sale_timestamp"] >= cutoff]
                                .groupby("product_id")["units_sold"]
                                .sum()
                                .reset_index()
                                .rename(columns={"units_sold": "units_sold_7d"}))
        else:
            recent_sales = pd.DataFrame(columns=["product_id", "units_sold_7d"])

        # Weather risk flag
        weather_risk = False
        if not weather.empty and "weather_description" in weather.columns:
            latest_weather = weather.sort_values("timestamp").iloc[-1]
            weather_risk = (latest_weather.get("temperature", 20) < 0 or
                            "storm" in str(latest_weather.get("weather_description", "")).lower())

        # Build Gold table
        gold = (warehouse[["product_id", "reorder_threshold", "max_capacity", "unit_cost"]]
                .merge(latest_stock, on="product_id", how="left")
                .merge(recent_sales, on="product_id", how="left"))

        gold["units_sold_7d"] = gold["units_sold_7d"].fillna(0)
        gold["current_stock_on_shelf"] = gold["current_stock_on_shelf"].fillna(0)
        gold["needs_replenishment"] = gold["current_stock_on_shelf"] < gold["reorder_threshold"]
        gold["urgency_score"] = ((gold["reorder_threshold"] - gold["current_stock_on_shelf"])
                                 / gold["max_capacity"].clip(lower=1)).clip(lower=0)
        gold["suggested_order_qty"] = (gold["max_capacity"] - gold["current_stock_on_shelf"]).clip(lower=0)
        gold["weather_risk"] = weather_risk
        gold["gold_computed_at"] = pd.Timestamp.now(tz="UTC")

        # Metric drift (day-over-day change in urgency) — compare to previous Gold snapshot
        gold["urgency_drift"] = 0.0  # placeholder; implement by loading previous Gold snapshot

        replenishment_signals = gold[gold["needs_replenishment"]]

        arrow_table = prepare_arrow_table_for_iceberg(
            pa.Table.from_pandas(replenishment_signals, preserve_index=False, safe=False)
        )
        gold_tbl_name = "gold.replenishment_signals"

        def _gold_write() -> None:
            try:
                gold_tbl = self.catalog.load_table(gold_tbl_name)
            except Exception:
                gold_tbl = self.catalog.create_table(gold_tbl_name, schema=arrow_table.schema)
            gold_tbl.overwrite(arrow_table)

        with iceberg_catalog_session():
            retry_catalog_mutation(_gold_write)

        kpi.records_read = len(warehouse) + len(iot) + len(sales) + len(weather)
        kpi.records_cleaned = len(gold)
        kpi.records_rejected = 0
        kpi.records_written = len(replenishment_signals)
        kpi.end_time = time.time()
        TransformationKPILogger.log_kpi(kpi.finalize())

        return {
            "records_written": len(replenishment_signals),
            "products_checked": len(gold),
            "weather_risk_active": weather_risk,
        }
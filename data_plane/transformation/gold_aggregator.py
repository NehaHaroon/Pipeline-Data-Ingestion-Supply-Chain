
import pandas as pd
import pyarrow as pa
from storage_plane.iceberg_catalog import get_catalog

class GoldAggregator:
    """
    Reads from Silver tables, computes the Gold replenishment signals table.
    Joins batch Silver (warehouse_master, sales_history) with
    streaming Silver (iot_rfid_stream) and weather Silver.
    """

    def __init__(self):
        self.catalog = get_catalog()

    def _load_silver(self, source_id: str) -> pd.DataFrame:
        tbl_name = f"silver.{source_id.replace('src_', '')}"
        if not self.catalog.table_exists(tbl_name):
            return pd.DataFrame()
        return self.catalog.load_table(tbl_name).scan().to_pandas()

    def run(self) -> dict:
        warehouse = self._load_silver("src_warehouse_master")
        iot       = self._load_silver("src_iot_rfid_stream")
        sales     = self._load_silver("src_sales_history")
        weather   = self._load_silver("src_weather_api")

        if warehouse.empty or iot.empty:
            return {"records_written": 0}

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

        arrow_table = pa.Table.from_pandas(replenishment_signals, preserve_index=False)
        gold_tbl_name = "gold.replenishment_signals"
        if not self.catalog.table_exists(gold_tbl_name):
            self.catalog.create_table(gold_tbl_name, schema=arrow_table.schema)
        self.catalog.load_table(gold_tbl_name).overwrite(arrow_table)

        return {
            "records_written": len(replenishment_signals),
            "products_checked": len(gold),
            "weather_risk_active": weather_risk,
        }
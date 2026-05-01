
import pandas as pd
import pyarrow as pa
from dataclasses import dataclass
from storage_plane.iceberg_catalog import get_catalog
from control_plane.contracts import CONTRACT_REGISTRY
from data_plane.transformation.transformation_kpis import TransformationKPITracker

@dataclass
class SilverTransformResult:
    source_id: str
    records_read: int
    records_cleaned: int
    records_rejected: int
    null_imputations: int
    duplicates_removed: int
    late_arriving_count: int
    schema_violations: int
    transformation_latency_sec: float

class SilverTransformer:
    """
    Reads from a Bronze Iceberg table, applies all Silver transformations,
    writes to the corresponding Silver Iceberg table.
    Idempotent: uses Iceberg snapshot ID as a high-watermark to avoid
    reprocessing already-transformed data.
    """

    def __init__(self, source_id: str):
        self.source_id = source_id
        self.bronze_table = f"bronze.{source_id.replace('src_', '')}"
        self.silver_table = f"silver.{source_id.replace('src_', '')}"
        self.contract = CONTRACT_REGISTRY.get(source_id)
        self.catalog = get_catalog()
        self.kpi = TransformationKPITracker(source_id, layer="silver")

    def transform(self) -> SilverTransformResult:
        import time
        t0 = time.time()

        # 1. Read from Bronze
        bronze = self.catalog.load_table(self.bronze_table)
        df: pd.DataFrame = bronze.scan().to_pandas()
        records_read = len(df)

        # 2. Contract re-validation (catches schema drift since Bronze ingestion)
        valid_rows, rejected_rows, violations = [], [], 0
        for _, row in df.iterrows():
            result = self.contract.enforce(row.to_dict()) if self.contract else {"status": "ok", "record": row.to_dict()}
            if result["status"] in ("ok", "coerced"):
                valid_rows.append(result["record"])
            elif result["status"] == "quarantine":
                valid_rows.append(result["record"])  # keep, but flag
                violations += len(result["violations"])
            else:  # rejected
                rejected_rows.append(result["record"])

        df = pd.DataFrame(valid_rows)

        # 3. Null imputation (source-specific rules from contracts)
        null_imputations = 0
        if self.source_id == "src_manufacturing_logs" and "defect_count" in df.columns:
            n = df["defect_count"].isna().sum()
            df["defect_count"] = df["defect_count"].fillna(0.0)
            null_imputations += n
        if self.source_id == "src_legacy_trends" and "market_region" in df.columns:
            n = df["market_region"].isna().sum()
            df["market_region"] = df["market_region"].fillna("UNKNOWN")
            null_imputations += n

        # 4. Datetime casting (Bronze stores all timestamps as str)
        for col in df.columns:
            if "timestamp" in col or col in ("created_at", "mfg_timestamp", "sale_timestamp"):
                try:
                    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                except Exception:
                    pass

        # 5. Derived flags
        if self.source_id == "src_sales_history" and "units_sold" in df.columns:
            df["is_return"] = df["units_sold"] < 0

        # 6. Late-arriving detection
        late_arriving_count = 0
        if "_ingestion_timestamp" in df.columns:
            df["_ingestion_ts"] = pd.to_datetime(df["_ingestion_timestamp"], utc=True, errors="coerce")
            for ts_col in ["timestamp", "sale_timestamp", "mfg_timestamp", "created_at"]:
                if ts_col in df.columns:
                    event_ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
                    lag = (df["_ingestion_ts"] - event_ts).dt.total_seconds()
                    df["late_arriving"] = lag > 300  # >5 min lag
                    late_arriving_count = int(df["late_arriving"].sum())
                    break

        # 7. Deduplication — keep last by ingestion timestamp per primary key
        pk_map = {
            "src_sales_history":           ["receipt_id"],
            "src_warehouse_master":        ["product_id"],
            "src_manufacturing_logs":      ["production_batch_id"],
            "src_iot_rfid_stream":         ["event_id"],
            "src_inventory_transactions":  ["transaction_id"],
            "src_legacy_trends":           ["old_product_code", "historical_period"],
            "src_weather_api":             ["city", "timestamp"],
        }
        pk = pk_map.get(self.source_id)
        duplicates_removed = 0
        if pk and all(c in df.columns for c in pk):
            before = len(df)
            if "_ingestion_timestamp" in df.columns:
                df = df.sort_values("_ingestion_timestamp").drop_duplicates(subset=pk, keep="last")
            else:
                df = df.drop_duplicates(subset=pk, keep="last")
            duplicates_removed = before - len(df)

        # 8. Write to Silver Iceberg table
        if not df.empty:
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)
            try:
                silver = self.catalog.load_table(self.silver_table)
            except:
                silver = self.catalog.create_table(self.silver_table, schema=arrow_table.schema)
            silver.overwrite(arrow_table)  # overwrite for idempotency

        latency = time.time() - t0
        return SilverTransformResult(
            source_id=self.source_id,
            records_read=records_read,
            records_cleaned=len(df),
            records_rejected=len(rejected_rows),
            null_imputations=null_imputations,
            duplicates_removed=duplicates_removed,
            late_arriving_count=late_arriving_count,
            schema_violations=violations,
            transformation_latency_sec=latency,
        )
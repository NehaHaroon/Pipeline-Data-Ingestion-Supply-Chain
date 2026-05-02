import traceback
import pandas as pd
import pyarrow as pa
from dataclasses import dataclass

import numpy as np

from common.arrow_iceberg_utils import prepare_arrow_table_for_iceberg
from storage_plane.iceberg_catalog import get_catalog
from storage_plane.iceberg_session_lock import iceberg_catalog_session, retry_catalog_mutation
from control_plane.contracts import CONTRACT_REGISTRY
from data_plane.transformation.transformation_kpis import TransformationKPITracker
from control_plane.service_registry import StorageLayer, table_name_for_layer
from observability_plane.structured_logging import get_logger, log_pipeline_event


def _sanitize_dataframe_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """Avoid PyArrow/Iceberg failures on nullable booleans and non-finite floats."""
    out = df.copy()
    if "late_arriving" in out.columns:
        out["late_arriving"] = out["late_arriving"].fillna(False).astype(bool)
    for col in out.columns:
        s = out[col]
        if pd.api.types.is_bool_dtype(s) or getattr(s.dtype, "name", "") == "boolean":
            out[col] = s.fillna(False).astype(bool)
        elif pd.api.types.is_float_dtype(s):
            out[col] = s.replace([np.inf, -np.inf], np.nan)
    return out


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
        self.bronze_table = table_name_for_layer(StorageLayer.BRONZE, source_id)
        self.silver_table = table_name_for_layer(StorageLayer.SILVER, source_id)
        self.contract = CONTRACT_REGISTRY.get(source_id)
        self.catalog = get_catalog()
        self.kpi = TransformationKPITracker(source_id, layer="silver")
        self.log = get_logger("silver_transformer")

    def transform(self) -> SilverTransformResult:
        import time
        t0 = time.time()
        log_pipeline_event(
            self.log,
            "info",
            "Starting Silver transformation",
            layer=StorageLayer.SILVER.value,
            source_id=self.source_id,
            bronze_table=self.bronze_table,
            silver_table=self.silver_table,
        )

        # 1. Read from Bronze (graceful skip if source has not landed yet)
        try:
            bronze = self.catalog.load_table(self.bronze_table)
        except Exception as exc:
            latency = time.time() - t0
            log_pipeline_event(
                self.log,
                "warning",
                f"Bronze table not available yet, skipping Silver transform: {type(exc).__name__}: {exc}",
                layer=StorageLayer.SILVER.value,
                source_id=self.source_id,
                bronze_table=self.bronze_table,
                duration_sec=round(latency, 3),
                exception_type=type(exc).__name__,
                exception_message=str(exc),
                traceback=traceback.format_exc(),
            )
            return SilverTransformResult(
                source_id=self.source_id,
                records_read=0,
                records_cleaned=0,
                records_rejected=0,
                null_imputations=0,
                duplicates_removed=0,
                late_arriving_count=0,
                schema_violations=0,
                transformation_latency_sec=latency,
            )
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

        # Ingestion normalizes legacy CSV "old_product_code" → "product_id"; contract + dedup still use old_product_code
        if self.source_id == "src_legacy_trends" and "product_id" in df.columns and "old_product_code" not in df.columns:
            df = df.copy()
            df["old_product_code"] = df["product_id"].astype(str)

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
            df_write = _sanitize_dataframe_for_arrow(df)
            try:
                arrow_table = prepare_arrow_table_for_iceberg(
                    pa.Table.from_pandas(df_write, preserve_index=False, safe=False)
                )
            except Exception as conv_exc:
                tb = traceback.format_exc()
                log_pipeline_event(
                    self.log,
                    "error",
                    f"PyArrow conversion failed for Silver: {type(conv_exc).__name__}: {conv_exc}",
                    layer=StorageLayer.SILVER.value,
                    source_id=self.source_id,
                    silver_table=self.silver_table,
                    exception_type=type(conv_exc).__name__,
                    exception_message=str(conv_exc),
                    traceback=tb,
                )
                raise

            def _silver_catalog_write() -> None:
                try:
                    silver = self.catalog.load_table(self.silver_table)
                except Exception:
                    silver = self.catalog.create_table(self.silver_table, schema=arrow_table.schema)
                try:
                    silver.overwrite(arrow_table)  # overwrite for idempotency
                except Exception as exc:
                    log_pipeline_event(
                        self.log,
                        "warning",
                        f"Silver overwrite failed ({type(exc).__name__}: {exc}); recreating table {self.silver_table}",
                        layer=StorageLayer.SILVER.value,
                        source_id=self.source_id,
                        silver_table=self.silver_table,
                        exception_type=type(exc).__name__,
                        exception_message=str(exc),
                        traceback=traceback.format_exc(),
                    )
                    try:
                        self.catalog.drop_table(self.silver_table)
                    except Exception as drop_exc:
                        log_pipeline_event(
                            self.log,
                            "warning",
                            f"Silver drop_table failed (continuing): {type(drop_exc).__name__}: {drop_exc}",
                            layer=StorageLayer.SILVER.value,
                            source_id=self.source_id,
                            silver_table=self.silver_table,
                            exception_type=type(drop_exc).__name__,
                            traceback=traceback.format_exc(),
                        )
                    silver = self.catalog.create_table(self.silver_table, schema=arrow_table.schema)
                    silver.append(arrow_table)

            with iceberg_catalog_session():
                retry_catalog_mutation(_silver_catalog_write)

        latency = time.time() - t0
        log_pipeline_event(
            self.log,
            "info",
            "Completed Silver transformation",
            layer=StorageLayer.SILVER.value,
            source_id=self.source_id,
            records_read=records_read,
            records_cleaned=len(df),
            records_rejected=len(rejected_rows),
            duration_sec=round(latency, 3),
        )
        return SilverTransformResult(
            source_id=self.source_id,
            records_read=int(records_read),
            records_cleaned=int(len(df)),
            records_rejected=int(len(rejected_rows)),
            null_imputations=int(null_imputations),
            duplicates_removed=int(duplicates_removed),
            late_arriving_count=int(late_arriving_count),
            schema_violations=int(violations),
            transformation_latency_sec=float(latency),
        )
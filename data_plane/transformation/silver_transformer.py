import os
import traceback
import pandas as pd
import pyarrow as pa
from dataclasses import dataclass, replace

import numpy as np

from common.arrow_iceberg_utils import prepare_arrow_table_for_iceberg
from storage_plane.iceberg_catalog import get_catalog
from storage_plane.iceberg_incremental import (
    clear_bronze_silver_watermark,
    load_bronze_silver_watermark,
    read_bronze_append_delta,
    save_bronze_silver_watermark,
)
from storage_plane.iceberg_session_lock import iceberg_catalog_session, retry_catalog_mutation
from control_plane.contracts import CONTRACT_REGISTRY
from data_plane.transformation.transformation_kpis import TransformationKPILogger, TransformationKPITracker
from control_plane.service_registry import StorageLayer, table_name_for_layer
from observability_plane.structured_logging import get_logger, log_pipeline_event


def _sanitize_dataframe_for_arrow(df: pd.DataFrame, *, copy: bool = True) -> pd.DataFrame:
    """Avoid PyArrow/Iceberg failures on nullable booleans and non-finite floats."""
    out = df.copy() if copy else df
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


PK_MAP = {
    "src_sales_history": ["receipt_id"],
    "src_warehouse_master": ["product_id"],
    "src_manufacturing_logs": ["production_batch_id"],
    "src_iot_rfid_stream": ["event_id"],
    "src_inventory_transactions": ["transaction_id"],
    "src_legacy_trends": ["old_product_code", "historical_period"],
    "src_weather_api": ["city", "timestamp"],
}


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes")


def _silver_row_ceiling() -> int:
    return int(os.getenv("SILVER_MAX_EXPECTED_ROWS", "50000"))


class SilverTransformer:
    """
    Reads from a Bronze Iceberg table, applies Silver transformations, writes Silver.

    CDC inventory (``src_inventory_transactions``): incremental Bronze append scan using
    a persisted snapshot watermark + MERGE into Silver by ``transaction_id``.
    Other sources: full Bronze scan + overwrite Silver (table sizes stay small).
    """

    def __init__(self, source_id: str):
        self.source_id = source_id
        self.bronze_table = table_name_for_layer(StorageLayer.BRONZE, source_id)
        self.silver_table = table_name_for_layer(StorageLayer.SILVER, source_id)
        self.contract = CONTRACT_REGISTRY.get(source_id)
        self.catalog = get_catalog()
        self.kpi = TransformationKPITracker(source_id, layer="silver")
        self.log = get_logger("silver_transformer")

    def _validate_contracts(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list, int]:
        """
        Row-wise contract enforcement is accurate but O(n) in Python.
        CDC inventory and other large Bronze tables use a bulk path (validated at ingestion).
        """
        max_rowwise = int(os.getenv("SILVER_ROW_WISE_CONTRACT_MAX", "5000"))
        use_bulk = (
            self.source_id == "src_inventory_transactions"
            or len(df) > max_rowwise
            or not self.contract
        )
        if use_bulk:
            log_pipeline_event(
                self.log,
                "info",
                "Silver bulk contract path (skip per-row enforce)",
                layer=StorageLayer.SILVER.value,
                source_id=self.source_id,
                rows=len(df),
                max_rowwise=max_rowwise,
            )
            if df.empty:
                return df, [], 0
            # Keep DataFrame — never to_dict('records') on 100k+ rows (OOM).
            return df, [], 0

        valid_rows, rejected_rows, violations = [], [], 0
        for _, row in df.iterrows():
            result = self.contract.enforce(row.to_dict())
            if result["status"] in ("ok", "coerced"):
                valid_rows.append(result["record"])
            elif result["status"] == "quarantine":
                valid_rows.append(result["record"])
                violations += len(result["violations"])
            else:
                rejected_rows.append(result["record"])
        out = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
        return out, rejected_rows, violations

    @staticmethod
    def _bronze_scan_batches(bronze, batch_rows: int):
        """Yield Bronze scan chunks without loading the full table at once when possible."""
        scan = bronze.scan()
        try:
            reader = scan.to_arrow_batch_reader()
            for record_batch in reader:
                chunk = record_batch.to_pandas()
                if not chunk.empty:
                    yield chunk
            return
        except AttributeError:
            pass
        try:
            arrow_table = scan.to_arrow()
            for batch in arrow_table.to_batches(max_chunksize=batch_rows):
                chunk = batch.to_pandas()
                if not chunk.empty:
                    yield chunk
            return
        except Exception:
            pass
        df = scan.to_pandas()
        if not df.empty:
            yield df

    @staticmethod
    def _dedup_by_pk(df: pd.DataFrame, pk: list[str]) -> pd.DataFrame:
        if not pk or df.empty or not all(c in df.columns for c in pk):
            return df
        if "_ingestion_timestamp" in df.columns:
            df = df.sort_values("_ingestion_timestamp", kind="mergesort")
        return df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)

    def _load_existing_silver(self) -> pd.DataFrame:
        try:
            silver = self.catalog.load_table(self.silver_table)
            return silver.scan().to_pandas()
        except Exception:
            return pd.DataFrame()

    def _merge_delta_into_silver(self, delta_df: pd.DataFrame, pk: list[str]) -> tuple[pd.DataFrame, int]:
        """Combine CDC delta with current Silver; keep latest row per PK."""
        existing = self._load_existing_silver()
        if delta_df.empty:
            return existing, 0
        if existing.empty:
            return self._dedup_by_pk(delta_df, pk), 0
        before = len(existing) + len(delta_df)
        combined = pd.concat([existing, delta_df], ignore_index=True)
        merged = self._dedup_by_pk(combined, pk)
        return merged, max(before - len(merged), 0)

    def _merge_or_replace_silver(self, delta_df: pd.DataFrame, pk: list[str]) -> tuple[pd.DataFrame, int]:
        """
        Merge delta into Silver, or replace entirely when Silver is bloated / rebuild requested.
        Inventory Silver should be ~unique(transaction_id), not millions of duplicate CDC rows.
        """
        existing = self._load_existing_silver()
        ceiling = _silver_row_ceiling()
        force_rebuild = _env_flag("SILVER_REBUILD_DEDUP")

        if delta_df.empty:
            return existing, 0

        deduped_delta = self._dedup_by_pk(delta_df, pk)

        if force_rebuild or (not existing.empty and len(existing) > ceiling and len(deduped_delta) < len(existing)):
            removed = max(len(existing) - len(deduped_delta), 0)
            log_pipeline_event(
                self.log,
                "warning",
                "Replacing bloated Silver from deduped Bronze (not merging duplicates forward)",
                layer=StorageLayer.SILVER.value,
                source_id=self.source_id,
                existing_silver_rows=len(existing),
                deduped_bronze_rows=len(deduped_delta),
                ceiling=ceiling,
                force_rebuild=force_rebuild,
            )
            return deduped_delta, removed

        return self._merge_delta_into_silver(deduped_delta, pk)

    def _transform_inventory_cdc_delta(self, bronze, pk: list[str]) -> tuple[pd.DataFrame, int, int, int, int, int, list, int]:
        """Incremental CDC Silver: read Bronze append delta since watermark, merge into Silver."""
        rejected_rows: list = []
        violations = 0

        watermark = load_bronze_silver_watermark(self.source_id)
        start_snap = watermark.get("bronze_snapshot_id") if watermark else None
        current_snap = bronze.current_snapshot()
        end_snap = int(current_snap.snapshot_id) if current_snap else None

        if _env_flag("SILVER_RESET_WATERMARK") or _env_flag("SILVER_REBUILD_DEDUP"):
            clear_bronze_silver_watermark(self.source_id)
            start_snap = None
            if _env_flag("SILVER_REBUILD_DEDUP"):
                log_pipeline_event(
                    self.log,
                    "info",
                    "Silver rebuild dedup — full Bronze read, replace Silver",
                    layer=StorageLayer.SILVER.value,
                    source_id=self.source_id,
                )

        if _env_flag("SILVER_STAMP_WATERMARK"):
            existing = self._load_existing_silver()
            if existing.empty:
                raise RuntimeError(
                    "SILVER_STAMP_WATERMARK set but Silver table is empty — run a full transform first."
                )
            ceiling = _silver_row_ceiling()
            if len(existing) > ceiling:
                raise RuntimeError(
                    f"Silver has {len(existing)} rows (expected <= {ceiling} unique transactions). "
                    "Stamp aborted — Silver is bloated from earlier runs. "
                    "Run once with SILVER_REBUILD_DEDUP=1 (remove SILVER_STAMP_WATERMARK), then stamp again."
                )
            if end_snap is None:
                return existing, 0, len(existing), 0, 0, 0, rejected_rows, violations
            save_bronze_silver_watermark(self.source_id, end_snap)
            log_pipeline_event(
                self.log,
                "info",
                "Stamped Silver Bronze watermark from existing Silver (no Bronze read)",
                layer=StorageLayer.SILVER.value,
                source_id=self.source_id,
                silver_rows=len(existing),
                bronze_snapshot_id=end_snap,
            )
            return existing, 0, len(existing), 0, 0, 0, rejected_rows, violations

        if _env_flag("SILVER_FORCE_FULL_BRONZE_SCAN"):
            start_snap = None

        # First run (no watermark): one batched full Bronze read, then watermark for O(delta) runs.
        if start_snap is None:
            log_pipeline_event(
                self.log,
                "info",
                "Silver CDC bootstrap — batched full Bronze read (one-time)",
                layer=StorageLayer.SILVER.value,
                source_id=self.source_id,
            )
            delta_df, records_read, dup_in_delta = self._load_bronze_incremental(bronze, pk)
            mode = "bootstrap_full"
            delta_df, null_imputations, late_arriving_count, _, violations = self._apply_silver_column_logic(
                delta_df, already_deduped=True
            )
            silver_df, dup_on_merge = self._merge_or_replace_silver(delta_df, pk)
            duplicates_removed = dup_in_delta + dup_on_merge
            if end_snap is not None:
                self._write_silver(silver_df)
                save_bronze_silver_watermark(self.source_id, end_snap)
            log_pipeline_event(
                self.log,
                "info",
                "Silver CDC bootstrap complete",
                layer=StorageLayer.SILVER.value,
                source_id=self.source_id,
                mode=mode,
                records_read=records_read,
                silver_rows=len(silver_df),
                bronze_snapshot_id=end_snap,
            )
            return (
                silver_df,
                records_read,
                len(silver_df),
                duplicates_removed,
                null_imputations,
                late_arriving_count,
                rejected_rows,
                violations,
            )

        delta_df, end_snap, mode = read_bronze_append_delta(bronze, start_snap)

        if mode == "none":
            existing = self._load_existing_silver()
            log_pipeline_event(
                self.log,
                "info",
                "Silver CDC — no Bronze delta; Silver unchanged",
                layer=StorageLayer.SILVER.value,
                source_id=self.source_id,
                silver_rows=len(existing),
                bronze_snapshot_id=end_snap,
            )
            return existing, 0, len(existing), 0, 0, 0, rejected_rows, violations

        records_read = len(delta_df)
        delta_df, rejected_rows, violations = self._validate_contracts(delta_df)
        delta_df, null_imputations, late_arriving_count, dup_in_delta, _ = self._apply_silver_column_logic(
            delta_df, already_deduped=False
        )

        silver_df, dup_on_merge = self._merge_or_replace_silver(delta_df, pk)
        duplicates_removed = dup_in_delta + dup_on_merge

        if end_snap is not None:
            self._write_silver(silver_df)
            save_bronze_silver_watermark(self.source_id, end_snap)

        log_pipeline_event(
            self.log,
            "info",
            "Silver CDC merge complete",
            layer=StorageLayer.SILVER.value,
            source_id=self.source_id,
            mode=mode,
            delta_rows=records_read,
            silver_rows=len(silver_df),
            duplicates_removed=duplicates_removed,
            bronze_snapshot_id=end_snap,
        )
        return (
            silver_df,
            records_read,
            len(silver_df),
            duplicates_removed,
            null_imputations,
            late_arriving_count,
            rejected_rows,
            violations,
        )

    def _load_bronze_incremental(self, bronze, pk: list[str]) -> tuple[pd.DataFrame, int, int]:
        """
        Stream Bronze in batches; merge + dedup after each batch so memory stays bounded
        (~unique PK count + one batch, not full duplicate-heavy CDC history).
        """
        batch_rows = int(os.getenv("SILVER_SCAN_BATCH_ROWS", "75000"))
        records_read = 0
        merged: pd.DataFrame | None = None

        for chunk in self._bronze_scan_batches(bronze, batch_rows):
            records_read += len(chunk)
            log_pipeline_event(
                self.log,
                "info",
                "Silver batched Bronze read",
                layer=StorageLayer.SILVER.value,
                source_id=self.source_id,
                batch_rows=len(chunk),
                total_read=records_read,
            )
            merged = chunk if merged is None else pd.concat([merged, chunk], ignore_index=True)
            merged = self._dedup_by_pk(merged, pk)
            del chunk

        if merged is None:
            merged = pd.DataFrame()
        duplicates_removed = max(records_read - len(merged), 0)
        log_pipeline_event(
            self.log,
            "info",
            "Silver batched dedup complete",
            layer=StorageLayer.SILVER.value,
            source_id=self.source_id,
            records_read=records_read,
            records_after_dedup=len(merged),
            duplicates_removed=duplicates_removed,
        )
        return merged, records_read, duplicates_removed

    def _apply_silver_column_logic(
        self,
        df: pd.DataFrame,
        *,
        already_deduped: bool = False,
    ) -> tuple[pd.DataFrame, int, int, int, int]:
        """Shared datetime / flags / dedup steps. Returns (df, null_imputations, late_arriving, duplicates_removed, violations)."""
        violations = 0
        pk = PK_MAP.get(self.source_id, [])
        duplicates_removed = 0

        if self.source_id == "src_legacy_trends" and "product_id" in df.columns and "old_product_code" not in df.columns:
            df = df.copy()
            df["old_product_code"] = df["product_id"].astype(str)

        null_imputations = 0
        if self.source_id == "src_manufacturing_logs" and "defect_count" in df.columns:
            n = int(df["defect_count"].isna().sum())
            df["defect_count"] = df["defect_count"].fillna(0.0)
            null_imputations += n
        if self.source_id == "src_legacy_trends" and "market_region" in df.columns:
            n = int(df["market_region"].isna().sum())
            df["market_region"] = df["market_region"].fillna("UNKNOWN")
            null_imputations += n

        for col in df.columns:
            if "timestamp" in col or col in ("created_at", "mfg_timestamp", "sale_timestamp"):
                try:
                    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
                except Exception:
                    pass

        if self.source_id == "src_sales_history" and "units_sold" in df.columns:
            df["is_return"] = df["units_sold"] < 0

        late_arriving_count = 0
        if "_ingestion_timestamp" in df.columns:
            df["_ingestion_ts"] = pd.to_datetime(df["_ingestion_timestamp"], utc=True, errors="coerce")
            for ts_col in ["timestamp", "sale_timestamp", "mfg_timestamp", "created_at"]:
                if ts_col in df.columns:
                    event_ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
                    lag = (df["_ingestion_ts"] - event_ts).dt.total_seconds()
                    max_lag_sec = 300
                    try:
                        from control_plane.advanced_contracts import ENTERPRISE_CONTRACTS
                        bundle = ENTERPRISE_CONTRACTS.get(self.source_id)
                        if bundle:
                            max_lag_sec = bundle.freshness.max_lag_seconds
                    except Exception:
                        pass
                    df["late_arriving"] = lag > max_lag_sec
                    late_arriving_count = int(df["late_arriving"].sum())
                    break

        if not already_deduped and pk and all(c in df.columns for c in pk):
            before = len(df)
            df = self._dedup_by_pk(df, pk)
            duplicates_removed = before - len(df)

        return df, null_imputations, late_arriving_count, duplicates_removed, violations

    def _write_silver(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        _sanitize_dataframe_for_arrow(df, copy=False)
        try:
            arrow_table = prepare_arrow_table_for_iceberg(
                pa.Table.from_pandas(df, preserve_index=False, safe=False)
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
                silver.overwrite(arrow_table)
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
            self.kpi.start_time = t0
            self.kpi.end_time = time.time()
            kpi_row = replace(
                self.kpi.finalize(),
                status="skipped",
                error_message=f"bronze_unavailable: {exc}",
            )
            TransformationKPILogger.log_kpi(kpi_row)
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

        pk = PK_MAP.get(self.source_id, [])
        rejected_rows: list = []
        violations = 0

        if self.source_id == "src_inventory_transactions":
            (
                df,
                records_read,
                records_cleaned,
                duplicates_removed,
                null_imputations,
                late_arriving_count,
                rejected_rows,
                violations,
            ) = self._transform_inventory_cdc_delta(bronze, pk)
            # Silver already written inside CDC path; skip second write below.
            write_silver = False
        else:
            write_silver = True
            df = bronze.scan().to_pandas()
            records_read = len(df)
            df, rejected_rows, violations = self._validate_contracts(df)
            df, null_imputations, late_arriving_count, duplicates_removed, _ = self._apply_silver_column_logic(df)
            records_cleaned = len(df)

        if write_silver:
            self._write_silver(df)

        latency = time.time() - t0
        self.kpi.records_read = records_read
        self.kpi.records_cleaned = records_cleaned
        self.kpi.records_rejected = len(rejected_rows)
        self.kpi.null_imputations = null_imputations
        self.kpi.duplicates_removed = duplicates_removed
        self.kpi.late_arrivals = late_arriving_count
        self.kpi.schema_violations = violations
        self.kpi.records_written = records_cleaned
        self.kpi.start_time = t0
        self.kpi.end_time = latency + t0
        TransformationKPILogger.log_kpi(self.kpi.finalize())

        log_pipeline_event(
            self.log,
            "info",
            "Completed Silver transformation",
            layer=StorageLayer.SILVER.value,
            source_id=self.source_id,
            records_read=records_read,
            records_cleaned=records_cleaned,
            records_rejected=len(rejected_rows),
            duration_sec=round(latency, 3),
        )
        return SilverTransformResult(
            source_id=self.source_id,
            records_read=int(records_read),
            records_cleaned=int(records_cleaned),
            records_rejected=int(len(rejected_rows)),
            null_imputations=int(null_imputations),
            duplicates_removed=int(duplicates_removed),
            late_arriving_count=int(late_arriving_count),
            schema_violations=int(violations),
            transformation_latency_sec=float(latency),
        )
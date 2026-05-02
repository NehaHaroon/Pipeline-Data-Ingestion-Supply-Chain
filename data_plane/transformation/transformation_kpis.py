
"""
Transformation KPI Tracking and Observability

This module tracks quantitative metrics during transformation execution:
- Data quality metrics (null %, schema violations, duplicates)
- Transformation efficiency (latency, throughput, stage duration)
- CDC consistency (late arrivals, out-of-order events)
- Business-level metrics (aggregation accuracy, metric drift)

All KPIs are persisted to `storage/ingested/detail_logs/transformation_kpis.jsonl`
for long-term analysis and alerting.
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List
from datetime import datetime
import json
import os
import logging

from config import TRANSFORMATION_KPI_LOG_PATH

log = logging.getLogger(__name__)


@dataclass
class TransformationKPIs:
    """Quantitative metrics for a single transformation run."""
    
    # ── Run metadata ──
    run_id: str
    source_id: str
    layer: str                         # "silver" or "gold"
    run_at: str                        # ISO timestamp
    
    # ── Data quality metrics ──
    records_read: int
    records_cleaned: int
    records_rejected: int
    records_written: int
    
    # ── Transformation efficiency ──
    records_transformed_per_sec: float
    transformation_latency_sec: float
    null_imputation_count: int
    
    # ── Null handling details ──
    null_percentage_per_column: Dict[str, float] = field(default_factory=dict)
    
    # ── Schema quality ──
    schema_violation_count: int = 0
    schema_violations: List[str] = field(default_factory=list)
    
    # ── Deduplication metrics ──
    duplicate_records_detected: int = 0
    duplicate_removal_rate: float = 0.0
    
    # ── CDC/Streaming quality ──
    late_arriving_records: int = 0
    late_arriving_percentage: float = 0.0
    out_of_order_events: int = 0
    cdc_conflicts_detected: int = 0
    cdc_conflicts_resolved: int = 0
    
    # ── Gold layer only ──
    aggregation_accuracy: Optional[float] = None  # % records matched to dimension
    metric_drift: Optional[float] = None  # day-over-day % change
    uniqueness_violations: int = 0
    
    # ── Status ──
    status: str = "completed"  # "completed", "failed", "skipped"
    error_message: Optional[str] = None
    

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    def to_json_line(self) -> str:
        """Convert to single JSON line for JSONL persistence."""
        return json.dumps(self.to_dict())


@dataclass  
class TransformationKPITracker:
    """
    In-memory accumulator for KPIs during transformation.
    
    Usage:
        tracker = TransformationKPITracker("src_sales_history", layer="silver")
        tracker.records_read = 1000
        tracker.records_cleaned = 990
        tracker.duplicates_removed = 5
        ...
        kpi = tracker.finalize()
        kpi.log_to_disk()
    """
    
    source_id: str
    layer: str  # "silver" or "gold"
    run_id: str = field(default_factory=lambda: f"run_{datetime.utcnow().isoformat()}")
    
    # Accumulators
    records_read: int = 0
    records_cleaned: int = 0
    records_rejected: int = 0
    records_written: int = 0
    null_imputations: int = 0
    duplicates_removed: int = 0
    late_arrivals: int = 0
    out_of_order: int = 0
    schema_violations: int = 0
    cdc_conflicts: int = 0
    
    # Details
    null_counts_per_col: Dict[str, int] = field(default_factory=dict)
    violation_details: List[str] = field(default_factory=list)
    
    # Timers
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    
    def finalize(self) -> TransformationKPIs:
        """Compute final KPIs from accumulated metrics."""
        import time
        
        if not self.start_time:
            self.start_time = time.time()
        if not self.end_time:
            self.end_time = time.time()
            
        latency_sec = self.end_time - self.start_time
        
        # Avoid division by zero
        if latency_sec == 0:
            latency_sec = 0.001
            
        throughput = self.records_cleaned / latency_sec if self.records_cleaned > 0 else 0.0
        
        # Compute null percentages
        null_pct_by_col = {}
        if self.records_read > 0:
            null_pct_by_col = {
                col: round(100.0 * count / self.records_read, 2)
                for col, count in self.null_counts_per_col.items()
                if count > 0
            }
        
        # Dedup rate: how many dupes were removed relative to input
        dedup_rate = (
            100.0 * self.duplicates_removed / self.records_read
            if self.records_read > 0
            else 0.0
        )
        
        # Late arrival rate
        late_rate = (
            100.0 * self.late_arrivals / self.records_read
            if self.records_read > 0
            else 0.0
        )
        
        return TransformationKPIs(
            run_id=self.run_id,
            source_id=self.source_id,
            layer=self.layer,
            run_at=datetime.utcnow().isoformat() + "Z",
            records_read=self.records_read,
            records_cleaned=self.records_cleaned,
            records_rejected=self.records_rejected,
            records_written=self.records_written,
            records_transformed_per_sec=round(throughput, 2),
            transformation_latency_sec=round(latency_sec, 3),
            null_imputation_count=self.null_imputations,
            null_percentage_per_column=null_pct_by_col,
            schema_violation_count=self.schema_violations,
            schema_violations=self.violation_details[:10],  # first 10 only
            duplicate_records_detected=self.duplicates_removed,
            duplicate_removal_rate=round(dedup_rate, 2),
            late_arriving_records=self.late_arrivals,
            late_arriving_percentage=round(late_rate, 2),
            out_of_order_events=self.out_of_order,
            cdc_conflicts_detected=self.cdc_conflicts,
        )


class TransformationKPILogger:
    """Persist transformation KPIs to disk and provide query interface."""
    
    KPIS_LOG_PATH = TRANSFORMATION_KPI_LOG_PATH
    
    @staticmethod
    def log_kpi(kpi: TransformationKPIs) -> None:
        """Append KPI record to JSONL file."""
        os.makedirs(os.path.dirname(TransformationKPILogger.KPIS_LOG_PATH), exist_ok=True)
        
        try:
            with open(TransformationKPILogger.KPIS_LOG_PATH, "a") as f:
                f.write(kpi.to_json_line() + "\n")
            log.info(
                f"KPI logged: source={kpi.source_id} layer={kpi.layer} "
                f"records_read={kpi.records_read} records_cleaned={kpi.records_cleaned}"
            )
        except Exception as e:
            log.error(f"Failed to log KPI: {e}")
    
    @staticmethod
    def load_kpis(
        source_id: Optional[str] = None,
        layer: Optional[str] = None,
        limit: int = 100,
    ) -> List[TransformationKPIs]:
        """Load KPI records from disk with optional filtering."""
        log.debug("Loading KPIs from %s", TransformationKPILogger.KPIS_LOG_PATH)
        if not os.path.exists(TransformationKPILogger.KPIS_LOG_PATH):
            return []
        
        records = []
        try:
            with open(TransformationKPILogger.KPIS_LOG_PATH, "r") as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        kpi = TransformationKPIs(**data)
                        
                        # Apply filters
                        if source_id and kpi.source_id != source_id:
                            continue
                        if layer and kpi.layer != layer:
                            continue
                        
                        records.append(kpi)
                    except json.JSONDecodeError:
                        log.warning(f"Skipping malformed KPI line: {line[:50]}")
        except Exception as e:
            log.error(f"Error loading KPIs: {e}")
        
        # Return most recent records first
        return sorted(records, key=lambda x: x.run_at, reverse=True)[:limit]
    
    @staticmethod
    def _aggregate_records(records: List[TransformationKPIs]) -> Dict[str, Any]:
        if not records:
            return {}
        total_read = sum(r.records_read for r in records)
        total_cleaned = sum(r.records_cleaned for r in records)
        total_rejected = sum(r.records_rejected for r in records)
        total_duped = sum(r.duplicate_records_detected for r in records)
        total_late = sum(r.late_arriving_records for r in records)
        avg_latency = sum(r.transformation_latency_sec for r in records) / len(records)
        avg_throughput = sum(r.records_transformed_per_sec for r in records) / len(records)
        return {
            "layer": records[0].layer if records else "",
            "run_count": len(records),
            "total_records_processed": total_read,
            "total_records_cleaned": total_cleaned,
            "total_records_rejected": total_rejected,
            "overall_quality_ratio": round(100.0 * total_cleaned / max(total_read, 1), 2),
            "total_duplicates_removed": total_duped,
            "total_late_arrivals": total_late,
            "avg_transformation_latency_sec": round(avg_latency, 2),
            "avg_throughput_rec_sec": round(avg_throughput, 2),
            "last_run_at": records[0].run_at if records else None,
        }
    
    @staticmethod
    def get_latest_kpi(source_id: str, layer: str) -> Optional[TransformationKPIs]:
        """Get most recent KPI for a source/layer combination."""
        records = TransformationKPILogger.load_kpis(source_id=source_id, layer=layer, limit=1)
        return records[0] if records else None
    
    @staticmethod
    def get_aggregate_stats(layer: str = "silver") -> Dict[str, Any]:
        """Get aggregate statistics across all transformation runs (JSONL only)."""
        records = TransformationKPILogger.load_kpis(layer=layer, limit=5000)
        return TransformationKPILogger._aggregate_records(records)
    
    @staticmethod
    def get_aggregate_stats_with_iceberg_fallback(layer: str = "silver") -> Dict[str, Any]:
        """Like get_aggregate_stats but fills from current Iceberg snapshots when JSONL is empty."""
        records = TransformationKPILogger.load_kpis(layer=layer, limit=5000)
        if not records:
            records = build_fallback_kpis_from_iceberg(layer=layer, limit=500)
        return TransformationKPILogger._aggregate_records(records)


def build_fallback_kpis_from_iceberg(
    source_id: Optional[str] = None,
    layer: Optional[str] = None,
    limit: int = 100,
) -> List[TransformationKPIs]:
    """
    When transformation_kpis.jsonl has no rows, synthesize one KPI per Silver/Gold table
    from Iceberg snapshot stats so dashboards stay aligned with storage metrics.
    """
    from storage_plane.storage_kpis import get_all_tables_kpis

    run_at = datetime.utcnow().isoformat() + "Z"
    all_tbl = get_all_tables_kpis()
    out: List[TransformationKPIs] = []
    for tbl_name, sk in all_tbl.items():
        if sk.get("error"):
            continue
        if not tbl_name.startswith("silver.") and not tbl_name.startswith("gold."):
            continue
        ns, short = tbl_name.split(".", 1)
        if layer and ns != layer:
            continue
        if ns == "gold" and short == "replenishment_signals":
            sid = "gold_replenishment_signals"
        else:
            sid = f"src_{short}"
        if source_id and sid != source_id:
            continue
        rc = int(sk.get("records_in_snapshot") or sk.get("record_count") or 0)
        if rc <= 0:
            continue
        out.append(
            TransformationKPIs(
                run_id=f"iceberg_snapshot_{tbl_name}",
                source_id=sid,
                layer=ns,
                run_at=run_at,
                records_read=rc,
                records_cleaned=rc,
                records_rejected=0,
                records_written=rc,
                records_transformed_per_sec=0.0,
                transformation_latency_sec=0.0,
                null_imputation_count=0,
                status="iceberg_snapshot",
                error_message=None,
            )
        )
    out.sort(key=lambda x: (x.layer, x.source_id), reverse=True)
    return out[:limit]


def load_transformation_kpis_for_dashboard(
    source_id: Optional[str] = None,
    layer: Optional[str] = None,
    limit: int = 100,
) -> List[TransformationKPIs]:
    """Prefer JSONL run history; if empty, derive KPI rows from Iceberg table stats."""
    records = TransformationKPILogger.load_kpis(
        source_id=source_id, layer=layer, limit=limit
    )
    if records:
        return records
    return build_fallback_kpis_from_iceberg(
        source_id=source_id, layer=layer, limit=limit
    )
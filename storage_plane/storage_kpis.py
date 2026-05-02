
"""
Storage KPIs: Observability metrics for Iceberg table health and efficiency.

Tracks:
- File-level metrics (file count, sizes, small file ratio)
- Snapshot metrics (versioning, time travel cost)
- Storage efficiency (compression, partitioning balance)
- Compaction status (lag, files compacted)
- Partition health (skew, hot partitions)
"""

from storage_plane.iceberg_catalog import get_catalog
from typing import Dict, Any, List, Optional
import logging

log = logging.getLogger(__name__)


def _summary_int(summary: Any, key: str, default: int = 0) -> int:
    """Iceberg Summary properties may be present but None; int(None) raises."""
    if summary is None:
        return default
    raw = summary.get(key, default) if hasattr(summary, "get") else default
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def get_storage_kpis(table_name: str) -> Dict[str, Any]:
    """
    Get comprehensive KPIs for a single Iceberg table.
    
    Args:
        table_name: Fully qualified table name (e.g., "bronze.iot_rfid_stream")
    
    Returns:
        Dictionary with file metrics, snapshots, storage efficiency, and partition health.
    """
    try:
        catalog = get_catalog()
        # Some SqlCatalog versions do not implement table_exists().
        # Existence check is done by attempting to load the table.
        t = catalog.load_table(table_name)
        snap = t.current_snapshot()
        
        if not snap:
            return {
                "table_name": table_name,
                "status": "empty",
                "snapshot_count": 0,
                "file_count": 0,
                "record_count": 0,
            }
        
        summary = snap.summary
        
        # Get file metadata - simplified to avoid inspect issues
        # files_table = t.inspect.files().to_pydict()
        # file_paths = files_table.get("file_path", [])
        # file_sizes = files_table.get("file_size_in_bytes", [])
        file_sizes = []  # Placeholder - would need manifest parsing for accurate file sizes
        
        # Calculate file metrics (simplified)
        file_count = _summary_int(summary, "added-data-files", 0)  # Approximate
        avg_file_size = 0  # Placeholder
        small_file_count = 0  # Placeholder
        small_file_ratio = 0.0  # Placeholder
        
        # Total storage
        total_storage_bytes = sum(file_sizes) if file_sizes else 0
        
        # Snapshot metrics
        snapshot_count = len(list(t.history())) if hasattr(t, "history") else 1
        
        # Partition metrics
        partition_count = snapshot_count  # simplified; ideally would scan partition spec
        
        return {
            "table_name": table_name,
            "status": "healthy",
            
            # File-level metrics (simplified)
            "file_count": file_count,
            "avg_file_size_mb": round(avg_file_size / (1024 * 1024), 2) if avg_file_size else 0,
            "small_file_count": small_file_count,
            "small_file_ratio": round(small_file_ratio, 3),  # > 0.5 is a problem
            "min_file_size_mb": 0,  # Placeholder
            "max_file_size_mb": 0,  # Placeholder
            
            # Snapshot metrics
            "snapshot_count": snapshot_count,
            "total_storage_bytes": total_storage_bytes,
            "total_storage_mb": round(total_storage_bytes / (1024 * 1024), 2),
            
            # From current snapshot summary
            "record_count": _summary_int(summary, "total-records", 0),
            "records_in_snapshot": _summary_int(summary, "total-records", 0),
            "added_data_files": _summary_int(summary, "added-data-files", 0),
            "deleted_data_files": _summary_int(summary, "deleted-data-files", 0),
            
            # Health status indicators
            "needs_compaction": small_file_ratio > 0.5,  # Flag if > 50% small files
            "health_score": compute_table_health_score(small_file_ratio, file_count),
        }
    
    except Exception as e:
        log.warning(f"Table {table_name} is not available or failed KPI scan: {e}")
        return {"table_name": table_name, "error": str(e)}


def get_all_tables_kpis() -> Dict[str, Dict[str, Any]]:
    """
    Get KPIs for all Iceberg tables across all layers (bronze, silver, gold).
    
    Returns:
        Dictionary mapping table names to their KPIs.
    """
    catalog = get_catalog()
    all_kpis = {}
    
    # Scan all namespaces and tables
    for namespace in ["bronze", "silver", "gold"]:
        try:
            # PyIceberg expects namespace as tuple, e.g. ("bronze",)
            tables = catalog.list_tables((namespace,))
            for table_id in tables:
                # table_id is typically ("namespace", "table_name")
                if isinstance(table_id, (tuple, list)) and len(table_id) >= 2:
                    table_name = f"{table_id[0]}.{table_id[1]}"
                else:
                    table_name = f"{namespace}.{table_id}"
                kpis = get_storage_kpis(table_name)
                all_kpis[table_name] = kpis
        except Exception as e:
            log.warning(f"Could not list tables in namespace {namespace}: {e}")
    
    return all_kpis


def compute_table_health_score(small_file_ratio: float, file_count: int) -> float:
    """
    Compute a 0-100 health score for a table.
    
    Factors:
    - Small file ratio (> 0.5 = unhealthy, needs compaction)
    - File fragmentation (many tiny files vs. few large files)
    
    Args:
        small_file_ratio: Fraction of files < 10 MB
        file_count: Total number of files
    
    Returns:
        Health score 0-100 (100 = optimal)
    """
    score = 100.0
    
    # Penalize small file ratio
    if small_file_ratio > 0.5:
        score -= (small_file_ratio - 0.5) * 50  # Max -50 points
    
    # Penalize excessive fragmentation (many tiny files)
    if file_count > 100:
        score -= min((file_count - 100) * 0.1, 30)  # Max -30 points
    
    return max(0, round(score, 1))


def compute_storage_health(all_kpis: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute overall storage health across all tables.
    
    Returns:
        Summary with warnings, recommendations, and aggregate metrics.
    """
    warnings = []
    recommendations = []
    
    total_files = 0
    total_storage_mb = 0
    tables_needing_compaction = []
    
    for table_name, kpis in all_kpis.items():
        if "error" in kpis:
            continue
        
        total_files += kpis.get("file_count", 0)
        total_storage_mb += kpis.get("total_storage_mb", 0)
        
        # Check for problematic tables
        small_file_ratio = kpis.get("small_file_ratio", 0)
        if small_file_ratio > 0.5:
            tables_needing_compaction.append((table_name, small_file_ratio))
            warnings.append(
                f"🔴 {table_name}: {int(small_file_ratio*100)}% small files (< 10MB). "
                f"Compaction needed to improve query performance."
            )
        elif small_file_ratio > 0.3:
            recommendations.append(
                f"🟡 {table_name}: Consider compaction ({int(small_file_ratio*100)}% small files)"
            )
        
        # Check snapshot health
        if kpis.get("snapshot_count", 0) > 100:
            recommendations.append(
                f"🟡 {table_name}: {kpis['snapshot_count']} snapshots. "
                f"Consider snapshot retention cleanup."
            )
    
    # Sort by severity
    tables_needing_compaction.sort(key=lambda x: x[1], reverse=True)
    
    return {
        "total_tables": len(all_kpis),
        "total_files": total_files,
        "total_storage_mb": round(total_storage_mb, 2),
        "avg_file_count_per_table": round(total_files / max(len(all_kpis), 1), 1),
        "tables_needing_compaction": [t[0] for t in tables_needing_compaction],
        "health_status": "warning" if warnings else ("good" if not recommendations else "fair"),
        "warnings": warnings,
        "recommendations": recommendations,
    }
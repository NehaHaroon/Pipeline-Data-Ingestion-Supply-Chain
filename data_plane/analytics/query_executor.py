"""
Execute cataloged SQL workloads with governance hooks and audit logging.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# HTTPException only when called from FastAPI path via access_control

from data_plane.analytics.duckdb_engine import execute_sql
from data_plane.analytics.query_catalog import QUERY_CATALOG, SqlWorkloadQuery, WorkloadType, get_query, load_sql
from observability_plane.analytics_metrics import record_query_execution

QUERY_AUDIT_PATH = os.getenv(
    "QUERY_AUDIT_LOG_PATH",
    "storage/analytics/query_audit.jsonl",
)

# Business hours cost policy (PKT UTC+5 → use env override)
BUSINESS_HOUR_START = int(os.getenv("GOV_BUSINESS_HOUR_START", "4"))  # 09:00 PKT ≈ 04:00 UTC
BUSINESS_HOUR_END = int(os.getenv("GOV_BUSINESS_HOUR_END", "13"))


def _in_business_hours() -> bool:
    hour = datetime.now(timezone.utc).hour
    return BUSINESS_HOUR_START <= hour < BUSINESS_HOUR_END


def enforce_cost_policy(query: SqlWorkloadQuery, sql: str) -> Optional[str]:
    """Return error message if query blocked; None if allowed."""
    join_count = sql.lower().count(" join ")
    if _in_business_hours() and query.workload_type == WorkloadType.AD_HOC and join_count > 3:
        return (
            f"Cost policy: ad_hoc joins ({join_count}) blocked during business hours "
            f"(max 3). Retry off-peak or use a dbt mart."
        )
    if _in_business_hours() and query.workload_type == WorkloadType.AD_HOC and join_count > 2:
        # soft warning only for 3 joins - already blocked above at >3
        pass
    return None


def _append_audit(entry: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(QUERY_AUDIT_PATH), exist_ok=True)
    with open(QUERY_AUDIT_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def run_query(query_id: str, role: Optional[str] = None) -> Dict[str, Any]:
    query = get_query(query_id)
    if not query:
        raise KeyError(f"Unknown query_id: {query_id}")

    if role:
        from control_plane.access_control import AnalyticsRole, assert_query_allowed

        assert_query_allowed(AnalyticsRole(role.strip().lower()), query_id, query.workload_type.value)

    sql = load_sql(query)
    blocked = enforce_cost_policy(query, sql)
    if blocked:
        entry = {
            "query_id": query_id,
            "status": "blocked",
            "reason": blocked,
            "at": datetime.now(timezone.utc).isoformat(),
        }
        _append_audit(entry)
        return {"status": "blocked", "reason": blocked, "query_id": query_id}

    max_wall = 120.0
    if query.workload_type == WorkloadType.STRATEGIC:
        max_wall = 240.0

    try:
        rows, stats = execute_sql(sql, max_wall_sec=max_wall)
        status = "ok"
        error = None
    except Exception as exc:
        rows, stats = [], {"elapsed_sec": 0, "row_count": 0}
        status = "error"
        error = str(exc)

    result = {
        "query_id": query_id,
        "name": query.name,
        "workload_type": query.workload_type.value,
        "complexity": query.complexity.value,
        "characteristics": query.characteristics,
        "optimization_plan": query.optimization_plan,
        "status": status,
        "error": error,
        "stats": stats,
        "row_count": len(rows),
        "rows": rows[:200],
        "rows_truncated": len(rows) > 200,
    }

    _append_audit(
        {
            "query_id": query_id,
            "workload_type": query.workload_type.value,
            "complexity": query.complexity.value,
            "status": status,
            "elapsed_sec": stats.get("elapsed_sec"),
            "row_count": stats.get("row_count"),
            "error": error,
            "at": datetime.now(timezone.utc).isoformat(),
        }
    )
    record_query_execution(query_id, query.workload_type.value, stats, status)
    return result


def run_all_queries() -> List[Dict[str, Any]]:
    return [run_query(q.query_id) for q in QUERY_CATALOG]


def catalog_response() -> Dict[str, Any]:
    from data_plane.analytics.query_catalog import catalog_by_workload

    return {
        "queries": [
            {
                "query_id": q.query_id,
                "workload_type": q.workload_type.value,
                "name": q.name,
                "complexity": q.complexity.value,
                "characteristics": q.characteristics,
                "optimization_plan": q.optimization_plan,
            }
            for q in QUERY_CATALOG
        ],
        "by_workload": catalog_by_workload(),
    }

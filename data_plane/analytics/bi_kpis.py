"""
Executive BI KPIs sourced from semantic layer (Gold + SQL workloads).
"""

from __future__ import annotations

from typing import Any, Dict

from data_plane.analytics.query_executor import run_query


def compute_bi_kpis() -> Dict[str, Any]:
    """Run executive queries and derive dashboard scorecard metrics."""
    scorecard = run_query("exec_001")
    profitability = run_query("exec_002")

    rows = scorecard.get("rows") or [{}]
    sc = rows[0] if rows else {}

    prof_rows = profitability.get("rows") or []
    replenishment_pkr = sum(
        float(r.get("replenishment_value_pkr") or 0) for r in prof_rows
    )

    return {
        "skus_needing_replenishment": int(sc.get("skus_needing_replenishment") or 0),
        "avg_urgency_score": float(sc.get("avg_urgency_score") or 0),
        "max_urgency_score": float(sc.get("max_urgency_score") or 0),
        "total_suggested_order_units": int(sc.get("total_suggested_order_units") or 0),
        "total_suggested_order_units_metric": int(sc.get("total_suggested_order_units") or 0),
        "weather_risk_active": 1 if sc.get("weather_risk_flag") else 0,
        "replenishment_value_pkr": round(replenishment_pkr, 2),
        "profitable_skus_at_risk": len(prof_rows),
        "last_gold_refresh": str(sc.get("last_gold_refresh") or ""),
        "scorecard_status": scorecard.get("status"),
        "profitability_status": profitability.get("status"),
    }

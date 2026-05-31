"""
Executive BI KPIs sourced from semantic layer (Gold + SQL workloads).
"""

from __future__ import annotations

from typing import Any, Dict

from data_plane.analytics.executive_dashboard import build_executive_dashboard


def compute_bi_kpis() -> Dict[str, Any]:
    """Business scorecards for executive dashboards (no technical metrics)."""
    payload = build_executive_dashboard()
    sc = payload["scorecards"]
    return {
        "skus_needing_replenishment": sc["skus_at_risk"],
        "catalog_products_total": sc["catalog_products"],
        "stockout_risk_pct": sc["stockout_risk_pct"],
        "critical_skus_count": sc["critical_skus"],
        "high_urgency_skus_count": sc["high_urgency_skus"],
        "avg_urgency_score": sc["avg_urgency_score"],
        "max_urgency_score": sc["max_urgency_score"],
        "total_suggested_order_units": sc["total_units_to_order"],
        "replenishment_value_pkr": sc["replenishment_value_pkr"],
        "replenishment_value_million_pkr": sc["replenishment_value_million_pkr"],
        "weather_risk_active": 1 if sc["weather_risk_active"] else 0,
        "delivery_delay_pct": sc["delivery_delay_pct"],
        "bottleneck_warehouse_count": sc["bottleneck_locations"],
        "profitable_skus_at_risk": len(payload.get("top_replenishment_priorities") or []),
        "last_gold_refresh": sc["last_gold_refresh"],
        "interpretations": payload.get("interpretations"),
        "scorecard_status": payload.get("query_status", {}).get("exec_001"),
        "profitability_status": payload.get("query_status", {}).get("exec_002"),
    }

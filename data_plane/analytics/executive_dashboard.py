"""
Executive replenishment dashboard payload — business KPIs for strategy / ops leaders.

Avoids technical metrics (query latency, dbt runtime) on the executive surface.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from data_plane.analytics.duckdb_engine import lakehouse_connection
from data_plane.analytics.query_executor import run_query

EXECUTIVE_SNAPSHOT_PATH = os.getenv(
    "EXECUTIVE_DASHBOARD_JSON",
    "storage/analytics/executive_dashboard.json",
)


def _scalar_sql(sql: str) -> Any:
    with lakehouse_connection() as con:
        row = con.execute(sql).fetchone()
        return row[0] if row else None


def build_executive_dashboard() -> Dict[str, Any]:
    """Aggregate scorecards, trends, and tables for replenishment decisions."""
    scorecard = run_query("exec_001")
    profitability = run_query("exec_002")
    revenue = run_query("str_001")
    warehouses = run_query("str_002")
    bottlenecks = run_query("op_002")

    sc_row = (scorecard.get("rows") or [{}])[0]
    prof_rows = profitability.get("rows") or []
    revenue_rows = revenue.get("rows") or []
    warehouse_rows = warehouses.get("rows") or []
    bottleneck_rows = bottlenecks.get("rows") or []

    skus_at_risk = int(sc_row.get("skus_needing_replenishment") or 0)
    catalog_size = int(_scalar_sql("SELECT COUNT(DISTINCT product_id) FROM silver_warehouse_master") or 0)
    stockout_risk_pct = round(100.0 * skus_at_risk / catalog_size, 1) if catalog_size else 0.0

    critical_sql = """
        SELECT COUNT(*) FROM gold_replenishment_signals
        WHERE COALESCE(urgency_score, 0) >= 0.5
    """
    high_urgency_sql = """
        SELECT COUNT(*) FROM gold_replenishment_signals
        WHERE COALESCE(urgency_score, 0) >= 0.8
    """
    critical_skus = int(_scalar_sql(critical_sql) or 0)
    high_urgency_skus = int(_scalar_sql(high_urgency_sql) or 0)

    replenishment_pkr = sum(float(r.get("replenishment_value_pkr") or 0) for r in prof_rows)
    top_priorities = [
        {
            "product_id": r.get("product_id"),
            "urgency_score": round(float(r.get("urgency_score") or 0), 3),
            "suggested_order_qty": int(r.get("suggested_order_qty") or 0),
            "replenishment_value_pkr": round(float(r.get("replenishment_value_pkr") or 0), 0),
            "current_stock_on_shelf": int(r.get("current_stock_on_shelf") or 0),
            "units_sold_7d": int(r.get("units_sold_7d") or 0),
        }
        for r in prof_rows[:10]
    ]

    monthly_trend = [
        {
            "month": str(r.get("month")),
            "region": str(r.get("region")),
            "current_units": int(r.get("current_units") or 0),
            "legacy_baseline_units": int(r.get("legacy_baseline_units") or 0),
        }
        for r in revenue_rows[:24]
    ]

    warehouse_performance = [
        {
            "warehouse_location": r.get("warehouse_location"),
            "transaction_count": int(r.get("transaction_count") or 0),
            "total_units_moved": int(r.get("total_units_moved") or 0),
            "adjustment_rate_pct": float(r.get("adjustment_rate_pct") or 0),
        }
        for r in warehouse_rows[:15]
    ]

    delivery_delay_pct = 0.0
    try:
        delay = _scalar_sql(
            "SELECT ROUND(100.0 * AVG(CASE WHEN late_arriving THEN 1.0 ELSE 0.0 END), 2) "
            "FROM silver_inventory_transactions"
        )
        delivery_delay_pct = float(delay or 0)
    except Exception:
        pass

    avg_shelf = _scalar_sql(
        "SELECT ROUND(AVG(current_stock_on_shelf), 1) FROM gold_replenishment_signals"
    )
    avg_threshold = _scalar_sql(
        """SELECT ROUND(AVG(w.reorder_threshold), 1)
           FROM gold_replenishment_signals g
           JOIN silver_warehouse_master w ON g.product_id = w.product_id"""
    )

    scorecards = {
        "skus_at_risk": skus_at_risk,
        "catalog_products": catalog_size,
        "stockout_risk_pct": stockout_risk_pct,
        "critical_skus": critical_skus,
        "high_urgency_skus": high_urgency_skus,
        "avg_urgency_score": float(sc_row.get("avg_urgency_score") or 0),
        "max_urgency_score": float(sc_row.get("max_urgency_score") or 0),
        "total_units_to_order": int(sc_row.get("total_suggested_order_units") or 0),
        "replenishment_value_pkr": round(replenishment_pkr, 0),
        "replenishment_value_million_pkr": round(replenishment_pkr / 1_000_000, 2),
        "weather_risk_active": bool(sc_row.get("weather_risk_flag")),
        "delivery_delay_pct": delivery_delay_pct,
        "bottleneck_locations": len(bottleneck_rows),
        "avg_shelf_stock": float(avg_shelf or 0),
        "avg_reorder_threshold": float(avg_threshold or 0),
        "last_gold_refresh": str(sc_row.get("last_gold_refresh") or ""),
    }

    interpretations = {
        "skus_at_risk": f"{skus_at_risk:,} products are below reorder threshold and need replenishment.",
        "stockout_risk_pct": f"{stockout_risk_pct}% of the {catalog_size:,}-SKU catalog is at stockout risk.",
        "critical_skus": f"{critical_skus} SKUs have urgency ≥ 50% — prioritize these purchase orders.",
        "replenishment_value_million_pkr": f"Estimated order spend to restore shelves: PKR {scorecards['replenishment_value_million_pkr']}M.",
        "weather_risk_active": "Weather disruption flag is ON — expect inbound delays." if scorecards["weather_risk_active"] else "No active weather risk on the network.",
        "delivery_delay_pct": f"{delivery_delay_pct}% of recent inventory movements arrived late (CDC lag).",
        "bottleneck_locations": f"{len(bottleneck_rows)} warehouse lanes show high outbound volume with low shelf stock.",
    }

    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "business_problem": "Inventory replenishment under uncertainty",
        "scorecards": scorecards,
        "interpretations": interpretations,
        "top_replenishment_priorities": top_priorities,
        "monthly_demand_trend": monthly_trend,
        "warehouse_performance": warehouse_performance,
        "active_bottlenecks": bottleneck_rows[:10],
        "query_status": {
            "exec_001": scorecard.get("status"),
            "exec_002": profitability.get("status"),
            "str_001": revenue.get("status"),
            "str_002": warehouses.get("status"),
            "op_002": bottlenecks.get("status"),
        },
    }


def persist_executive_dashboard() -> Dict[str, Any]:
    payload = build_executive_dashboard()
    path = EXECUTIVE_SNAPSHOT_PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
    except OSError:
        pass
    return payload


def scorecards_for_prometheus(payload: Dict[str, Any] | None = None) -> Dict[str, float]:
    """Flatten scorecards for Grafana gauges (business metrics only)."""
    p = payload or build_executive_dashboard()
    sc = p["scorecards"]
    return {
        "skus_needing_replenishment": float(sc["skus_at_risk"]),
        "catalog_products_total": float(sc["catalog_products"]),
        "stockout_risk_pct": float(sc["stockout_risk_pct"]),
        "critical_skus_count": float(sc["critical_skus"]),
        "high_urgency_skus_count": float(sc["high_urgency_skus"]),
        "avg_urgency_score": float(sc["avg_urgency_score"]),
        "max_urgency_score": float(sc["max_urgency_score"]),
        "total_suggested_order_units": float(sc["total_units_to_order"]),
        "replenishment_value_pkr": float(sc["replenishment_value_pkr"]),
        "replenishment_value_million_pkr": float(sc["replenishment_value_million_pkr"]),
        "weather_risk_active": 1.0 if sc["weather_risk_active"] else 0.0,
        "delivery_delay_pct": float(sc["delivery_delay_pct"]),
        "bottleneck_warehouse_count": float(sc["bottleneck_locations"]),
        "avg_shelf_stock_units": float(sc["avg_shelf_stock"]),
        "avg_reorder_threshold_units": float(sc["avg_reorder_threshold"]),
    }

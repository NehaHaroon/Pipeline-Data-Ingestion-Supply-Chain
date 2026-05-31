"""Fix Grafana PromQL to match exported metric labels (weather_state=any)."""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "monitoring" / "grafana" / "dashboards" / "supply-chain-bi.json"
dash = json.loads(path.read_text(encoding="utf-8"))

# Network rollup stats — use ALL warehouse slice, unified weather_state=any
STAT_METRICS = {
    "skus_needing_replenishment",
    "stockout_risk_pct",
    "critical_skus_count",
    "replenishment_value_million_pkr",
    "total_suggested_order_units",
    "avg_urgency_score",
    "max_urgency_score",
    "total_suggested_order_units",
}

WH_BREAKDOWN = {
    "warehouse_units_moved",
    "warehouse_adjustment_rate_pct",
    "warehouse_transaction_count",
}

URGENCY = {
    "urgency_tier_critical_count",
    "urgency_tier_high_count",
    "urgency_tier_medium_count",
    "urgency_tier_low_count",
}


def stat_expr(metric: str, demand: str = "any") -> str:
    return (
        f'analytics_bi_metric{{metric="{metric}", warehouse_location="ALL", '
        f'urgency_tier=~"$urgency_tier", weather_state="any", demand_window="{demand}"}}'
    )


def wh_expr(metric: str) -> str:
    return f'analytics_bi_metric{{metric="{metric}", warehouse_location=~"WAREHOUSE-.*"}}'


def urgency_expr(metric: str) -> str:
    return (
        f'analytics_bi_metric{{metric="{metric}", warehouse_location="ALL", '
        f'urgency_tier="all", weather_state="any", demand_window="any"}}'
    )


for panel in dash["panels"]:
    if panel.get("type") == "text":
        continue
    pid = panel.get("id")
    for t in panel.get("targets", []):
        expr = t.get("expr", "")
        if not expr or "analytics_bi_metric" not in expr:
            continue

        m = re.search(r'metric="([^"]+)"', expr)
        if not m:
            continue
        metric = m.group(1)

        if pid == 17:  # shelf fill gauge
            t["expr"] = (
                'analytics_bi_metric{metric="shelf_fill_rate_pct", warehouse_location="ALL", '
                'urgency_tier="all", weather_state="any", demand_window="any"}'
            )
        elif metric in WH_BREAKDOWN:
            t["expr"] = wh_expr(metric)
        elif metric in URGENCY:
            t["expr"] = urgency_expr(metric)
        elif metric == "shelf_fill_rate_pct":
            t["expr"] = stat_expr("shelf_fill_rate_pct")
        elif metric == "delivery_delay_pct":
            t["expr"] = (
                'avg without(warehouse_location) (analytics_bi_metric{metric="delivery_delay_pct", '
                'warehouse_location=~"WAREHOUSE-.*", weather_state="any"})'
            )
        elif metric == "weather_risk_active":
            t["expr"] = (
                'analytics_bi_metric{metric="weather_risk_active", warehouse_location="ALL", '
                'weather_state="any", demand_window="any"}'
            )
            if "*" in expr:
                t["expr"] += " * 100"
        elif metric == "bottleneck_warehouse_count":
            t["expr"] = stat_expr("bottleneck_warehouse_count")
        elif metric == "total_units_sold":
            t["expr"] = (
                f'analytics_bi_metric{{metric="total_units_sold", warehouse_location="ALL", '
                f'urgency_tier=~"$urgency_tier", weather_state="any", demand_window="$time_window"}}'
            )
        elif metric in STAT_METRICS or metric in {
            "skus_needing_replenishment",
            "stockout_risk_pct",
            "critical_skus_count",
            "replenishment_value_million_pkr",
            "total_suggested_order_units",
            "avg_urgency_score",
            "max_urgency_score",
        }:
            t["expr"] = stat_expr(metric)
        elif metric in {"avg_shelf_stock_units", "avg_reorder_threshold_units", "avg_max_capacity_units"}:
            t["expr"] = stat_expr(metric)

path.write_text(json.dumps(dash, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
print(f"Patched {path}")

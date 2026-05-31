"""
Dimensional executive BI metrics for Grafana filters (warehouse, urgency, weather, demand window).

Silver-only metrics (warehouse throughput, transaction counts) are always exported.
Gold-backed metrics (replenishment, urgency tiers, shelf fill) export when Gold is available.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

from data_plane.analytics.duckdb_engine import lakehouse_connection

LabelSet = Dict[str, str]
MetricRow = Tuple[LabelSet, float]

FX_PKR = 278.0


def _urgency_tier(score: float) -> str:
    if score >= 0.7:
        return "critical"
    if score >= 0.5:
        return "high"
    if score >= 0.3:
        return "medium"
    return "low"


def _weather_state(active: bool) -> str:
    return "active" if active else "inactive"


def _emit(
    out: List[MetricRow],
    metric: str,
    value: float,
    *,
    warehouse_location: str = "ALL",
    urgency_tier: str = "all",
    weather_state: str = "any",
    demand_window: str = "any",
) -> None:
    if value is None:
        return
    try:
        num = float(value)
    except (TypeError, ValueError):
        return
    out.append(
        (
            {
                "metric": metric,
                "warehouse_location": warehouse_location,
                "urgency_tier": urgency_tier,
                "weather_state": weather_state,
                "demand_window": demand_window,
            },
            num,
        )
    )


def _table_exists(con, name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [name],
    ).fetchone()
    return bool(row and row[0])


def _export_warehouse_throughput(con, rows: List[MetricRow]) -> None:
    """Always available from Silver inventory — powers warehouse bar charts & timeseries."""
    if not _table_exists(con, "silver_inventory_transactions"):
        return

    wh_perf = con.execute(
        """
        SELECT
            warehouse_location,
            COUNT(*) AS transaction_count,
            SUM(ABS(quantity_change)) AS total_units_moved,
            ROUND(
                100.0 * SUM(CASE WHEN transaction_type = 'ADJUSTMENT' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                2
            ) AS adjustment_rate_pct
        FROM silver_inventory_transactions
        GROUP BY warehouse_location
        ORDER BY total_units_moved DESC
        """
    ).fetchall()

    for wh_loc, txn_count, units_moved, adj_pct in wh_perf:
        wh = str(wh_loc)
        _emit(rows, "warehouse_transaction_count", float(txn_count), warehouse_location=wh)
        _emit(rows, "warehouse_units_moved", float(units_moved or 0), warehouse_location=wh)
        _emit(rows, "warehouse_adjustment_rate_pct", float(adj_pct or 0), warehouse_location=wh)

    delay_by_wh = con.execute(
        """
        SELECT warehouse_location,
               ROUND(100.0 * AVG(CASE WHEN late_arriving THEN 1.0 ELSE 0.0 END), 2)
        FROM silver_inventory_transactions
        GROUP BY warehouse_location
        """
    ).fetchall()
    for wh_loc, delay_pct in delay_by_wh:
        _emit(
            rows,
            "delivery_delay_pct",
            float(delay_pct or 0),
            warehouse_location=str(wh_loc),
        )


def _export_urgency_tiers(con, rows: List[MetricRow]) -> None:
    """Network + per-warehouse urgency band counts for trend panel."""
    if not _table_exists(con, "gold_replenishment_signals"):
        return

    tiers = ("critical", "high", "medium", "low")
    counts = {t: 0.0 for t in tiers}

    network = con.execute(
        """
        SELECT
            CASE
                WHEN COALESCE(urgency_score, 0) >= 0.7 THEN 'critical'
                WHEN COALESCE(urgency_score, 0) >= 0.5 THEN 'high'
                WHEN COALESCE(urgency_score, 0) >= 0.3 THEN 'medium'
                ELSE 'low'
            END AS tier,
            COUNT(*) AS cnt
        FROM gold_replenishment_signals
        WHERE needs_replenishment = TRUE
        GROUP BY 1
        """
    ).fetchall()
    for tier, cnt in network:
        if tier in counts:
            counts[tier] = float(cnt or 0)

    for tier, cnt in counts.items():
        _emit(
            rows,
            f"urgency_tier_{tier}_count",
            cnt,
            warehouse_location="ALL",
            urgency_tier="all",
        )

    if not _table_exists(con, "silver_inventory_transactions"):
        return

    wh_counts: Dict[str, Dict[str, float]] = {}
    per_wh = con.execute(
        """
        WITH product_wh AS (
            SELECT product_id, warehouse_location
            FROM (
                SELECT product_id, warehouse_location,
                       ROW_NUMBER() OVER (
                           PARTITION BY product_id ORDER BY timestamp DESC
                       ) AS rn
                FROM silver_inventory_transactions
            ) x
            WHERE rn = 1
        )
        SELECT
            COALESCE(p.warehouse_location, 'UNKNOWN') AS warehouse_location,
            CASE
                WHEN COALESCE(g.urgency_score, 0) >= 0.7 THEN 'critical'
                WHEN COALESCE(g.urgency_score, 0) >= 0.5 THEN 'high'
                WHEN COALESCE(g.urgency_score, 0) >= 0.3 THEN 'medium'
                ELSE 'low'
            END AS tier,
            COUNT(*) AS cnt
        FROM gold_replenishment_signals g
        LEFT JOIN product_wh p ON g.product_id = p.product_id
        WHERE g.needs_replenishment = TRUE
        GROUP BY 1, 2
        """
    ).fetchall()
    for wh_loc, tier, cnt in per_wh:
        wh = str(wh_loc)
        if wh in ("ALL", "UNKNOWN"):
            continue
        wh_counts.setdefault(wh, {t: 0.0 for t in tiers})
        if tier in wh_counts[wh]:
            wh_counts[wh][tier] = float(cnt or 0)

    for wh, tier_map in wh_counts.items():
        for tier, cnt in tier_map.items():
            _emit(
                rows,
                f"urgency_tier_{tier}_count",
                cnt,
                warehouse_location=wh,
                urgency_tier="all",
            )


def _export_shelf_fill(con, rows: List[MetricRow]) -> None:
    """Precomputed shelf fill rate — avoids fragile PromQL division in Grafana."""
    if not _table_exists(con, "gold_replenishment_signals"):
        return
    if not _table_exists(con, "silver_warehouse_master"):
        return
    if not _table_exists(con, "silver_inventory_transactions"):
        return

    base_sql = """
        WITH product_wh AS (
            SELECT product_id, warehouse_location
            FROM (
                SELECT product_id, warehouse_location,
                       ROW_NUMBER() OVER (
                           PARTITION BY product_id ORDER BY timestamp DESC
                       ) AS rn
                FROM silver_inventory_transactions
            ) x
            WHERE rn = 1
        )
        SELECT
            {wh_expr} AS warehouse_location,
            ROUND(AVG(g.current_stock_on_shelf), 4) AS avg_shelf,
            ROUND(AVG(w.reorder_threshold), 4) AS avg_threshold,
            ROUND(AVG(w.max_capacity), 4) AS avg_capacity,
            ROUND(
                AVG(g.current_stock_on_shelf) / NULLIF(AVG(w.max_capacity), 0),
                4
            ) AS fill_rate
        FROM gold_replenishment_signals g
        LEFT JOIN product_wh p ON g.product_id = p.product_id
        LEFT JOIN silver_warehouse_master w ON g.product_id = w.product_id
        WHERE g.needs_replenishment = TRUE
        {group_clause}
    """

    def _publish_fill(wh: str, avg_shelf, avg_threshold, avg_capacity, fill_rate) -> None:
        if wh in ("UNKNOWN",):
            return
        labels = dict(warehouse_location=wh, urgency_tier="all")
        _emit(rows, "avg_shelf_stock_units", float(avg_shelf or 0), **labels)
        _emit(rows, "avg_reorder_threshold_units", float(avg_threshold or 0), **labels)
        _emit(rows, "avg_max_capacity_units", float(avg_capacity or 0), **labels)
        _emit(rows, "shelf_fill_rate_pct", float(fill_rate or 0), **labels)

    network = con.execute(
        base_sql.format(wh_expr="'ALL'", group_clause="")
    ).fetchone()
    if network:
        _publish_fill("ALL", network[1], network[2], network[3], network[4])

    per_wh = con.execute(
        base_sql.format(
            wh_expr="COALESCE(p.warehouse_location, 'UNKNOWN')",
            group_clause="GROUP BY COALESCE(p.warehouse_location, 'UNKNOWN')",
        )
    ).fetchall()
    for wh_loc, avg_shelf, avg_threshold, avg_capacity, fill_rate in per_wh:
        _publish_fill(str(wh_loc), avg_shelf, avg_threshold, avg_capacity, fill_rate)


def _export_gold_slices(con, rows: List[MetricRow], ws: str) -> None:
    if not _table_exists(con, "gold_replenishment_signals"):
        return

    catalog_size = con.execute(
        "SELECT COUNT(DISTINCT product_id) FROM silver_warehouse_master"
    ).fetchone()[0] or 0

    weather_active = bool(
        con.execute(
            """
            SELECT COALESCE(MAX(CASE WHEN weather_risk THEN 1 ELSE 0 END), 0)
            FROM gold_replenishment_signals
            """
        ).fetchone()[0]
    )
    ws = _weather_state(weather_active)

    enriched = con.execute(
        """
        WITH product_wh AS (
            SELECT product_id, warehouse_location
            FROM (
                SELECT product_id, warehouse_location,
                       ROW_NUMBER() OVER (
                           PARTITION BY product_id ORDER BY timestamp DESC
                       ) AS rn
                FROM silver_inventory_transactions
            ) x
            WHERE rn = 1
        )
        SELECT
            g.product_id,
            g.urgency_score,
            g.suggested_order_qty,
            g.current_stock_on_shelf,
            g.units_sold_7d,
            g.weather_risk,
            COALESCE(p.warehouse_location, 'UNKNOWN') AS warehouse_location,
            w.unit_cost,
            w.reorder_threshold,
            w.max_capacity
        FROM gold_replenishment_signals g
        LEFT JOIN product_wh p ON g.product_id = p.product_id
        LEFT JOIN silver_warehouse_master w ON g.product_id = w.product_id
        WHERE g.needs_replenishment = TRUE
        """
    ).fetchdf()

    warehouses = sorted(enriched["warehouse_location"].dropna().unique().tolist())
    if not warehouses and _table_exists(con, "silver_inventory_transactions"):
        warehouses = [
            str(r[0])
            for r in con.execute(
                "SELECT DISTINCT warehouse_location FROM silver_inventory_transactions ORDER BY 1"
            ).fetchall()
        ]

    def _slice(df, wh: str | None, tier: str | None, weather_only: str | None):
        d = df
        if wh and wh != "ALL":
            d = d[d["warehouse_location"] == wh]
        if tier and tier != "all":
            d = d[d["urgency_score"].apply(_urgency_tier) == tier]
        if weather_only == "active":
            d = d[d["weather_risk"] == True]  # noqa: E712
        elif weather_only == "inactive":
            d = d[d["weather_risk"] == False]  # noqa: E712
        return d

    def _publish_slice(wh: str, tier: str, weather_only: str | None) -> None:
        d = _slice(enriched, wh if wh != "ALL" else None, tier, weather_only)
        sku_count = len(d)
        avg_urg = float(d["urgency_score"].mean()) if sku_count else 0.0
        max_urg = float(d["urgency_score"].max()) if sku_count else 0.0
        total_order = int(d["suggested_order_qty"].sum()) if sku_count else 0
        repl_pkr = float((d["suggested_order_qty"] * d["unit_cost"].fillna(0) * FX_PKR).sum())
        critical = int((d["urgency_score"] >= 0.5).sum()) if sku_count else 0
        high = int((d["urgency_score"] >= 0.8).sum()) if sku_count else 0
        stockout_pct = round(100.0 * sku_count / catalog_size, 1) if catalog_size else 0.0
        labels = dict(
            warehouse_location=wh,
            urgency_tier=tier,
        )
        for metric, val in (
            ("skus_needing_replenishment", sku_count),
            ("stockout_risk_pct", stockout_pct),
            ("critical_skus_count", critical),
            ("high_urgency_skus_count", high),
            ("avg_urgency_score", avg_urg),
            ("max_urgency_score", max_urg),
            ("total_suggested_order_units", total_order),
            ("replenishment_value_pkr", repl_pkr),
            ("replenishment_value_million_pkr", repl_pkr / 1_000_000),
        ):
            _emit(rows, metric, val, **labels, demand_window="any")

    for wh in warehouses:
        if wh in ("UNKNOWN",):
            continue
        for tier in ["all", "critical", "high", "medium", "low"]:
            _publish_slice(wh, tier, None)

    for tier in ["all", "critical", "high", "medium", "low"]:
        _publish_slice("ALL", tier, None)

    _emit(rows, "weather_risk_active", 1.0 if weather_active else 0.0)
    _emit(rows, "catalog_products_total", float(catalog_size))

    if _table_exists(con, "silver_inventory_transactions"):
        bottleneck_count = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT t.warehouse_location, t.product_id
                FROM silver_inventory_transactions t
                LEFT JOIN (
                    SELECT product_id, current_stock_on_shelf
                    FROM (
                        SELECT product_id, current_stock_on_shelf,
                               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY timestamp DESC) AS rn
                        FROM silver_iot_rfid_stream
                    ) x WHERE rn = 1
                ) i ON t.product_id = i.product_id
                LEFT JOIN silver_warehouse_master w ON t.product_id = w.product_id
                WHERE t.timestamp >= (CURRENT_TIMESTAMP - INTERVAL '24 hours')
                GROUP BY t.warehouse_location, t.product_id, w.reorder_threshold
                HAVING MAX(i.current_stock_on_shelf) < w.reorder_threshold
                   AND SUM(CASE WHEN t.transaction_type = 'OUT' THEN 1 ELSE 0 END) >= 3
            ) b
            """
        ).fetchone()[0] or 0
        _emit(rows, "bottleneck_warehouse_count", float(bottleneck_count), warehouse_location="ALL")

    if _table_exists(con, "silver_sales_history"):
        for days in (7, 14, 30):
            window = f"{days}d"
            sold_wh = con.execute(
                f"""
                SELECT i.warehouse_location, COALESCE(SUM(s.units_sold), 0)
                FROM silver_sales_history s
                JOIN (
                    SELECT DISTINCT product_id, warehouse_location
                    FROM silver_inventory_transactions
                ) i ON s.product_id = i.product_id
                WHERE s.sale_timestamp >= (CURRENT_TIMESTAMP - INTERVAL '{days} days')
                GROUP BY i.warehouse_location
                """
            ).fetchall()
            network_sold = con.execute(
                f"""
                SELECT COALESCE(SUM(units_sold), 0)
                FROM silver_sales_history
                WHERE sale_timestamp >= (CURRENT_TIMESTAMP - INTERVAL '{days} days')
                """
            ).fetchone()[0] or 0
            _emit(
                rows,
                "total_units_sold",
                float(network_sold),
                warehouse_location="ALL",
                demand_window=window,
            )
            for wh_loc, units in sold_wh:
                _emit(
                    rows,
                    "total_units_sold",
                    float(units or 0),
                    warehouse_location=str(wh_loc),
                    demand_window=window,
                )


def build_labeled_bi_metrics() -> List[MetricRow]:
    """Compute filterable BI gauges from Gold + Silver lakehouse tables."""
    rows: List[MetricRow] = []

    with lakehouse_connection() as con:
        _export_warehouse_throughput(con, rows)
        _export_urgency_tiers(con, rows)
        _export_shelf_fill(con, rows)
        _export_gold_slices(con, rows, "any")

    return rows


def labeled_metrics_to_prometheus(rows: List[MetricRow]) -> str:
    lines = [
        "# HELP analytics_bi_metric Executive BI KPI from semantic layer",
        "# TYPE analytics_bi_metric gauge",
    ]
    for labels, value in rows:
        label_str = ",".join(f'{k}="{labels[k]}"' for k in sorted(labels))
        lines.append(f"analytics_bi_metric{{{label_str}}} {value}")
    return "\n".join(lines) + "\n"

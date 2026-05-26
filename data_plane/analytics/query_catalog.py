"""
Part 3 — Task 3: SQL workload catalog with complexity, characteristics, and optimization plans.
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional


class WorkloadType(str, Enum):
    OPERATIONAL = "operational"
    STRATEGIC = "strategic"
    EXECUTIVE_BI = "executive_bi"
    AD_HOC = "ad_hoc"


class QueryComplexity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    ANALYTICAL = "analytical"
    RESOURCE_INTENSIVE = "resource_intensive"


@dataclass
class SqlWorkloadQuery:
    query_id: str
    workload_type: WorkloadType
    name: str
    sql_file: str
    complexity: QueryComplexity
    characteristics: List[str]
    optimization_plan: List[str]
    description: str = ""


SQL_DIR = Path(__file__).resolve().parent / "sql"


QUERY_CATALOG: List[SqlWorkloadQuery] = [
    # Operational (2)
    SqlWorkloadQuery(
        query_id="op_001",
        workload_type=WorkloadType.OPERATIONAL,
        name="Current shelf inventory by product",
        sql_file="operational/current_inventory.sql",
        complexity=QueryComplexity.LOW,
        characteristics=["partition_pruning:product_id", "latest_by_timestamp", "no_join"],
        optimization_plan=[
            "Partition silver_iot_rfid_stream on product_id",
            "Iceberg snapshot for point-in-time shelf levels",
            "Cache latest_stock view in analytics-service (TTL 60s)",
        ],
        description="Near real-time shelf stock for replenishment ops.",
    ),
    SqlWorkloadQuery(
        query_id="op_002",
        workload_type=WorkloadType.OPERATIONAL,
        name="Supplier / warehouse bottlenecks",
        sql_file="operational/supplier_bottlenecks.sql",
        complexity=QueryComplexity.MEDIUM,
        characteristics=["partition_pruning:warehouse_location", "joins", "aggregations", "24h_window"],
        optimization_plan=[
            "Pre-filter inventory_transactions by timestamp (24h) for partition pruning",
            "Latest IoT stock as materialized view int_latest_shelf_stock",
            "Cache bottleneck list TTL 60s for ops dashboards",
        ],
        description="Warehouses with high outbound volume and shelf stock below reorder threshold.",
    ),
    # Strategic (2)
    SqlWorkloadQuery(
        query_id="str_001",
        workload_type=WorkloadType.STRATEGIC,
        name="Monthly revenue trend by region",
        sql_file="strategic/monthly_revenue_trends.sql",
        complexity=QueryComplexity.ANALYTICAL,
        characteristics=["large_aggregation", "time_bucket_month", "historical_scan"],
        optimization_plan=[
            "Pre-aggregate in dbt mart int_monthly_sales",
            "Z-order legacy + sales on sale_timestamp",
            "Iceberg snapshot time travel for YoY compare",
        ],
        description="Regional demand patterns from sales + legacy trends.",
    ),
    SqlWorkloadQuery(
        query_id="str_002",
        workload_type=WorkloadType.STRATEGIC,
        name="Supplier / warehouse performance scorecard",
        sql_file="strategic/supplier_performance.sql",
        complexity=QueryComplexity.RESOURCE_INTENSIVE,
        characteristics=["multi_join", "window_functions", "group_by_warehouse"],
        optimization_plan=[
            "Star schema: dim_warehouse + fct_inventory_movements",
            "Broadcast small dimension in DuckDB",
            "Partition pruning on warehouse_location",
        ],
        description="Throughput and adjustment rates by warehouse location.",
    ),
    # Executive BI (2)
    SqlWorkloadQuery(
        query_id="exec_001",
        workload_type=WorkloadType.EXECUTIVE_BI,
        name="Executive KPI scorecard",
        sql_file="executive_bi/kpi_scorecard.sql",
        complexity=QueryComplexity.MEDIUM,
        characteristics=["star_schema", "prejoined_mart", "scalar_aggregates"],
        optimization_plan=[
            "Query gold_replenishment_signals + dbt marts only",
            "Columnar parquet cache refreshed post-dbt",
            "Limit scan to needs_replenishment partition",
        ],
        description="Single-row KPIs: at-risk SKUs, avg urgency, weather risk.",
    ),
    SqlWorkloadQuery(
        query_id="exec_002",
        workload_type=WorkloadType.EXECUTIVE_BI,
        name="Profitability and replenishment efficiency",
        sql_file="executive_bi/profitability_dashboard.sql",
        complexity=QueryComplexity.ANALYTICAL,
        characteristics=["joins", "aggregations", "derived_metrics"],
        optimization_plan=[
            "Materialized view mart_profitability_daily",
            "PKR conversion applied once in semantic layer",
            "Iceberg metadata-only commits for dashboard refresh",
        ],
        description="Revenue at risk and suggested order value (PKR).",
    ),
    # Ad hoc (2)
    SqlWorkloadQuery(
        query_id="adhoc_001",
        workload_type=WorkloadType.AD_HOC,
        name="Exploratory cross-source join",
        sql_file="ad_hoc/exploratory_join.sql",
        complexity=QueryComplexity.RESOURCE_INTENSIVE,
        characteristics=["multi_join", "full_scan_risk", "investigative"],
        optimization_plan=[
            "Run off-peak; enforce cost policy max 3 joins",
            "EXPLAIN before execute; bytes_scanned in audit log",
            "Spill to disk via DuckDB threads=2",
        ],
        description="Root-cause: low stock products with high returns.",
    ),
    SqlWorkloadQuery(
        query_id="adhoc_002",
        workload_type=WorkloadType.AD_HOC,
        name="Defect spike root cause",
        sql_file="ad_hoc/defect_root_cause.sql",
        complexity=QueryComplexity.ANALYTICAL,
        characteristics=["joins", "filters", "time_travel_optional"],
        optimization_plan=[
            "Filter mfg_timestamp last 7d first",
            "Iceberg snapshot_id param for before/after compare",
            "Result cache keyed by query hash 10 min",
        ],
        description="Manufacturing defect rate vs sales returns by product.",
    ),
]


def get_query(query_id: str) -> Optional[SqlWorkloadQuery]:
    for q in QUERY_CATALOG:
        if q.query_id == query_id:
            return q
    return None


def load_sql(query: SqlWorkloadQuery) -> str:
    path = SQL_DIR / query.sql_file
    if not path.exists():
        raise FileNotFoundError(f"SQL file missing: {path}")
    return path.read_text(encoding="utf-8")


def catalog_by_workload() -> dict:
    out = {wt.value: [] for wt in WorkloadType}
    for q in QUERY_CATALOG:
        out[q.workload_type.value].append({
            "query_id": q.query_id,
            "name": q.name,
            "complexity": q.complexity.value,
            "characteristics": q.characteristics,
            "optimization_plan": q.optimization_plan,
            "description": q.description,
        })
    return out

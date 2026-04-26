"""
Transformation Contracts: Declarative transformation rules for Bronze → Silver → Gold layers.

This module defines the business rules and transformations applied to data as it moves
through the pipeline. Each transformation is documented with its cost, ROI, and justification.

Design Pattern: Contracts separate the "what" (transformation rules) from the "how"
(implementation in silver_transformer.py and gold_aggregator.py).
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum


class TransformationLayer(Enum):
    """Which layer of the pipeline this transformation applies to."""
    SILVER = "silver"
    GOLD = "gold"


class CostEstimate(Enum):
    """CPU/IO cost of applying this transformation."""
    LOW = "low"      # < 1ms per record
    MEDIUM = "medium"  # 1-10ms per record
    HIGH = "high"     # > 10ms per record


@dataclass
class TransformationRule:
    """
    One atomic transformation: input column(s) → output column(s).
    
    This documents WHAT transformation happens, WHY it matters to the business,
    and HOW much it costs to implement. The actual implementation lives in
    silver_transformer.py or gold_aggregator.py.
    """
    rule_id: str
    layer: TransformationLayer
    source_id: str
    description: str
    input_columns: List[str]
    output_columns: List[str]
    business_justification: str
    cost_estimate: CostEstimate
    roi: str
    transformation_type: str  # "deduplication", "null_handling", "casting", "aggregation", etc.
    is_mandatory: bool = True


# ═══════════════════════════════════════════════════════════════════════════════════
# SILVER LAYER TRANSFORMATIONS
# ═══════════════════════════════════════════════════════════════════════════════════

SILVER_TRANSFORMATIONS: List[TransformationRule] = [
    # ── Schema Validation (applies to ALL sources) ──
    TransformationRule(
        rule_id="txr_schema_validation_001",
        layer=TransformationLayer.SILVER,
        source_id="*",  # all sources
        description="Re-validate schema against DataContract after Bronze ingestion",
        input_columns=["*"],
        output_columns=["*"],
        business_justification=(
            "Catches schema drift that may have occurred since ingestion. "
            "Prevents downstream aggregations from failing due to missing/invalid columns."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Prevents mid-pipeline failures; high business value",
        transformation_type="schema_validation",
    ),

    # ── Deduplication (applies to sources with primary keys) ──
    TransformationRule(
        rule_id="txr_dedup_sales_001",
        layer=TransformationLayer.SILVER,
        source_id="src_sales_history",
        description="Remove duplicate receipt_ids, keep latest by ingestion_timestamp",
        input_columns=["receipt_id", "_ingestion_timestamp"],
        output_columns=["receipt_id"],
        business_justification=(
            "Duplicate receipts cause double-counting of revenue KPIs. "
            "Keeping 'latest' ensures we see the most recent version of a receipt."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Prevents incorrect revenue reports; critical to finance",
        transformation_type="deduplication",
    ),

    TransformationRule(
        rule_id="txr_dedup_warehouse_001",
        layer=TransformationLayer.SILVER,
        source_id="src_warehouse_master",
        description="Remove duplicate product_ids, keep latest by ingestion_timestamp",
        input_columns=["product_id", "_ingestion_timestamp"],
        output_columns=["product_id"],
        business_justification=(
            "Warehouse master is infrequently updated; duplicates suggest retry logic or CDC re-processing. "
            "Keeping latest ensures current inventory truth."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Maintains inventory accuracy",
        transformation_type="deduplication",
    ),

    TransformationRule(
        rule_id="txr_dedup_mfg_001",
        layer=TransformationLayer.SILVER,
        source_id="src_manufacturing_logs",
        description="Remove duplicate production_batch_ids, keep latest by ingestion_timestamp",
        input_columns=["production_batch_id", "_ingestion_timestamp"],
        output_columns=["production_batch_id"],
        business_justification=(
            "Batch IDs are unique per production run. Duplicates indicate reprocessing; "
            "latest version has final QC metrics."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Ensures accurate defect rates for process improvement",
        transformation_type="deduplication",
    ),

    TransformationRule(
        rule_id="txr_dedup_iot_001",
        layer=TransformationLayer.SILVER,
        source_id="src_iot_rfid_stream",
        description="Remove duplicate event_ids, keep latest by event_timestamp",
        input_columns=["event_id", "event_timestamp"],
        output_columns=["event_id"],
        business_justification=(
            "IoT events are tagged with unique IDs. Duplicates can occur due to network retries. "
            "Keeping latest ensures no double-counting of inventory movements."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Prevents phantom inventory alerts",
        transformation_type="deduplication",
    ),

    # ── Null Handling ──
    TransformationRule(
        rule_id="txr_null_defect_001",
        layer=TransformationLayer.SILVER,
        source_id="src_manufacturing_logs",
        description="Impute NULL defect_count → 0",
        input_columns=["defect_count"],
        output_columns=["defect_count"],
        business_justification=(
            "NULL defect_count likely means zero defects (not recorded). "
            "Treating NULL as 0 is safer than dropping the record and losing all batch metadata."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Preserves batch context while ensuring valid KPI computation",
        transformation_type="null_handling",
    ),

    TransformationRule(
        rule_id="txr_null_region_001",
        layer=TransformationLayer.SILVER,
        source_id="src_legacy_trends",
        description="Impute NULL market_region → 'UNKNOWN'",
        input_columns=["market_region"],
        output_columns=["market_region"],
        business_justification=(
            "Legacy data may lack region info. Setting to 'UNKNOWN' allows it to be aggregated "
            "separately rather than dropped, preserving historical trend continuity."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Enables historical trend analysis despite data gaps",
        transformation_type="null_handling",
    ),

    # ── Type Casting ──
    TransformationRule(
        rule_id="txr_cast_timestamps_001",
        layer=TransformationLayer.SILVER,
        source_id="*",
        description="Cast all *_timestamp columns from string → datetime (UTC)",
        input_columns=["timestamp", "event_timestamp", "sale_timestamp", "mfg_timestamp", "created_at"],
        output_columns=["timestamp", "event_timestamp", "sale_timestamp", "mfg_timestamp", "created_at"],
        business_justification=(
            "Bronze stores all timestamps as strings (from CSV/JSON). "
            "Silver must cast to datetime for time-based partitioning, windowing, and range queries in Gold."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Enables all downstream time-series analytics and windowing",
        transformation_type="casting",
    ),

    # ── Derived Columns (Feature Engineering) ──
    TransformationRule(
        rule_id="txr_derived_is_return_001",
        layer=TransformationLayer.SILVER,
        source_id="src_sales_history",
        description="Add derived column: is_return = (units_sold < 0)",
        input_columns=["units_sold"],
        output_columns=["is_return"],
        business_justification=(
            "Negative units_sold are product returns, not discards. "
            "Flagging them separately allows revenue KPIs to handle returns explicitly "
            "(e.g., 'net revenue' = gross - returns)."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Enables return-aware revenue analytics",
        transformation_type="feature_engineering",
    ),

    # ── Late Arrival Detection ──
    TransformationRule(
        rule_id="txr_late_arriving_001",
        layer=TransformationLayer.SILVER,
        source_id="*",  # any streaming source
        description="Flag records with (_ingestion_timestamp - event_timestamp > 5 min) as late_arriving=True",
        input_columns=["_ingestion_timestamp", "event_timestamp"],
        output_columns=["late_arriving"],
        business_justification=(
            "High-latency records (e.g., due to network delays) can skew real-time dashboards. "
            "Flagging allows downstream consumers to decide: include in real-time? or wait for recompute?"
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Improves trust in real-time metrics by separating timely vs. late events",
        transformation_type="quality_flagging",
    ),

    # ── Out-of-Order CDC Correction ──
    TransformationRule(
        rule_id="txr_cdc_ordering_001",
        layer=TransformationLayer.SILVER,
        source_id="src_inventory_transactions",
        description="Sort CDC events by created_at, apply updates in order, flag conflicts",
        input_columns=["transaction_id", "created_at", "operation_type"],
        output_columns=["created_at", "cdc_conflict"],
        business_justification=(
            "CDC logs can arrive out of order due to network issues or replication lag. "
            "Reordering ensures inventory deltas are applied in the correct sequence."
        ),
        cost_estimate=CostEstimate.MEDIUM,
        roi="Prevents inventory inconsistencies from CDC replay issues",
        transformation_type="cdc_correction",
    ),
]


# ═══════════════════════════════════════════════════════════════════════════════════
# GOLD LAYER TRANSFORMATIONS
# ═══════════════════════════════════════════════════════════════════════════════════

GOLD_TRANSFORMATIONS: List[TransformationRule] = [
    TransformationRule(
        rule_id="txr_agg_daily_sales_001",
        layer=TransformationLayer.GOLD,
        source_id="src_sales_history",
        description="Aggregate: daily_units_sold_per_product = SUM(units_sold) GROUP BY date(sale_timestamp), product_id",
        input_columns=["sale_timestamp", "product_id", "units_sold"],
        output_columns=["date", "product_id", "daily_units_sold"],
        business_justification=(
            "Sales dashboards need daily trends by product. "
            "Aggregating in Gold avoids re-computing every query."
        ),
        cost_estimate=CostEstimate.MEDIUM,
        roi="Improves dashboard query latency 100x",
        transformation_type="aggregation",
    ),

    TransformationRule(
        rule_id="txr_window_defect_7d_001",
        layer=TransformationLayer.GOLD,
        source_id="src_manufacturing_logs",
        description="Compute: avg_defect_rate_7d = 7-day rolling avg of (defect_count / quantity_produced)",
        input_columns=["mfg_timestamp", "defect_count", "quantity_produced"],
        output_columns=["avg_defect_rate_7d"],
        business_justification=(
            "Quality teams need rolling defect rates to detect process degradation early. "
            "7-day window balances noise filtering with recency."
        ),
        cost_estimate=CostEstimate.MEDIUM,
        roi="Enables proactive quality alerts",
        transformation_type="window_function",
    ),

    TransformationRule(
        rule_id="txr_latest_shelf_stock_001",
        layer=TransformationLayer.GOLD,
        source_id="src_iot_rfid_stream",
        description="Get latest stock per product: current_stock_on_shelf = LATEST_BY(product_id, event_timestamp)",
        input_columns=["product_id", "event_timestamp", "current_stock_on_shelf"],
        output_columns=["current_stock_on_shelf"],
        business_justification=(
            "Replenishment decisions need current shelf levels, not historical. "
            "Latest-by-timestamp ensures freshness."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Real-time inventory visibility",
        transformation_type="aggregation",
    ),

    TransformationRule(
        rule_id="txr_replenishment_threshold_001",
        layer=TransformationLayer.GOLD,
        source_id="*",
        description="Flag: needs_replenishment = (current_stock < reorder_threshold)",
        input_columns=["current_stock_on_shelf", "reorder_threshold"],
        output_columns=["needs_replenishment", "urgency_score"],
        business_justification=(
            "Warehouse teams need explicit alerts for products needing restocking. "
            "Urgency score = (threshold - current) / capacity helps prioritize."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Prevents stock-outs; critical for supply chain ops",
        transformation_type="feature_engineering",
    ),

    TransformationRule(
        rule_id="txr_weather_risk_flag_001",
        layer=TransformationLayer.GOLD,
        source_id="src_weather_api",
        description="Flag: weather_risk = (temperature < 0 OR weather LIKE '%storm%')",
        input_columns=["temperature", "weather_description"],
        output_columns=["weather_risk"],
        business_justification=(
            "Extreme weather affects lead times and shipping routes. "
            "Flagging allows supply chain to adjust reorder quantities and buffer stock."
        ),
        cost_estimate=CostEstimate.LOW,
        roi="Improves order planning under adverse conditions",
        transformation_type="feature_engineering",
    ),

    TransformationRule(
        rule_id="txr_gold_replenishment_signals_001",
        layer=TransformationLayer.GOLD,
        source_id="*",
        description=(
            "Build gold_replenishment_signals table: join warehouse_master + iot_rfid_stream + "
            "sales_history + weather_api; keep rows where needs_replenishment=True"
        ),
        input_columns=[
            "product_id", "current_stock_on_shelf", "reorder_threshold", "max_capacity",
            "units_sold_7d", "weather_risk"
        ],
        output_columns=[
            "product_id", "needs_replenishment", "urgency_score", "suggested_order_qty",
            "weather_risk", "gold_computed_at"
        ],
        business_justification=(
            "This is the primary use case: real-time shelf replenishment intelligence. "
            "A single table joining batch (warehouse, sales) and streaming (IoT, weather) "
            "gives warehouse ops everything needed to make reorder decisions."
        ),
        cost_estimate=CostEstimate.HIGH,
        roi="Prevents stock-outs and excess inventory; core business value",
        transformation_type="aggregation",
    ),
]


# ═══════════════════════════════════════════════════════════════════════════════════
# Utility Functions
# ═══════════════════════════════════════════════════════════════════════════════════

def get_transformations_for_source(source_id: str, layer: TransformationLayer) -> List[TransformationRule]:
    """Get all transformation rules for a given source and layer."""
    rules = SILVER_TRANSFORMATIONS if layer == TransformationLayer.SILVER else GOLD_TRANSFORMATIONS
    return [
        r for r in rules
        if r.source_id == source_id or r.source_id == "*"
    ]


def get_mandatory_transformations(layer: TransformationLayer) -> List[TransformationRule]:
    """Get all mandatory transformation rules for a layer."""
    rules = SILVER_TRANSFORMATIONS if layer == TransformationLayer.SILVER else GOLD_TRANSFORMATIONS
    return [r for r in rules if r.is_mandatory]


@dataclass
class TransformationContractRegistry:
    """Registry of all transformation rules, indexed by rule_id."""
    rules: Dict[str, TransformationRule] = field(default_factory=dict)

    def __post_init__(self):
        """Populate registry from all defined transformations."""
        for rule in SILVER_TRANSFORMATIONS + GOLD_TRANSFORMATIONS:
            self.rules[rule.rule_id] = rule

    def get_rule(self, rule_id: str) -> Optional[TransformationRule]:
        """Fetch a transformation rule by ID."""
        return self.rules.get(rule_id)

    def get_cost_summary(self) -> Dict[CostEstimate, int]:
        """Count transformations by cost estimate."""
        summary = {cost: 0 for cost in CostEstimate}
        for rule in self.rules.values():
            summary[rule.cost_estimate] += 1
        return summary


# Global registry instance
TRANSFORMATION_REGISTRY = TransformationContractRegistry()

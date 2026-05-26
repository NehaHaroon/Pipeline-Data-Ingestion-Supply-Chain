"""
Part 3 — Task 2: Enterprise-grade data contracts (structural, semantic, freshness, lineage).

Extends ingestion contracts in control_plane/contracts.py with lakehouse-wide guarantees.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class ContractLayer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    SEMANTIC = "semantic"
    DASHBOARD = "dashboard"


@dataclass
class StructuralContract:
    """Schema, types, keys, partitioning."""
    contract_id: str
    dataset_name: str
    layer: ContractLayer
    schema_version: str
    columns: Dict[str, str]  # name -> dtype
    required_fields: List[str]
    primary_key: List[str]
    partition_keys: List[str] = field(default_factory=list)
    nullable_fields: List[str] = field(default_factory=list)


@dataclass
class SemanticContract:
    """Business meaning and value constraints."""
    contract_id: str
    dataset_name: str
    rules: List[str]
    field_rules: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def validate_record(self, record: Dict[str, Any]) -> List[str]:
        violations = []
        for field_name, rule in self.field_rules.items():
            if field_name not in record:
                continue
            val = record[field_name]
            if val is None:
                continue
            if "min" in rule:
                try:
                    if float(val) < rule["min"]:
                        violations.append(f"{field_name}={val} below min {rule['min']}")
                except (TypeError, ValueError):
                    pass
            if "allowed_values" in rule and val not in rule["allowed_values"]:
                violations.append(f"{field_name}={val!r} not in {rule['allowed_values']}")
            if "unit" in rule and rule.get("enforce_unit"):
                pass  # documented in contract catalog
        return violations


@dataclass
class FreshnessContract:
    """Max acceptable lag between event time and availability."""
    contract_id: str
    dataset_name: str
    max_lag_seconds: int
    quarantine_on_breach: bool = True

    def evaluate_lag(self, event_ts: datetime, ingestion_ts: datetime) -> Dict[str, Any]:
        lag = (ingestion_ts - event_ts).total_seconds()
        breached = lag > self.max_lag_seconds
        return {
            "lag_seconds": lag,
            "breached": breached,
            "action": "quarantine" if breached and self.quarantine_on_breach else "flag",
        }


@dataclass
class LineageEdge:
    upstream: str
    downstream: str
    transformation: str


@dataclass
class LineageContract:
    """Source → Bronze → Silver → Gold → Dashboard chain."""
    contract_id: str
    dataset_name: str
    edges: List[LineageEdge]

    def to_lineage_graph(self) -> List[Dict[str, str]]:
        return [
            {"from": e.upstream, "to": e.downstream, "via": e.transformation}
            for e in self.edges
        ]


@dataclass
class EnterpriseDatasetContract:
    """Full contract bundle for one logical dataset."""
    source_id: str
    structural: StructuralContract
    semantic: SemanticContract
    freshness: FreshnessContract
    lineage: LineageContract


# ── Supply-chain dataset contracts ─────────────────────────────────────────────

def _warehouse_structural() -> StructuralContract:
    return StructuralContract(
        contract_id="struct_warehouse_v1",
        dataset_name="silver.warehouse_master",
        layer=ContractLayer.SILVER,
        schema_version="v1",
        columns={
            "product_id": "string",
            "reorder_threshold": "int",
            "max_capacity": "int",
            "unit_cost": "double",
        },
        required_fields=["product_id", "reorder_threshold", "max_capacity", "unit_cost"],
        primary_key=["product_id"],
        partition_keys=["product_id"],
    )


def _warehouse_semantic() -> SemanticContract:
    return SemanticContract(
        contract_id="semantic_warehouse_v1",
        dataset_name="silver.warehouse_master",
        rules=[
            "unit_cost denominated in USD at source; Gold converts to PKR via fx_rate",
            "inventory level cannot be negative",
            "reorder_threshold <= max_capacity",
        ],
        field_rules={
            "reorder_threshold": {"min": 0},
            "max_capacity": {"min": 1},
            "unit_cost": {"min": 0.0, "unit": "USD"},
        },
    )


def _inventory_txn_semantic() -> SemanticContract:
    return SemanticContract(
        contract_id="semantic_inventory_txn_v1",
        dataset_name="silver.inventory_transactions",
        rules=[
            "shipment_status mapped from transaction_type for analytics",
            "quantity_change sign must match transaction_type IN/OUT",
        ],
        field_rules={
            "transaction_type": {
                "allowed_values": {"IN", "OUT", "ADJUSTMENT", "RETURN", "CORRECTION"},
            },
            "warehouse_location": {
                "allowed_values": {
                    "WAREHOUSE-LONDON", "WAREHOUSE-DUBAI", "WAREHOUSE-KARACHI",
                    "WAREHOUSE-PARIS", "WAREHOUSE-BERLIN", "WAREHOUSE-NYC", "WAREHOUSE-SINGAPORE",
                },
            },
        },
    )


def _gold_replenishment_semantic() -> SemanticContract:
    return SemanticContract(
        contract_id="semantic_gold_replenishment_v1",
        dataset_name="gold.replenishment_signals",
        rules=[
            "revenue metrics in PKR for executive dashboards",
            "needs_replenishment=true implies current_stock < reorder_threshold",
            "urgency_score in [0,1]",
        ],
        field_rules={
            "current_stock_on_shelf": {"min": 0},
            "urgency_score": {"min": 0.0, "max": 1.0},
            "suggested_order_qty": {"min": 0},
        },
    )


def _load_enterprise_contracts() -> Dict[str, EnterpriseDatasetContract]:
    from control_plane.enterprise_contract_builder import build_enterprise_contracts

    return build_enterprise_contracts()


ENTERPRISE_CONTRACTS: Dict[str, EnterpriseDatasetContract] = _load_enterprise_contracts()


def contracts_catalog() -> Dict[str, Any]:
    """API-friendly catalog of all enterprise contracts."""
    catalog = {}
    for sid, bundle in ENTERPRISE_CONTRACTS.items():
        catalog[sid] = {
            "structural": {
                "contract_id": bundle.structural.contract_id,
                "dataset": bundle.structural.dataset_name,
                "primary_key": bundle.structural.primary_key,
                "partition_keys": bundle.structural.partition_keys,
                "required_fields": bundle.structural.required_fields,
                "columns": bundle.structural.columns,
            },
            "semantic": {
                "contract_id": bundle.semantic.contract_id,
                "rules": bundle.semantic.rules,
                "field_rules": bundle.semantic.field_rules,
            },
            "freshness": {
                "contract_id": bundle.freshness.contract_id,
                "max_lag_seconds": bundle.freshness.max_lag_seconds,
                "quarantine_on_breach": bundle.freshness.quarantine_on_breach,
            },
            "lineage": bundle.lineage.to_lineage_graph(),
        }
    return catalog


def enforce_freshness(
    source_id: str,
    event_ts: datetime,
    ingestion_ts: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Evaluate freshness contract; used by Silver late_arriving logic."""
    bundle = ENTERPRISE_CONTRACTS.get(source_id)
    if not bundle:
        return {"breached": False, "lag_seconds": 0}
    ing = ingestion_ts or datetime.now(timezone.utc)
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=timezone.utc)
    if ing.tzinfo is None:
        ing = ing.replace(tzinfo=timezone.utc)
    return bundle.freshness.evaluate_lag(event_ts, ing)

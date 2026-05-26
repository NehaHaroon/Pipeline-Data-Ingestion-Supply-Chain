"""
Build enterprise contracts for all registered sources from entities.py + contracts.py.
"""

from typing import Dict, List

from control_plane.advanced_contracts import (
    ContractLayer,
    EnterpriseDatasetContract,
    FreshnessContract,
    LineageContract,
    LineageEdge,
    SemanticContract,
    StructuralContract,
)
from control_plane.contracts import CONTRACT_REGISTRY, DataContract
from control_plane.entities import ALL_DATASETS, ALL_SOURCES, IngestionFrequency

_FRESHNESS_SEC = {
    IngestionFrequency.REAL_TIME: 120,
    IngestionFrequency.EVERY_2_MINUTES: 180,
    IngestionFrequency.HOURLY: 3600,
    IngestionFrequency.DAILY: 86400,
    IngestionFrequency.WEEKLY: 604800,
    IngestionFrequency.ON_DEMAND: 86400,
}

_PK = {
    "src_sales_history": ["receipt_id"],
    "src_warehouse_master": ["product_id"],
    "src_manufacturing_logs": ["production_batch_id"],
    "src_legacy_trends": ["old_product_code", "historical_period"],
    "src_iot_rfid_stream": ["event_id"],
    "src_inventory_transactions": ["transaction_id"],
    "src_weather_api": ["city", "timestamp"],
}

_PARTITION = {
    "src_sales_history": ["sale_timestamp"],
    "src_warehouse_master": ["product_id"],
    "src_manufacturing_logs": ["mfg_timestamp"],
    "src_legacy_trends": ["market_region"],
    "src_iot_rfid_stream": ["product_id"],
    "src_inventory_transactions": ["warehouse_location", "timestamp"],
    "src_weather_api": ["city"],
}


def _ingest_edge(source_id: str) -> str:
    src = next(s for s in ALL_SOURCES if s.source_id == source_id)
    if src.source_type.value == "db":
        return "postgres.inventory_transactions"
    if src.source_type.value == "stream":
        return "kafka:supply_chain_inventory"
    if src.source_type.value == "api":
        return "openweathermap.api"
    return f"file:{source_id.replace('src_', '')}.csv"


def _semantic_from_ingestion(contract: DataContract, source_id: str) -> SemanticContract:
    rules = [
        f"Ingestion contract {contract.contract_id} enforced at bronze boundary",
        f"Violation policy: {contract.violation_policy.value}",
    ]
    field_rules: Dict[str, dict] = {}
    for name, fc in contract.field_constraints.items():
        fr: dict = {}
        if fc.min_value is not None:
            fr["min"] = fc.min_value
        if fc.max_value is not None:
            fr["max"] = fc.max_value
        if fc.allowed_values:
            fr["allowed_values"] = set(fc.allowed_values)
        if fc.unit:
            fr["unit"] = fc.unit
        if fr:
            field_rules[name] = fr

    if source_id == "src_sales_history":
        rules.append("Revenue analytics use PKR in Gold/dbt (fx_rate=278); source unit_cost in USD")
        rules.append("units_sold < 0 treated as product returns (is_return flag in Silver)")
    if source_id == "src_inventory_transactions":
        rules.append(
            "shipment_status for logistics dashboards: Pending (IN), InTransit (OUT without delivery), "
            "Delivered (OUT with reference_order_id) — derived in mart fct_inventory_movements"
        )
        field_rules["shipment_status"] = {
            "allowed_values": {"Pending", "InTransit", "Delivered"},
        }
    if source_id == "src_manufacturing_logs":
        rules.append("defect_count / quantity_produced must yield defect rate <= 100%")
    if source_id == "src_weather_api":
        rules.append("Extreme weather flags drive gold.weather_risk on replenishment mart")

    return SemanticContract(
        contract_id=f"semantic_{source_id.replace('src_', '')}_v1",
        dataset_name=f"silver.{source_id.replace('src_', '')}",
        rules=rules,
        field_rules=field_rules,
    )


def _structural_from_ingestion(contract: DataContract, source_id: str) -> StructuralContract:
    short = source_id.replace("src_", "")
    columns = {k: v for k, v in contract.field_constraints.items()}
    for req in contract.required_fields:
        columns.setdefault(req, "string")
    return StructuralContract(
        contract_id=f"struct_{short}_v1",
        dataset_name=f"silver.{short}",
        layer=ContractLayer.SILVER,
        schema_version=contract.version,
        columns={name: "string" for name in columns},
        required_fields=list(contract.required_fields),
        primary_key=_PK.get(source_id, [contract.required_fields[0]]),
        partition_keys=_PARTITION.get(source_id, []),
        nullable_fields=list(contract.nullable_fields),
    )


def _lineage(source_id: str) -> LineageContract:
    short = source_id.replace("src_", "")
    ingest = _ingest_edge(source_id)
    return LineageContract(
        contract_id=f"lineage_{short}_v1",
        dataset_name=source_id,
        edges=[
            LineageEdge(ingest, f"bronze.{short}", "ingest"),
            LineageEdge(f"bronze.{short}", f"silver.{short}", "silver_transform"),
            LineageEdge(f"silver.{short}", "gold.replenishment_signals", "gold_aggregate"),
            LineageEdge(f"silver.{short}", f"marts.fct_{short}", "dbt_semantic"),
            LineageEdge("marts.*", "grafana:supply-chain-bi", "executive_dashboard"),
        ],
    )


def build_enterprise_contracts() -> Dict[str, EnterpriseDatasetContract]:
    out: Dict[str, EnterpriseDatasetContract] = {}
    for source in ALL_SOURCES:
        sid = source.source_id
        ic = CONTRACT_REGISTRY[sid]
        freq = source.ingestion_frequency
        out[sid] = EnterpriseDatasetContract(
            source_id=sid,
            structural=_structural_from_ingestion(ic, sid),
            semantic=_semantic_from_ingestion(ic, sid),
            freshness=FreshnessContract(
                contract_id=f"fresh_{sid.replace('src_', '')}_v1",
                dataset_name=f"silver.{sid.replace('src_', '')}",
                max_lag_seconds=_FRESHNESS_SEC.get(freq, 86400),
                quarantine_on_breach=freq in (
                    IngestionFrequency.REAL_TIME,
                    IngestionFrequency.EVERY_2_MINUTES,
                    IngestionFrequency.HOURLY,
                ),
            ),
            lineage=_lineage(sid),
        )

    # Gold mart + dataset retention metadata from entities
    out["gold_replenishment_signals"] = EnterpriseDatasetContract(
        source_id="gold_replenishment_signals",
        structural=StructuralContract(
            contract_id="struct_gold_replenishment_v1",
            dataset_name="gold.replenishment_signals",
            layer=ContractLayer.GOLD,
            schema_version="v1",
            columns={
                "product_id": "string",
                "needs_replenishment": "boolean",
                "urgency_score": "double",
                "suggested_order_qty": "int",
                "units_sold_7d": "int",
            },
            required_fields=["product_id", "needs_replenishment", "urgency_score"],
            primary_key=["product_id"],
            partition_keys=["needs_replenishment"],
        ),
        semantic=SemanticContract(
            contract_id="semantic_gold_replenishment_v1",
            dataset_name="gold.replenishment_signals",
            rules=[
                "Revenue and replenishment value reported in PKR (fx_rate=278)",
                "needs_replenishment=true iff current_stock_on_shelf < reorder_threshold",
                "urgency_score clipped to [0,1]",
            ],
            field_rules={
                "current_stock_on_shelf": {"min": 0},
                "urgency_score": {"min": 0.0, "max": 1.0},
                "suggested_order_qty": {"min": 0},
            },
        ),
        freshness=FreshnessContract(
            contract_id="fresh_gold_v1",
            dataset_name="gold.replenishment_signals",
            max_lag_seconds=300,
            quarantine_on_breach=False,
        ),
        lineage=LineageContract(
            contract_id="lineage_gold_v1",
            dataset_name="gold.replenishment_signals",
            edges=[
                LineageEdge("silver.warehouse_master", "gold.replenishment_signals", "join"),
                LineageEdge("silver.iot_rfid_stream", "gold.replenishment_signals", "latest_by"),
                LineageEdge("silver.sales_history", "gold.replenishment_signals", "7d_velocity"),
                LineageEdge("silver.weather_api", "gold.replenishment_signals", "weather_risk"),
                LineageEdge("gold.replenishment_signals", "marts.fct_replenishment_signals", "dbt"),
                LineageEdge("marts.fct_replenishment_signals", "grafana:supply-chain-bi", "BI"),
            ],
        ),
    )

    # Attach dataset retention from control plane datasets
    for ds in ALL_DATASETS:
        sid = f"src_{ds.dataset_id.removeprefix('ds_')}"
        if sid in out:
            out[sid].semantic.rules.append(
                f"Dataset retention (control plane): {ds.retention_policy}; classification: {ds.classification_level.value}"
            )

    return out

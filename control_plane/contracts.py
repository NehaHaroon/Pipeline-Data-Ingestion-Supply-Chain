# Phase 2 — Task 2: Data Contracts for every ingestion endpoint.
# A contract is an agreement between the data producer (source) and consumer (pipeline).
# It prevents schema drift from silently breaking dashboards or ML models.

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import logging

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# region: region: VIOLATION POLICY
# ─────────────────────────────────────────────
class ViolationPolicy(Enum):
    REJECT      = "reject"       # Discard record entirely; pipeline may raise an alert
    QUARANTINE  = "quarantine"   # Bad record → dirty store; good data continues
    AUTO_COERCE = "auto_coerce"  # System attempts to fix the value automatically


# ─────────────────────────────────────────────
# region: region:FIELD CONSTRAINT
# ─────────────────────────────────────────────
@dataclass
class FieldConstraint:
    """
    Defines the rules for a single column.
    Justification: per-field contracts catch issues at the field level
    rather than rejecting the whole record for one bad column.
    """
    name:           str
    dtype:          str                       # Expected Python type name: "str","int","float","datetime"
    nullable:       bool = False              # True = column may be NULL
    allowed_values: Optional[Set[Any]] = None # Enumeration of valid values (None = any)
    unit:           Optional[str] = None      # e.g. "PKR", "USD", "units"
    min_value:      Optional[float] = None    # Inclusive lower bound for numeric fields
    max_value:      Optional[float] = None    # Inclusive upper bound for numeric fields

    def validate(self, value: Any) -> List[str]:
        """
        Returns a list of violation messages (empty = valid).
        """
        errors = []

        # Null check
        if value is None or (isinstance(value, float) and __import__('math').isnan(value)):
            if not self.nullable:
                errors.append(f"Field '{self.name}' is NULL but not nullable.")
            return errors   # No further checks if null

        # Type check — attempt soft type matching
        type_map = {"str": str, "int": (int, float), "float": float}
        expected = type_map.get(self.dtype)
        if expected and not isinstance(value, expected):
            errors.append(f"Field '{self.name}' expected {self.dtype}, got {type(value).__name__} (value={value!r}).")

        # Enumeration check
        if self.allowed_values and value not in self.allowed_values:
            errors.append(f"Field '{self.name}' value {value!r} not in allowed set {self.allowed_values}.")

        # Numeric range checks
        if self.min_value is not None:
            try:
                if float(value) < self.min_value:
                    errors.append(f"Field '{self.name}' value {value} below min {self.min_value}.")
            except (TypeError, ValueError):
                pass
        if self.max_value is not None:
            try:
                if float(value) > self.max_value:
                    errors.append(f"Field '{self.name}' value {value} above max {self.max_value}.")
            except (TypeError, ValueError):
                pass

        return errors


# ─────────────────────────────────────────────
# region: region: DATA CONTRACT
# ─────────────────────────────────────────────
@dataclass
class DataContract:
    """
    Full contract for one data source.
    Enforced on every record at the ingestion boundary.

    required_fields  — must always be present and non-null
    nullable_fields  — explicitly permitted to be null
    field_constraints — detailed per-column rules
    violation_policy  — what to do when a rule is broken
    """
    contract_id:       str
    source_id:         str
    version:           str
    required_fields:   List[str]
    nullable_fields:   List[str]
    field_constraints: Dict[str, FieldConstraint]   # col_name -> constraint
    violation_policy:  ViolationPolicy = ViolationPolicy.QUARANTINE

    def enforce(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validates a single record against this contract.

        Returns a result dict:
          {
            "status":    "ok" | "coerced" | "quarantine" | "rejected",
            "record":    <possibly coerced record or None>,
            "violations": [<error strings>]
          }
        """
        violations: List[str] = []

        # ── 1. Required field presence ───────────────────────────────────
        for fname in self.required_fields:
            if fname not in record or record[fname] is None:
                violations.append(f"Required field '{fname}' is missing or null.")

        # ── 2. Per-field constraint validation ───────────────────────────
        for col, constraint in self.field_constraints.items():
            if col in record:
                violations.extend(constraint.validate(record[col]))

        # ── 3. Apply violation policy ────────────────────────────────────
        if not violations:
            return {"status": "ok", "record": record, "violations": []}

        if self.violation_policy == ViolationPolicy.REJECT:
            log.warning(f"[CONTRACT:{self.contract_id}] REJECT — {violations}")
            return {"status": "rejected", "record": None, "violations": violations}

        if self.violation_policy == ViolationPolicy.QUARANTINE:
            log.warning(f"[CONTRACT:{self.contract_id}] QUARANTINE — {violations}")
            return {"status": "quarantine", "record": record, "violations": violations}

        if self.violation_policy == ViolationPolicy.AUTO_COERCE:
            coerced, coerce_log = _auto_coerce(record, self.field_constraints)
            if coerce_log:
                log.info(f"[CONTRACT:{self.contract_id}] AUTO_COERCE — {coerce_log}")
            # Re-validate after coercion; quarantine anything still broken
            remaining = []
            for col, constraint in self.field_constraints.items():
                if col in coerced:
                    remaining.extend(constraint.validate(coerced[col]))
            if remaining:
                return {"status": "quarantine", "record": coerced, "violations": remaining}
            return {"status": "coerced", "record": coerced, "violations": coerce_log}

        return {"status": "ok", "record": record, "violations": []}


def _auto_coerce(record: Dict[str, Any], constraints: Dict[str, FieldConstraint]):
    """
    Attempt type coercion for common mismatches.
    Returns (coerced_record, list_of_changes_made).
    """
    coerced = dict(record)
    changes = []
    type_casts = {"str": str, "int": int, "float": float}

    for col, constraint in constraints.items():
        if col not in coerced or coerced[col] is None:
            continue
        cast = type_casts.get(constraint.dtype)
        if cast and not isinstance(coerced[col], cast):
            try:
                original = coerced[col]
                coerced[col] = cast(coerced[col])
                changes.append(f"'{col}': {original!r} → {coerced[col]!r}")
            except (ValueError, TypeError):
                pass   # Leave unchanged; validator will flag it

    return coerced, changes


# ─────────────────────────────────────────────
# region: region: PROJECT CONTRACTS — one per source
# ─────────────────────────────────────────────

WAREHOUSE_CONTRACT = DataContract(
    contract_id="contract_warehouse_v1",
    source_id="src_warehouse_master",
    version="v1",
    required_fields=["product_id", "reorder_threshold", "max_capacity", "unit_cost"],
    nullable_fields=[],
    violation_policy=ViolationPolicy.QUARANTINE,
    field_constraints={
        "product_id":         FieldConstraint("product_id",         "str",   nullable=False),
        "article_id":         FieldConstraint("article_id",         "str",   nullable=False),
        "product_name":       FieldConstraint("product_name",       "str",   nullable=False),
        "color":              FieldConstraint("color",              "str",   nullable=False),
        "size":               FieldConstraint("size",               "str",   nullable=False,
                                              allowed_values={"XS","S","M","L","XL","XXL"}),
        "reorder_threshold":  FieldConstraint("reorder_threshold",  "int",   nullable=False, min_value=0),
        "max_capacity":       FieldConstraint("max_capacity",       "int",   nullable=False, min_value=1),
        "unit_cost":          FieldConstraint("unit_cost",          "float", nullable=False,
                                              min_value=0.0, unit="USD"),
    }
)

MANUFACTURING_CONTRACT = DataContract(
    contract_id="contract_manufacturing_v1",
    source_id="src_manufacturing_logs",
    version="v1",
    required_fields=["production_batch_id", "product_id", "mfg_timestamp", "quantity_produced"],
    nullable_fields=["defect_count"],   # Imputation policy: null → 0
    violation_policy=ViolationPolicy.AUTO_COERCE,
    field_constraints={
        "production_batch_id": FieldConstraint("production_batch_id", "str",   nullable=False),
        "product_id":          FieldConstraint("product_id",          "str",   nullable=False),
        "mfg_timestamp":       FieldConstraint("mfg_timestamp",       "str",   nullable=False),
        "quantity_produced":   FieldConstraint("quantity_produced",   "int",   nullable=False, min_value=0),
        "defect_count":        FieldConstraint("defect_count",        "float", nullable=True,  min_value=0.0),
    }
)

SALES_CONTRACT = DataContract(
    contract_id="contract_sales_v1",
    source_id="src_sales_history",
    version="v1",
    required_fields=["receipt_id", "sale_timestamp"],
    nullable_fields=["product_id", "store_id"],   # Null product_id → quarantine
    violation_policy=ViolationPolicy.QUARANTINE,
    field_constraints={
        "receipt_id":      FieldConstraint("receipt_id",      "str",   nullable=False),
        "product_id":      FieldConstraint("product_id",      "str",   nullable=True),   # dirty data expected
        "sale_timestamp":  FieldConstraint("sale_timestamp",  "str",   nullable=False),
        "units_sold":      FieldConstraint("units_sold",      "int",   nullable=False),  # negatives = returns
        "store_id":        FieldConstraint("store_id",        "str",   nullable=True),
    }
)

LEGACY_CONTRACT = DataContract(
    contract_id="contract_legacy_v1",
    source_id="src_legacy_trends",
    version="v1",
    required_fields=["old_product_code", "historical_period", "total_monthly_sales"],
    nullable_fields=["market_region"],
    violation_policy=ViolationPolicy.AUTO_COERCE,
    field_constraints={
        "old_product_code":     FieldConstraint("old_product_code",    "str", nullable=False),
        "historical_period":    FieldConstraint("historical_period",   "str", nullable=False),
        "total_monthly_sales":  FieldConstraint("total_monthly_sales", "int", nullable=False, min_value=0),
        "market_region":        FieldConstraint("market_region",       "str", nullable=True),
    }
)

IOT_CONTRACT = DataContract(
    contract_id="contract_iot_v1",
    source_id="src_iot_rfid_stream",
    version="v1",
    required_fields=["event_id", "timestamp", "product_id", "current_stock_on_shelf"],
    nullable_fields=["battery_level"],
    violation_policy=ViolationPolicy.QUARANTINE,
    field_constraints={
        "event_id":              FieldConstraint("event_id",              "str", nullable=False),
        "timestamp":             FieldConstraint("timestamp",             "str", nullable=False),
        "product_id":            FieldConstraint("product_id",            "str", nullable=False),
        "shelf_location":        FieldConstraint("shelf_location",        "str", nullable=False,
                                                 allowed_values={"ZONE-A","ZONE-B","ZONE-C"}),
        "current_stock_on_shelf":FieldConstraint("current_stock_on_shelf","int", nullable=False,
                                                  min_value=0, max_value=10000),
        "battery_level":         FieldConstraint("battery_level",        "str", nullable=True),
    }
)

WEATHER_CONTRACT = DataContract(
    contract_id="contract_weather_v1",
    source_id="src_weather_api",
    version="v1",
    required_fields=["city", "temperature", "humidity", "weather_description", "timestamp"],
    nullable_fields=[],
    violation_policy=ViolationPolicy.QUARANTINE,
    field_constraints={
        "city":                 FieldConstraint("city",                 "str", nullable=False),
        "temperature":          FieldConstraint("temperature",          "float", nullable=False, min_value=-50, max_value=60, unit="Celsius"),
        "humidity":             FieldConstraint("humidity",             "int", nullable=False, min_value=0, max_value=100, unit="%"),
        "weather_description":  FieldConstraint("weather_description",  "str", nullable=False),
        "timestamp":            FieldConstraint("timestamp",            "str", nullable=False),
    }
)

INVENTORY_TRANSACTIONS_CONTRACT = DataContract(
    contract_id="contract_inventory_txn_v1",
    source_id="src_inventory_transactions",
    version="v1",
    required_fields=["transaction_id", "product_id", "warehouse_location", "transaction_type", "quantity_change", "timestamp", "created_at"],
    nullable_fields=["reference_order_id"],
    violation_policy=ViolationPolicy.QUARANTINE,
    field_constraints={
        "transaction_id":       FieldConstraint("transaction_id",       "str", nullable=False),
        "product_id":           FieldConstraint("product_id",           "str", nullable=False),
        "warehouse_location":   FieldConstraint("warehouse_location",   "str", nullable=False,
                                                allowed_values={"WAREHOUSE-LONDON","WAREHOUSE-DUBAI","WAREHOUSE-KARACHI","WAREHOUSE-PARIS","WAREHOUSE-BERLIN","WAREHOUSE-NYC","WAREHOUSE-SINGAPORE"}),
        "transaction_type":     FieldConstraint("transaction_type",     "str", nullable=False,
                                                allowed_values={"IN","OUT","ADJUSTMENT","RETURN","CORRECTION"}),
        "quantity_change":      FieldConstraint("quantity_change",      "int", nullable=False),   # Can be negative for OUT
        "timestamp":            FieldConstraint("timestamp",            "str", nullable=False),
        "reference_order_id":   FieldConstraint("reference_order_id",   "str", nullable=True),
        "created_by":           FieldConstraint("created_by",           "str", nullable=True),
        "created_at":           FieldConstraint("created_at",           "str", nullable=False),
    }
)

CONTRACT_REGISTRY = {
    "src_warehouse_master":        WAREHOUSE_CONTRACT,
    "src_manufacturing_logs":      MANUFACTURING_CONTRACT,
    "src_sales_history":           SALES_CONTRACT,
    "src_legacy_trends":           LEGACY_CONTRACT,
    "src_inventory_transactions":  INVENTORY_TRANSACTIONS_CONTRACT,
    "src_iot_rfid_stream":         IOT_CONTRACT,
    "src_weather_api":             WEATHER_CONTRACT,
}
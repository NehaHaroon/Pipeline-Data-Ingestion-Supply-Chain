# Phase 3 — Source-Specific Generators
# One generator per data source. Each subclass:
#   - implements _get_raw_path() and _post_process() from BaseGenerator ABC
#   - calls profile() to learn real data distributions
#   - adds source-specific IDs, dirty data injection, referential integrity rules

import os
import random
import uuid
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np

import config
from .base_generator import BaseGenerator

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# WAREHOUSE MASTER GENERATOR
# Role: Dimension table (central reference). Relatively clean data.
# Generates new product variants with realistic SKU patterns.
# ──────────────────────────────────────────────────────────────────────────────

class WarehouseMasterGenerator(BaseGenerator):
    RAW_PATH    = "storage/raw/warehouse_master.csv"
    COLORS      = ["Midnight Black", "Navy Blue", "Olive Green", "Classic White",
                   "Charcoal Grey", "Burgundy Red"]
    SIZES       = ["XS", "S", "M", "L", "XL", "XXL"]
    COLOR_CODES = {
        "Midnight Black": "MID", "Navy Blue": "NAV", "Olive Green": "OLI",
        "Classic White": "CLA", "Charcoal Grey": "GRY", "Burgundy Red": "BUR"
    }

    def __init__(self, real_df: Optional[pd.DataFrame] = None):
        super().__init__("src_warehouse_master")
        df = real_df if real_df is not None else pd.read_csv(self._get_raw_path())
        self.profile(df)
        self.real_product_ids = df["product_id"].tolist()
        self._article_counter = 2000

    def _get_raw_path(self) -> str:
        return self.RAW_PATH

    def _post_process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Override synthetic values with deterministic, business-rule-compliant fields."""
        self._article_counter += random.randint(1, 5)
        color = random.choice(self.COLORS)
        size  = random.choice(self.SIZES)
        record["product_id"]        = f"ART-{self._article_counter}-{self.COLOR_CODES[color]}-{size}"
        record["article_id"]        = f"ART-{self._article_counter}"
        record["color"]             = color
        record["size"]              = size
        record["reorder_threshold"] = max(1,  int(abs(record.get("reorder_threshold", 50)  or 50)))
        record["max_capacity"]      = max(10, int(abs(record.get("max_capacity",      500) or 500)))
        record["unit_cost"]         = round(max(0.01, abs(record.get("unit_cost", 30.0) or 30.0)), 2)
        return record

    def generate_one(self) -> Dict[str, Any]:
        """Generate one new warehouse record."""
        record = {}
        for col, profile in self._profiles.items():
            record[col] = self._sample(profile)
        return self._post_process(record)


# ──────────────────────────────────────────────────────────────────────────────
# MANUFACTURING LOGS GENERATOR
# Role: Fact table — supply side.
# Dirty data injected:
#   • ~20% lowercase product_ids  → tests normalization policy
#   • ~5%  null defect_count      → tests imputation policy
# ──────────────────────────────────────────────────────────────────────────────

class ManufacturingLogsGenerator(BaseGenerator):
    RAW_PATH = "storage/raw/manufacturing_logs.csv"

    def __init__(self, real_df: Optional[pd.DataFrame] = None,
                 product_ids: Optional[List[str]] = None):
        super().__init__("src_manufacturing_logs")
        df = real_df if real_df is not None else pd.read_csv(self._get_raw_path())
        self.profile(df)
        self.product_ids    = product_ids or df["product_id"].tolist()
        self._batch_counter = 60000

    def _get_raw_path(self) -> str:
        return self.RAW_PATH

    def _post_process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        self._batch_counter += 1
        record["production_batch_id"] = f"BATCH-{self._batch_counter}"

        pid = random.choice(self.product_ids)
        # DIRTY DATA: 20% lowercase product_id → normalization policy test
        if random.random() < 0.20:
            pid = pid.lower()
        record["product_id"]       = pid
        record["quantity_produced"] = max(1, int(abs(record.get("quantity_produced", 100) or 100)))

        # DIRTY DATA: 5% null defect_count → imputation policy test
        if random.random() < 0.05:
            record["defect_count"] = None
        else:
            record["defect_count"] = round(max(0.0, float(record.get("defect_count", 0.0) or 0.0)), 1)

        record["mfg_timestamp"] = datetime.now(timezone.utc).isoformat()
        return record

    def generate_one(self) -> Dict[str, Any]:
        record = {}
        for col, profile in self._profiles.items():
            record[col] = self._sample(profile)
        return self._post_process(record)


# ──────────────────────────────────────────────────────────────────────────────
# SALES HISTORY GENERATOR
# Role: Fact table — demand side.
# Dirty data injected:
#   • ~5%  null product_id   → quarantine policy test
#   • ~3%  negative units    → returns handling test
# ──────────────────────────────────────────────────────────────────────────────

class SalesHistoryGenerator(BaseGenerator):
    RAW_PATH  = "storage/raw/sales_history.csv"
    STORE_IDS = [
        "LDN-OXFORD", "LDN-KINGS", "NYC-5AVE", "NYC-MAIN",
        "DXB-MALL", "KHI-DOLMEN", "ISB-CENTAURUS",
        "LHR-LIBERTY", "PAR-CHAMPS", "BER-KURFUERSTENDAMM"
    ]

    def __init__(self, real_df: Optional[pd.DataFrame] = None,
                 product_ids: Optional[List[str]] = None):
        super().__init__("src_sales_history")
        df = real_df if real_df is not None else pd.read_csv(self._get_raw_path())
        self.profile(df)
        self.product_ids = product_ids or df["product_id"].tolist()

    def _get_raw_path(self) -> str:
        return self.RAW_PATH

    def _post_process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        record["receipt_id"] = uuid.uuid4().hex[:8].upper()

        # DIRTY DATA: 5% null product_id → quarantine policy test
        if random.random() < 0.05:
            record["product_id"] = None
        else:
            record["product_id"] = random.choice(self.product_ids)

        record["sale_timestamp"] = datetime.now(timezone.utc).isoformat()
        record["store_id"]       = random.choice(self.STORE_IDS)

        # DIRTY DATA: 3% negative units → return event
        units = max(1, int(abs(record.get("units_sold", 1) or 1)))
        if random.random() < 0.03:
            units = -units
        record["units_sold"] = units
        return record

    def generate_one(self) -> Dict[str, Any]:
        record = {}
        for col, profile in self._profiles.items():
            record[col] = self._sample(profile)
        return self._post_process(record)


# ──────────────────────────────────────────────────────────────────────────────
# LEGACY TRENDS GENERATOR
# Role: Historical analytical table.
# Schema enforcement: old_product_code → product_id
# ──────────────────────────────────────────────────────────────────────────────

class LegacyTrendsGenerator(BaseGenerator):
    RAW_PATH = "storage/raw/legacy_trends.csv"
    REGIONS  = ["Europe", "Americas", "Asia", "MENA", "APAC", "North America"]

    def __init__(self, real_df: Optional[pd.DataFrame] = None,
                 product_ids: Optional[List[str]] = None):
        super().__init__("src_legacy_trends")
        df = real_df if real_df is not None else pd.read_csv(self._get_raw_path())
        # Schema enforcement: rename old_product_code → product_id before profiling
        df = df.rename(columns={"old_product_code": "product_id"})
        self.profile(df)
        self.product_ids = product_ids or df["product_id"].tolist()

    def _get_raw_path(self) -> str:
        return self.RAW_PATH

    def _post_process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        record["product_id"] = random.choice(self.product_ids)
        months_ago = random.randint(1, 36)
        period_dt  = datetime.now(timezone.utc) - timedelta(days=months_ago * 30)
        record["historical_period"]   = period_dt.strftime("%Y-%m")
        record["total_monthly_sales"] = max(0, int(abs(record.get("total_monthly_sales", 500) or 500)))
        record["market_region"]       = random.choice(self.REGIONS)
        return record

    def generate_one(self) -> Dict[str, Any]:
        record = {}
        for col, profile in self._profiles.items():
            record[col] = self._sample(profile)
        return self._post_process(record)


# ──────────────────────────────────────────────────────────────────────────────
# IOT RFID STREAM GENERATOR
# Role: Real-time live inventory pings. Fully synthetic (no source CSV).
# Deduplication test: ~3% duplicate event_id injection.
# ──────────────────────────────────────────────────────────────────────────────

class IoTStreamGenerator(BaseGenerator):
    ZONES = ["ZONE-A", "ZONE-B", "ZONE-C"]

    def __init__(self, product_ids: List[str]):
        super().__init__("src_iot_rfid_stream")
        self.product_ids   = product_ids
        self._last_event: Optional[Dict[str, Any]] = None
        # IoT is fully synthetic — no CSV to profile; mark profiled to bypass guard
        self._profiles = {"__synthetic__": {"type": "constant", "value": None}}

    def _get_raw_path(self) -> str:
        return ""  # No CSV source for IoT

    def _post_process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return record  # All logic in generate_one

    def generate_one(self) -> Dict[str, Any]:
        event_id = str(uuid.uuid4())
        rec = {
            "event_id":               event_id,
            "timestamp":              datetime.now(timezone.utc).isoformat(),
            "product_id":             random.choice(self.product_ids),
            "shelf_location":         random.choice(self.ZONES),
            "current_stock_on_shelf": random.randint(5, 150),
            "battery_level":          f"{random.randint(10, 100)}%",
        }
        # DUPLICATE INJECTION: 3% chance of resending previous event (dedup policy test)
        if self._last_event and random.random() < 0.03:
            logger.debug(f"[IoT] Injecting duplicate event_id={self._last_event['event_id']}")
            return dict(self._last_event)  # exact duplicate with same event_id

        self._last_event = rec
        return rec


# ──────────────────────────────────────────────────────────────────────────────
# WEATHER API GENERATOR
# Role: External API data source for supply chain planning.
# Generates weather data based on real API calls or synthetic profiles.
# ──────────────────────────────────────────────────────────────────────────────

class WeatherAPIGenerator(BaseGenerator):
    CITIES = ["Lahore", "Karachi", "Islamabad", "Rawalpindi", "Faisalabad"]

    def __init__(self, api_key: Optional[str] = None):
        super().__init__("src_weather_api")
        self.api_key = api_key or os.getenv("WEATHER_API_KEY", "your_api_key_here")
        # Weather is API-based; no CSV to profile; use synthetic profiles
        self._profiles = {
            "city": {"type": "categorical", "values": self.CITIES, "probs": [0.4, 0.3, 0.15, 0.1, 0.05], "null_rate": 0.0},
            "temperature": {"type": "numeric", "mean": 25.0, "std": 10.0, "min": -10.0, "max": 50.0, "null_rate": 0.0},
            "humidity": {"type": "numeric", "mean": 60.0, "std": 20.0, "min": 0.0, "max": 100.0, "null_rate": 0.0},
            "weather_description": {"type": "categorical", "values": ["clear sky", "few clouds", "scattered clouds", "broken clouds", "shower rain", "rain", "thunderstorm", "snow", "mist"], "probs": [0.2, 0.15, 0.15, 0.1, 0.1, 0.1, 0.05, 0.03, 0.02], "null_rate": 0.0},
            "timestamp": {"type": "timestamp", "min": datetime(2023, 1, 1).timestamp(), "max": datetime.now().timestamp()},
        }
        self._col_types = {k: v["type"] for k, v in self._profiles.items()}

    def _get_raw_path(self) -> str:
        return ""  # API-based, no file

    def _post_process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return record  # All logic in generate_one

    def generate_one(self) -> Dict[str, Any]:
        """Generate synthetic weather data or fetch from real API."""
        if self.api_key and self.api_key != "your_api_key_here":
            return self._fetch_real_weather()
        else:
            return self._generate_synthetic()

    def _generate_synthetic(self) -> Dict[str, Any]:
        """Generate synthetic weather record."""
        record = {}
        for col, profile in self._profiles.items():
            record[col] = self._sample(profile)
        record["timestamp"] = datetime.now(timezone.utc).isoformat()
        return record

    def _fetch_real_weather(self) -> Dict[str, Any]:
        """Fetch real weather data from OpenWeatherMap API."""
        import requests
        city = random.choice(self.CITIES)
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={self.api_key}&units=metric"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return {
                "city": city,
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather_description": data["weather"][0]["description"],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except requests.RequestException as e:
            logger.warning(f"API call failed for {city}: {e}. Falling back to synthetic.")
            return self._generate_synthetic()


# ──────────────────────────────────────────────────────────────────────────────
# INVENTORY TRANSACTIONS GENERATOR
# Role: High-volume transactional DB source with continuous inventory movements.
# CDC-friendly: simulates IN (purchases), OUT (sales), ADJUSTMENT (stocktakes).
# Dirty data injected:
#   • ~2%  null reference_order_id → referential integrity test
#   • ~1%  negative quantities for IN → data quality test
# ──────────────────────────────────────────────────────────────────────────────

class InventoryTransactionsGenerator(BaseGenerator):
    RAW_PATH = "storage/raw/inventory_transactions.csv"
    WAREHOUSES = [
        "WAREHOUSE-LONDON", "WAREHOUSE-DUBAI", "WAREHOUSE-KARACHI",
        "WAREHOUSE-PARIS", "WAREHOUSE-BERLIN", "WAREHOUSE-NYC", "WAREHOUSE-SINGAPORE"
    ]
    TRANSACTION_TYPES = ["IN", "OUT", "ADJUSTMENT", "RETURN", "WRITE_OFF"]
    CREATED_BY_ROLES = ["system", "warehouse_staff", "quality_control", "supervisor", "manager"]

    def __init__(self, real_df: Optional[pd.DataFrame] = None,
                 product_ids: Optional[List[str]] = None):
        super().__init__("src_inventory_transactions")
        df = real_df if real_df is not None else pd.read_csv(self._get_raw_path())
        self.profile(df)
        self.product_ids = product_ids or df["product_id"].tolist()
        self._txn_counter = 10000  # Start from TXN-001-2025-10001

    def _get_raw_path(self) -> str:
        return self.RAW_PATH

    def _post_process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Post-process with business rules: IDs, warehouse constraints, DQ tests."""
        self._txn_counter += 1
        year = datetime.now(timezone.utc).year
        record["transaction_id"] = f"TXN-{random.randint(1,3):03d}-{year}-{self._txn_counter:05d}"

        record["product_id"] = random.choice(self.product_ids)
        record["warehouse_location"] = random.choice(self.WAREHOUSES)
        record["transaction_type"] = random.choice(self.TRANSACTION_TYPES)

        # ── Quantity logic by transaction type ──
        qty = int(abs(record.get("quantity_change", 100) or 100))
        if record["transaction_type"] == "OUT":
            qty = -qty  # OUT is negative
        elif record["transaction_type"] == "RETURN":
            qty = -qty  # RETURN is also negative
        
        # DIRTY DATA: 1% negative quantities for IN type → data quality test
        if record["transaction_type"] == "IN" and random.random() < 0.01:
            qty = -qty
        
        record["quantity_change"] = qty

        # Timestamp logic
        record["timestamp"] = datetime.now(timezone.utc).isoformat()
        record["created_at"] = datetime.now(timezone.utc).isoformat()

        # DIRTY DATA: 2% null reference_order_id → referential integrity test
        if random.random() < 0.02:
            record["reference_order_id"] = None
        else:
            txn_type = record["transaction_type"]
            if txn_type == "IN":
                record["reference_order_id"] = f"PO-{datetime.now().year}-{random.randint(1, 10000):05d}"
            elif txn_type == "OUT":
                record["reference_order_id"] = f"SO-{datetime.now().year}-{random.randint(1, 10000):05d}"
            else:
                record["reference_order_id"] = None

        record["created_by"] = random.choice(self.CREATED_BY_ROLES)

        return record

    def generate_one(self) -> Dict[str, Any]:
        """Generate one inventory transaction record."""
        record = {}
        for col, profile in self._profiles.items():
            record[col] = self._sample(profile)
        return self._post_process(record)
